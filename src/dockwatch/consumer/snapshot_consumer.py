"""Consume station.status events and upsert into station_status_snapshots.

Kafka only guarantees at-least-once delivery here (no transactions), so the
same message can land twice — after a rebalance, a crash between the DB
commit and the offset commit, or a producer retry. The upsert below makes
redelivery a no-op instead of a duplicate row: it's keyed on (station_id,
last_reported, observed_at), which is exactly the GBFS station_id plus the
station's own self-reported timestamp, so replaying the same event always
resolves to the same primary key.
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any

import psycopg
from confluent_kafka import Consumer, Message
from prometheus_client import Counter, start_http_server

from dockwatch.common.config import settings

logger = logging.getLogger(__name__)

KAFKA_TOPIC = "station.status"
CONSUMER_GROUP = "snapshot-writer"
BATCH_SIZE = 100
BATCH_TIMEOUT_SECONDS = 5.0
METRICS_PORT = 9101

# Counts rows the consumer has upserted, i.e. successfully committed to
# Postgres — including ones ON CONFLICT DO NOTHING later no-ops on, since
# that's still a write the consumer performed. This is the rows/min number
# behind the Grafana panel proving the pipeline is live end-to-end.
ROWS_WRITTEN = Counter(
    "dockwatch_snapshot_rows_written_total",
    "Rows upserted into station_status_snapshots by the consumer",
)

UPSERT_SQL = """
    INSERT INTO station_status_snapshots (
        station_id, last_reported, observed_at,
        num_bikes_available, num_bikes_disabled,
        num_docks_available, num_docks_disabled,
        num_ebikes_available, is_installed, is_renting, is_returning
    )
    VALUES (
        %(station_id)s, %(last_reported)s, to_timestamp(%(last_reported)s),
        %(num_bikes_available)s, %(num_bikes_disabled)s,
        %(num_docks_available)s, %(num_docks_disabled)s,
        %(num_ebikes_available)s, %(is_installed)s, %(is_renting)s, %(is_returning)s
    )
    ON CONFLICT (station_id, last_reported, observed_at) DO NOTHING
"""


def build_consumer() -> Consumer:
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": CONSUMER_GROUP,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([KAFKA_TOPIC])
    return consumer


def connect_db() -> psycopg.Connection:
    # psycopg (v3) connects with a plain postgresql:// DSN; the app-wide
    # DATABASE_URL uses SQLAlchemy's "+psycopg" driver suffix, so strip it.
    dsn = settings.database_url.replace("postgresql+psycopg://", "postgresql://")
    return psycopg.connect(dsn)


def station_to_row(station: dict[str, Any]) -> dict[str, Any]:
    return {
        "station_id": station["station_id"],
        "last_reported": station["last_reported"],
        "num_bikes_available": station.get("num_bikes_available", 0),
        "num_bikes_disabled": station.get("num_bikes_disabled", 0),
        "num_docks_available": station.get("num_docks_available", 0),
        "num_docks_disabled": station.get("num_docks_disabled", 0),
        "num_ebikes_available": station.get("num_ebikes_available", 0),
        "is_installed": bool(station.get("is_installed")),
        "is_renting": bool(station.get("is_renting")),
        "is_returning": bool(station.get("is_returning")),
    }


def write_batch(conn: psycopg.Connection, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    with conn.cursor() as cur:
        cur.executemany(UPSERT_SQL, rows)
    conn.commit()
    ROWS_WRITTEN.inc(len(rows))


def run(consumer: Consumer, conn: psycopg.Connection) -> None:
    rows: list[dict[str, Any]] = []
    messages: list[Message] = []
    last_flush = time.monotonic()

    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is not None:
            if msg.error():
                logger.error("consumer error: %s", msg.error())
            else:
                value = msg.value()
                if value is not None:
                    station = json.loads(value)
                    rows.append(station_to_row(station))
                    messages.append(msg)

        due = rows and (
            len(rows) >= BATCH_SIZE or time.monotonic() - last_flush >= BATCH_TIMEOUT_SECONDS
        )
        if due:
            write_batch(conn, rows)
            # Offsets commit only after the DB write succeeds, so a crash in
            # between just means the same batch is redelivered — safe, since
            # the upsert above is a no-op on replay.
            consumer.commit(message=messages[-1], asynchronous=False)
            logger.info("wrote batch: %d rows", len(rows))
            rows.clear()
            messages.clear()
            last_flush = time.monotonic()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    start_http_server(METRICS_PORT)
    consumer = build_consumer()
    try:
        with connect_db() as conn:
            run(consumer, conn)
    except KeyboardInterrupt:
        logger.info("shutting down snapshot consumer")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
