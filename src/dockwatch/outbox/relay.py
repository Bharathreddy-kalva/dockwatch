"""Outbox relay: publishes committed outbox rows to Kafka.

This is what makes the outbox pattern real rather than inert: POST
/rebalance-tasks commits a task and its event row in one transaction, but
nothing was ever actually sent to Kafka until something reads the outbox
table and publishes it. That's this process's only job.

No double-publish on a normal rerun: a row is only eligible for publishing
while published_at IS NULL, and published_at is only set after Kafka's
delivery callback confirms the broker actually acked the message (not
just that the client queued it) -- so a batch that partially fails only
marks the rows that really landed, and the rest are picked up again next
poll cycle. Retries are just that next poll cycle, same shape as
gbfs_poller's backoff loop, plus librdkafka's own internal retry/backoff
on transient broker errors underneath `producer.produce()`.

What this does NOT solve, deliberately, matching the "exactly-once
simulated via at-least-once + dedupe, don't reach for Kafka transactions"
tradeoff CLAUDE.md calls out for the rest of this project: a crash in the
narrow window after Kafka acks a message but before the UPDATE marking it
published commits would republish that row on restart. The Kafka message
key is the outbox row's own id specifically so a downstream consumer can
dedupe on it exactly like snapshot_consumer already dedupes redelivered
GBFS messages -- the fix for at-least-once is a dedupe-aware consumer, not
a more complicated producer.
"""

from __future__ import annotations

import json
import logging
import random
import time
from typing import Any

import psycopg
from confluent_kafka import Producer
from psycopg.rows import dict_row

from dockwatch.common.config import settings

logger = logging.getLogger(__name__)

POLL_INTERVAL_SECONDS = 5.0
BATCH_SIZE = 100
BACKOFF_INITIAL_SECONDS = 1.0
BACKOFF_MAX_SECONDS = 30.0
DELIVERY_TIMEOUT_SECONDS = 30.0

# Maps outbox.event_type -> the Kafka topic it publishes to. An event_type
# with no entry is logged and left unpublished (not dropped) rather than
# guessed at, so a typo'd or new-but-unwired event_type fails loud instead
# of silently vanishing.
EVENT_TOPICS = {
    "rebalance_task.created": "rebalance.tasks",
}


def build_producer() -> Producer:
    return Producer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "acks": "all",
        }
    )


def connect_db() -> psycopg.Connection:
    dsn = settings.database_url.replace("postgresql+psycopg://", "postgresql://")
    return psycopg.connect(dsn)


def fetch_unpublished(conn: psycopg.Connection, limit: int) -> list[dict[str, Any]]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT id, aggregate_type, aggregate_id, event_type, payload, created_at
            FROM outbox
            WHERE published_at IS NULL
            ORDER BY created_at
            LIMIT %s
            """,
            (limit,),
        )
        return cur.fetchall()


def publish_batch(producer: Producer, conn: psycopg.Connection, rows: list[dict[str, Any]]) -> int:
    """Publish each row to its topic; mark only the ones Kafka actually acked."""
    published_ids: list[int] = []
    delivery_errors: dict[int, str] = {}

    def make_callback(outbox_id: int):
        def _on_delivery(err: Any, _msg: Any) -> None:
            if err is not None:
                delivery_errors[outbox_id] = str(err)
            else:
                published_ids.append(outbox_id)

        return _on_delivery

    for row in rows:
        topic = EVENT_TOPICS.get(row["event_type"])
        if topic is None:
            logger.error(
                "no topic mapping for event_type=%s (outbox id=%d); leaving unpublished",
                row["event_type"],
                row["id"],
            )
            continue

        message = {
            "outbox_id": row["id"],
            "event_type": row["event_type"],
            "aggregate_type": row["aggregate_type"],
            "aggregate_id": row["aggregate_id"],
            "payload": row["payload"],
            "created_at": row["created_at"].isoformat(),
        }
        producer.produce(
            topic,
            key=str(row["id"]).encode("utf-8"),
            value=json.dumps(message).encode("utf-8"),
            callback=make_callback(row["id"]),
        )

    # Blocks until every delivery callback above has fired (or the timeout
    # elapses) -- this is what turns "queued client-side" into "confirmed
    # by the broker" before anything gets marked published.
    producer.flush(DELIVERY_TIMEOUT_SECONDS)

    if published_ids:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE outbox SET published_at = now() WHERE id = ANY(%s)",
                (published_ids,),
            )
        conn.commit()

    if delivery_errors:
        logger.warning("failed to deliver %d outbox row(s): %s", len(delivery_errors), delivery_errors)

    return len(published_ids)


def run(producer: Producer, conn: psycopg.Connection) -> None:
    backoff = BACKOFF_INITIAL_SECONDS

    while True:
        try:
            rows = fetch_unpublished(conn, BATCH_SIZE)
        except psycopg.OperationalError as exc:
            sleep_for = backoff + random.uniform(0, backoff * 0.1)
            logger.warning("outbox fetch failed: %s (retrying in %.1fs)", exc, sleep_for)
            time.sleep(sleep_for)
            backoff = min(backoff * 2, BACKOFF_MAX_SECONDS)
            continue

        backoff = BACKOFF_INITIAL_SECONDS

        if rows:
            published = publish_batch(producer, conn, rows)
            logger.info("published %d/%d outbox row(s)", published, len(rows))

        time.sleep(POLL_INTERVAL_SECONDS)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    producer = build_producer()
    try:
        with connect_db() as conn:
            run(producer, conn)
    except KeyboardInterrupt:
        logger.info("shutting down outbox relay")
    finally:
        producer.flush(10)


if __name__ == "__main__":
    main()
