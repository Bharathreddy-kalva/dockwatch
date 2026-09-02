"""Poll the GBFS station_status feed and publish diffs to Kafka.

Fetches the full station_status snapshot every 30s, keeps the previous
snapshot in memory, and publishes only the stations whose tracked fields
changed since the last poll — most of a bike-share network is quiet at any
given moment, so this keeps the Kafka topic proportional to real activity
instead of ~2,000 rows every poll.
"""

from __future__ import annotations

import json
import logging
import random
import time
from typing import Any

import httpx
from confluent_kafka import Producer

from dockwatch.common.config import settings

logger = logging.getLogger(__name__)

KAFKA_TOPIC = "station.status"
POLL_INTERVAL_SECONDS = 30.0
BACKOFF_INITIAL_SECONDS = 1.0
BACKOFF_MAX_SECONDS = 60.0

# A station is republished only when one of these moves. last_reported is
# included because a station can report the same bike/dock counts at a new
# timestamp (e.g. a lock/unlock with no net change) and downstream freshness
# tracking cares about that.
TRACKED_FIELDS = ("num_bikes_available", "num_docks_available", "last_reported")

StationRecord = dict[str, Any]


def fetch_station_status(client: httpx.Client) -> dict[str, StationRecord]:
    """Fetch the GBFS station_status feed, keyed by station_id."""
    response = client.get(settings.gbfs_station_status_url)
    response.raise_for_status()
    payload = response.json()
    stations = payload["data"]["stations"]
    return {station["station_id"]: station for station in stations}


def diff_stations(
    previous: dict[str, StationRecord], current: dict[str, StationRecord]
) -> list[StationRecord]:
    """Return stations in `current` whose tracked fields differ from `previous`."""
    changed = []
    for station_id, station in current.items():
        prior = previous.get(station_id)
        if prior is None or any(prior.get(field) != station.get(field) for field in TRACKED_FIELDS):
            changed.append(station)
    return changed


def build_producer() -> Producer:
    return Producer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "acks": "all",
        }
    )


def _delivery_callback(err: Any, msg: Any) -> None:
    if err is not None:
        logger.error("delivery failed for key=%s: %s", msg.key(), err)


def publish_changed_stations(producer: Producer, changed: list[StationRecord]) -> None:
    for station in changed:
        producer.produce(
            KAFKA_TOPIC,
            key=str(station["station_id"]).encode("utf-8"),
            value=json.dumps(station).encode("utf-8"),
            callback=_delivery_callback,
        )
    # Drains delivery-report callbacks without blocking; the full flush
    # happens on shutdown so we never sleep the poll loop waiting on Kafka.
    producer.poll(0)


def run(client: httpx.Client, producer: Producer) -> None:
    previous: dict[str, StationRecord] = {}
    backoff = BACKOFF_INITIAL_SECONDS

    while True:
        cycle_start = time.monotonic()
        try:
            current = fetch_station_status(client)
        except (httpx.HTTPError, KeyError, ValueError) as exc:
            sleep_for = backoff + random.uniform(0, backoff * 0.1)
            logger.warning("GBFS fetch failed: %s (retrying in %.1fs)", exc, sleep_for)
            time.sleep(sleep_for)
            backoff = min(backoff * 2, BACKOFF_MAX_SECONDS)
            continue

        backoff = BACKOFF_INITIAL_SECONDS

        changed = diff_stations(previous, current)
        publish_changed_stations(producer, changed)
        previous = current

        logger.info(
            "poll cycle: %d stations fetched, %d published",
            len(current),
            len(changed),
        )

        elapsed = time.monotonic() - cycle_start
        time.sleep(max(0.0, POLL_INTERVAL_SECONDS - elapsed))


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    producer = build_producer()
    try:
        with httpx.Client(timeout=10.0) as client:
            run(client, producer)
    except KeyboardInterrupt:
        logger.info("shutting down gbfs poller")
    finally:
        producer.flush(10)


if __name__ == "__main__":
    main()
