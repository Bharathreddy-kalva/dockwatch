"""Consume station.status events and upsert into station_status_snapshots.

Phase 1 stub. Fill in with:
- Kafka consumer group
- upsert keyed on (station_id, last_reported) for idempotent redelivery
- write into the current day's partition
"""

from __future__ import annotations


def main() -> None:
    raise NotImplementedError("Phase 1: implement the consumer loop here.")


if __name__ == "__main__":
    main()
