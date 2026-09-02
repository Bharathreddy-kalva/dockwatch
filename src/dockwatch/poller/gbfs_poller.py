"""Poll the GBFS station_status feed and publish diffs to Kafka.

Phase 1 stub. Fill in with:
- httpx client polling GBFS_STATION_STATUS_URL every 30-60s
- diff against previous payload (only publish changed stations)
- exponential backoff on fetch failure
- publish to Kafka topic "station.status", key=station_id
"""

from __future__ import annotations


def main() -> None:
    raise NotImplementedError("Phase 1: implement the polling loop here.")


if __name__ == "__main__":
    main()
