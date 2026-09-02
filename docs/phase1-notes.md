# Phase 1 notes: diffing and idempotency

Two decisions in the poller/consumer pipeline aren't obvious from the code alone. Both exist because the pipeline only promises **at-least-once** delivery — nothing here uses Kafka transactions or exactly-once semantics — so every piece downstream has to tolerate the same event arriving twice.

## Why the poller diffs instead of publishing everything

GBFS's `station_status.json` is a full snapshot: NYC's ~2,000 stations, every 30-60s, whether or not anything changed. Publishing that wholesale would mean ~2,000 Kafka messages/poll and ~2,000 upserts/poll downstream, even at 3am when almost nothing is moving.

Instead the poller keeps the last snapshot in memory and only publishes a station when `num_bikes_available`, `num_docks_available`, or `last_reported` differs from what it saw last cycle. `last_reported` is included even though it's not itself interesting, because a station can report the same bike/dock counts at a new timestamp — a lock/unlock with no net change — and freshness tracking (e.g. "this station hasn't reported in 20 minutes, is it offline?") needs that event.

The tradeoff: the in-memory `previous` snapshot is lost on restart, so the first poll after a poller restart republishes every station once (everything looks "changed" against an empty `previous`). That's a full flush of ~2,000 messages, which the consumer's upsert absorbs for free — see below.

## Why the upsert key doubles as the idempotency key

`station_status_snapshots` is partitioned by day on `observed_at`, and Postgres requires the partition key to be part of every unique/primary key on a partitioned table. That collides with wanting to dedupe on `(station_id, last_reported)` — the same message redelivered a minute later would get a *different* `observed_at` if `observed_at` were "when we wrote it," and the unique constraint would let the duplicate through.

The fix: `observed_at` isn't wall-clock write time, it's derived deterministically from the feed's own `last_reported` field (`observed_at = to_timestamp(last_reported)`). Same input always produces the same `observed_at`, so the partitioned-table-mandated primary key `(station_id, last_reported, observed_at)` behaves exactly like a plain `(station_id, last_reported)` unique key for dedup purposes, while still satisfying Postgres's constraint.

Combined with `ON CONFLICT (station_id, last_reported, observed_at) DO NOTHING` in the consumer, this is what makes replay safe: the consumer commits its Kafka offset only *after* the DB write succeeds, so a crash in between just means the same batch gets redelivered on restart — and redelivering rows that already landed is a no-op, not a duplicate. That's the "exactly-once simulated via at-least-once + dedupe" approach called out in the project brief, applied concretely.

## Partition management

The migration pre-creates a rolling window of daily partitions (yesterday through +13 days) plus a `DEFAULT` partition that catches anything outside that window so writes never fail. It doesn't yet automate rolling the window forward — a scheduled job (cron or an Airflow DAG) creating tomorrow's partition ahead of time is future work, not phase 1 scope.
