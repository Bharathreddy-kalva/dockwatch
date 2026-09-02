# Dockwatch

A real-time bike-share rebalancing platform. It predicts which stations will run out of bikes or docks in the next 30–60 minutes and produces a ranked list for rebalancing crews, built on live GBFS feeds and historical trip data.

Status: **Phase 1 (live GBFS pipeline) and Phase 2 (batch + warehouse) complete** — see `CLAUDE.md` for the full architecture, data model, and phased build plan.

## Quickstart

```bash
cp .env.example .env
make up          # starts Postgres, Redpanda (Kafka-API), Redis, MinIO, Prometheus, Grafana
make migrate
make test
```

## Architecture

See `CLAUDE.md` for the full picture. Short version: a GBFS poller streams station status through Kafka into partitioned PostgreSQL; a monthly Airflow DAG batches historical trips through PySpark into the same warehouse; dbt models both into analysis-ready marts; a load-balanced FastAPI tier (Nginx + Redis) serves predictions.

## Phase 2 — batch + warehouse

Proven against a real month of Citi Bike trip data (February 2025), not synthetic fixtures:

- **2,031,257 trip rows** cleaned, deduped, and loaded through the Airflow DAG (download → PySpark → Parquet on MinIO → Postgres), alongside an hourly weather backfill.
- Running the DAG through `airflow dags test` — not just the standalone script — caught a real idempotency bug: the load step deleted rows by calendar-month date range, but ~2.4% of the source file's trips cross the month boundary, so a re-run collided with itself on the primary key. Fixed by deleting on the natural key (`ride_id`) instead of a date range.
- The `station_hourly_demand` mart's per-station history query went from a 44 ms full scan to 0.22 ms with a supporting index — a **~198x** speedup. Full before/after `EXPLAIN ANALYZE` output in `docs/query-log.md`.

## Project log

Query optimizations and their measured before/after are tracked in `docs/query-log.md`.
