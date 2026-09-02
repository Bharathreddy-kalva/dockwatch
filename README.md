# Dockwatch

A real-time bike-share rebalancing platform. It predicts which stations will run out of bikes or docks in the next 30–60 minutes and produces a ranked list for rebalancing crews, built on live GBFS feeds and historical trip data.

Status: **scaffolding stage** — see `CLAUDE.md` for the full architecture, data model, and phased build plan.

## Quickstart

```bash
cp .env.example .env
make up          # starts Postgres, Redpanda (Kafka-API), Redis, MinIO, Prometheus, Grafana
make migrate
make test
```

## Architecture

See `CLAUDE.md` for the full picture. Short version: a GBFS poller streams station status through Kafka into partitioned PostgreSQL; a monthly Airflow DAG batches historical trips through PySpark into the same warehouse; dbt models both into analysis-ready marts; a load-balanced FastAPI tier (Nginx + Redis) serves predictions.

## Project log

Query optimizations and their measured before/after are tracked in `docs/query-log.md`.
