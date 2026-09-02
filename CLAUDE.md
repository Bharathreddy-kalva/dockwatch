# Dockwatch — project brief for Claude Code

Read this file at the start of every session in this repo. It is the source of truth for what we're building, why, and in what order. If anything in a task conflicts with this file, ask before deviating.

## What this is

Dockwatch is a real-time bike-share rebalancing platform, built as a portfolio project to demonstrate backend + data engineering skills to hiring managers (target: SWE roles with 2+ yrs experience, Python/backend/data-engineer flavored).

**The problem it solves:** bike-share operators (Citi Bike NYC, Divvy, Bay Wheels) lose money when riders hit an empty or full dock. Crews drive vans to move bikes ("rebalancing"). Dockwatch predicts which stations will run out of bikes or docks in the next 30–60 minutes and produces a ranked list for the crew.

**The design rule:** every component must be justified by the problem, not by the resume. If you can't explain in one sentence why a piece exists, cut it or simplify it.

## Data sources (all free, no API keys)

- **GBFS `station_status.json`** — real-time bikes/docks per station, refreshes every 30–60s. NYC has ~2,000 stations. Spec: https://gbfs.org/documentation/reference/. Find the live feed URL via Citi Bike's `gbfs.json` auto-discovery endpoint (search "Citi Bike GBFS feed URL" if it's changed).
- **Citi Bike trip history CSVs** — monthly, millions of rows/month, back to 2013. https://citibikenyc.com/system-data
- **Open-Meteo** — free hourly weather, no key. https://open-meteo.com

## Architecture

```
GBFS feed ──poll──> Kafka (topic: station.status, key=station_id) ──consume──> PostgreSQL (partitioned snapshots)
Citi Bike CSVs ──Airflow DAG (monthly)──> PySpark clean/cast ──> Parquet (S3/MinIO) ──> Postgres
Postgres ──dbt (staging → intermediate → marts)──> analysis-ready tables
marts ──prediction worker──> risk scores ──> rebalance_tasks (+ outbox for Kafka event)
Nginx (least_conn, health checks) ──> 3x FastAPI replicas ──> Redis (cache + rate-limit) ──> Postgres
Prometheus + Grafana + OpenTelemetry across every hop
```

Full narrative version with diagram and the "hard parts" writeup: see `docs/project-brief.md` (paste the artifact content there once you have it, or ask Claude to regenerate it).

### Tech choices and why

- **Redpanda instead of raw Kafka** in `docker-compose.yml` — Kafka-API-compatible, single container, no Zookeeper, much lighter for local dev. Swap for real Kafka/MSK only if a task specifically calls for it.
- **PostgreSQL with native partitioning** (not Timescale) unless a task says otherwise — keep the infra surface small; partitioning + BRIN indexes is the actual skill being demonstrated.
- **FastAPI** for the API layer — async, OpenAPI docs for free, fast to iterate.
- **Airflow** for the monthly batch DAG — industry-standard orchestrator, worth having on the resume even though the DAG itself is simple.
- **dbt-postgres** for warehouse modeling — staging → intermediate → marts, with schema tests.
- **Terraform** targets AWS free tier (EC2 + RDS + S3) — do this last, after everything works in Compose.

## Core data model

- `stations` — slowly changing dimension (SCD-2: `valid_from`/`valid_to`), because stations get renamed/relocated.
- `station_status_snapshots` — one row per station per poll. ~5.8M rows/day at NYC scale. Partition by day, BRIN index on `observed_at`, upsert keyed on `(station_id, last_reported)` for idempotency.
- `trips` — historical batch data from the monthly CSVs.
- `rebalance_tasks` + `outbox` — the write side of the API; task creation and its Kafka event commit in the same transaction (outbox pattern).

## Non-negotiables (things that make the resume bullets true)

1. **Idempotency everywhere it matters**: idempotency keys on `POST /rebalance-tasks`, upsert-based snapshot writes, exactly-once semantics *simulated* via at-least-once + dedupe (don't reach for Kafka transactions — explain the tradeoff instead).
2. **Load balancing must be real**: 3 FastAPI replicas behind Nginx `least_conn` with active health checks, proven with a k6 load test that kills a replica mid-run and shows zero 5xx.
3. **Caching must be measured**: Redis read-through cache on the hot read path, with a before/after p95 number captured from a k6 run.
4. **Rate limiting**: token-bucket in Redis (atomic Lua script), `429` + `Retry-After` header, proven with a k6 scenario.
5. **Every non-trivial query gets an `EXPLAIN ANALYZE` before/after** its supporting index, logged in `docs/query-log.md`.
6. **Tests**: pytest with testcontainers for Postgres/Kafka integration tests; dbt schema tests; CI runs both on every PR.
7. **Observability**: Prometheus metrics from every service, one full OpenTelemetry trace waterfall screenshotted for the README, structured JSON logs.

## Conventions

- Python 3.11+, managed with `uv` if available, else plain `venv` + `pip`.
- Format/lint with `ruff`. Type-check with `mypy` on `src/`.
- Conventional commits (`feat:`, `fix:`, `chore:`, `docs:`) — the commit history is part of what a reviewer reads.
- Work on feature branches (`feat/gbfs-poller`, `feat/api-cache`, ...), open a PR into `main`, even solo — a repo with 40 small PRs reads better than one with a single commit.
- Every phase below ends with something that *runs* and a number written down in `docs/query-log.md` or the README. Don't move to the next phase without both.
- Scope discipline: **one city (New York)** only. No second city, no auth/user accounts, no mobile app, until the phase-4 skill-coverage table (see project brief) is fully green.

## Phased plan (6 weeks part-time — adjust pace as needed, but keep the order)

1. **Skeleton + live feed** — Compose (Postgres, Redpanda, Redis), GBFS poller with diffing + backoff, consumer writing partitioned snapshots, Alembic migrations, first Grafana panel (rows/min).
2. **Batch + warehouse** — Airflow DAG for monthly trips → PySpark → Parquet → Postgres; dbt project (staging/intermediate/marts + tests); Open-Meteo join.
3. **API** — FastAPI endpoints (`/stations`, `/stations/{id}/history`, `/stations/risk`, `POST /rebalance-tasks`), cursor pagination, idempotency keys, outbox, Redis cache + rate limit, pytest w/ testcontainers.
4. **Load balancing, load testing, observability** — Nginx + 3 replicas + health checks, Prometheus + OTel, k6 scenarios (baseline, cache on/off, kill-a-replica, rate-limit-engage).
5. **Cloud + CI** — Terraform (EC2/RDS/S3), GitHub Actions (pytest + dbt tests + nightly k6 smoke), public URL.
6. **Prediction + polish** — dock-out classifier or text-to-SQL bonus, final README with diagram + numbers, write-up.

Ask me which phase we're in if it's not obvious from the current branch/PR.

## Commands (fill in as they're established)

```bash
make up        # docker compose up, migrations, seed
make down
make test      # pytest + dbt test
make lint      # ruff + mypy
```

## What "done" looks like

A public URL showing a map of stations colored by predicted risk, a working API, and a Grafana dashboard showing load-balanced traffic surviving a killed replica during a load test — plus a README with the architecture diagram and five measured numbers up top.
