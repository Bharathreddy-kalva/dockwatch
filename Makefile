.PHONY: up down logs lint test fmt migrate batch-run dbt-run dbt-test stations-backfill outbox-relay

up:
	docker compose up -d
	@echo "Waiting for Postgres..."
	@until docker exec dockwatch-postgres pg_isready -U dockwatch >/dev/null 2>&1; do sleep 1; done
	$(MAKE) migrate
	@echo "Postgres: localhost:5432 | Redpanda: localhost:9092 | Redis: localhost:6379"
	@echo "MinIO console: http://localhost:9001 | Grafana: http://localhost:3000"

down:
	docker compose down

logs:
	docker compose logs -f

migrate:
	alembic upgrade head

lint:
	ruff check src tests
	mypy src

fmt:
	ruff format src tests

test:
	pytest -v

# Runs the Phase 2 batch pipeline (download -> PySpark clean -> Parquet ->
# Postgres -> weather backfill) for one month, e.g. `make batch-run YEAR=2025 MONTH=2`.
batch-run:
	python -m dockwatch.batch.run_month --year $(YEAR) --month $(MONTH)

dbt-run:
	cd dbt && dbt run --profiles-dir .

dbt-test:
	cd dbt && dbt test --profiles-dir .

# SCD-2 backfill of the stations table from GBFS station_information.json.
stations-backfill:
	python -m dockwatch.poller.backfill_stations

# Polls the outbox table and publishes unpublished rows to Kafka.
outbox-relay:
	python -m dockwatch.outbox.relay
