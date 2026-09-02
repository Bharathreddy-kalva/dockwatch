.PHONY: up down logs lint test fmt migrate

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
