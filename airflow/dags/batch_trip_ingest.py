"""Monthly Citi Bike trip ingest: download -> PySpark clean -> Parquet (MinIO) -> Postgres.

Runs for a fixed (year, month) passed as DAG params rather than Airflow's
own execution_date/schedule: Citi Bike trip files publish on a roughly
one-month lag, and Phase 2 scope is proving the pipeline against a single
backfilled month, not a rolling monthly schedule yet. schedule=None means
this only runs when triggered manually (or by `airflow dags test`), with
the target month passed explicitly.

Each task below is a thin wrapper around a function in dockwatch.batch.* —
the DAG's job is orchestration and retry/observability granularity, not
business logic, so the same functions are also callable directly (see
dockwatch.batch.run_month for a no-Airflow-needed local entrypoint).
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from airflow.decorators import dag, task
from airflow.models.param import Param

DATA_DIR = Path("/tmp/dockwatch/trips")


@dag(
    dag_id="batch_trip_ingest",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    params={
        "year": Param(2025, type="integer", minimum=2013),
        "month": Param(2, type="integer", minimum=1, maximum=12),
    },
    tags=["phase2", "batch"],
)
def batch_trip_ingest() -> None:
    @task
    def download(**context) -> list[str]:
        from dockwatch.batch.download import download_and_extract

        params = context["params"]
        year, month = params["year"], params["month"]
        paths = download_and_extract(year, month, DATA_DIR / f"{year:04d}{month:02d}")
        return [str(path) for path in paths]

    @task
    def clean_and_upload(csv_paths: list[str], **context) -> str:
        from dockwatch.batch.clean_trips import run as clean_run

        params = context["params"]
        return clean_run(
            params["year"], params["month"], [Path(p) for p in csv_paths], DATA_DIR / "parquet"
        )

    @task
    def load_postgres(_prefix: str, **context) -> int:
        from dockwatch.batch.load_trips import load_month

        params = context["params"]
        return load_month(params["year"], params["month"])

    @task
    def backfill_weather(**context) -> int:
        from dockwatch.batch.backfill_weather import backfill_month

        params = context["params"]
        return backfill_month(params["year"], params["month"])

    csv_paths = download()
    prefix = clean_and_upload(csv_paths)
    load_postgres(prefix)
    # Independent of the trip tasks above — no upstream dependency, so
    # Airflow is free to run it in parallel.
    backfill_weather()


batch_trip_ingest()
