"""Run the full monthly trip batch pipeline standalone, without Airflow.

Useful for local development and for proving the pipeline works end-to-end;
the Airflow DAG in airflow/dags/ calls these same underlying functions as
separate tasks for retry/observability granularity.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from dockwatch.batch.backfill_weather import backfill_month
from dockwatch.batch.clean_trips import run as clean_and_upload
from dockwatch.batch.download import download_and_extract
from dockwatch.batch.load_trips import load_month

logger = logging.getLogger(__name__)

DATA_DIR = Path("data/raw/trips")
PARQUET_DIR = Path("data/processed/trips")


def run(year: int, month: int) -> None:
    csv_paths = download_and_extract(year, month, DATA_DIR / f"{year:04d}{month:02d}")
    clean_and_upload(year, month, csv_paths, PARQUET_DIR)
    rows_loaded = load_month(year, month)
    weather_rows = backfill_month(year, month)
    logger.info(
        "pipeline complete for %04d-%02d: %d trip row(s), %d weather row(s)",
        year,
        month,
        rows_loaded,
        weather_rows,
    )


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()
    run(args.year, args.month)


if __name__ == "__main__":
    main()
