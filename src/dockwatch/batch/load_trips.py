"""Load a month of cleaned trip Parquet from the MinIO data lake into Postgres.

Loads via psycopg's COPY rather than PySpark's JDBC writer: the cleaning
step already produced a small, local Parquet file per month, so there's no
need to keep Spark in the loop just to move it into Postgres — COPY is the
idiomatic, fast way to bulk-load from Python, and it's consistent with how
the rest of this codebase already talks to Postgres (psycopg, not SQLAlchemy
Core, for the write-heavy paths).

Idempotent via delete-then-reload rather than upsert: trips has no natural
per-row conflict target the way station_status_snapshots does (a redelivered
Kafka message always carries the same key; a re-run of this DAG for the same
month is instead re-deriving the whole month from scratch). Deleting the
month's date range before loading means a re-run always converges on exactly
one copy of the month, however many times it's retried.
"""

from __future__ import annotations

import io
import logging

import pandas as pd
import psycopg

from dockwatch.common.config import settings

logger = logging.getLogger(__name__)

COLUMNS = [
    "ride_id",
    "rideable_type",
    "started_at",
    "ended_at",
    "start_station_id",
    "start_station_name",
    "end_station_id",
    "end_station_name",
    "start_lat",
    "start_lng",
    "end_lat",
    "end_lng",
    "member_casual",
]


def _connect_db() -> psycopg.Connection:
    dsn = settings.database_url.replace("postgresql+psycopg://", "postgresql://")
    return psycopg.connect(dsn)


def _storage_options() -> dict:
    return {
        "key": settings.s3_access_key,
        "secret": settings.s3_secret_key,
        "client_kwargs": {"endpoint_url": settings.s3_endpoint_url},
    }


def _month_bounds(year: int, month: int) -> tuple[str, str]:
    start = f"{year:04d}-{month:02d}-01"
    next_year, next_month = (year, month + 1) if month < 12 else (year + 1, 1)
    end = f"{next_year:04d}-{next_month:02d}-01"
    return start, end


def load_month(year: int, month: int) -> int:
    path = f"s3://{settings.s3_bucket}/trips/year={year:04d}/month={month:02d}/"
    df = pd.read_parquet(path, storage_options=_storage_options())[COLUMNS]

    month_start, month_end = _month_bounds(year, month)

    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    with _connect_db() as conn, conn.cursor() as cur:
        cur.execute(
            "DELETE FROM trips WHERE started_at >= %s AND started_at < %s",
            (month_start, month_end),
        )
        deleted = cur.rowcount
        with cur.copy(f"COPY trips ({', '.join(COLUMNS)}) FROM STDIN WITH (FORMAT csv)") as copy:
            copy.write(buffer.read())
        conn.commit()

    logger.info(
        "deleted %d existing row(s), loaded %d row(s) for %04d-%02d",
        deleted,
        len(df),
        year,
        month,
    )
    return len(df)
