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
month is instead re-deriving the whole month from scratch). The delete is
keyed on ride_id, not on a started_at date range: Citi Bike's monthly files
aren't strictly bounded to the calendar month (Feb 2025's file, for example,
has ~48k trips — 2.4% — starting before Feb 1 or after Feb 28), so a
date-range delete misses exactly those boundary rows on a re-run, and the
subsequent COPY then collides with itself on the trips_pkey (ride_id)
constraint. Deleting by the incoming batch's own ride_ids has no such gap:
whatever rows this load is about to (re)insert are exactly the rows it
deletes first, so a re-run always converges on exactly one copy of the
month regardless of how its data straddles month boundaries.
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


def load_month(year: int, month: int) -> int:
    path = f"s3://{settings.s3_bucket}/trips/year={year:04d}/month={month:02d}/"
    df = pd.read_parquet(path, storage_options=_storage_options())[COLUMNS]

    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    columns_sql = ", ".join(COLUMNS)

    with _connect_db() as conn, conn.cursor() as cur:
        # Staged in a temp table rather than deleted-by-ride_id via a huge
        # parameter list: COPY into staging, then a set-based DELETE/INSERT
        # joined on ride_id lets Postgres do the matching, and it's a single
        # data transfer instead of building a multi-million-element array.
        cur.execute("CREATE TEMP TABLE trips_staging (LIKE trips INCLUDING DEFAULTS) ON COMMIT DROP")
        with cur.copy(f"COPY trips_staging ({columns_sql}) FROM STDIN WITH (FORMAT csv)") as copy:
            copy.write(buffer.read())

        cur.execute("DELETE FROM trips USING trips_staging WHERE trips.ride_id = trips_staging.ride_id")
        deleted = cur.rowcount

        cur.execute(f"INSERT INTO trips ({columns_sql}) SELECT {columns_sql} FROM trips_staging")
        conn.commit()

    logger.info(
        "deleted %d existing row(s), loaded %d row(s) for %04d-%02d",
        deleted,
        len(df),
        year,
        month,
    )
    return len(df)
