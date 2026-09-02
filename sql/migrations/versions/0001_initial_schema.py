"""Initial schema: stations (SCD-2) and station_status_snapshots (daily partitions).

Revision ID: 0001
Revises:
Create Date: 2026-09-02
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op

revision: str = "0001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

# station_status_snapshots is partitioned by day on observed_at, and Postgres
# requires the partition key to be part of every unique/primary key on a
# partitioned table. observed_at is derived deterministically from the
# feed's own last_reported timestamp (observed_at = to_timestamp(last_reported)),
# so the PK (station_id, last_reported, observed_at) behaves exactly like a
# plain (station_id, last_reported) unique key for upsert purposes, while
# still satisfying Postgres's partitioned-table requirement.
PARTITION_WINDOW_SQL = """
DO $$
DECLARE
    day date;
BEGIN
    FOR day IN
        SELECT generate_series(
            CURRENT_DATE - INTERVAL '1 day',
            CURRENT_DATE + INTERVAL '13 days',
            INTERVAL '1 day'
        )::date
    LOOP
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS station_status_snapshots_%s '
            'PARTITION OF station_status_snapshots FOR VALUES FROM (%L) TO (%L)',
            to_char(day, 'YYYYMMDD'),
            day,
            day + 1
        );
    END LOOP;
END $$;
"""


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE stations (
            id BIGSERIAL PRIMARY KEY,
            station_id TEXT NOT NULL,
            name TEXT NOT NULL,
            short_name TEXT,
            lat DOUBLE PRECISION,
            lon DOUBLE PRECISION,
            capacity INTEGER,
            region_id TEXT,
            valid_from TIMESTAMPTZ NOT NULL DEFAULT now(),
            valid_to TIMESTAMPTZ,
            CONSTRAINT ck_stations_valid_range CHECK (valid_to IS NULL OR valid_to > valid_from)
        )
        """
    )
    op.execute("CREATE INDEX ix_stations_station_id ON stations (station_id)")
    op.execute(
        """
        CREATE UNIQUE INDEX ux_stations_station_id_current
        ON stations (station_id)
        WHERE valid_to IS NULL
        """
    )

    op.execute(
        """
        CREATE TABLE station_status_snapshots (
            station_id TEXT NOT NULL,
            last_reported BIGINT NOT NULL,
            observed_at TIMESTAMPTZ NOT NULL,
            num_bikes_available INTEGER NOT NULL DEFAULT 0,
            num_bikes_disabled INTEGER NOT NULL DEFAULT 0,
            num_docks_available INTEGER NOT NULL DEFAULT 0,
            num_docks_disabled INTEGER NOT NULL DEFAULT 0,
            num_ebikes_available INTEGER NOT NULL DEFAULT 0,
            is_installed BOOLEAN NOT NULL,
            is_renting BOOLEAN NOT NULL,
            is_returning BOOLEAN NOT NULL,
            ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (station_id, last_reported, observed_at)
        ) PARTITION BY RANGE (observed_at)
        """
    )

    op.execute(PARTITION_WINDOW_SQL)

    # Catches any write outside the pre-created rolling window above (e.g. the
    # poller was down for a while, or the box clock drifted). A scheduled job
    # should create the next day's partition ahead of time in production;
    # this default keeps ingestion correct in the meantime.
    op.execute(
        """
        CREATE TABLE station_status_snapshots_default
        PARTITION OF station_status_snapshots DEFAULT
        """
    )

    # Created once on the parent; Postgres propagates it to every existing
    # partition and to any partition created later.
    op.execute(
        """
        CREATE INDEX ix_station_status_snapshots_observed_at
        ON station_status_snapshots USING BRIN (observed_at)
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS station_status_snapshots")
    op.execute("DROP TABLE IF EXISTS stations")
