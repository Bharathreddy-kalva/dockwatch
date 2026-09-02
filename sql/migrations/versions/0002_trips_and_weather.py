"""Trips (batch trip history) and weather_hourly (Open-Meteo backfill).

Revision ID: 0002
Revises: 0001
Create Date: 2026-09-02
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op

revision: str = "0002"
down_revision: Union[str, None] = "0001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Not partitioned (unlike station_status_snapshots): a single month of
    # trips is ~2M rows, and CLAUDE.md's partitioning requirement is scoped
    # to the high-volume GBFS snapshot table, not this batch-loaded one.
    # ride_id is the source's own unique trip identifier, so it's a natural
    # primary key — the batch loader deletes-then-reloads by month rather
    # than relying on ON CONFLICT, but the PK still guards against corrupt
    # duplicate rows slipping through the Spark dedupe step.
    op.execute(
        """
        CREATE TABLE trips (
            ride_id TEXT PRIMARY KEY,
            rideable_type TEXT NOT NULL,
            started_at TIMESTAMP NOT NULL,
            ended_at TIMESTAMP NOT NULL,
            start_station_id TEXT,
            start_station_name TEXT,
            end_station_id TEXT,
            end_station_name TEXT,
            start_lat DOUBLE PRECISION,
            start_lng DOUBLE PRECISION,
            end_lat DOUBLE PRECISION,
            end_lng DOUBLE PRECISION,
            member_casual TEXT NOT NULL,
            loaded_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    op.execute("CREATE INDEX ix_trips_started_at ON trips (started_at)")
    op.execute("CREATE INDEX ix_trips_start_station_id ON trips (start_station_id)")
    op.execute("CREATE INDEX ix_trips_end_station_id ON trips (end_station_id)")

    # Citywide (not per-station) hourly weather, per CLAUDE.md's single-city
    # scope — Open-Meteo's forecast/archive APIs are queried for one NYC
    # coordinate, not per station.
    op.execute(
        """
        CREATE TABLE weather_hourly (
            observed_at TIMESTAMPTZ PRIMARY KEY,
            temperature_c DOUBLE PRECISION,
            precipitation_mm DOUBLE PRECISION,
            loaded_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS weather_hourly")
    op.execute("DROP TABLE IF EXISTS trips")
