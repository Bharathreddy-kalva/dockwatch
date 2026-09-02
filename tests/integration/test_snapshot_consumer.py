from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Iterator

import psycopg
import pytest
from testcontainers.community.postgres import PostgresContainer

from dockwatch.consumer.snapshot_consumer import station_to_row, write_batch

REPO_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def postgres_dsn() -> Iterator[str]:
    with PostgresContainer("postgres:16", username="dockwatch", password="dockwatch", dbname="dockwatch") as pg:
        sqlalchemy_url = pg.get_connection_url(driver="psycopg")
        subprocess.run(
            [sys.executable, "-m", "alembic", "upgrade", "head"],
            cwd=REPO_ROOT,
            env={**os.environ, "DATABASE_URL": sqlalchemy_url},
            check=True,
            capture_output=True,
            text=True,
        )
        yield pg.get_connection_url(driver=None)


@pytest.fixture
def conn(postgres_dsn: str) -> Iterator[psycopg.Connection]:
    with psycopg.connect(postgres_dsn) as connection:
        yield connection


def _row_count(conn: psycopg.Connection, station_id: str) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM station_status_snapshots WHERE station_id = %s",
            (station_id,),
        )
        return cur.fetchone()[0]


def _station(station_id: str, last_reported: int, num_bikes_available: int = 5) -> dict:
    return {
        "station_id": station_id,
        "last_reported": last_reported,
        "num_bikes_available": num_bikes_available,
        "num_bikes_disabled": 0,
        "num_docks_available": 10,
        "num_docks_disabled": 0,
        "num_ebikes_available": 0,
        "is_installed": True,
        "is_renting": True,
        "is_returning": True,
    }


def test_replaying_the_same_message_does_not_create_a_duplicate_row(conn: psycopg.Connection) -> None:
    station_id = "dedupe-test-station"
    row = station_to_row(_station(station_id, last_reported=int(time.time())))

    # Simulates Kafka redelivering the same message twice — e.g. a crash
    # between the DB commit and the offset commit.
    write_batch(conn, [row])
    write_batch(conn, [row])

    assert _row_count(conn, station_id) == 1


def test_two_different_reports_for_the_same_station_both_land(conn: psycopg.Connection) -> None:
    station_id = "multi-report-test-station"
    now = int(time.time())
    first = station_to_row(_station(station_id, last_reported=now, num_bikes_available=5))
    second = station_to_row(_station(station_id, last_reported=now + 1, num_bikes_available=4))

    write_batch(conn, [first])
    write_batch(conn, [second])

    # Different last_reported means a genuinely new observation, not a
    # redelivery — the upsert must not collapse these into one row.
    assert _row_count(conn, station_id) == 2
