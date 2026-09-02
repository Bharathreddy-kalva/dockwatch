"""Integration tests for GET /stations and GET /stations/{id}/history."""

from __future__ import annotations

import os
import subprocess
import sys
from collections.abc import AsyncIterator, Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import psycopg
import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from psycopg_pool import AsyncConnectionPool
from testcontainers.community.postgres import PostgresContainer

from dockwatch.api.db import get_db
from dockwatch.api.main import app

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


@pytest.fixture(scope="module")
def seed(postgres_dsn: str) -> None:
    now = datetime.now(UTC)
    with psycopg.connect(postgres_dsn) as conn, conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO stations (station_id, name, short_name, lat, lon, capacity, region_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            [
                ("station-a", "Station A", "1.01", 40.70, -74.00, 20, "71"),
                ("station-b", "Station B", "1.02", 40.71, -74.01, 25, "71"),
                ("station-c", "Station C", "1.03", 40.72, -74.02, 30, "71"),
            ],
        )
        cur.executemany(
            """
            INSERT INTO station_status_snapshots (
                station_id, last_reported, observed_at,
                num_bikes_available, num_bikes_disabled,
                num_docks_available, num_docks_disabled,
                num_ebikes_available, is_installed, is_renting, is_returning
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            [
                (
                    "station-a",
                    int((now - timedelta(minutes=i)).timestamp()),
                    now - timedelta(minutes=i),
                    5,
                    0,
                    10,
                    0,
                    0,
                    True,
                    True,
                    True,
                )
                for i in range(5)
            ],
        )
        conn.commit()


@pytest_asyncio.fixture
async def client(postgres_dsn: str, seed: None) -> AsyncIterator[AsyncClient]:
    test_pool = AsyncConnectionPool(postgres_dsn, open=False)
    await test_pool.open()

    async def override_get_db() -> AsyncIterator[psycopg.AsyncConnection]:
        async with test_pool.connection() as conn:
            yield conn

    app.dependency_overrides[get_db] = override_get_db
    try:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
            yield ac
    finally:
        app.dependency_overrides.clear()
        await test_pool.close()


@pytest.mark.asyncio
async def test_list_stations_paginates(client: AsyncClient) -> None:
    first = await client.get("/stations", params={"limit": 2})
    assert first.status_code == 200
    body = first.json()
    assert [s["station_id"] for s in body["items"]] == ["station-a", "station-b"]
    assert body["next_cursor"] is not None

    second = await client.get("/stations", params={"limit": 2, "cursor": body["next_cursor"]})
    assert second.status_code == 200
    body2 = second.json()
    assert [s["station_id"] for s in body2["items"]] == ["station-c"]
    assert body2["next_cursor"] is None


@pytest.mark.asyncio
async def test_station_history_returns_newest_first_and_paginates(client: AsyncClient) -> None:
    response = await client.get("/stations/station-a/history", params={"limit": 2})
    assert response.status_code == 200
    body = response.json()
    assert len(body["items"]) == 2
    assert body["items"][0]["observed_at"] > body["items"][1]["observed_at"]
    assert body["next_cursor"] is not None


@pytest.mark.asyncio
async def test_station_history_404s_for_unknown_station(client: AsyncClient) -> None:
    response = await client.get("/stations/does-not-exist/history")
    assert response.status_code == 404
