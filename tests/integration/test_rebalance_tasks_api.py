"""Integration tests for POST /rebalance-tasks: idempotency and the outbox."""

from __future__ import annotations

import os
import subprocess
import sys
from collections.abc import AsyncIterator, Iterator
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
STATION_ID = "station-a"


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
    with psycopg.connect(postgres_dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO stations (station_id, name, short_name, lat, lon, capacity, region_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (STATION_ID, "Station A", "1.01", 40.70, -74.00, 20, "71"),
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


@pytest_asyncio.fixture
async def db_conn(postgres_dsn: str) -> AsyncIterator[psycopg.AsyncConnection]:
    async with await psycopg.AsyncConnection.connect(postgres_dsn) as conn:
        yield conn


def _body(**overrides: object) -> dict:
    payload = {"station_id": STATION_ID, "action": "pickup", "bike_count": 5}
    payload.update(overrides)
    return payload


@pytest.mark.asyncio
async def test_creates_task_and_outbox_event_in_one_transaction(
    client: AsyncClient, db_conn: psycopg.AsyncConnection
) -> None:
    response = await client.post(
        "/rebalance-tasks", json=_body(), headers={"Idempotency-Key": "key-create"}
    )
    assert response.status_code == 201
    task = response.json()
    assert task["station_id"] == STATION_ID
    assert task["action"] == "pickup"
    assert task["bike_count"] == 5
    assert task["status"] == "pending"

    async with db_conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        await cur.execute("SELECT * FROM rebalance_tasks WHERE id = %s", (task["id"],))
        assert await cur.fetchone() is not None

        await cur.execute(
            "SELECT event_type, payload, published_at FROM outbox WHERE aggregate_id = %s",
            (task["id"],),
        )
        event = await cur.fetchone()
        assert event is not None
        assert event["event_type"] == "rebalance_task.created"
        assert event["payload"]["station_id"] == STATION_ID
        assert event["published_at"] is None


@pytest.mark.asyncio
async def test_replaying_the_same_key_and_body_returns_the_original_task(
    client: AsyncClient, db_conn: psycopg.AsyncConnection
) -> None:
    first = await client.post(
        "/rebalance-tasks", json=_body(), headers={"Idempotency-Key": "key-replay"}
    )
    second = await client.post(
        "/rebalance-tasks", json=_body(), headers={"Idempotency-Key": "key-replay"}
    )

    assert first.status_code == 201
    assert second.status_code == 201
    assert first.json()["id"] == second.json()["id"]
    assert first.json()["created_at"] == second.json()["created_at"]

    async with db_conn.cursor() as cur:
        await cur.execute(
            "SELECT count(*) FROM rebalance_tasks WHERE idempotency_key = %s", ("key-replay",)
        )
        row = await cur.fetchone()
        assert row is not None
        assert row[0] == 1


@pytest.mark.asyncio
async def test_replaying_the_same_key_with_a_different_body_is_rejected(
    client: AsyncClient,
) -> None:
    first = await client.post(
        "/rebalance-tasks", json=_body(), headers={"Idempotency-Key": "key-conflict"}
    )
    second = await client.post(
        "/rebalance-tasks",
        json=_body(action="dropoff", bike_count=10),
        headers={"Idempotency-Key": "key-conflict"},
    )

    assert first.status_code == 201
    assert second.status_code == 409


@pytest.mark.asyncio
async def test_missing_idempotency_key_is_rejected(client: AsyncClient) -> None:
    response = await client.post("/rebalance-tasks", json=_body())
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_unknown_station_404s(client: AsyncClient) -> None:
    response = await client.post(
        "/rebalance-tasks",
        json=_body(station_id="does-not-exist"),
        headers={"Idempotency-Key": "key-unknown-station"},
    )
    assert response.status_code == 404
