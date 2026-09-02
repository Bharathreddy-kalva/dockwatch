"""Integration tests for the outbox relay's publish/mark-published logic.

Uses a real testcontainers Postgres (proving the actual DB bookkeeping:
fetch, mark published, no double-fetch of an already-published row) and a
fake Producer that fires delivery callbacks synchronously from flush()
instead of talking to a real broker -- this exercises publish_batch()'s
real "only mark what Kafka actually acked" logic, including a simulated
delivery failure, without needing a running Kafka in CI.

The end-to-end proof against a real Redpanda broker (including recovery
from a genuine broker outage) was done manually against the live stack,
not as part of this automated suite -- see the PR description.

Each test filters fetch_unpublished()'s result down to the row ids it
itself created before handing them to publish_batch(): the container is
shared (module scope) for speed, so a test must not blindly sweep up
whatever another test may have left unpublished.
"""

from __future__ import annotations

import os
import subprocess
import sys
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import psycopg
import pytest
from testcontainers.community.postgres import PostgresContainer

from dockwatch.outbox.relay import fetch_unpublished, publish_batch

REPO_ROOT = Path(__file__).resolve().parents[2]


class FakeProducer:
    """Stand-in for confluent_kafka.Producer: records produced messages and
    fires delivery callbacks synchronously from flush(), optionally failing
    delivery for specific keys."""

    def __init__(self, fail_keys: set[str] | None = None) -> None:
        self.produced: list[tuple[str, str, bytes]] = []
        self._pending: list[tuple[str, Callable[[Any, Any], None]]] = []
        self.fail_keys = fail_keys or set()

    def produce(self, topic: str, key: bytes, value: bytes, callback: Callable[[Any, Any], None]) -> None:
        key_str = key.decode("utf-8")
        self.produced.append((topic, key_str, value))
        self._pending.append((key_str, callback))

    def flush(self, timeout: float | None = None) -> int:
        for key_str, callback in self._pending:
            if key_str in self.fail_keys:
                callback(Exception("simulated delivery failure"), None)
            else:
                callback(None, None)
        self._pending.clear()
        return 0


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


def _insert_outbox_row(
    conn: psycopg.Connection, aggregate_id: int, event_type: str = "rebalance_task.created"
) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO outbox (aggregate_type, aggregate_id, event_type, payload)
            VALUES ('rebalance_task', %s, %s, '{"station_id": "s1", "action": "pickup"}'::jsonb)
            RETURNING id
            """,
            (aggregate_id, event_type),
        )
        row = cur.fetchone()
        assert row is not None
        conn.commit()
        return row[0]


def _is_published(conn: psycopg.Connection, row_id: int) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT published_at FROM outbox WHERE id = %s", (row_id,))
        row = cur.fetchone()
        assert row is not None
        return row[0] is not None


def _fetch_by_ids(conn: psycopg.Connection, row_ids: set[int]) -> list[dict]:
    """fetch_unpublished(), filtered to just the ids a test created -- the
    container is shared across tests in this module, so this keeps one
    test's publish_batch() call from sweeping up another test's leftovers.
    """
    return [row for row in fetch_unpublished(conn, limit=100) if row["id"] in row_ids]


def test_publish_batch_marks_delivered_rows_as_published(conn: psycopg.Connection) -> None:
    row_id = _insert_outbox_row(conn, aggregate_id=1)
    rows = _fetch_by_ids(conn, {row_id})
    assert len(rows) == 1

    producer = FakeProducer()
    published_count = publish_batch(producer, conn, rows)

    assert published_count == 1
    assert [key for _topic, key, _value in producer.produced] == [str(row_id)]
    assert _is_published(conn, row_id)
    # And fetch_unpublished() no longer surfaces it -- this is the actual
    # mechanism that makes a rerun safe: a published row just isn't
    # eligible to be picked up and republished.
    assert row_id not in {row["id"] for row in _fetch_by_ids(conn, {row_id})}


def test_publish_batch_does_not_mark_failed_deliveries_as_published(conn: psycopg.Connection) -> None:
    ok_id = _insert_outbox_row(conn, aggregate_id=2)
    fail_id = _insert_outbox_row(conn, aggregate_id=3)
    rows = _fetch_by_ids(conn, {ok_id, fail_id})
    assert len(rows) == 2

    producer = FakeProducer(fail_keys={str(fail_id)})
    published_count = publish_batch(producer, conn, rows)

    assert published_count == 1
    assert _is_published(conn, ok_id)
    assert not _is_published(conn, fail_id)


def test_publish_batch_leaves_unknown_event_types_unpublished(conn: psycopg.Connection) -> None:
    row_id = _insert_outbox_row(conn, aggregate_id=4, event_type="some.unmapped.event")
    rows = _fetch_by_ids(conn, {row_id})
    assert len(rows) == 1

    producer = FakeProducer()
    published_count = publish_batch(producer, conn, rows)

    assert published_count == 0
    assert producer.produced == []
    assert not _is_published(conn, row_id)
