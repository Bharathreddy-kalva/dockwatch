"""Rebalance tasks and the outbox table backing their Kafka events.

Revision ID: 0003
Revises: 0002
Create Date: 2026-09-02
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op

revision: str = "0003"
down_revision: Union[str, None] = "0002"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # station_id here is the live GBFS UUID (matches stations.station_id /
    # station_status_snapshots.station_id), not stations.short_name — a
    # rebalance task is about a station's current live state, not its
    # historical batch identity.
    op.execute(
        """
        CREATE TABLE rebalance_tasks (
            id BIGSERIAL PRIMARY KEY,
            idempotency_key TEXT NOT NULL,
            station_id TEXT NOT NULL,
            action TEXT NOT NULL CHECK (action IN ('pickup', 'dropoff')),
            bike_count INTEGER NOT NULL CHECK (bike_count > 0),
            status TEXT NOT NULL DEFAULT 'pending'
                CHECK (status IN ('pending', 'in_progress', 'completed', 'cancelled')),
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    # Unique, not part of the primary key: the idempotency key is a client
    # concern (dedupe a retried POST), not the task's identity.
    op.execute(
        "CREATE UNIQUE INDEX ux_rebalance_tasks_idempotency_key ON rebalance_tasks (idempotency_key)"
    )
    op.execute("CREATE INDEX ix_rebalance_tasks_station_id ON rebalance_tasks (station_id)")

    # Outbox pattern: a task row and its Kafka event commit in the same
    # transaction (see the API's POST /rebalance-tasks handler), so a crash
    # between "task created" and "event published" can't happen — a
    # separate relay process publishes unpublished (published_at IS NULL)
    # rows to Kafka and marks them, decoupling the write path from Kafka's
    # own availability.
    op.execute(
        """
        CREATE TABLE outbox (
            id BIGSERIAL PRIMARY KEY,
            aggregate_type TEXT NOT NULL,
            aggregate_id BIGINT NOT NULL,
            event_type TEXT NOT NULL,
            payload JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            published_at TIMESTAMPTZ
        )
        """
    )
    op.execute(
        """
        CREATE INDEX ix_outbox_unpublished ON outbox (created_at)
        WHERE published_at IS NULL
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS outbox")
    op.execute("DROP TABLE IF EXISTS rebalance_tasks")
