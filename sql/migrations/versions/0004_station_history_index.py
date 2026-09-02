"""Supporting index for GET /stations/{id}/history.

Revision ID: 0004
Revises: 0003
Create Date: 2026-09-02
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op

revision: str = "0004"
down_revision: Union[str, None] = "0003"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # station_status_snapshots' existing indexes don't serve "one station,
    # newest first": the BRIN index is observed_at-only (good for time-range
    # scans, useless for a station_id filter), and the PK's column order
    # (station_id, last_reported, observed_at) puts last_reported between
    # the two columns this query actually needs. Created on the parent
    # partitioned table, same as the existing BRIN index, so it propagates
    # to every partition (existing and future).
    op.execute(
        """
        CREATE INDEX ix_station_status_snapshots_station_id_observed_at
        ON station_status_snapshots (station_id, observed_at DESC)
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_station_status_snapshots_station_id_observed_at")
