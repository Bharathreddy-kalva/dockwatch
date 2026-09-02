"""POST /rebalance-tasks -- idempotency-key aware, outbox pattern.

Idempotency: the client supplies an `Idempotency-Key` header. Rather than
SELECT-then-INSERT (a TOCTOU race under concurrent retries with the same
key), this always attempts the INSERT first and only falls back to a
lookup on a UniqueViolation against rebalance_tasks' unique idempotency_key
index -- the DB's own constraint is the source of truth for "was this key
already used," not a check the application race against. A replayed key
with the *same* request body returns the original task (still 201, same
as Stripe's convention: the resource already exists exactly as asked for).
A replayed key with a *different* body is a client bug, not a legitimate
retry, so it's rejected with 409 rather than silently returning stale data
for a request that was never actually made.

Outbox: the task row and its "rebalance_task.created" event are written
in the same transaction and committed together, so a crash between them
can't happen -- a separate relay process (not built yet) is what actually
publishes unpublished (published_at IS NULL) outbox rows to Kafka.
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal

import psycopg
from fastapi import APIRouter, Depends, Header, HTTPException
from psycopg import AsyncConnection
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from pydantic import BaseModel, Field

from dockwatch.api.db import get_db

router = APIRouter(prefix="/rebalance-tasks", tags=["rebalance-tasks"])


class RebalanceTaskCreate(BaseModel):
    station_id: str
    action: Literal["pickup", "dropoff"]
    bike_count: int = Field(gt=0)


class RebalanceTask(BaseModel):
    id: int
    station_id: str
    action: str
    bike_count: int
    status: str
    created_at: datetime


TASK_COLUMNS = "id, station_id, action, bike_count, status, created_at"


@router.post("", response_model=RebalanceTask, status_code=201)
async def create_rebalance_task(
    body: RebalanceTaskCreate,
    idempotency_key: str = Header(alias="Idempotency-Key"),
    conn: AsyncConnection = Depends(get_db),
) -> RebalanceTask:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            "SELECT 1 FROM stations WHERE station_id = %(station_id)s AND valid_to IS NULL",
            {"station_id": body.station_id},
        )
        if await cur.fetchone() is None:
            raise HTTPException(status_code=404, detail="station not found")

        try:
            async with conn.transaction():
                await cur.execute(
                    f"""
                    INSERT INTO rebalance_tasks (idempotency_key, station_id, action, bike_count)
                    VALUES (%(idempotency_key)s, %(station_id)s, %(action)s, %(bike_count)s)
                    RETURNING {TASK_COLUMNS}
                    """,
                    {
                        "idempotency_key": idempotency_key,
                        "station_id": body.station_id,
                        "action": body.action,
                        "bike_count": body.bike_count,
                    },
                )
                task = await cur.fetchone()
                assert task is not None

                await cur.execute(
                    """
                    INSERT INTO outbox (aggregate_type, aggregate_id, event_type, payload)
                    VALUES (%(aggregate_type)s, %(aggregate_id)s, %(event_type)s, %(payload)s)
                    """,
                    {
                        "aggregate_type": "rebalance_task",
                        "aggregate_id": task["id"],
                        "event_type": "rebalance_task.created",
                        "payload": Jsonb(
                            {
                                "id": task["id"],
                                "station_id": task["station_id"],
                                "action": task["action"],
                                "bike_count": task["bike_count"],
                                "status": task["status"],
                            }
                        ),
                    },
                )
            return RebalanceTask(**task)

        except psycopg.errors.UniqueViolation:
            await cur.execute(
                f"SELECT {TASK_COLUMNS} FROM rebalance_tasks WHERE idempotency_key = %(key)s",
                {"key": idempotency_key},
            )
            existing = await cur.fetchone()
            assert existing is not None

            if (
                existing["station_id"] != body.station_id
                or existing["action"] != body.action
                or existing["bike_count"] != body.bike_count
            ):
                raise HTTPException(
                    status_code=409,
                    detail="idempotency key already used with a different request",
                ) from None

            return RebalanceTask(**existing)
