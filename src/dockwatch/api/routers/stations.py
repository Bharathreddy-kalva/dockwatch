"""GET /stations and GET /stations/{station_id}/history.

station_id here is always the live GBFS UUID (stations.station_id /
station_status_snapshots.station_id) -- not stations.short_name, which is
the historical-batch identifier used by the trips table. See
backfill_stations.py for why the two aren't the same id space.
"""

from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query
from psycopg import AsyncConnection
from psycopg.rows import dict_row
from pydantic import BaseModel

from dockwatch.api.db import get_db
from dockwatch.api.pagination import DEFAULT_LIMIT, MAX_LIMIT, decode_cursor, encode_cursor

router = APIRouter(prefix="/stations", tags=["stations"])


class Station(BaseModel):
    station_id: str
    name: str
    short_name: str | None
    lat: float | None
    lon: float | None
    capacity: int | None
    region_id: str | None


class StationPage(BaseModel):
    items: list[Station]
    next_cursor: str | None


class StationStatusSnapshot(BaseModel):
    observed_at: datetime
    num_bikes_available: int
    num_bikes_disabled: int
    num_docks_available: int
    num_docks_disabled: int
    num_ebikes_available: int
    is_installed: bool
    is_renting: bool
    is_returning: bool


class StationHistoryPage(BaseModel):
    items: list[StationStatusSnapshot]
    next_cursor: str | None


@router.get("", response_model=StationPage)
async def list_stations(
    cursor: str | None = Query(default=None),
    limit: int = Query(default=DEFAULT_LIMIT, ge=1, le=MAX_LIMIT),
    conn: AsyncConnection = Depends(get_db),
) -> StationPage:
    after_station_id = decode_cursor(cursor) if cursor else None

    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT station_id, name, short_name, lat, lon, capacity, region_id
            FROM stations
            WHERE valid_to IS NULL
              AND (%(after)s::text IS NULL OR station_id > %(after)s::text)
            ORDER BY station_id
            LIMIT %(fetch_limit)s
            """,
            {"after": after_station_id, "fetch_limit": limit + 1},
        )
        rows = await cur.fetchall()

    has_more = len(rows) > limit
    page_rows = rows[:limit]
    next_cursor = encode_cursor(page_rows[-1]["station_id"]) if has_more else None

    return StationPage(items=[Station(**row) for row in page_rows], next_cursor=next_cursor)


@router.get("/{station_id}/history", response_model=StationHistoryPage)
async def station_history(
    station_id: str,
    cursor: str | None = Query(default=None),
    limit: int = Query(default=DEFAULT_LIMIT, ge=1, le=MAX_LIMIT),
    conn: AsyncConnection = Depends(get_db),
) -> StationHistoryPage:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            "SELECT 1 FROM stations WHERE station_id = %(station_id)s AND valid_to IS NULL",
            {"station_id": station_id},
        )
        if await cur.fetchone() is None:
            raise HTTPException(status_code=404, detail="station not found")

        before_observed_at = decode_cursor(cursor) if cursor else None
        await cur.execute(
            """
            SELECT observed_at, num_bikes_available, num_bikes_disabled,
                   num_docks_available, num_docks_disabled, num_ebikes_available,
                   is_installed, is_renting, is_returning
            FROM station_status_snapshots
            WHERE station_id = %(station_id)s
              AND (%(before)s::timestamptz IS NULL OR observed_at < %(before)s::timestamptz)
            ORDER BY observed_at DESC
            LIMIT %(fetch_limit)s
            """,
            {"station_id": station_id, "before": before_observed_at, "fetch_limit": limit + 1},
        )
        rows = await cur.fetchall()

    has_more = len(rows) > limit
    page_rows = rows[:limit]
    next_cursor = encode_cursor(page_rows[-1]["observed_at"].isoformat()) if has_more else None

    return StationHistoryPage(
        items=[StationStatusSnapshot(**row) for row in page_rows], next_cursor=next_cursor
    )
