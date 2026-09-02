"""Backfill hourly NYC weather from Open-Meteo's historical archive API.

Citywide, not per-station — Open-Meteo isn't station-granular, and
CLAUDE.md scopes this project to one city, so a single (lat, lon) is enough.

Idempotent via upsert on observed_at (the hour bucket), same pattern as the
GBFS consumer's ON CONFLICT dedup: re-running for a month that's already
loaded just overwrites those hours with the same values.
"""

from __future__ import annotations

import argparse
import calendar
import logging

import httpx
import psycopg

from dockwatch.common.config import settings

logger = logging.getLogger(__name__)

NYC_LATITUDE = 40.7128
NYC_LONGITUDE = -74.0060

UPSERT_SQL = """
    INSERT INTO weather_hourly (observed_at, temperature_c, precipitation_mm)
    VALUES (%(observed_at)s, %(temperature_c)s, %(precipitation_mm)s)
    ON CONFLICT (observed_at) DO UPDATE SET
        temperature_c = EXCLUDED.temperature_c,
        precipitation_mm = EXCLUDED.precipitation_mm
"""


def _connect_db() -> psycopg.Connection:
    dsn = settings.database_url.replace("postgresql+psycopg://", "postgresql://")
    return psycopg.connect(dsn)


def fetch_month(year: int, month: int) -> list[dict]:
    last_day = calendar.monthrange(year, month)[1]
    params: dict[str, str | float] = {
        "latitude": NYC_LATITUDE,
        "longitude": NYC_LONGITUDE,
        "start_date": f"{year:04d}-{month:02d}-01",
        "end_date": f"{year:04d}-{month:02d}-{last_day:02d}",
        "hourly": "temperature_2m,precipitation",
        "timezone": "UTC",
    }
    response = httpx.get(settings.open_meteo_archive_url, params=params, timeout=30.0)
    response.raise_for_status()
    hourly = response.json()["hourly"]

    return [
        {"observed_at": observed_at, "temperature_c": temperature_c, "precipitation_mm": precipitation_mm}
        for observed_at, temperature_c, precipitation_mm in zip(
            hourly["time"], hourly["temperature_2m"], hourly["precipitation"]
        )
    ]


def backfill_month(year: int, month: int) -> int:
    rows = fetch_month(year, month)
    with _connect_db() as conn, conn.cursor() as cur:
        cur.executemany(UPSERT_SQL, rows)
        conn.commit()
    logger.info("upserted %d hourly weather row(s) for %04d-%02d", len(rows), year, month)
    return len(rows)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()
    backfill_month(args.year, args.month)


if __name__ == "__main__":
    main()
