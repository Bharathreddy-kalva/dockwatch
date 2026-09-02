"""Backfill the `stations` SCD-2 dimension table from GBFS station_information.json.

Two different identifier namespaces are in play here, both preserved:
- `station_id` is GBFS's own station_id, a UUID. It's what
  station_status_snapshots.station_id (the live poller/consumer pipeline)
  is keyed on.
- `short_name` is the short numeric identifier (e.g. "3460.01"). It's what
  trips.start_station_id/end_station_id (the historical monthly CSVs) is
  keyed on -- Citi Bike's batch exports predate the UUID scheme and never
  switched. Confirmed against the live feed and the loaded data, not
  assumed: they genuinely don't match.

SCD-2, not upsert-in-place: a station's name/location/capacity can change
(renamed, relocated, re-docked), and CLAUDE.md's data model wants that
history preserved via valid_from/valid_to rather than overwritten. The
update logic only ever opens a new "current" row (valid_to IS NULL) when a
tracked attribute actually changed, or inserts one for a station_id seen
for the first time -- it never closes out a station that simply doesn't
appear in a given feed fetch, mirroring gbfs_poller.diff_stations()'s same
choice not to treat "missing from this poll" as a deletion.
"""

from __future__ import annotations

import csv
import io
import logging

import httpx
import psycopg

from dockwatch.common.config import settings

logger = logging.getLogger(__name__)

COLUMNS = ["station_id", "name", "short_name", "lat", "lon", "capacity", "region_id"]


def _connect_db() -> psycopg.Connection:
    dsn = settings.database_url.replace("postgresql+psycopg://", "postgresql://")
    return psycopg.connect(dsn)


def fetch_stations(client: httpx.Client) -> list[dict]:
    response = client.get(settings.gbfs_station_info_url)
    response.raise_for_status()
    return response.json()["data"]["stations"]


def _to_csv(stations: list[dict]) -> io.StringIO:
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    for station in stations:
        writer.writerow(
            [
                station["station_id"],
                station["name"],
                station.get("short_name"),
                station.get("lat"),
                station.get("lon"),
                station.get("capacity"),
                station.get("region_id"),
            ]
        )
    buffer.seek(0)
    return buffer


def backfill(stations: list[dict], conn: psycopg.Connection) -> tuple[int, int]:
    """Load the feed's stations into the SCD-2 table.

    Returns (new_or_changed, closed_out) row counts.
    """
    columns_sql = ", ".join(COLUMNS)
    buffer = _to_csv(stations)

    with conn.cursor() as cur:
        # Explicit columns rather than `LIKE stations` -- stations.id is a
        # BIGSERIAL, and INCLUDING DEFAULTS would carry over its nextval()
        # default, burning real values from the production sequence for
        # rows that only ever live in this temp table.
        cur.execute(
            """
            CREATE TEMP TABLE stations_staging (
                station_id TEXT,
                name TEXT,
                short_name TEXT,
                lat DOUBLE PRECISION,
                lon DOUBLE PRECISION,
                capacity INTEGER,
                region_id TEXT
            ) ON COMMIT DROP
            """
        )
        with cur.copy(f"COPY stations_staging ({columns_sql}) FROM STDIN WITH (FORMAT csv)") as copy:
            copy.write(buffer.read())

        # Close out the current row for any station whose tracked attributes
        # changed. IS DISTINCT FROM (not !=) so a NULL region_id/short_name
        # compares correctly instead of silently evaluating to NULL.
        cur.execute(
            """
            UPDATE stations
            SET valid_to = now()
            FROM stations_staging AS staged
            WHERE stations.station_id = staged.station_id
              AND stations.valid_to IS NULL
              AND (stations.name, stations.short_name, stations.lat, stations.lon,
                   stations.capacity, stations.region_id)
                  IS DISTINCT FROM
                  (staged.name, staged.short_name, staged.lat, staged.lon,
                   staged.capacity, staged.region_id)
            """
        )
        closed_out = cur.rowcount

        # Open a current row for every station_id that doesn't have one --
        # covers both brand-new stations and ones just closed out above.
        staged_columns_sql = ", ".join(f"staged.{column}" for column in COLUMNS)
        cur.execute(
            f"""
            INSERT INTO stations ({columns_sql})
            SELECT {staged_columns_sql}
            FROM stations_staging AS staged
            LEFT JOIN stations AS current
              ON current.station_id = staged.station_id AND current.valid_to IS NULL
            WHERE current.station_id IS NULL
            """
        )
        new_or_changed = cur.rowcount

        conn.commit()

    return new_or_changed, closed_out


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    with httpx.Client(timeout=30.0) as client:
        stations = fetch_stations(client)

    with _connect_db() as conn:
        new_or_changed, closed_out = backfill(stations, conn)

    logger.info(
        "fetched %d station(s): %d new/changed row(s) opened, %d prior row(s) closed out",
        len(stations),
        new_or_changed,
        closed_out,
    )


if __name__ == "__main__":
    main()
