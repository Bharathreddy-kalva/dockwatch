"""Download a month of Citi Bike trip history from the public tripdata bucket.

https://s3.amazonaws.com/tripdata/index.html — one zip per month, named
YYYYMM-citibike-tripdata.zip, containing one or more CSVs (large months are
split into multiple files).
"""

from __future__ import annotations

import logging
import zipfile
from pathlib import Path

import httpx

logger = logging.getLogger(__name__)

TRIPDATA_BASE_URL = "https://s3.amazonaws.com/tripdata"


def trip_data_url(year: int, month: int) -> str:
    return f"{TRIPDATA_BASE_URL}/{year:04d}{month:02d}-citibike-tripdata.zip"


def download_and_extract(year: int, month: int, dest_dir: Path) -> list[Path]:
    """Download the month's trip data zip and extract its CSVs into dest_dir.

    Idempotent: overwrites the zip and re-extracts over any existing files,
    so re-running for the same month always produces the same CSVs on disk.
    """
    dest_dir.mkdir(parents=True, exist_ok=True)
    url = trip_data_url(year, month)
    zip_path = dest_dir / f"{year:04d}{month:02d}-citibike-tripdata.zip"

    logger.info("downloading %s", url)
    with httpx.stream("GET", url, timeout=120.0, follow_redirects=True) as response:
        response.raise_for_status()
        with zip_path.open("wb") as f:
            for chunk in response.iter_bytes():
                f.write(chunk)

    with zipfile.ZipFile(zip_path) as archive:
        csv_names = [name for name in archive.namelist() if name.endswith(".csv")]
        archive.extractall(dest_dir, members=csv_names)

    zip_path.unlink()
    paths = sorted(dest_dir / name for name in csv_names)
    logger.info("extracted %d CSV file(s) to %s", len(paths), dest_dir)
    return paths
