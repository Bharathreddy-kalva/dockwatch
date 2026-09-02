"""Clean, cast, and dedupe a month of raw Citi Bike trip CSVs with PySpark.

Spark writes the result to local disk as Parquet (partitioned by year/month
in the path, not a Spark partitionBy column — one month per run, so a plain
overwrite of that month's directory is simplest) and s3_lake then uploads it
to MinIO. Spark itself never talks to S3 directly here: pulling in Hadoop's
S3A connector (extra jars, version-sensitive AWS SDK pinning) isn't worth it
for a local-mode, one-month-at-a-time pipeline — boto3 is simpler and it's
what the rest of this codebase already uses for cloud-shaped I/O.
"""

from __future__ import annotations

import logging
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from dockwatch.batch.s3_lake import overwrite_prefix

logger = logging.getLogger(__name__)

RAW_SCHEMA = T.StructType(
    [
        T.StructField("ride_id", T.StringType(), nullable=True),
        T.StructField("rideable_type", T.StringType(), nullable=True),
        T.StructField("started_at", T.TimestampType(), nullable=True),
        T.StructField("ended_at", T.TimestampType(), nullable=True),
        T.StructField("start_station_name", T.StringType(), nullable=True),
        T.StructField("start_station_id", T.StringType(), nullable=True),
        T.StructField("end_station_name", T.StringType(), nullable=True),
        T.StructField("end_station_id", T.StringType(), nullable=True),
        T.StructField("start_lat", T.DoubleType(), nullable=True),
        T.StructField("start_lng", T.DoubleType(), nullable=True),
        T.StructField("end_lat", T.DoubleType(), nullable=True),
        T.StructField("end_lng", T.DoubleType(), nullable=True),
        T.StructField("member_casual", T.StringType(), nullable=True),
    ]
)


def build_spark(app_name: str = "dockwatch-clean-trips") -> SparkSession:
    return SparkSession.builder.appName(app_name).master("local[*]").getOrCreate()


def clean_trips(spark: SparkSession, csv_paths: list[Path]) -> DataFrame:
    raw = spark.read.csv(
        [str(p) for p in csv_paths],
        header=True,
        schema=RAW_SCHEMA,
        timestampFormat="yyyy-MM-dd HH:mm:ss.SSS",
    )

    # end_station_id/name and end_lat/lng are legitimately null for trips
    # where the bike was never docked (lost/stolen) — ~0.07% of rows in a
    # spot-check of this data — so those are kept, not dropped. Only rows
    # missing their required identity/time fields, or with an end before
    # their start, are corrupt enough to drop.
    return (
        raw.filter(F.col("ride_id").isNotNull())
        .filter(F.col("started_at").isNotNull() & F.col("ended_at").isNotNull())
        .filter(F.col("member_casual").isNotNull())
        .filter(F.col("ended_at") >= F.col("started_at"))
        .dropDuplicates(["ride_id"])
    )


def run(year: int, month: int, csv_paths: list[Path], local_out_dir: Path) -> str:
    """Clean the month's trips and land them in the data lake.

    Idempotent: both the local Parquet write and the MinIO upload overwrite
    this month's partition/prefix, so re-running for the same month replaces
    rather than duplicates the data.
    """
    spark = build_spark()
    try:
        cleaned = clean_trips(spark, csv_paths).cache()
        row_count = cleaned.count()
        local_path = local_out_dir / f"year={year:04d}" / f"month={month:02d}"
        cleaned.write.mode("overwrite").parquet(str(local_path))
    finally:
        spark.stop()

    prefix = f"trips/year={year:04d}/month={month:02d}/"
    overwrite_prefix(local_path, prefix)
    logger.info("cleaned %d row(s) for %04d-%02d -> s3://.../%s", row_count, year, month, prefix)
    return prefix
