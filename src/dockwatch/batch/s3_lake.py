"""Helpers for the MinIO-backed (S3-compatible) data lake."""

from __future__ import annotations

import logging
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

from dockwatch.common.config import settings

logger = logging.getLogger(__name__)


def _client():
    return boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint_url,
        aws_access_key_id=settings.s3_access_key,
        aws_secret_access_key=settings.s3_secret_key,
    )


def ensure_bucket() -> None:
    client = _client()
    try:
        client.head_bucket(Bucket=settings.s3_bucket)
    except ClientError:
        client.create_bucket(Bucket=settings.s3_bucket)
        logger.info("created bucket %s", settings.s3_bucket)


def overwrite_prefix(local_dir: Path, prefix: str) -> None:
    """Replace every object under `prefix` with the contents of `local_dir`.

    This is what makes the monthly trip load idempotent at the storage
    layer: re-running for the same month deletes that month's old objects
    before uploading the freshly cleaned ones, rather than appending to them.
    """
    ensure_bucket()
    client = _client()

    paginator = client.get_paginator("list_objects_v2")
    existing = [
        obj["Key"]
        for page in paginator.paginate(Bucket=settings.s3_bucket, Prefix=prefix)
        for obj in page.get("Contents", [])
    ]
    if existing:
        client.delete_objects(
            Bucket=settings.s3_bucket,
            Delete={"Objects": [{"Key": key} for key in existing]},
        )
        logger.info("deleted %d existing object(s) under %s", len(existing), prefix)

    uploaded = 0
    for path in local_dir.rglob("*"):
        if path.is_file():
            key = f"{prefix}{path.relative_to(local_dir)}"
            client.upload_file(str(path), settings.s3_bucket, key)
            uploaded += 1
    logger.info("uploaded %d object(s) to s3://%s/%s", uploaded, settings.s3_bucket, prefix)
