"""Push one backfill's system outputs to the raw bucket (spec §6, §9).

This is M1's one-time historical load, which is also how real projects
start: the files the MES, the ERP export, and the brake gateways produced
land in the raw bucket exactly as written, under

    source=<erp|mes|machines>/entity=<name>/ingest_date=YYYY-MM-DD/<file>

The entity is the file's own name without its date (`Jobs.jsonl` is `Jobs`,
`labor_20260703.csv` is `labor`), so each entity becomes one Athena table.
`ingest_date` is the day the extract was taken, the backfill's `--end`, so
re-running the same backfill writes the same keys.

Only the three system folders are read, by name. The answer key sits next to
them in the output folder and is never listed, let alone uploaded. Neither is
the ERP's SQLite file: Athena can't read it, and the export holds the same rows.

Re-running is safe. Files already in the bucket with the same content are
skipped, changed ones are replaced, and files in this ingest date's folders
that the backfill no longer writes are deleted, so each date holds exactly
one extract (the same rule the backfill applies to its own output folder).
Other dates are never touched, and the bucket's versioning keeps anything
replaced or deleted for 30 days.

This is data-platform code: it never imports the simulator.
"""

import base64
import hashlib
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import boto3

from crestline.stack import foundation_outputs

# Each source's folder in a backfill's output, and the files in it that go up.
SOURCES = {
    "erp": ("erp/export", "*.jsonl"),
    "mes": ("mes/outbound", "labor_*.csv"),
    "machines": ("machines", "*.jsonl"),
}

CONTENT_TYPES = {".jsonl": "application/x-ndjson", ".csv": "text/csv"}
MAX_KEYS_PER_DELETE = 1000  # S3's limit for one DeleteObjects call


@dataclass(frozen=True)
class UploadSummary:
    bucket: str
    uploaded: list[str]
    unchanged: list[str]
    deleted: list[str]


def raw_bucket_name(cloudformation=None) -> str:
    """The raw bucket's name, read from FoundationStack's outputs."""
    return foundation_outputs(cloudformation)["RawBucketName"]


def raw_key(source: str, path: Path, ingest_date: date) -> str:
    entity = path.stem.split("_", 1)[0]
    return (
        f"source={source}/entity={entity}/"
        f"ingest_date={ingest_date.isoformat()}/{path.name}"
    )


def planned_uploads(out_dir: Path, ingest_date: date) -> dict[str, Path]:
    """Every raw key this backfill's output should have, and its local file."""
    return {
        raw_key(source, path, ingest_date): path
        for source, (folder, pattern) in SOURCES.items()
        for path in sorted((out_dir / folder).glob(pattern))
    }


def upload_backfill(
    out_dir: Path, ingest_date: date, bucket: str, s3=None
) -> UploadSummary:
    """Make the bucket's `ingest_date` folders match the backfill in `out_dir`."""
    s3 = s3 or boto3.client("s3")
    planned = planned_uploads(out_dir, ingest_date)
    in_bucket = _objects_for_date(s3, bucket, ingest_date)

    uploaded, unchanged = [], []
    for key, path in planned.items():
        body = path.read_bytes()
        md5 = hashlib.md5(body)
        # A single-part upload's ETag is the MD5 of its content.
        if in_bucket.get(key) == md5.hexdigest():
            unchanged.append(key)
            continue
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=body,
            ContentMD5=base64.b64encode(md5.digest()).decode(),
            ContentType=CONTENT_TYPES.get(path.suffix, "application/octet-stream"),
        )
        uploaded.append(key)

    stale = sorted(in_bucket.keys() - planned.keys())
    for start in range(0, len(stale), MAX_KEYS_PER_DELETE):
        batch = stale[start : start + MAX_KEYS_PER_DELETE]
        s3.delete_objects(
            Bucket=bucket, Delete={"Objects": [{"Key": key} for key in batch]}
        )
    return UploadSummary(bucket, uploaded, unchanged, stale)


def _objects_for_date(s3, bucket: str, ingest_date: date) -> dict[str, str]:
    """Key -> content MD5 for every file in the sources' `ingest_date` folders."""
    folder = f"ingest_date={ingest_date.isoformat()}"
    objects = {}
    for source in SOURCES:
        pages = s3.get_paginator("list_objects_v2").paginate(
            Bucket=bucket, Prefix=f"source={source}/"
        )
        for page in pages:
            for item in page.get("Contents", []):
                parts = item["Key"].split("/")  # source, entity, date, file
                if len(parts) == 4 and parts[2] == folder:
                    objects[item["Key"]] = item["ETag"].strip('"')
    return objects
