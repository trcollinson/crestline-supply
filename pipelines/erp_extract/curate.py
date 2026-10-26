"""The curate step: fold an entity's new raw reads into its curated table (M2 step 8).

Step Functions calls this once per entity, right after extracting it
(`ErpIngestStack`); `crestline curate` runs the same function from a laptop.
Each call:

1. loads how far the entity's curated table has merged: `merged_through`,
   the name of the last raw read folded in, kept on the entity's state item
   next to the poller's watermark;
2. lists the entity's raw reads in the bucket after that one;
3. picks the window to merge: from the newest full read or sweep among them,
   if there is one, since it supersedes everything before it; otherwise from
   the first;
4. checks the window before touching the table (`curated_check.sql`): Athena
   must see exactly the rows the raw files hold, and the MERGE may delete at
   most 5% of the table. Either check failing fails the call, loudly;
5. runs the MERGE (`curated.sql`, filled in by `curated.merge_sql`) on Athena
   and waits for it;
6. saves `merged_through` as the window's last read;
7. once a day, compacts the table and expires old snapshots (`upkeep_sql`),
   noting the day as `upkept_on`.

Why step 4: in M2 the replayed ERP's clock ran past the raw table's projected
dates, Athena stopped seeing new pages, every sweep looked empty, and the
MERGE did what an empty sweep says: it deleted every row, and the run
succeeded. A check that skipped quietly would only have swapped that for a
table that silently stops updating; failing makes the run fail, and the
alarm on failed runs tells someone.

The window comes from listing file names, not from a query: an S3 listing
costs nothing next to an Athena scan, and the MERGE then filters on
constant dates that Athena can prune partitions with. If the MERGE succeeds
but saving fails, the next call merges the same reads again, which changes
nothing. `rebuild` ignores `merged_through` and merges from the first raw
read, which rebuilds a table from the raw bucket alone.
"""

import json
import os
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime
from functools import cache

from .curated import check_sql, merge_sql, upkeep_sql
from .entities import ENTITIES
from .merge import COMPLETE
from .raw import SOURCE, ingest_date_of

# Setting -> environment variable (the stack sets them all).
ENV = {
    "bucket": "CRESTLINE_RAW_BUCKET",
    "table": "CRESTLINE_STATE_TABLE",
    "workgroup": "CRESTLINE_ATHENA_WORKGROUP",
    "raw_database": "CRESTLINE_RAW_DATABASE",
    "curated_database": "CRESTLINE_CURATED_DATABASE",
}
POLL_SECONDS = 1.0
# The most of a table one MERGE may delete. The ERP deletes a few cancelled
# duplicates a day (7 of 1,309 jobs over 90 days); `crestline curate
# --allow-mass-delete` lifts the limit once someone has checked.
MAX_DELETED_SHARE = 0.05


class CurateFailed(Exception):
    """A statement failed; `merged_through` is unchanged, so the next run tries again."""


@dataclass(frozen=True)
class Window:
    first: str  # raw read names, without .jsonl
    last: str
    reads: int
    starts_complete: bool  # the first read is a full read or a sweep


def kind_of(read: str) -> str:
    """A read's kind, from its name: `20260806T061351Z-001-sweep` -> `sweep`."""
    return read.rsplit("-", 1)[1]


def pick_window(reads: list[str]) -> Window | None:
    """The reads to merge, given every read not merged yet, oldest first."""
    if not reads:
        return None
    complete = [i for i, read in enumerate(reads) if kind_of(read) in COMPLETE]
    start = complete[-1] if complete else 0
    return Window(
        first=reads[start],
        last=reads[-1],
        reads=len(reads) - start,
        starts_complete=bool(complete),
    )


def new_reads(s3, bucket: str, entity: str, merged_through: str | None) -> list[str]:
    """The entity's raw reads after `merged_through`, oldest first.

    Keys sort in the order the reads were made (`ingest_date=` is the run's
    date, and the file name starts with the run), so the listing can start
    right after the last read merged.
    """
    prefix = f"{SOURCE}/entity={entity}/"
    options = {"Bucket": bucket, "Prefix": prefix}
    if merged_through:
        folder = f"ingest_date={ingest_date_of(merged_through)}"
        options["StartAfter"] = f"{prefix}{folder}/{merged_through}.jsonl"
    reads = []
    for page in s3.get_paginator("list_objects_v2").paginate(**options):
        for item in page.get("Contents", []):
            name = item["Key"].rsplit("/", 1)[1]
            if name.endswith(".jsonl"):
                reads.append(name.removesuffix(".jsonl"))
    return sorted(reads)


def rows_in(s3, bucket: str, entity: str, read: str) -> int:
    """How many rows a raw read holds, counted from its file: what the extractor wrote."""
    key = f"{SOURCE}/entity={entity}/ingest_date={ingest_date_of(read)}/{read}.jsonl"
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")
    return sum(len(json.loads(line)["body"]["value"]) for line in body.splitlines())


def counts(athena, query_id: str) -> tuple[int, int, int]:
    """The check's one row: rows seen, rows held, rows the MERGE would delete."""
    rows = athena.get_query_results(QueryExecutionId=query_id)["ResultSet"]["Rows"]
    seen, held, gone = (int(cell["VarCharValue"]) for cell in rows[1]["Data"])
    return seen, held, gone


def curate(
    entity: str,
    setup: dict,
    *,
    rebuild: bool = False,
    max_deleted_share: float = MAX_DELETED_SHARE,
    today: date | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> dict:
    """Merge the entity's new raw reads into its curated table; report what was done.

    `max_deleted_share` is the most of the table the MERGE may delete (1.0 lifts
    the limit). `today` (the wall clock's UTC date unless given) decides
    whether the table's daily upkeep is due.
    """
    table = setup["table"]
    item = table.get_item(Key={"entity": entity}).get("Item", {})
    merged_through = None if rebuild else item.get("merged_through")
    reads = new_reads(setup["s3"], setup["bucket"], entity, merged_through)
    window = pick_window(reads)
    report = {"entity": entity, "merged_through": merged_through, "reads": 0}
    scanned = 0

    if window is not None:
        options = {
            "curated_database": setup["curated_database"],
            "raw_database": setup["raw_database"],
            "first_read": window.first,
            "last_read": window.last,
            "starts_complete": window.starts_complete,
        }
        written = sum(
            rows_in(setup["s3"], setup["bucket"], entity, read)
            for read in reads
            if window.first <= read <= window.last
        )
        query_id, scanned = run_athena(
            setup["athena"], setup["workgroup"], check_sql(entity, **options), sleep
        )
        seen, held, gone = counts(setup["athena"], query_id)
        if seen != written:
            raise CurateFailed(
                f"Athena sees {seen:,} rows in {entity}'s reads {window.first} to "
                f"{window.last}, but the raw files hold {written:,}; nothing merged"
            )
        if gone > max_deleted_share * held:
            raise CurateFailed(
                f"merging {entity}'s reads {window.first} to {window.last} would "
                f"delete {gone:,} of its {held:,} rows; nothing merged "
                f"(`crestline curate {entity} --allow-mass-delete` if that is right)"
            )
        sql = merge_sql(entity, **options)
        query_id, merge_scanned = run_athena(
            setup["athena"], setup["workgroup"], sql, sleep
        )
        scanned += merge_scanned
        table.update_item(
            Key={"entity": entity},
            UpdateExpression="SET merged_through = :read",
            ExpressionAttributeValues={":read": window.last},
        )
        report |= {
            "merged_through": window.last,
            "reads": window.reads,
            "from": window.first,
            "starts_complete": window.starts_complete,
            "query_id": query_id,
            "rows_read": written,
            "rows_deleted": gone,
        }

    today = (today or datetime.now(UTC).date()).isoformat()
    if item.get("upkept_on") != today:
        for statement in upkeep_sql(entity, curated_database=setup["curated_database"]):
            scanned += run_athena(
                setup["athena"], setup["workgroup"], statement, sleep
            )[1]
        table.update_item(
            Key={"entity": entity},
            UpdateExpression="SET upkept_on = :today",
            ExpressionAttributeValues={":today": today},
        )
        report["upkept_on"] = today
    return report | {"bytes_scanned": scanned}


def run_athena(
    athena, workgroup: str, sql: str, sleep: Callable[[float], None]
) -> tuple[str, int]:
    """Run one statement in the workgroup and wait; returns its ID and bytes scanned."""
    query_id = athena.start_query_execution(QueryString=sql, WorkGroup=workgroup)[
        "QueryExecutionId"
    ]
    while True:
        execution = athena.get_query_execution(QueryExecutionId=query_id)
        status = execution["QueryExecution"]["Status"]
        if status["State"] in {"SUCCEEDED", "FAILED", "CANCELLED"}:
            break
        sleep(POLL_SECONDS)
    if status["State"] != "SUCCEEDED":
        reason = status.get("StateChangeReason", status["State"])
        raise CurateFailed(f"{query_id}: {reason}")
    statistics = execution["QueryExecution"].get("Statistics", {})
    return query_id, statistics.get("DataScannedInBytes", 0)


@cache
def _setup() -> dict:
    """AWS clients and names, once per cold start."""
    import boto3

    settings = {name: os.environ[variable] for name, variable in ENV.items()}
    return settings | {
        "s3": boto3.client("s3"),
        "athena": boto3.client("athena"),
        "table": boto3.resource("dynamodb").Table(settings["table"]),
    }


def handler(event: dict, context: object) -> dict:
    """Curate one entity: `{"entity": "Jobs", ...}` -> what was merged."""
    entity = event["entity"]
    if entity not in ENTITIES:
        raise ValueError(f"no entity {entity}")
    return curate(entity, _setup())
