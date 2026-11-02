"""The machine curate step: new Firehose files into the curated tables (M3 step 8).

Runs hourly (MachineIngestStack's schedule) and from `crestline curate-machines`.
For each device it does what the ERP's curate step does, simplified by one
fact: machine data only ever arrives, it never changes or goes away.

1. **The window**: the device's Firehose files after the last one merged
   (`merged_through`, on the device's DynamoDB state item). Firehose names a
   file for when it was written, so the keys sort in arrival order. A file
   younger than `SETTLE` is left for the next run, and so is every file
   after it, so a file can't be skipped by one that landed first.
2. **The check**: Athena must see exactly as many lines in the window as
   the files hold, counted from S3, or nothing is merged.
3. **The MERGE**: insert every line whose `message_id` the table doesn't
   hold. No update, no delete: a re-run inserts nothing twice.
4. **The guard**: a machine table never shrinks. If the row count after the
   MERGE is below the count before, the step alerts and fails without
   saving `merged_through` (the ERP's guard refuses a MERGE that would
   delete more than 5%; for insert-only data, any delete is a refusal).
5. **Upkeep**, once a day: OPTIMIZE and VACUUM, as for the ERP's tables.
6. **The data-shape checks** (`checks`), once every device is merged.

A step that fails raises, so the Lambda's error alarm fires.
"""

import os
import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from functools import cache

from erp_extract.curate import CurateFailed, run_athena

from .tables import COLUMNS, CURATED_DATABASE, KEY, RAW_TABLE, TABLES

SOURCE = "source=machines-iot"
SETTLE = timedelta(
    minutes=2
)  # a Firehose file this new may have a sibling still landing

ENV = {
    "bucket": "CRESTLINE_RAW_BUCKET",
    "table": "CRESTLINE_MACHINE_STATE_TABLE",
    "workgroup": "CRESTLINE_ATHENA_WORKGROUP",
    "raw_database": "CRESTLINE_RAW_DATABASE",
    "alarm_topic": "CRESTLINE_ALARM_TOPIC",
}


def new_files(
    s3, bucket: str, device: str, merged_through: str | None, now: datetime
) -> list[str]:
    """The device's settled Firehose files after `merged_through`, in arrival order."""
    prefix = f"{SOURCE}/entity={device}/"
    options = {"Bucket": bucket, "Prefix": prefix}
    if merged_through:
        options["StartAfter"] = merged_through
    files = []
    for page in s3.get_paginator("list_objects_v2").paginate(**options):
        for item in page.get("Contents", []):
            files.append((item["Key"], item["LastModified"]))
    settled = []
    for key, landed in sorted(files):
        if now - landed < SETTLE:
            break
        settled.append(key)
    return settled


def lines_in(s3, bucket: str, keys: list[str]) -> int:
    """Lines the files hold, counted from S3: what Firehose delivered."""
    total = 0
    for key in keys:
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")
        total += sum(1 for line in body.splitlines() if line.strip())
    return total


def ingest_date_of(key: str) -> str:
    return key.split("ingest_date=", 1)[1].split("/", 1)[0]


def window_filter(device: str, bucket: str, first: str, last: str) -> str:
    """The window's lines in the raw table: partition pruning, then the files."""
    return (
        f"entity = '{device}'"
        f" and ingest_date between '{ingest_date_of(first)}' and '{ingest_date_of(last)}'"
        f" and \"$path\" between 's3://{bucket}/{first}' and 's3://{bucket}/{last}'"
        " and trim(line) <> ''"
    )


def count_sql(
    device: str, bucket: str, first: str, last: str, raw_database: str
) -> str:
    return (
        f"select count(*) from {raw_database}.{RAW_TABLE}"
        f" where {window_filter(device, bucket, first, last)}"
    )


def merge_sql(
    device: str, bucket: str, first: str, last: str, raw_database: str
) -> str:
    """Insert the window's messages the table doesn't hold yet."""
    columns = COLUMNS[device]
    names = ", ".join(name for name, _, _ in columns)
    typed = ",\n           ".join(f"{expr} as {name}" for name, _, expr in columns)
    values = ", ".join(f"s.{name}" for name, _, _ in columns)
    return f"""merge into {CURATED_DATABASE}.{TABLES[device]} as t
using (
    select distinct
           {typed}
    from {raw_database}.{RAW_TABLE}
    where {window_filter(device, bucket, first, last)}
) as s
on t.{KEY} = s.{KEY}
when not matched then insert ({names}) values ({values})"""


def rows_sql(device: str) -> str:
    return f"select count(*) from {CURATED_DATABASE}.{TABLES[device]}"


def upkeep_sql(device: str) -> list[str]:
    table = f"{CURATED_DATABASE}.{TABLES[device]}"
    return [f"optimize {table} rewrite data using bin_pack", f"vacuum {table}"]


def scalar(athena, query_id: str) -> int:
    rows = athena.get_query_results(QueryExecutionId=query_id)["ResultSet"]["Rows"]
    return int(rows[1]["Data"][0]["VarCharValue"])


def curate(
    device: str,
    setup: dict,
    *,
    now: datetime | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> dict:
    """Merge the device's new Firehose files into its curated table; report what was done."""
    now = now or datetime.now(UTC)
    athena, workgroup = setup["athena"], setup["workgroup"]
    state = setup["table"]
    item = state.get_item(Key={"entity": device}).get("Item", {})
    files = new_files(
        setup["s3"], setup["bucket"], device, item.get("merged_through"), now
    )
    report: dict = {"device": device, "files": len(files)}

    if files:
        first, last = files[0], files[-1]
        args = (device, setup["bucket"], first, last, setup["raw_database"])
        written = lines_in(setup["s3"], setup["bucket"], files)
        query_id, _ = run_athena(athena, workgroup, count_sql(*args), sleep)
        seen = scalar(athena, query_id)
        if seen != written:
            raise CurateFailed(
                f"Athena sees {seen:,} lines in {device}'s files {first} to {last}, "
                f"but they hold {written:,}; nothing merged"
            )
        before = scalar(
            athena, run_athena(athena, workgroup, rows_sql(device), sleep)[0]
        )
        run_athena(athena, workgroup, merge_sql(*args), sleep)
        after = scalar(
            athena, run_athena(athena, workgroup, rows_sql(device), sleep)[0]
        )
        if after < before:
            message = (
                f"{TABLES[device]} went from {before:,} rows to {after:,} merging "
                f"{first} to {last}: machine data is insert-only, so any delete is "
                "a fault. merged_through not moved."
            )
            alert(setup, f"{device} merge deleted rows", message)
            raise CurateFailed(message)
        state.update_item(
            Key={"entity": device},
            UpdateExpression="SET merged_through = :last",
            ExpressionAttributeValues={":last": last},
        )
        report |= {"lines": written, "inserted": after - before, "rows": after}

    today = now.date().isoformat()
    if item.get("upkept_on") != today:
        for statement in upkeep_sql(device):
            run_athena(athena, workgroup, statement, sleep)
        state.update_item(
            Key={"entity": device},
            UpdateExpression="SET upkept_on = :today",
            ExpressionAttributeValues={":today": today},
        )
        report["upkept_on"] = today
    return report


def alert(setup: dict, subject: str, message: str) -> None:
    setup["sns"].publish(
        TopicArn=setup["alarm_topic"],
        Subject=f"Crestline machines: {subject}"[:100],
        Message=message,
    )


def curate_all(setup: dict, devices: list[str] | None = None) -> list[dict]:
    """Every device, each on its own; any failure fails the run once all have tried."""
    reports, failures = [], []
    for device in devices or TABLES:
        try:
            reports.append(curate(device, setup))
        except CurateFailed as failure:
            failures.append(f"{device}: {failure}")
    if failures:
        raise CurateFailed("; ".join(failures))
    # The data-shape checks, once everything is merged; a breach is emailed
    # with what it found, and fails the run.
    from checks.aws import ChecksFailed, athena_sql, machine_checks

    try:
        machine_checks(
            athena_sql(setup["athena"], setup["workgroup"]),
            setup["table"],
            setup["raw_database"],
        )
    except ChecksFailed as breach:
        alert(setup, "data-shape check failed", str(breach))
        raise
    return reports


@cache
def _setup() -> dict:
    import boto3

    settings = {name: os.environ[variable] for name, variable in ENV.items()}
    return settings | {
        "s3": boto3.client("s3"),
        "athena": boto3.client("athena"),
        "sns": boto3.client("sns"),
        "table": boto3.resource("dynamodb").Table(settings["table"]),
    }


def handler(event: dict, context: object) -> list[dict]:
    """The hourly run: every device (or `{"devices": [...]}`)."""
    return curate_all(_setup(), event.get("devices"))
