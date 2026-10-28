"""The curated layer's schema, its MERGE, and the curate step (M2 step 8).

The MERGE itself runs only in Athena (eval/curated.py checks the result
against the ERP). Here: the curated columns match the store, the SQL is
filled in completely, the window rule gives the same copy as replaying
every read (`merge.rebuild`) however often the curate step runs, and the
step against moto's S3 and DynamoDB with a stand-in Athena.
"""

import itertools
import json
import random
import re
from datetime import date
from urllib.parse import unquote

import boto3
import pytest
from moto import mock_aws

from crestline.sim.systems import erp
from crestline_erp_api.store import Store
from erp_extract import aws
from erp_extract.client import ErpClient, Response
from erp_extract.curate import (
    CurateFailed,
    curate,
    current_snapshot,
    kind_of,
    new_reads,
    pick_window,
)
from erp_extract.curated import (
    COLUMNS,
    TABLES,
    check_sql,
    column_name,
    deleted_sql,
    merge_sql,
    upkeep_sql,
)
from erp_extract.entities import ENTITIES, key_of
from erp_extract.merge import rebuild
from erp_extract.raw import SOURCE, Page, RawWriter, encode, raw_key
from erp_extract.robust import RobustExtractor, State

ICEBERG_TYPE = {
    "TEXT": "string",
    "REAL": "double",
    "INTEGER": "int",
    "BOOLEAN": "boolean",
    "DATE": "date",
    "TIMESTAMP": "timestamp",
}
BUCKET = "crestline-raw-test"
CURATED_BUCKET = "crestline-curated-test"


# --- The schema and the SQL ---------------------------------------------------


def test_every_entity_has_a_curated_table_with_the_stores_columns_and_types(
    erp_changes, tmp_path
):
    erp.write_erp(erp_changes, tmp_path)
    store = Store(tmp_path / "erp.db")
    assert list(TABLES) == list(COLUMNS) == list(ENTITIES)
    for entity in ENTITIES:
        declared = store.entities[entity].columns
        assert [field for field, _ in COLUMNS[entity]] == list(declared), entity
        assert [kind for _, kind in COLUMNS[entity]] == [
            ICEBERG_TYPE[d] for d in declared.values()
        ], entity
    assert len(set(TABLES.values())) == len(TABLES)
    assert all(name.startswith("erp_") for name in TABLES.values())


def jobs_merge(**options) -> str:
    settings = {
        "curated_database": "crestline_curated",
        "raw_database": "crestline_raw",
        "first_read": "20260805T061400Z-001-sweep",
        "last_read": "20260806T061351Z-000-incremental",
        "starts_complete": True,
    }
    return merge_sql("JobOperations", **(settings | options))


def test_the_merge_is_filled_in_for_one_entity_and_one_window():
    sql = jobs_merge()
    assert "${" not in sql.replace("the ${...} names", "")
    assert sql.startswith("-- Curated ERP tables")
    assert 'merge into "crestline_curated"."erp_job_operations" as t' in sql
    assert 'from "crestline_raw"."raw_erp_api"' in sql
    assert "where entity = 'JobOperations'" in sql
    # A constant partition filter, so Athena reads only the window's days.
    assert "ingest_date between '2026-08-05' and '2026-08-06'" in sql
    assert (
        "between '20260805T061400Z-001-sweep' and '20260806T061351Z-000-incremental'"
        in sql
    )
    # A composite key matches on both columns.
    assert "on t.jobnum = s.jobnum and t.oprseq = s.oprseq" in sql
    assert "partition by jobnum, oprseq" in sql
    # Every column is read by its ERP name and written by its curated one.
    for field, _ in COLUMNS["JobOperations"]:
        assert (
            f"json_extract_scalar(record, '$.{field}') as {column_name(field)}" in sql
        )


def test_a_window_without_a_complete_read_deletes_nothing():
    assert "where true\n" in jobs_merge(starts_complete=True)
    assert "where false\n" in jobs_merge(starts_complete=False)


def test_the_check_counts_what_athena_sees_and_what_the_merge_would_delete():
    sql = check_sql(
        "JobOperations",
        curated_database="crestline_curated",
        raw_database="crestline_raw",
        first_read="20260805T061400Z-001-sweep",
        last_read="20260806T061351Z-000-incremental",
        starts_complete=True,
    )
    assert "${" not in sql.replace("the ${...} names", "")
    # The same window as the MERGE, pruned the same way.
    assert "ingest_date between '2026-08-05' and '2026-08-06'" in sql
    # Keys typed as the table types them, so they match its rows.
    assert "json_extract_scalar(record, '$.OprSeq') as oprseq" in sql
    assert "cast(cast(oprseq as double) as integer) as oprseq" in sql
    assert "where n.jobnum = c.jobnum and n.oprseq = c.oprseq" in sql
    for column in ("rows_seen", "rows_held", "rows_gone"):
        assert f"as {column}" in sql


def test_types_are_fixed_from_what_the_api_served():
    sql = merge_sql(
        "LaborDetails",
        curated_database="c",
        raw_database="r",
        first_read="20260805T061400Z-000-sequence",
        last_read="20260805T061400Z-000-sequence",
        starts_complete=False,
    )
    # "37.00000000" casts to a whole number only through double.
    assert "cast(cast(laborqty as double) as integer) as laborqty" in sql
    assert "cast(laborhrs as double) as laborhrs" in sql
    # /Date(ms)/ or ISO 8601, either way a date or a UTC timestamp.
    assert "when payrolldate like '/Date(%'" in sql
    assert "from_iso8601_date(payrolldate)" in sql
    assert "from_iso8601_timestamp(lastmodified) end at time zone 'UTC'" in sql
    assert "\n               jobnum,\n" in sql  # text stays as it is


def test_upkeep_compacts_then_vacuums_the_table():
    assert upkeep_sql("Jobs", curated_database="crestline_curated") == [
        "optimize crestline_curated.erp_jobs rewrite data using bin_pack",
        "vacuum crestline_curated.erp_jobs",
    ]


# --- The window ----------------------------------------------------------------


def test_a_reads_kind_is_the_end_of_its_name():
    assert kind_of("20260806T061351Z-001-sweep") == "sweep"
    assert kind_of("20260806T061351Z-000-incremental") == "incremental"
    assert kind_of("20260806T061351Z-001-shortsweep") == "shortsweep"


def test_the_window_starts_at_the_newest_complete_read_or_else_the_first():
    assert pick_window([]) is None
    increments = ["20260803T0600Z-000-incremental", "20260804T0600Z-000-incremental"]
    window = pick_window(increments)
    assert (window.first, window.last, window.reads, window.starts_complete) == (
        increments[0],
        increments[1],
        2,
        False,
    )
    reads = [
        "20260803T0600Z-000-incremental",
        "20260803T0600Z-001-sweep",
        "20260804T0600Z-000-incremental",
        "20260804T0600Z-001-sweep",
        "20260805T0600Z-000-incremental",
    ]
    window = pick_window(reads)
    assert (window.first, window.last, window.reads, window.starts_complete) == (
        reads[3],
        reads[4],
        2,
        True,
    )


def test_a_short_read_never_starts_a_window_that_deletes():
    short = ["20260803T0600Z-000-incremental", "20260803T0600Z-001-shortsweep"]
    window = pick_window(short)
    assert (window.first, window.reads, window.starts_complete) == (short[0], 2, False)
    # A later good sweep (the retry) supersedes it, and starts the window.
    healed = [*short, "20260803T0700Z-000-incremental", "20260803T0700Z-001-sweep"]
    window = pick_window(healed)
    assert (window.first, window.reads, window.starts_complete) == (healed[3], 1, True)
    assert pick_window(["20260803T0600Z-000-shortfull"]).starts_complete is False


# Jobs as a short history of reads: J2 is deleted (the first sweep lacks it),
# J3 changes twice, J1 is read again unchanged.
HISTORY = [
    ("incremental", [{"JobNum": "J1", "Qty": 1}, {"JobNum": "J2", "Qty": 1}]),
    ("sweep", [{"JobNum": "J1", "Qty": 1}, {"JobNum": "J3", "Qty": 1}]),
    ("incremental", [{"JobNum": "J4", "Qty": 1}]),
    ("incremental", [{"JobNum": "J3", "Qty": 2}, {"JobNum": "J3", "Qty": 3}]),
    (
        "sweep",
        [
            {"JobNum": "J1", "Qty": 1},
            {"JobNum": "J3", "Qty": 3},
            {"JobNum": "J4", "Qty": 2},
        ],
    ),
    ("incremental", []),
]


def write_history(root) -> list[str]:
    """The history as raw files, one read per run; returns the read names."""
    writer, names = RawWriter(root), []
    for day, (kind, rows) in enumerate(HISTORY, start=3):
        run = f"202608{day:02d}T061400Z"
        pages = [
            Page("Jobs", run, kind, 1, "/Jobs", run, run, {"value": rows[:1]}),
            Page("Jobs", run, kind, 2, "/Jobs", run, run, {"value": rows[1:]}),
        ]
        names.append(writer.write(pages, 0, f"2026-08-{day:02d}").stem)
    return names


def merge_window(copy: dict, pages_by_read: dict, window) -> None:
    """What the MERGE does with a window, in Python: the newest version of
    each key wins; if the window starts complete, keys it lacks are deleted."""
    newest = {}
    for read in sorted(pages_by_read):
        if window.first <= read <= window.last:
            for row in pages_by_read[read]:
                newest[key_of("Jobs", row)] = row
    if window.starts_complete:
        for key in copy.keys() - newest.keys():
            del copy[key]
    copy.update(newest)


def test_any_curate_cadence_gives_the_copy_a_full_replay_gives(tmp_path):
    names = write_history(tmp_path)
    pages_by_read = {name: rows for name, (_, rows) in zip(names, HISTORY, strict=True)}
    expected = rebuild(tmp_path)[0].rows["Jobs"]
    assert set(expected) == {("J1",), ("J3",), ("J4",)}

    # Every way of splitting the reads into curate calls: after each call the
    # copy has merged through that call's last read.
    for cuts in itertools.product([False, True], repeat=len(names) - 1):
        copy, merged = {}, 0
        for end, cut in enumerate([*cuts, True], start=1):
            if cut:
                merge_window(copy, pages_by_read, pick_window(names[merged:end]))
                merged = end
        assert copy == expected, cuts


# --- The curate step -------------------------------------------------------------


class FakeAthena:
    """A stand-in for Athena over toy curated tables: the keys each table held
    at every Iceberg snapshot, and the raw reads in the bucket.

    It answers the curate step's statements as Athena would: the check counts
    the window's rows and what the MERGE would delete (keys the table holds
    that a window starting complete lacks), the gone query lists them, the
    MERGE commits a new snapshot, and the diff lists the keys a snapshot held
    that the table doesn't now. Upkeep succeeds and changes nothing.

    `seen` makes the check see a different number of rows (a read Athena
    can't see); `also_deletes` makes a MERGE delete more than it should (a
    bug in it); `fail` makes every statement fail.
    """

    def __init__(self, s3, glue) -> None:
        self.s3, self.glue = s3, glue
        self.statements: list[str] = []
        self.fail = False
        self.seen: int | None = None
        self.also_deletes: set[tuple] = set()
        self.snapshots: dict[int, set[tuple]] = {}  # snapshot -> the table's keys
        self.current: dict[str, int] = {}  # table -> its current snapshot
        self.results: dict[str, list[list[str]]] = {}
        self.ids = itertools.count(4417000000000000001)

    # -- the tables -------------------------------------------------------

    def hold(self, entity: str, keys) -> int:
        """Commit a snapshot of `entity`'s table holding `keys`; returns its ID."""
        return self.commit(TABLES[entity], {tuple(k) for k in keys})

    def keys(self, entity: str) -> set[tuple]:
        current = self.current.get(TABLES[entity])
        return set() if current is None else self.snapshots[current]

    def commit(self, table: str, keys: set[tuple]) -> int:
        """A new snapshot, and the metadata file Glue points to, as Iceberg writes them."""
        snapshot = next(self.ids)
        self.snapshots[snapshot] = keys
        self.current[table] = snapshot
        key = f"{table}/metadata/{snapshot}.metadata.json"
        body = json.dumps({"current-snapshot-id": snapshot}).encode()
        self.s3.put_object(Bucket=CURATED_BUCKET, Key=key, Body=body)
        self.glue.update_table(
            DatabaseName="crestline_curated",
            TableInput={
                "Name": table,
                "Parameters": {
                    "table_type": "ICEBERG",
                    "metadata_location": f"s3://{CURATED_BUCKET}/{key}",
                },
            },
        )
        return snapshot

    # -- reading a statement ----------------------------------------------

    def window(self, sql: str) -> tuple[str, set[tuple], int, bool]:
        """The entity, the keys the window's raw reads hold, their rows, and
        whether the window starts complete."""
        entity = re.search(r"entity = '(\w+)'", sql).group(1)
        first, last = re.search(r"read_name between '(\S+)' and '(\S+)'", sql).groups()
        keys, rows = set(), 0
        listing = self.s3.list_objects_v2(
            Bucket=BUCKET, Prefix=f"{SOURCE}/entity={entity}/"
        )
        for item in listing.get("Contents", []):
            name = item["Key"].rsplit("/", 1)[1].removesuffix(".jsonl")
            if first <= name <= last:
                body = self.s3.get_object(Bucket=BUCKET, Key=item["Key"])["Body"].read()
                for line in body.decode().splitlines():
                    for row in json.loads(line)["body"]["value"]:
                        keys.add(tuple(str(v) for v in key_of(entity, row)))
                        rows += 1
        starts_complete = re.search(r"where (true|false)\b", sql).group(1) == "true"
        return entity, keys, rows, starts_complete

    def gone(self, sql: str) -> tuple[str, set[tuple], int]:
        entity, keys, rows, starts_complete = self.window(sql)
        held = self.keys(entity)
        return entity, (held - keys if starts_complete else set()), rows

    def run(self, sql: str) -> list[list[str]]:
        if sql.startswith("-- Before a curated"):  # the check
            entity, gone, rows = self.gone(sql)
            seen = rows if self.seen is None else self.seen
            return [
                ["rows_seen", "rows_held", "rows_gone"],
                [str(seen), str(len(self.keys(entity))), str(len(gone))],
            ]
        if sql.startswith("-- The keys a refused MERGE"):
            entity, gone, _ = self.gone(sql)
            return [list(ENTITIES[entity][0]), *map(list, sorted(gone))]
        if sql.startswith("-- Curated ERP tables"):  # the MERGE
            entity, gone, _ = self.gone(sql)
            _, window_keys, _, _ = self.window(sql)
            kept = ((self.keys(entity) - gone) | window_keys) - self.also_deletes
            self.commit(TABLES[entity], kept)
            return []
        if sql.startswith("-- After a MERGE that could delete"):  # the diff
            table = re.search(r'"crestline_curated"\."(\w+)"', sql).group(1)
            entity = next(e for e, t in TABLES.items() if t == table)
            before = int(re.search(r"for version as of (\d+)", sql).group(1))
            went = self.snapshots[before] - self.snapshots[self.current[table]]
            return [list(ENTITIES[entity][0]), *map(list, sorted(went))]
        return []  # optimize, vacuum

    # -- the Athena API -----------------------------------------------------

    def start_query_execution(self, QueryString, WorkGroup):
        assert WorkGroup == "crestline"
        self.statements.append(QueryString)
        query_id = f"q{len(self.statements)}"
        if not self.fail:
            self.results[query_id] = self.run(QueryString)
        return {"QueryExecutionId": query_id}

    def get_query_execution(self, QueryExecutionId):
        state = "FAILED" if self.fail else "SUCCEEDED"
        status = {"State": state, "StateChangeReason": "COMMIT_CONFLICT"}
        return {
            "QueryExecution": {
                "Status": status,
                "Statistics": {"DataScannedInBytes": 1000},
            }
        }

    def get_query_results(self, QueryExecutionId, NextToken=None):
        """A page of rows, the header first, like Athena: two rows a page here,
        so a test sees the curate step follow NextToken."""
        rows = self.results[QueryExecutionId]
        start = int(NextToken or 0)
        page = rows[start : start + 2]
        result = {
            "ResultSet": {
                "Rows": [{"Data": [{"VarCharValue": v} for v in row]} for row in page]
            }
        }
        if start + 2 < len(rows):
            result["NextToken"] = str(start + 2)
        return result


class FakeGlue:
    """The Data Catalog, as far as the curate step uses it: a table's parameters."""

    def __init__(self) -> None:
        self.tables: dict[tuple[str, str], dict] = {}

    def update_table(self, DatabaseName, TableInput):
        self.tables[DatabaseName, TableInput["Name"]] = TableInput

    def get_table(self, DatabaseName, Name):
        return {"Table": self.tables[DatabaseName, Name]}


class FakeSns:
    def __init__(self) -> None:
        self.messages: list[dict] = []

    def publish(self, TopicArn, Subject, Message):
        assert len(Subject) <= 100
        self.messages.append(
            {"topic": TopicArn, "subject": Subject, "message": Message}
        )


@pytest.fixture
def world(fake_aws_credentials):
    with mock_aws():
        s3 = boto3.client("s3")
        for bucket in (BUCKET, CURATED_BUCKET):
            s3.create_bucket(
                Bucket=bucket,
                CreateBucketConfiguration={"LocationConstraint": "us-west-2"},
            )
        table = boto3.resource("dynamodb").create_table(
            TableName="state",
            KeySchema=[{"AttributeName": "entity", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "entity", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        # The curated tables as CuratedStack makes them: Iceberg, no snapshot yet.
        glue = FakeGlue()
        for name in TABLES.values():
            key = f"{name}/metadata/00000.metadata.json"
            body = json.dumps({"current-snapshot-id": -1}).encode()
            s3.put_object(Bucket=CURATED_BUCKET, Key=key, Body=body)
            glue.update_table(
                DatabaseName="crestline_curated",
                TableInput={
                    "Name": name,
                    "Parameters": {
                        "table_type": "ICEBERG",
                        "metadata_location": f"s3://{CURATED_BUCKET}/{key}",
                    },
                },
            )
        yield {
            "s3": s3,
            "bucket": BUCKET,
            "table": table,
            "athena": FakeAthena(s3, glue),
            "glue": glue,
            "sns": FakeSns(),
            "workgroup": "crestline",
            "raw_database": "crestline_raw",
            "curated_database": "crestline_curated",
            "curated_bucket": CURATED_BUCKET,
            "alarm_topic": "arn:aws:sns:us-west-2:000000000000:alarm",
        }


def jobs(n: int, first: int = 0) -> list[dict]:
    return [{"JobNum": f"J{i:06d}"} for i in range(first, first + n)]


def put_read(
    world, run: str, ordinal: int, kind: str, rows=0, entity: str = "Jobs"
) -> str:
    """A raw read in the bucket: `rows` jobs, or the rows given; returns its name."""
    value = jobs(rows) if isinstance(rows, int) else rows
    pages = [Page(entity, run, kind, 1, f"/{entity}", run, run, {"value": value})]
    key = raw_key(pages, ordinal, f"{run[0:4]}-{run[4:6]}-{run[6:8]}")
    world["s3"].put_object(Bucket=BUCKET, Key=key, Body=encode(pages))
    return key.rsplit("/", 1)[1].removesuffix(".jsonl")


def curate_on(world, entity: str = "Jobs", **options) -> dict:
    return curate(
        entity, world, today=date(2026, 10, 23), sleep=lambda s: None, **options
    )


def audit_of(world, name: str, entity: str = "Jobs") -> dict:
    key = f"audit/merges/entity={entity}/{name}.json"
    body = world["s3"].get_object(Bucket=CURATED_BUCKET, Key=key)["Body"].read()
    return json.loads(body)


def audits(world) -> list[str]:
    listing = world["s3"].list_objects_v2(Bucket=CURATED_BUCKET, Prefix="audit/")
    return [item["Key"] for item in listing.get("Contents", [])]


def merges(world) -> list[str]:
    return [s for s in world["athena"].statements if "merge into" in s]


def test_curate_merges_from_the_newest_sweep_and_remembers_where_it_stopped(world):
    first = [
        put_read(world, "20260803T061400Z", 0, "incremental"),
        put_read(world, "20260803T061400Z", 1, "sweep"),
        put_read(world, "20260804T061400Z", 0, "incremental"),
    ]
    world["s3"].put_object(
        Bucket=BUCKET, Key=f"{SOURCE}/entity=Jobs/x.partial", Body=b""
    )
    assert new_reads(world["s3"], BUCKET, "Jobs", None) == first

    report = curate("Jobs", world, today=date(2026, 10, 23), sleep=lambda s: None)
    [sql] = merges(world)
    assert f"between '{first[1]}' and '{first[2]}'" in sql and "where true\n" in sql
    assert report["reads"] == 2 and report["merged_through"] == first[2]
    item = world["table"].get_item(Key={"entity": "Jobs"})["Item"]
    assert item["merged_through"] == first[2]

    # Nothing new: no MERGE. The day's upkeep is done, so no upkeep either.
    world["athena"].statements.clear()
    report = curate("Jobs", world, today=date(2026, 10, 23), sleep=lambda s: None)
    assert world["athena"].statements == [] and report["reads"] == 0

    # A new increment: merged on its own, deleting nothing.
    later = put_read(world, "20260805T061400Z", 0, "incremental")
    curate("Jobs", world, today=date(2026, 10, 23), sleep=lambda s: None)
    [sql] = merges(world)
    assert f"between '{later}' and '{later}'" in sql and "where false\n" in sql


def test_upkeep_runs_once_a_day(world):
    put_read(world, "20260803T061400Z", 0, "full")
    for day in (23, 23, 24):
        curate("Jobs", world, today=date(2026, 10, day), sleep=lambda s: None)
    upkeep = [
        s for s in world["athena"].statements if s.startswith(("optimize", "vacuum"))
    ]
    assert len(upkeep) == 4  # optimize and vacuum on the 23rd, again on the 24th
    item = world["table"].get_item(Key={"entity": "Jobs"})["Item"]
    assert item["upkept_on"] == "2026-10-24"


def test_rebuild_merges_from_the_first_read_again(world):
    reads = [put_read(world, f"2026080{d}T061400Z", 0, "incremental") for d in (3, 4)]
    curate("Jobs", world, today=date(2026, 10, 23), sleep=lambda s: None)
    world["athena"].statements.clear()
    curate("Jobs", world, rebuild=True, today=date(2026, 10, 23), sleep=lambda s: None)
    [sql] = merges(world)
    assert f"between '{reads[0]}' and '{reads[1]}'" in sql


def test_a_failed_merge_leaves_merged_through_alone(world):
    put_read(world, "20260803T061400Z", 0, "sweep")
    world["athena"].fail = True
    with pytest.raises(CurateFailed, match="COMMIT_CONFLICT"):
        curate("Jobs", world, today=date(2026, 10, 23), sleep=lambda s: None)
    item = world["table"].get_item(Key={"entity": "Jobs"}).get("Item", {})
    assert "merged_through" not in item


def merged_through(world) -> str | None:
    return (
        world["table"]
        .get_item(Key={"entity": "Jobs"})
        .get("Item", {})
        .get("merged_through")
    )


def test_the_check_runs_before_the_merge_on_the_same_window(world):
    put_read(world, "20260803T061400Z", 0, "sweep", rows=3)
    report = curate("Jobs", world, today=date(2026, 10, 23), sleep=lambda s: None)
    check, merge = world["athena"].statements[:2]
    assert "as rows_seen" in check and check.startswith("-- Before a curated")
    assert merge.startswith("-- Curated ERP tables")
    assert report["rows_read"] == 3 and report["rows_deleted"] == 0


def keys_of_jobs(n: int, first: int = 0) -> list[tuple]:
    return [(job["JobNum"],) for job in jobs(n, first)]


def test_a_read_athena_cant_fully_see_fails_and_merges_nothing(world):
    # Athena sees none of the sweep's rows (filed where it doesn't look): an
    # empty sweep would delete every row, so the run fails instead.
    world["athena"].hold("Jobs", keys_of_jobs(1309))
    read = put_read(world, "20260803T061400Z", 0, "sweep", rows=1309)
    world["athena"].seen = 0
    with pytest.raises(CurateFailed, match="Athena sees 0 rows .* hold 1,309"):
        curate_on(world)
    assert merges(world) == [] and merged_through(world) is None
    # Once Athena sees them all, the same window merges.
    world["athena"].seen = None
    curate_on(world)
    assert len(merges(world)) == 1 and merged_through(world) == read


def test_a_merge_that_would_delete_much_of_a_table_fails_unless_allowed(world):
    athena = world["athena"]
    athena.hold("Jobs", keys_of_jobs(1309))
    read = put_read(world, "20260803T061400Z", 0, "sweep", rows=1200)
    with pytest.raises(CurateFailed, match="delete 109 of its 1,309 rows"):  # 8%
        curate_on(world)
    assert merges(world) == [] and merged_through(world) is None
    # A few cancelled duplicates are within the limit.
    athena.hold("Jobs", keys_of_jobs(1207))
    curate_on(world)
    assert len(merges(world)) == 1 and merged_through(world) == read
    # And someone who has checked can lift it.
    athena.hold("Jobs", keys_of_jobs(1309))
    report = curate_on(world, rebuild=True, max_deleted_share=1.0)
    assert len(merges(world)) == 2 and report["rows_deleted_actual"] == 109


def test_the_extractor_saving_its_state_keeps_the_curate_steps(world):
    world["table"].put_item(
        Item={"entity": "Jobs", "merged_through": "20260803T061400Z-001-sweep"}
    )
    aws.StateTable(world["table"]).save("Jobs", State(runs={"Jobs": 5}))
    item = world["table"].get_item(Key={"entity": "Jobs"})["Item"]
    assert item["runs"] == 5 and item["merged_through"] == "20260803T061400Z-001-sweep"


# --- What each merge deleted -----------------------------------------------------


def statement_kinds(world) -> list[str]:
    names = {
        "-- Before a curated": "check",
        "-- Curated ERP tables": "merge",
        "-- After a MERGE that could delete": "diff",
        "-- The keys a refused MERGE": "gone",
        "optimize": "optimize",
        "vacuum": "vacuum",
    }
    return [
        next(name for start, name in names.items() if s.startswith(start))
        for s in world["athena"].statements
    ]


# The cancelled duplicate J900001, as each job table holds it, and a real job.
DUPLICATE = {
    "Jobs": [{"JobNum": "J900001"}],
    "JobOperations": [
        {"JobNum": "J900001", "OprSeq": f"{seq}.00000000"} for seq in (10, 20, 30)
    ],
    "JobMaterials": [{"JobNum": "J900001", "MtlSeq": "10.00000000"}],
    "JobCosts": [{"JobNum": "J900001"}],
}


@pytest.mark.parametrize("entity", DUPLICATE)
def test_a_sweep_that_drops_a_cancelled_duplicate_records_exactly_its_keys(
    world, entity
):
    # 40 real jobs beside it, so one job is well under the 5% limit.
    real = [
        {**row, "JobNum": f"J1042{n:02d}"}
        for n in range(40)
        for row in DUPLICATE[entity]
    ]
    rows = real + DUPLICATE[entity]
    athena = world["athena"]
    before = athena.hold(entity, {tuple(r.values()) for r in rows})
    read = put_read(world, "20260804T050000Z", 1, "sweep", real, entity)

    report = curate_on(world, entity)

    audit = audit_of(world, read, entity)
    assert audit["keys"] == DUPLICATE[entity]  # exactly the duplicate's rows
    deleted = len(DUPLICATE[entity])
    assert audit["rows_deleted"] == audit["rows_deleted_predicted"] == deleted
    assert audit["snapshot_before"] == before != audit["snapshot_after"]
    assert audit["snapshot_after"] == athena.current[TABLES[entity]]
    assert audit["window"] == {"first": read, "last": read, "reads": 1}
    assert report["rows_deleted_actual"] == deleted
    assert (
        report["audit"]
        == f"s3://{CURATED_BUCKET}/audit/merges/entity={entity}/{read}.json"
    )
    # An ordinary delete is recorded, not emailed.
    assert world["sns"].messages == []


def test_a_mass_delete_someone_allowed_is_recorded_key_by_key(world):
    world["athena"].hold("Jobs", keys_of_jobs(1309))
    read = put_read(world, "20260803T061400Z", 0, "sweep", rows=1200)
    curate_on(world, max_deleted_share=1.0)
    assert audit_of(world, read)["keys"] == jobs(109, first=1200)
    assert world["sns"].messages == []


def test_a_window_that_cannot_delete_needs_no_audit(world):
    world["athena"].hold("Jobs", keys_of_jobs(3))
    put_read(world, "20260803T061400Z", 0, "incremental", rows=2)
    report = curate_on(world)
    assert "diff" not in statement_kinds(world) and audits(world) == []
    assert "audit" not in report


def test_a_table_with_no_snapshot_yet_had_nothing_to_delete(world):
    put_read(world, "20260803T061400Z", 0, "sweep", rows=3)  # its first merge
    curate_on(world)
    assert statement_kinds(world) == ["check", "merge", "optimize", "vacuum"]


def test_the_audit_comes_before_the_upkeep_which_expires_snapshots(world):
    world["athena"].hold("Jobs", keys_of_jobs(30))
    put_read(world, "20260803T061400Z", 0, "sweep", rows=29)
    curate_on(world)
    assert statement_kinds(world) == ["check", "merge", "diff", "optimize", "vacuum"]


def test_a_merge_that_deletes_other_than_predicted_fails_and_says_so(world):
    athena = world["athena"]
    before = athena.hold("Jobs", keys_of_jobs(40))
    read = put_read(world, "20260803T061400Z", 0, "sweep", rows=39)
    athena.also_deletes = {("J000001",)}  # a bug in the MERGE takes one more

    with pytest.raises(CurateFailed, match="deleted 2 rows, but the check predicted 1"):
        curate_on(world)

    audit = audit_of(world, f"{read}-mismatch")
    assert audit["keys"] == [{"JobNum": "J000001"}, {"JobNum": "J000039"}]
    assert (audit["rows_deleted_predicted"], audit["rows_deleted"]) == (1, 2)
    assert audit["snapshot_before"] == before
    assert merged_through(world) is None  # so the next run looks again
    [message] = world["sns"].messages
    assert message["topic"] == world["alarm_topic"]
    assert "deleted 2 rows, but its check said 1" in message["message"]
    assert f"{read}-mismatch.json" in message["message"]
    assert str(before) in message["message"]


def erp_that_never_links_on(all_rows: list[dict]):
    """An ERP transport whose sweep pages come back a clean 200 with no
    nextLink, so the first page looks like the last. It counts honestly."""
    date = {"date": "Mon, 03 Aug 2026 05:00:00 GMT"}

    def transport(method, path, headers, body) -> Response:
        if path.endswith("/auth/token"):
            return Response(200, date, {"access_token": "t"})
        query = unquote(path)
        if "LastModified gt" in query:
            return Response(200, date, {"value": []})
        after = re.search(r"JobNum gt '([^']+)'", query)
        rows = [r for r in all_rows if not after or r["JobNum"] > after.group(1)]
        top = int(re.search(r"\$top=(\d+)", query).group(1))
        page = {"value": rows[:top]}
        if "$count=true" in query:
            page["@odata.count"] = len(all_rows)
        return Response(200, date, page)

    return transport


def test_a_sweep_cut_short_never_deletes_two_layers_deep(world):
    """A sweep that stops at page 1 of 3, once as the extractor
    files it, and once as if a bug had fooled the extractor's check."""
    athena = world["athena"]
    snapshot = athena.hold("Jobs", keys_of_jobs(1200))
    watermark = State(watermarks={"Jobs": "2026-08-03T04:00:00Z"})
    extractor = RobustExtractor(
        ErpClient(erp_that_never_links_on(jobs(1200)), "id", "secret"),
        aws.S3RawWriter(world["s3"], BUCKET),
        watermark,
        rng=random.Random(1),
        sleep=lambda s: None,
    )

    # Layer 1: the extractor's check files it short, so the window can't delete.
    extractor.run(["Jobs"])
    [short] = [r for r in new_reads(world["s3"], BUCKET, "Jobs", None) if "sweep" in r]
    assert short.endswith("-shortsweep")
    curate_on(world)
    assert len(athena.keys("Jobs")) == 1200  # nothing deleted
    assert "where false" in merges(world)[0]
    assert audits(world) == [] and world["sns"].messages == []
    snapshot, merged = athena.current["erp_jobs"], merged_through(world)

    # Layer 2: the same pages filed as a sweep. The 5% check refuses it.
    lines = [json.loads(line) for line in raw_body(world, short).splitlines()]
    planted = "20260803T060000Z-000-sweep"
    pages = [
        Page(**(line | {"read": "sweep", "run": "20260803T060000Z"})) for line in lines
    ]
    key = raw_key(pages, 0, "2026-08-03")
    world["s3"].put_object(Bucket=BUCKET, Key=key, Body=encode(pages))
    athena.statements.clear()

    with pytest.raises(CurateFailed, match="delete 700 of its 1,200 rows"):
        curate_on(world)

    assert "merge" not in statement_kinds(world)  # nothing destructive ran
    assert athena.current["erp_jobs"] == snapshot and merged_through(world) == merged
    refused = audit_of(world, f"{planted}-refused")
    assert refused["keys"] == jobs(700, first=500)
    assert (refused["rows_deleted_proposed"], refused["rows_held"]) == (700, 1200)
    assert refused["snapshot"] == snapshot
    assert refused["complete_read"] == planted
    assert refused["source_reads"] == [{"read": planted, "key": f"s3://{BUCKET}/{key}"}]
    assert refused["to_accept"] == "crestline curate Jobs --allow-mass-delete"
    [message] = world["sns"].messages
    text = message["message"]
    assert "Jobs: merge refused. It would delete 700 of 1,200 rows." in text
    assert str(snapshot) in text and planted in text
    assert (
        f"s3://{CURATED_BUCKET}/audit/merges/entity=Jobs/{planted}-refused.json" in text
    )


def raw_body(world, read: str) -> str:
    key = f"{SOURCE}/entity=Jobs/ingest_date={read[0:4]}-{read[4:6]}-{read[6:8]}/{read}.jsonl"
    return world["s3"].get_object(Bucket=BUCKET, Key=key)["Body"].read().decode()


def test_the_snapshot_comes_from_the_tables_iceberg_metadata(world):
    assert current_snapshot(world, "Jobs") is None  # created, never written
    snapshot = world["athena"].hold("Jobs", keys_of_jobs(1))
    assert current_snapshot(world, "Jobs") == snapshot


def test_the_diff_reads_the_table_as_of_the_snapshot_before_the_merge():
    sql = deleted_sql(
        "JobOperations", curated_database="crestline_curated", snapshot=42
    )
    assert 'from "crestline_curated"."erp_job_operations" for version as of 42' in sql
    assert "except" in sql and "order by jobnum, oprseq" in sql
    assert not re.search(r"\$\{\w+\}", sql)
