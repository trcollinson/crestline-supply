"""The curated layer's schema, its MERGE, and the curate step (M2 step 8).

The MERGE itself runs only in Athena (eval/curated.py checks the result
against the ERP). Here: the curated columns match the store, the SQL is
filled in completely, the window rule gives the same copy as replaying
every read (`merge.rebuild`) however often the curate step runs, and the
step against moto's S3 and DynamoDB with a stand-in Athena.
"""

import itertools
from datetime import date

import boto3
import pytest
from moto import mock_aws

from crestline.sim.systems import erp
from crestline_erp_api.store import Store
from erp_extract import aws
from erp_extract.curate import CurateFailed, curate, kind_of, new_reads, pick_window
from erp_extract.curated import (
    COLUMNS,
    TABLES,
    check_sql,
    column_name,
    merge_sql,
    upkeep_sql,
)
from erp_extract.entities import ENTITIES, key_of
from erp_extract.merge import rebuild
from erp_extract.raw import SOURCE, Page, RawWriter, encode, raw_key
from erp_extract.robust import State

ICEBERG_TYPE = {
    "TEXT": "string",
    "REAL": "double",
    "INTEGER": "int",
    "BOOLEAN": "boolean",
    "DATE": "date",
    "TIMESTAMP": "timestamp",
}
BUCKET = "crestline-raw-test"


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
    """Records each statement and answers like Athena: it succeeds, or fails.

    The check before a MERGE answers with `counts` (rows seen, held, gone),
    or, if that is None, sees exactly the rows the test's raw reads hold.
    """

    def __init__(self) -> None:
        self.statements: list[str] = []
        self.fail = False
        self.counts: tuple[int, int, int] | None = None
        self.rows_written = 0  # what put_read wrote, for the default answer

    def get_query_results(self, QueryExecutionId):
        seen, held, gone = self.counts or (self.rows_written, 0, 0)
        cells = [{"VarCharValue": str(n)} for n in (seen, held, gone)]
        header = [
            {"VarCharValue": name} for name in ("rows_seen", "rows_held", "rows_gone")
        ]
        return {"ResultSet": {"Rows": [{"Data": header}, {"Data": cells}]}}

    def start_query_execution(self, QueryString, WorkGroup):
        assert WorkGroup == "crestline"
        self.statements.append(QueryString)
        return {"QueryExecutionId": f"q{len(self.statements)}"}

    def get_query_execution(self, QueryExecutionId):
        state = "FAILED" if self.fail else "SUCCEEDED"
        status = {"State": state, "StateChangeReason": "COMMIT_CONFLICT"}
        return {
            "QueryExecution": {
                "Status": status,
                "Statistics": {"DataScannedInBytes": 1000},
            }
        }


@pytest.fixture
def world(fake_aws_credentials):
    with mock_aws():
        s3 = boto3.client("s3")
        s3.create_bucket(
            Bucket=BUCKET, CreateBucketConfiguration={"LocationConstraint": "us-west-2"}
        )
        table = boto3.resource("dynamodb").create_table(
            TableName="state",
            KeySchema=[{"AttributeName": "entity", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "entity", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        yield {
            "s3": s3,
            "bucket": BUCKET,
            "table": table,
            "athena": FakeAthena(),
            "workgroup": "crestline",
            "raw_database": "crestline_raw",
            "curated_database": "crestline_curated",
        }


def put_read(world, run: str, ordinal: int, kind: str, rows: int = 0) -> str:
    value = [{"JobNum": f"J{n:06d}"} for n in range(rows)]
    pages = [Page("Jobs", run, kind, 1, "/Jobs", run, run, {"value": value})]
    key = raw_key(pages, ordinal, f"{run[0:4]}-{run[4:6]}-{run[6:8]}")
    world["s3"].put_object(Bucket=BUCKET, Key=key, Body=encode(pages))
    world["athena"].rows_written += rows
    return key.rsplit("/", 1)[1].removesuffix(".jsonl")


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


def test_a_read_athena_cant_fully_see_fails_and_merges_nothing(world):
    # Athena sees none of the sweep's rows (filed where it doesn't look): an
    # empty sweep would delete every row, so the run fails instead.
    read = put_read(world, "20260803T061400Z", 0, "sweep", rows=1309)
    world["athena"].counts = (0, 1309, 1309)
    with pytest.raises(CurateFailed, match="Athena sees 0 rows .* hold 1,309"):
        curate("Jobs", world, today=date(2026, 10, 23), sleep=lambda s: None)
    assert merges(world) == [] and merged_through(world) is None
    # Once Athena sees them all, the same window merges.
    world["athena"].counts = (1309, 1309, 0)
    curate("Jobs", world, today=date(2026, 10, 23), sleep=lambda s: None)
    assert len(merges(world)) == 1 and merged_through(world) == read


def test_a_merge_that_would_delete_much_of_a_table_fails_unless_allowed(world):
    read = put_read(world, "20260803T061400Z", 0, "sweep", rows=1200)
    world["athena"].counts = (1200, 1309, 109)  # 8% of the table
    with pytest.raises(CurateFailed, match="delete 109 of its 1,309 rows"):
        curate("Jobs", world, today=date(2026, 10, 23), sleep=lambda s: None)
    assert merges(world) == [] and merged_through(world) is None
    # A few cancelled duplicates are within the limit.
    world["athena"].counts = (1200, 1309, 7)
    curate("Jobs", world, today=date(2026, 10, 23), sleep=lambda s: None)
    assert len(merges(world)) == 1 and merged_through(world) == read
    # And someone who has checked can lift it.
    world["athena"].counts = (1200, 1309, 109)
    curate(
        "Jobs",
        world,
        rebuild=True,
        max_deleted_share=1.0,
        today=date(2026, 10, 23),
        sleep=lambda s: None,
    )
    assert len(merges(world)) == 2


def test_the_extractor_saving_its_state_keeps_the_curate_steps(world):
    world["table"].put_item(
        Item={"entity": "Jobs", "merged_through": "20260803T061400Z-001-sweep"}
    )
    aws.StateTable(world["table"]).save("Jobs", State(runs={"Jobs": 5}))
    item = world["table"].get_item(Key={"entity": "Jobs"})["Item"]
    assert item["runs"] == 5 and item["merged_through"] == "20260803T061400Z-001-sweep"
