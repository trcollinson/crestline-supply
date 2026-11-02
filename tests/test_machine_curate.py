"""The machine curate step (pipelines/machine_curate), against fake AWS clients."""

import io
from datetime import UTC, datetime, timedelta

import pytest

from erp_extract.curate import CurateFailed
from machine_curate import curate, tables

NOW = datetime(2026, 10, 31, 12, 0, tzinfo=UTC)
BUCKET = "raw"
PREFIX = "source=machines-iot/entity=pb01/ingest_date=2026-10-31/"


class FakeS3:
    def __init__(self, files: dict[str, tuple[str, datetime]]) -> None:
        self.files = files  # key -> (body, landed)

    def get_paginator(self, name):
        return self

    def paginate(self, Bucket, Prefix, StartAfter=None):
        keys = sorted(k for k in self.files if k.startswith(Prefix))
        if StartAfter:
            keys = [k for k in keys if k > StartAfter]
        yield {"Contents": [{"Key": k, "LastModified": self.files[k][1]} for k in keys]}

    def get_object(self, Bucket, Key):
        return {"Body": io.BytesIO(self.files[Key][0].encode())}


class FakeAthena:
    """Answers count queries from a script; remembers every statement."""

    def __init__(self, answers: list[int]) -> None:
        self.answers = answers
        self.statements: list[str] = []

    def start_query_execution(self, QueryString, WorkGroup):
        self.statements.append(QueryString)
        return {"QueryExecutionId": str(len(self.statements))}

    def get_query_execution(self, QueryExecutionId):
        return {"QueryExecution": {"Status": {"State": "SUCCEEDED"}, "Statistics": {}}}

    def get_query_results(self, QueryExecutionId):
        value = self.answers.pop(0)
        return {"ResultSet": {"Rows": [{}, {"Data": [{"VarCharValue": str(value)}]}]}}


class FakeState:
    def __init__(self, item=None) -> None:
        self.item = dict(item or {})

    def get_item(self, Key):
        return {"Item": dict(self.item)}

    def update_item(self, Key, UpdateExpression, ExpressionAttributeValues):
        field = UpdateExpression.split()[1]
        self.item[field] = next(iter(ExpressionAttributeValues.values()))


class FakeSns:
    def __init__(self) -> None:
        self.sent = []

    def publish(self, **message):
        self.sent.append(message)


def setup(files, answers, item=None):
    return {
        "bucket": BUCKET,
        "workgroup": "crestline",
        "raw_database": "crestline_raw",
        "alarm_topic": "arn:topic",
        "s3": FakeS3(files),
        "athena": FakeAthena(answers),
        "table": FakeState(item or {"upkept_on": NOW.date().isoformat()}),
        "sns": FakeSns(),
    }


def landed(minutes_ago: int) -> datetime:
    return NOW - timedelta(minutes=minutes_ago)


FILES = {
    PREFIX + "s-1-2026-10-31-11-00-00-a.jsonl": ('{"x":1}\n{"x":2}\n', landed(60)),
    PREFIX + "s-1-2026-10-31-11-30-00-b.jsonl": ('{"x":3}\n', landed(30)),
    # Still settling: left for the next run, and so is anything after it.
    PREFIX + "s-1-2026-10-31-11-59-00-c.jsonl": ('{"x":4}\n', landed(1)),
}


def test_settled_files_after_the_last_merged_are_the_window():
    s3 = FakeS3(FILES)
    keys = curate.new_files(s3, BUCKET, "pb01", None, NOW)
    assert [k.rsplit("-", 1)[1] for k in keys] == ["a.jsonl", "b.jsonl"]
    assert curate.new_files(s3, BUCKET, "pb01", keys[0], NOW) == keys[1:]


def test_a_window_is_checked_merged_insert_only_and_saved():
    s = setup(FILES, answers=[3, 100, 103])  # lines seen, rows before, rows after
    report = curate.curate("pb01", s, now=NOW)
    assert report == {
        "device": "pb01",
        "files": 2,
        "lines": 3,
        "inserted": 3,
        "rows": 103,
    }
    count, before, merge, after = s["athena"].statements
    assert before == after == "select count(*) from crestline_curated.machine_pb01"
    assert (
        "count(*)" in count
        and "ingest_date between '2026-10-31' and '2026-10-31'" in count
    )
    assert merge.startswith("merge into crestline_curated.machine_pb01")
    assert "when not matched then insert" in merge
    assert "delete" not in merge and "update" not in merge
    assert s["table"].item["merged_through"].endswith("b.jsonl")


def test_athena_seeing_other_than_the_files_hold_merges_nothing():
    s = setup(FILES, answers=[2])
    with pytest.raises(CurateFailed, match="Athena sees 2 lines"):
        curate.curate("pb01", s, now=NOW)
    assert len(s["athena"].statements) == 1
    assert "merged_through" not in s["table"].item


def test_a_table_that_shrinks_is_an_alarm_and_nothing_is_saved():
    s = setup(FILES, answers=[3, 100, 99])
    with pytest.raises(CurateFailed, match="insert-only"):
        curate.curate("pb01", s, now=NOW)
    [message] = s["sns"].sent
    assert message["Subject"].startswith("Crestline machines: pb01")
    assert "merged_through" not in s["table"].item


def test_upkeep_runs_once_a_day():
    s = setup({}, answers=[], item={"upkept_on": "2026-10-30"})
    report = curate.curate("pb01", s, now=NOW)
    assert report["upkept_on"] == "2026-10-31"
    assert s["athena"].statements == [
        "optimize crestline_curated.machine_pb01 rewrite data using bin_pack",
        "vacuum crestline_curated.machine_pb01",
    ]


def test_every_device_is_tried_before_the_run_fails():
    tried = []

    def fake(device, setup):
        tried.append(device)
        if device == "pb02":
            raise CurateFailed("boom")
        return {"device": device}

    original = curate.curate
    curate.curate = fake
    try:
        with pytest.raises(CurateFailed, match="pb02: boom"):
            curate.curate_all({})
    finally:
        curate.curate = original
    assert tried == list(tables.TABLES)


def test_every_table_is_keyed_by_its_line_and_names_its_clocks():
    for device, columns in tables.COLUMNS.items():
        names = [name for name, _, _ in columns]
        assert names[:5] == [
            "message_id",
            "topic",
            "published_at_utc",
            "received_at_utc",
            "replay",
        ]
        times = [name for name, kind, _ in columns if kind == "timestamp"]
        assert all(name.endswith("_utc") for name in times), device


def test_column_types_are_iceberg_s_own_names():
    # Glue's Iceberg tables take Iceberg's primitive names: `long`, not `bigint`.
    iceberg = {"string", "int", "long", "double", "boolean", "timestamp", "date"}
    for device, columns in tables.COLUMNS.items():
        assert {kind for _, kind, _ in columns} <= iceberg, device
