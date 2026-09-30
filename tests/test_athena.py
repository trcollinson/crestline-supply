"""The Athena runner and DDL, without AWS: botocore's Stubber plays Athena."""

import contextlib
import io
import re
from datetime import date
from pathlib import Path

import boto3
import pytest
from botocore.stub import ANY, Stubber

from crestline import athena as athena_module
from crestline.athena import Athena, AthenaQueryError, render, split_statements
from crestline.cli import main
from crestline.upload import planned_uploads

REPO_ROOT = Path(__file__).resolve().parent.parent
QUERIES = REPO_ROOT / "queries"

pytestmark = pytest.mark.usefixtures("fake_aws_credentials")


def statements(path: str) -> list[str]:
    return split_statements((QUERIES / path).read_text(encoding="utf-8"))


def test_margin_gap_splits_into_its_views_and_the_answer():
    parts = statements("margin_gap.sql")
    views = [re.match(r"create or replace view (\w+)", part)[1] for part in parts[:-1]]
    assert views == [
        "plant_work_windows",
        "work_windows",
        "scans",
        "work_segments",
        "brake_operations",
        "job_margins",
    ]
    assert parts[-1].startswith("select")
    assert not any("--" in part for part in parts)


def test_setup_files_hold_ten_tables_and_eight_views():
    tables = statements("athena/tables.sql")
    for verb in ("drop table if exists", "create external table", "msck repair table"):
        assert sum(part.startswith(verb) for part in tables) == 10, verb
    assert len(tables) == 30
    assert all(
        part.startswith("create or replace view")
        for part in statements("athena/sources.sql")
    )
    assert len(statements("athena/sources.sql")) == 8


def test_render_fills_in_the_bucket_and_refuses_a_missing_value():
    sql = render((QUERIES / "athena/tables.sql").read_text(), raw_bucket="my-raw")
    assert "s3://my-raw/source=erp/entity=Jobs/" in sql
    assert "${" not in sql
    with pytest.raises(KeyError, match="raw_bucket"):
        render("location 's3://${raw_bucket}/'")


def test_every_uploaded_entity_has_a_raw_table(tmp_path, monkeypatch):
    out = tmp_path / "out"
    monkeypatch.chdir(REPO_ROOT)
    with contextlib.redirect_stdout(io.StringIO()):
        main(
            ["sim", "backfill", "--days", "3", "--end", "2026-08-03", "--out", str(out)]
        )
    uploaded = {
        key.split("ingest_date=")[0] for key in planned_uploads(out, date(2026, 8, 3))
    }
    tables_sql = (QUERIES / "athena/tables.sql").read_text()
    locations = set(re.findall(r"location 's3://\$\{raw_bucket\}/([^']+)'", tables_sql))
    assert locations == uploaded


def stub_statement(stub: Stubber, query_id: str, scanned: int, polls: int = 1):
    stub.add_response(
        "start_query_execution",
        {"QueryExecutionId": query_id},
        {
            "QueryString": ANY,
            "QueryExecutionContext": {"Database": "crestline_raw"},
            "WorkGroup": "crestline",
        },
    )
    for _ in range(polls - 1):
        stub.add_response(
            "get_query_execution",
            {"QueryExecution": {"Status": {"State": "RUNNING"}}},
            {"QueryExecutionId": query_id},
        )
    stub.add_response(
        "get_query_execution",
        {
            "QueryExecution": {
                "Status": {"State": "SUCCEEDED"},
                "Statistics": {"DataScannedInBytes": scanned},
            }
        },
        {"QueryExecutionId": query_id},
    )


@pytest.fixture
def athena(monkeypatch) -> Athena:
    monkeypatch.setattr(athena_module, "POLL_SECONDS", 0)
    return Athena("crestline_raw", "crestline", "raw-bucket", boto3.client("athena"))


def test_run_query_runs_each_statement_and_types_the_answer(athena, tmp_path):
    (tmp_path / "two_step.sql").write_text(
        "-- a view, then the answer\n"
        "create or replace view v as select 1 as x;\n"
        "select x, 'a' as label from v;\n"
    )
    with Stubber(athena.client) as stub:
        stub_statement(stub, "q1", scanned=0)
        stub_statement(stub, "q2", scanned=10_485_760, polls=3)
        stub.add_response(
            "get_query_results",
            {
                "ResultSet": {
                    "Rows": [
                        {"Data": [{"VarCharValue": "x"}, {"VarCharValue": "label"}]},
                        {"Data": [{"VarCharValue": "1"}, {"VarCharValue": "a"}]},
                        {"Data": [{}, {"VarCharValue": "b"}]},
                    ],
                    "ResultSetMetadata": {
                        "ColumnInfo": [
                            {"Name": "x", "Type": "bigint"},
                            {"Name": "label", "Type": "varchar"},
                        ]
                    },
                }
            },
            {"QueryExecutionId": "q2"},
        )
        result = athena.run_query("two-step", tmp_path)
        stub.assert_no_pending_responses()

    assert result.frame.to_dicts() == [
        {"x": 1, "label": "a"},
        {"x": None, "label": "b"},
    ]
    assert result.bytes_scanned == 10_485_760


def test_a_failed_statement_raises_athenas_reason(athena):
    with Stubber(athena.client) as stub:
        stub.add_response("start_query_execution", {"QueryExecutionId": "q1"}, None)
        stub.add_response(
            "get_query_execution",
            {
                "QueryExecution": {
                    "Status": {
                        "State": "FAILED",
                        "StateChangeReason": "COLUMN_NOT_FOUND: line 1:8",
                    }
                }
            },
            {"QueryExecutionId": "q1"},
        )
        with pytest.raises(AthenaQueryError, match="COLUMN_NOT_FOUND"):
            athena.execute("select nope")
