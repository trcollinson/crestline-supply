"""Upload to the raw bucket, against moto's in-memory S3 (no AWS calls)."""

import contextlib
import io
from datetime import UTC, date, datetime
from pathlib import Path

import boto3
import pytest
from botocore.stub import Stubber
from moto import mock_aws

from crestline import upload
from crestline.cli import main
from crestline.upload import planned_uploads, raw_bucket_name, upload_backfill

REPO_ROOT = Path(__file__).resolve().parent.parent
BUCKET = "crestline-raw-test"
INGEST = date(2026, 10, 1)

pytestmark = pytest.mark.usefixtures("fake_aws_credentials")


@pytest.fixture
def s3():
    """A versioned bucket, like FoundationStack's raw bucket."""
    with mock_aws():
        client = boto3.client("s3")
        client.create_bucket(
            Bucket=BUCKET,
            CreateBucketConfiguration={"LocationConstraint": "us-west-2"},
        )
        client.put_bucket_versioning(
            Bucket=BUCKET, VersioningConfiguration={"Status": "Enabled"}
        )
        yield client


@pytest.fixture
def out(tmp_path) -> Path:
    """A small backfill output folder, answer key included."""
    files = {
        "erp/export/Jobs.jsonl": '{"JobNum": "J100001"}\n',
        "erp/export/JobCosts.jsonl": '{"JobNum": "J100001"}\n',
        "erp/erp.db": "sqlite",
        "mes/outbound/labor_20260929.csv": "record_id,record_type\r\n1,A\r\n",
        "mes/outbound/labor_20260930.csv": "record_id,record_type\r\n",
        "machines/pb01.jsonl": '{"topic": "crestline/p10/gw01/pb01/data"}\n',
        "machines/pb02.jsonl": '{"topic": "crestline/p10/gw02/pb02/cnt"}\n',
        "truth/truth_costs.parquet": "answer key",
    }
    out = tmp_path / "out"
    for name, text in files.items():
        (out / name).parent.mkdir(parents=True, exist_ok=True)
        (out / name).write_text(text)
    return out


def bucket_keys(s3) -> list[str]:
    return sorted(item["Key"] for item in s3.list_objects_v2(Bucket=BUCKET)["Contents"])


def version_count(s3) -> int:
    return len(s3.list_object_versions(Bucket=BUCKET)["Versions"])


def test_files_land_under_source_entity_and_ingest_date(out, s3):
    upload_backfill(out, INGEST, BUCKET, s3)
    assert bucket_keys(s3) == [
        "source=erp/entity=JobCosts/ingest_date=2026-10-01/JobCosts.jsonl",
        "source=erp/entity=Jobs/ingest_date=2026-10-01/Jobs.jsonl",
        "source=machines/entity=pb01/ingest_date=2026-10-01/pb01.jsonl",
        "source=machines/entity=pb02/ingest_date=2026-10-01/pb02.jsonl",
        "source=mes/entity=labor/ingest_date=2026-10-01/labor_20260929.csv",
        "source=mes/entity=labor/ingest_date=2026-10-01/labor_20260930.csv",
    ]


def test_answer_key_and_erp_database_stay_local(out, s3):
    upload_backfill(out, INGEST, BUCKET, s3)
    keys = bucket_keys(s3)
    assert not any("truth" in key for key in keys)
    assert not any(key.endswith(".db") for key in keys)


def test_files_go_up_byte_for_byte(out, s3):
    upload_backfill(out, INGEST, BUCKET, s3)
    key = "source=mes/entity=labor/ingest_date=2026-10-01/labor_20260929.csv"
    stored = s3.get_object(Bucket=BUCKET, Key=key)
    assert (
        stored["Body"].read() == (out / "mes/outbound/labor_20260929.csv").read_bytes()
    )
    assert stored["ContentType"] == "text/csv"


def test_second_upload_changes_nothing(out, s3):
    first = upload_backfill(out, INGEST, BUCKET, s3)
    second = upload_backfill(out, INGEST, BUCKET, s3)
    assert len(first.uploaded) == 6
    assert (second.uploaded, second.deleted) == ([], [])
    assert second.unchanged == first.uploaded
    assert version_count(s3) == 6  # identical files add no new versions


def test_changed_file_is_replaced(out, s3):
    upload_backfill(out, INGEST, BUCKET, s3)
    (out / "erp/export/Jobs.jsonl").write_text('{"JobNum": "J100002"}\n')
    second = upload_backfill(out, INGEST, BUCKET, s3)
    key = "source=erp/entity=Jobs/ingest_date=2026-10-01/Jobs.jsonl"
    assert second.uploaded == [key]
    assert s3.get_object(Bucket=BUCKET, Key=key)["Body"].read() == (
        b'{"JobNum": "J100002"}\n'
    )
    assert version_count(s3) == 7  # the replaced copy is kept as a version


def test_files_the_backfill_no_longer_writes_are_deleted_from_that_date_only(out, s3):
    upload_backfill(out, INGEST, BUCKET, s3)
    other_date = "source=mes/entity=labor/ingest_date=2026-09-30/labor_20260929.csv"
    s3.put_object(Bucket=BUCKET, Key=other_date, Body=b"an earlier extract")
    (out / "mes/outbound/labor_20260929.csv").unlink()  # e.g. a shorter re-run

    second = upload_backfill(out, INGEST, BUCKET, s3)
    stale = "source=mes/entity=labor/ingest_date=2026-10-01/labor_20260929.csv"
    assert second.deleted == [stale]
    assert stale not in bucket_keys(s3)
    assert other_date in bucket_keys(s3)


def test_raw_bucket_name_comes_from_the_stack_outputs():
    cloudformation = boto3.client("cloudformation")
    stack = {
        "StackName": "CrestlineFoundation",
        "CreationTime": datetime(2026, 9, 30, tzinfo=UTC),
        "StackStatus": "CREATE_COMPLETE",
        "Outputs": [
            {"OutputKey": "GlueDatabaseName", "OutputValue": "crestline_raw"},
            {"OutputKey": "RawBucketName", "OutputValue": "raw-bucket-from-stack"},
        ],
    }
    with Stubber(cloudformation) as stub:
        stub.add_response(
            "describe_stacks",
            {"Stacks": [stack]},
            {"StackName": "CrestlineFoundation"},
        )
        assert raw_bucket_name(cloudformation) == "raw-bucket-from-stack"


def test_backfill_upload_twice_leaves_one_copy_of_each_file(tmp_path, monkeypatch):
    out = tmp_path / "out"
    argv = [
        "sim", "backfill", "--days", "3", "--end", "2026-08-03",
        "--out", str(out), "--upload",
    ]  # fmt: skip
    monkeypatch.chdir(REPO_ROOT)
    monkeypatch.setattr(upload, "raw_bucket_name", lambda: BUCKET)
    with mock_aws():
        s3 = boto3.client("s3")
        s3.create_bucket(
            Bucket=BUCKET,
            CreateBucketConfiguration={"LocationConstraint": "us-west-2"},
        )
        printed = []
        for _ in range(2):
            with contextlib.redirect_stdout(io.StringIO()) as stdout:
                main(argv)
            printed.append(stdout.getvalue().splitlines()[-1])
        planned = planned_uploads(out, date(2026, 8, 3))
        assert bucket_keys(s3) == sorted(planned)

    files = len(planned)  # 7 ERP entities, 3 MES days, 2 brakes
    assert files == 12
    where = f"Uploaded to s3://{BUCKET}/ (ingest_date=2026-08-03):"
    assert printed == [
        f"{where} {files} new or changed, 0 unchanged, 0 deleted",
        f"{where} 0 new or changed, {files} unchanged, 0 deleted",
    ]
