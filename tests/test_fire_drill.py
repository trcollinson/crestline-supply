"""The fire drill plants a cut-short sweep where the merge will see it, and removes it."""

import boto3
import pytest
from moto import mock_aws

from erp_extract.curate import new_reads, pick_window
from erp_extract.raw import Page, encode, raw_key
from fire_drill import plant, remove

BUCKET = "crestline-raw-test"


@pytest.fixture
def s3(fake_aws_credentials):
    with mock_aws():
        client = boto3.client("s3")
        client.create_bucket(
            Bucket=BUCKET, CreateBucketConfiguration={"LocationConstraint": "us-west-2"}
        )
        yield client


def put(s3, run: str, ordinal: int, kind: str, pages: int) -> None:
    read = [
        Page("Jobs", run, kind, n, "/Jobs", run, run, {"value": [{"JobNum": f"J{n}"}]})
        for n in range(1, pages + 1)
    ]
    s3.put_object(
        Bucket=BUCKET, Key=raw_key(read, ordinal, "2026-08-04"), Body=encode(read)
    )


def test_the_planted_read_is_page_one_of_the_newest_sweep_filed_as_the_newest_read(s3):
    put(s3, "20260804T050000Z", 6, "incremental", 1)
    put(s3, "20260804T050000Z", 7, "sweep", 3)
    put(s3, "20260804T060000Z", 6, "incremental", 1)

    planted = plant(s3, BUCKET)

    assert planted == "20260804T060000Z-900-sweep"
    reads = new_reads(s3, BUCKET, "Jobs", None)
    assert reads[-1] == planted
    # The merge's window starts at it, so it would delete what page 1 lacks.
    window = pick_window(reads)
    assert window.first == planted and window.starts_complete
    key = f"source=erp-api/entity=Jobs/ingest_date=2026-08-04/{planted}.jsonl"
    lines = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read().decode().splitlines()
    assert len(lines) == 1 and '"read":"sweep"' in lines[0] and '"J1"' in lines[0]

    assert remove(s3, BUCKET) == [planted]
    assert planted not in new_reads(s3, BUCKET, "Jobs", None)


def test_a_one_page_sweep_cannot_be_cut_short(s3):
    put(s3, "20260804T050000Z", 7, "sweep", 1)
    with pytest.raises(SystemExit, match="one page"):
        plant(s3, BUCKET)
