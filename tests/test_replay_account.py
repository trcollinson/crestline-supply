"""Accounting for a replay (crestline.replay_account): sent = raw - redelivered + errors."""

import json

import boto3
import pytest
from moto import mock_aws

from crestline import replay_account

BUCKET = "raw"


def manifest(tmp_path, sent: dict[str, int], acknowledged=None, refused=None):
    gateways = {
        "EDGE-LASER": ["lsr01"],
        "GW01": ["pb01"],
        "GW02": ["pb02", "pem01"],
        "GW03": ["pcline"],
    }
    acked = acknowledged or {
        g: sum(sent.get(d, 0) for d in ds) for g, ds in gateways.items()
    }
    path = tmp_path / "replays" / "R1.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "replay": "R1",
                "started_at": "2026-10-31T10:00:00+00:00",
                "finished_at": "2026-10-31T10:50:00+00:00",
                "devices": {d: {"sent": n} for d, n in sent.items()},
                "acknowledged": acked,
                "refused": refused or dict.fromkeys(gateways, 0),
            }
        )
    )
    return path


def raw(s3, device: str, day: str, replays: list[str], name: str = "f1.jsonl") -> None:
    """One line per message, each published at its own time."""
    lines = [line(r, f"{name}-{n}", n) for n, r in enumerate(replays)]
    put_lines(s3, device, day, name, lines)


def line(replay: str, published_at: str, received_at: int) -> dict:
    return {
        "x": 1,
        "published_at": published_at,
        "replay": replay,
        "received_at_utc": received_at,
    }


def put_lines(s3, device: str, day: str, name: str, lines: list[dict]) -> None:
    body = "".join(json.dumps(record) + "\n" for record in lines)
    key = f"source=machines-iot/entity={device}/ingest_date={day}/{name}"
    s3.put_object(Bucket=BUCKET, Key=key, Body=body.encode())


@pytest.fixture
def cloudwatch(s3):
    return boto3.client("cloudwatch", region_name="us-west-2")


@pytest.fixture
def s3():
    with mock_aws():
        client = boto3.client("s3", region_name="us-west-2")
        client.create_bucket(
            Bucket=BUCKET, CreateBucketConfiguration={"LocationConstraint": "us-west-2"}
        )
        yield client


def test_a_replay_that_all_landed_is_accounted_for(s3, cloudwatch, tmp_path):
    # An older replay's line doesn't count; a file flushed after midnight does.
    raw(s3, "pb01", "2026-10-31", ["R1"] * 3 + ["R0"])
    raw(s3, "pb01", "2026-11-01", ["R1"], "f2.jsonl")
    s3.put_object(
        Bucket=BUCKET,
        Key="source=machines-iot/errors/entity=pb01/ingest_date=2026-10-31/abc.json",
        Body=b"{}",
    )
    raw(s3, "pb02", "2026-10-31", ["R1"] * 2)

    result = replay_account.account(
        manifest(tmp_path, {"pb01": 5, "pb02": 2}), BUCKET, s3, cloudwatch
    )

    counts = {d.device: (d.sent, d.raw, d.errors) for d in result.devices}
    assert all(d.redelivered == 0 for d in result.devices)
    assert counts["pb01"] == (5, 4, 1)
    assert counts["pb02"] == (2, 2, 0)
    assert counts["lsr01"] == (0, 0, 0)
    assert result.ok
    assert "Accounted for." in result.report()


def test_a_message_missing_from_raw_fails_the_account(s3, cloudwatch, tmp_path):
    raw(s3, "pb01", "2026-10-31", ["R1"] * 2)
    result = replay_account.account(
        manifest(tmp_path, {"pb01": 3}), BUCKET, s3, cloudwatch
    )
    assert not result.ok
    assert "NOT accounted for." in result.report()


def test_a_refused_publish_fails_the_account(s3, cloudwatch, tmp_path):
    raw(s3, "pb01", "2026-10-31", ["R1"])
    path = manifest(
        tmp_path,
        {"pb01": 1},
        acknowledged={"EDGE-LASER": 0, "GW01": 0, "GW02": 0, "GW03": 0},
        refused={"EDGE-LASER": 0, "GW01": 1, "GW02": 0, "GW03": 0},
    )
    assert not replay_account.account(path, BUCKET, s3, cloudwatch).ok


def test_firehose_errors_count_by_line(s3, cloudwatch, tmp_path):
    raw(s3, "pcline", "2026-10-31", ["R1"])
    s3.put_object(
        Bucket=BUCKET,
        Key="source=machines-iot/firehose-errors/entity=pcline/ingest_date=2026-10-31/processing-failed/x",
        Body=b'{"a":1}\n{"a":2}\n',
    )
    result = replay_account.account(
        manifest(tmp_path, {"pcline": 3}), BUCKET, s3, cloudwatch
    )
    assert result.ok


def test_the_latest_manifest_is_found(tmp_path):
    manifest(tmp_path, {})
    assert replay_account.latest_manifest(tmp_path).name == "R1.json"


def test_a_message_the_rules_engine_could_not_parse_is_counted(
    s3, cloudwatch, tmp_path
):
    from datetime import datetime

    raw(s3, "pb01", "2026-10-31", ["R1"] * 271)
    cloudwatch.put_metric_data(
        Namespace="AWS/IoT",
        MetricData=[
            {
                "MetricName": "ParseError",
                "Dimensions": [{"Name": "RuleName", "Value": "crestline_pb01"}],
                "Timestamp": datetime.fromisoformat("2026-10-31T10:20:00+00:00"),
                "Value": 1,
            }
        ],
    )
    result = replay_account.account(
        manifest(tmp_path, {"pb01": 272}), BUCKET, s3, cloudwatch
    )
    [pb01] = [d for d in result.devices if d.device == "pb01"]
    assert (pb01.raw, pb01.errors, pb01.parse_errors) == (271, 0, 1)
    assert result.ok


def test_a_message_delivered_twice_is_a_redelivery_not_a_message(
    s3, cloudwatch, tmp_path
):
    # QoS 1 after a dropped connection: the second copy lands in a later
    # Firehose file, received 1.4 s after the first.
    put_lines(
        s3, "pem01", "2026-10-31", "f1.jsonl", [line("R1", "a", 1), line("R1", "b", 2)]
    )
    put_lines(s3, "pem01", "2026-10-31", "f2.jsonl", [line("R1", "a", 1401)])
    path = manifest(
        tmp_path,
        {"pem01": 2},
        # A resent message can be acknowledged twice.
        acknowledged={"EDGE-LASER": 0, "GW01": 0, "GW02": 3, "GW03": 0},
    )
    result = replay_account.account(path, BUCKET, s3, cloudwatch)
    [pem01] = [d for d in result.devices if d.device == "pem01"]
    assert (pem01.sent, pem01.raw, pem01.redelivered) == (2, 3, 1)
    assert result.ok


def test_a_redelivery_cannot_stand_in_for_a_lost_message(s3, cloudwatch, tmp_path):
    # Three sent, "a" twice and "c" never: three lines, but one is missing.
    put_lines(
        s3,
        "pem01",
        "2026-10-31",
        "f1.jsonl",
        [line("R1", "a", 1), line("R1", "b", 2), line("R1", "a", 3)],
    )
    result = replay_account.account(
        manifest(tmp_path, {"pem01": 3}), BUCKET, s3, cloudwatch
    )
    assert not result.ok


def test_fewer_acknowledgements_than_messages_fails_the_account(
    s3, cloudwatch, tmp_path
):
    raw(s3, "pb01", "2026-10-31", ["R1"] * 2)
    path = manifest(
        tmp_path,
        {"pb01": 2},
        acknowledged={"EDGE-LASER": 0, "GW01": 1, "GW02": 0, "GW03": 0},
    )
    assert not replay_account.account(path, BUCKET, s3, cloudwatch).ok
