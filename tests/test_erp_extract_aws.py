"""The extractor's Lambda side, against moto's S3 and DynamoDB and the API in-process."""

import itertools
import time
from datetime import UTC, datetime

import boto3
import pytest
from fastapi.testclient import TestClient
from moto import mock_aws

from crestline.mess import MessConfig
from crestline.sim.systems import erp
from crestline_erp_api.app import create_app
from crestline_erp_api.settings import Settings
from erp_extract import aws
from erp_extract.client import Response
from erp_extract.merge import rebuild
from erp_extract.raw import SOURCE, Page, RawWriter
from erp_extract.robust import State

pytestmark = pytest.mark.usefixtures("fake_aws_credentials")
BUCKET = "crestline-raw-test"


@pytest.fixture
def aws_world():
    """A raw bucket and a state table, as the stacks make them."""
    with mock_aws():
        s3 = boto3.client("s3")
        s3.create_bucket(
            Bucket=BUCKET, CreateBucketConfiguration={"LocationConstraint": "us-west-2"}
        )
        dynamodb = boto3.resource("dynamodb")
        table = dynamodb.create_table(
            TableName="state",
            KeySchema=[{"AttributeName": "entity", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "entity", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        yield {
            "s3": s3,
            "bucket": BUCKET,
            "table": table,
            "client_id": "crestline-local",
            "client_secret": "local-secret",
            "url": "http://unused",
            "sweep_every": 1,
        }


@pytest.fixture(scope="module")
def api(erp_changes, tmp_path_factory):
    """The clean API over the plant run's store, as a transport."""
    out = tmp_path_factory.mktemp("store")
    erp.write_erp(erp_changes, out)
    # The ERP's clock moves on a minute a request, so two runs never share
    # a name (a run is named for the ERP's time when it signs in).
    ticks = itertools.count(int(time.time()) - 86_400, 60)  # from a day ago
    http = TestClient(
        create_app(
            Settings(db_path=out / "erp.db"),
            clock=lambda: next(ticks),
            mess=MessConfig(mess_level=0),
        )
    )

    def transport(method, path, headers, body) -> Response:
        reply = http.request(method, path, headers=dict(headers), content=body)
        lowered = {k.lower(): v for k, v in reply.headers.items()}
        return Response(reply.status_code, lowered, reply.json())

    return transport, erp_changes


def download(s3, tmp_path):
    for item in s3.list_objects_v2(Bucket=BUCKET).get("Contents", []):
        path = tmp_path / item["Key"]
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(s3.get_object(Bucket=BUCKET, Key=item["Key"])["Body"].read())
    return tmp_path


def test_s3_keys_and_bodies_are_what_a_local_folder_would_hold(aws_world, tmp_path):
    pages = [
        Page(
            "Jobs",
            "20260803T050000Z",
            "sweep",
            1,
            "/u",
            "t",
            "t",
            {"value": [{"JobNum": "J1"}]},
        )
    ]
    key = aws.S3RawWriter(aws_world["s3"], BUCKET).write(pages, 7, "2026-08-03")
    local = RawWriter(tmp_path).write(pages, 7, "2026-08-03")

    assert key == str(local.relative_to(tmp_path))
    assert (
        key
        == f"{SOURCE}/entity=Jobs/ingest_date=2026-08-03/20260803T050000Z-007-sweep.jsonl"
    )
    body = aws_world["s3"].get_object(Bucket=BUCKET, Key=key)["Body"].read()
    assert body == local.read_bytes()


def test_an_entitys_state_is_one_item_and_comes_back_whole(aws_world):
    table = aws.StateTable(aws_world["table"])
    assert table.load("Jobs") == State()  # never run

    state = State(
        runs={"Jobs": 4, "Parts": 9},
        watermarks={"Jobs": "2026-08-03T05:00:00Z"},
        sequences={"LaborDetails": 7},
        last_runs={"Jobs": "20260803T050000Z"},
    )
    table.save("Jobs", state)
    table.save(
        "LaborDetails",
        State(runs={"LaborDetails": 2}, sequences={"LaborDetails": 1207400}),
    )

    assert table.load("Jobs") == State(
        runs={"Jobs": 4},
        watermarks={"Jobs": "2026-08-03T05:00:00Z"},
        last_runs={"Jobs": "20260803T050000Z"},
    )
    assert table.load("LaborDetails") == State(
        runs={"LaborDetails": 2}, sequences={"LaborDetails": 1207400}
    )


def test_one_entity_lands_in_the_bucket_and_a_second_run_reads_only_changes(
    aws_world, api, tmp_path
):
    transport, erp_changes = api
    first = aws.extract("Jobs", aws_world, seed="r1", transport=transport)
    second = aws.extract("Jobs", aws_world, seed="r2", transport=transport)

    jobs = len(erp_changes.as_of()["Jobs"])
    assert first == first | {
        "entity": "Jobs",
        "run": 1,
        "reads": {"incremental": 1, "sweep": 1},
    }
    assert first["rows"] == {"incremental": jobs, "sweep": jobs}  # a first load
    # The store isn't changing: the second incremental read finds nothing new;
    # only the sweep, by design, reads every job again.
    assert second["rows"] == {"incremental": 0, "sweep": jobs}
    assert second["run"] == 2

    replica, _ = rebuild(download(aws_world["s3"], tmp_path))
    assert list(replica.rows["Jobs"].values()) == sorted(
        erp_changes.as_of()["Jobs"], key=lambda r: r["JobNum"]
    )
    saved = aws_world["table"].get_item(Key={"entity": "Jobs"})["Item"]
    assert saved["runs"] == 2 and datetime.fromisoformat(
        saved["watermark"]
    ) <= datetime.now(UTC)


def test_labor_keeps_its_sequence_between_invocations(aws_world, api):
    transport, erp_changes = api
    report = aws.extract("LaborDetails", aws_world, seed="r1", transport=transport)

    labor = erp_changes.as_of()["LaborDetails"]
    assert report["rows"] == {"sequence": len(labor)}
    item = aws_world["table"].get_item(Key={"entity": "LaborDetails"})["Item"]
    assert item["sequence"] == max(row["LaborDtlSeq"] for row in labor)
    assert aws.extract("LaborDetails", aws_world, seed="r2", transport=transport)[
        "rows"
    ] == {"sequence": 0}


def test_a_dropped_read_fails_the_entity_after_saving_what_completed(aws_world, api):
    transport, _ = api
    aws.extract("Jobs", aws_world, seed="r1", transport=transport)
    first = aws_world["table"].get_item(Key={"entity": "Jobs"})["Item"]

    def sweep_fails(method, path, headers, body):
        # The sweep is the read with no filter; the incremental one has one.
        if method == "GET" and "%24filter" not in path:
            return Response(500, {"date": "Mon, 03 Aug 2026 05:00:00 GMT"}, {})
        return transport(method, path, headers, body)

    waits = []
    with pytest.raises(aws.EntityFailed, match="Jobs: 1 read"):
        aws.extract(
            "Jobs", aws_world, seed="r2", transport=sweep_fails, sleep=waits.append
        )

    # The incremental read completed, so its watermark moved and was saved
    # before the failure was raised; Step Functions retries the entity.
    item = aws_world["table"].get_item(Key={"entity": "Jobs"})["Item"]
    assert item["runs"] == 2
    assert item["watermark"] >= first["watermark"]
    assert len(waits) == 7  # backed off between the eight tries of the sweep's page


def test_the_handler_names_its_entity(monkeypatch):
    with pytest.raises(ValueError, match="no entity Widgets"):
        aws.handler({"entity": "Widgets"}, None)
