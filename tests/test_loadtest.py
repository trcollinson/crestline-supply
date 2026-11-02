"""The 1,000-tag load test: the generator, the opt-in stack, and its cleanup."""

import json
from pathlib import Path

import boto3
import pytest
from aws_cdk import assertions
from moto import mock_aws

from app import build_app
from crestline import machine_loadtest
from crestline_machines import loadtest

INFRA_DIR = Path(__file__).resolve().parent.parent / "infra"


class FakeClient:
    def __init__(self) -> None:
        self.sent: list[tuple[str, bytes]] = []

    def send(self, topic, payload, properties):
        self.sent.append((topic, payload))

    def acknowledged(self, timeout):
        return len(self.sent), 0

    def disconnects(self):
        return []


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


def run(seconds: int = 3, tags: int = 1000):
    clock = FakeClock()
    a = [FakeClient() for _ in range(loadtest.A_CONNECTIONS)]
    b = FakeClient()
    manifest = loadtest.run(
        a,
        b,
        name="T",
        seconds=seconds,
        tags=tags,
        clock=clock,
        sleep=clock.sleep,
        wall_ms=lambda: 1,
    )
    return manifest, a, b, clock


def test_mode_a_sends_a_small_message_per_tag_per_second_through_its_rule():
    manifest, a, _, _ = run()
    assert manifest.sent["a"].messages == 3000
    topic, payload = a[0].sent[0]
    assert (
        topic
        == "$aws/rules/crestline_loadtest_a/crestline/loadtest/LOADTEST-GW/tag/T0001"
    )
    assert set(json.loads(payload)) == {"ts", "tag", "v"}
    assert len(payload) < 60
    # Shared out evenly, under IoT Core's 100 a second per connection.
    per_connection = [len(c.sent) / 3 for c in a]
    assert max(per_connection) < 100 and min(per_connection) > 80


def test_mode_b_sends_one_batch_a_second_with_every_tag():
    manifest, _, b, _ = run()
    assert manifest.sent["b"].messages == 3
    topic, payload = b.sent[0]
    assert (
        topic == "$aws/rules/crestline_loadtest_b/crestline/loadtest/LOADTEST-GW/batch"
    )
    batch = json.loads(payload)
    assert len(batch["tags"]) == 1000
    assert 10_000 < len(payload) < 128 * 1024  # IoT Core's payload limit


def test_it_keeps_time_and_sends_the_same_bytes_every_run():
    manifest, a1, _, clock = run()
    assert clock.now == pytest.approx(3.0)
    assert manifest.late_seconds == 0
    _, a2, _, _ = run()
    assert a1[3].sent == a2[3].sent


def test_values_change_every_second():
    _, _, b, _ = run()
    first, second = (json.loads(p)["tags"] for _, p in b.sent[:2])
    assert sum(first[t] != second[t] for t in first) > 900


@pytest.fixture(scope="module")
def template() -> assertions.Template:
    base = json.loads((INFRA_DIR / "cdk.json").read_text())["context"]
    app = build_app(base | {"aws:cdk:bundling-stacks": [], "machineLoadtest": "true"})
    return assertions.Template.from_stack(
        app.node.find_child("CrestlineMachineLoadtest")
    )


def test_the_stack_is_only_there_when_asked_for():
    base = json.loads((INFRA_DIR / "cdk.json").read_text())["context"]
    app = build_app(base | {"aws:cdk:bundling-stacks": []})
    assert app.node.try_find_child("CrestlineMachineLoadtest") is None


def test_each_mode_bills_to_its_own_layer(template):
    def layers(kind: str) -> list[str]:
        return sorted(
            next(t["Value"] for t in r["Properties"]["Tags"] if t["Key"] == "layer")
            for r in template.find_resources(kind).values()
        )

    assert layers("AWS::IoT::TopicRule") == ["machine-loadtest-a", "machine-loadtest-b"]
    assert layers("AWS::KinesisFirehose::DeliveryStream") == [
        "machine-loadtest-a",
        "machine-loadtest-b",
    ]
    assert layers("AWS::S3::Bucket") == ["machine-loadtest"]


def test_the_gateway_may_open_several_connections_but_only_as_itself(template):
    [policy] = template.find_resources("AWS::IoT::Policy").values()
    connect, publish = policy["Properties"]["PolicyDocument"]["Statement"]
    assert json.dumps(connect["Resource"]).endswith(':client/LOADTEST-GW-*"]]}')
    assert len(publish["Resource"]) == 2
    for log_group in template.find_resources("AWS::Logs::LogGroup").values():
        assert log_group["DeletionPolicy"] == "Delete"


def test_cleanup_deletes_the_certificate_and_empties_the_bucket(monkeypatch, tmp_path):
    with mock_aws():
        iot = boto3.client("iot", region_name="us-west-2")
        s3 = boto3.client("s3", region_name="us-west-2")
        iot.create_thing(thingName="LOADTEST-GW")
        iot.create_policy(policyName="crestline-LOADTEST-GW", policyDocument="{}")
        s3.create_bucket(
            Bucket="landing",
            CreateBucketConfiguration={"LocationConstraint": "us-west-2"},
        )
        for n in range(3):
            s3.put_object(Bucket="landing", Key=f"mode=a/{n}.jsonl", Body=b"{}")
        monkeypatch.setattr(machine_loadtest.machine_setup, "CERTS_DIR", tmp_path)
        monkeypatch.setattr(
            machine_loadtest,
            "stack_outputs",
            lambda name, cf=None: {"BucketName": "landing"},
        )
        machine_loadtest.machine_setup.ensure(tmp_path, iot, gateways=("LOADTEST-GW",))

        assert machine_loadtest.cleanup(s3=s3, iot=iot) == (1, 3)
        assert s3.list_objects_v2(Bucket="landing").get("KeyCount") == 0
        assert iot.list_thing_principals(thingName="LOADTEST-GW")["principals"] == []
