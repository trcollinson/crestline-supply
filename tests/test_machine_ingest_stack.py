"""MachineIngestStack: the gateways into IoT Core, checked in its synthesized template."""

import json
from pathlib import Path

import pytest
from aws_cdk import assertions

from app import build_app
from crestline_machines.devices import DEVICES, GATEWAYS

INFRA_DIR = Path(__file__).resolve().parent.parent / "infra"
TAGS = {"project": "crestline", "layer": "machine-ingest", "milestone": "M3"}


@pytest.fixture(scope="module")
def template() -> assertions.Template:
    base = json.loads((INFRA_DIR / "cdk.json").read_text())["context"]
    app = build_app(base | {"aws:cdk:bundling-stacks": []})
    return assertions.Template.from_stack(app.node.find_child("CrestlineMachineIngest"))


def flat(value) -> str:
    """A CloudFormation value as text, references stubbed: enough to read a string."""
    if isinstance(value, str):
        return value
    if isinstance(value, dict) and "Fn::Join" in value:
        return "".join(flat(part) for part in value["Fn::Join"][1])
    return "REF"


def rules(template) -> dict[str, dict]:
    return {
        r["Properties"]["RuleName"]: r["Properties"]["TopicRulePayload"]
        for r in template.find_resources("AWS::IoT::TopicRule").values()
    }


def test_a_thing_per_gateway(template):
    names = {
        r["Properties"]["ThingName"]
        for r in template.find_resources("AWS::IoT::Thing").values()
    }
    assert names == set(GATEWAYS) == {"EDGE-LASER", "GW01", "GW02", "GW03"}


def test_each_gateway_may_connect_as_itself_and_publish_only_to_its_own_rules(template):
    policies = {
        r["Properties"]["PolicyName"]: r["Properties"]["PolicyDocument"]["Statement"]
        for r in template.find_resources("AWS::IoT::Policy").values()
    }
    assert set(policies) == {f"crestline-{g}" for g in GATEWAYS}
    for gateway in GATEWAYS:
        connect, publish = policies[f"crestline-{gateway}"]
        assert connect["Action"] == "iot:Connect"
        clients = sorted(flat(r).split(":client/")[1] for r in connect["Resource"])
        assert clients == [
            gateway,
            f"{gateway}-*",
        ]  # itself, or a replay's extra connections
        assert publish["Action"] == "iot:Publish"
        topics = sorted(flat(r).split(":topic/")[1] for r in publish["Resource"])
        mine = sorted(f"$aws/rules/{d.rule}/*" for d in DEVICES if d.gateway == gateway)
        assert topics == mine
    # GW02 carries two devices, so two rules.
    assert len(policies["crestline-GW02"][1]["Resource"]) == 2


def test_a_rule_per_device_keeps_the_payload_and_says_where_and_when(template):
    by_name = rules(template)
    assert set(by_name) == {d.rule for d in DEVICES}
    for device in DEVICES:
        rule = by_name[device.rule]
        sql = flat(rule["Sql"])
        assert sql.endswith(f"FROM '{device.topic_filter}'")
        for kept in (
            "topic() AS topic",
            "get(get_user_properties('published_at'), 0) AS published_at",
            "get(get_user_properties('replay'), 0) AS replay",
            "timestamp() AS received_at_utc",
        ):
            assert kept in sql
        if device.sparkplug:
            assert "encode(*, 'base64') AS payload_b64" in sql
            assert (
                "'descriptors/sparkplug_b.desc', 'sparkplug_b', 'Payload') AS decoded"
                in sql
            )
        else:
            assert sql.startswith("SELECT *, ")
        [action] = rule["Actions"]
        assert action["Firehose"]["Separator"] == "\n"
        key = rule["ErrorAction"]["S3"]["Key"]
        assert key.startswith(
            f"source=machines-iot/errors/entity={device.name}/ingest_date="
        )


def test_a_stream_per_device_writes_its_raw_folder(template):
    prefixes = sorted(
        r["Properties"]["ExtendedS3DestinationConfiguration"]["Prefix"]
        for r in template.find_resources(
            "AWS::KinesisFirehose::DeliveryStream"
        ).values()
    )
    assert prefixes == sorted(
        f"source=machines-iot/entity={d.name}/ingest_date=!{{timestamp:yyyy-MM-dd}}/"
        for d in DEVICES
    )


def test_iot_core_can_read_the_schema_and_nothing_is_left_behind(template):
    [policy] = template.find_resources("AWS::S3::BucketPolicy").values()
    statements = policy["Properties"]["PolicyDocument"]["Statement"]
    assert any(
        s.get("Principal") == {"Service": "iot.amazonaws.com"}
        and s["Action"] == "s3:GetObject"
        for s in statements
    )
    for log_group in template.find_resources("AWS::Logs::LogGroup").values():
        assert log_group["DeletionPolicy"] == "Delete"
    [bucket] = template.find_resources("AWS::S3::Bucket").values()
    assert bucket["DeletionPolicy"] == "Delete"
    # No custom resources (their Lambdas' log groups outlive the stack): the
    # only function is the curator, with a log group the stack owns.
    [curator] = template.find_resources("AWS::Lambda::Function").values()
    assert curator["Properties"]["Handler"] == "machine_curate.curate.handler"
    assert curator["Properties"]["ReservedConcurrentExecutions"] == 1
    assert "LoggingConfig" in curator["Properties"]


def test_the_curator_runs_hourly_and_its_failures_reach_someone(template):
    [schedule] = template.find_resources("AWS::Scheduler::Schedule").values()
    assert schedule["Properties"]["ScheduleExpression"] == "rate(1 hour)"
    [alarm] = template.find_resources("AWS::CloudWatch::Alarm").values()
    assert alarm["Properties"]["MetricName"] == "Errors"
    assert alarm["Properties"]["AlarmActions"]
    [raw] = [
        t
        for t in template.find_resources("AWS::Glue::Table").values()
        if t["Properties"]["TableInput"]["Name"] == "raw_machines_iot"
    ]
    projection = raw["Properties"]["TableInput"]["Parameters"]
    assert projection["projection.entity.values"] == "lsr01,pb01,pb02,pem01,pcline"


def test_the_curator_may_read_the_partitioned_mes_table_its_checks_join(template):
    """Athena lists a partitioned table's partitions with the caller's own rights.

    The brake reconciliation reads raw_mes_labor (partitioned by ingest date);
    without GetPartitions every run merged, then failed in its checks.
    """
    actions = {
        action
        for policy in template.find_resources("AWS::IAM::Policy").values()
        for statement in policy["Properties"]["PolicyDocument"]["Statement"]
        for action in (
            statement["Action"]
            if isinstance(statement["Action"], list)
            else [statement["Action"]]
        )
    }
    assert {"glue:GetTable", "glue:GetPartitions"} <= actions


def test_the_descriptor_set_is_the_vendored_schema():
    from google.protobuf import descriptor_pb2

    descriptors = descriptor_pb2.FileDescriptorSet()
    path = INFRA_DIR.parent / "interfaces/machines/proto/sparkplug_b.desc"
    descriptors.ParseFromString(path.read_bytes())
    [schema] = descriptors.file
    assert schema.name == "sparkplug_b.proto"
    assert [m.name for m in schema.message_type] == ["Payload"]


def test_everything_is_tagged(template):
    for kind in (
        "AWS::IoT::TopicRule",
        "AWS::KinesisFirehose::DeliveryStream",
        "AWS::S3::Bucket",
    ):
        for resource in template.find_resources(kind).values():
            tags = resource["Properties"]["Tags"]
            assert {(t["Key"], t["Value"]) for t in tags} >= set(TAGS.items())
