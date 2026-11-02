"""Run, measure, and clean up the 1,000-tag load test (M3 step 9).

`run()` sends the load (`crestline_machines.loadtest`) through the deployed
CrestlineMachineLoadtest stack and writes a manifest to `out/loadtests/`.
`report()` reads what AWS counted while it ran, from CloudWatch: each
mode's messages matched and actions run by its rule, and records and bytes
received by its Firehose stream. The bill comes later, from Cost Explorer
by `layer` tag. `cleanup()` deletes the test's certificate and empties its
bucket, before `cdk destroy`.

Data-platform code: it never reads the answer key or imports the simulator.
"""

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import boto3

from crestline import machine_setup
from crestline.stack import stack_outputs
from crestline_machines import loadtest
from crestline_machines.devices import LOADTEST_GATEWAY, LOADTEST_RULES
from crestline_machines.publisher import ACK_TIMEOUT, PahoClient

STACK_NAME = "CrestlineMachineLoadtest"
LOADTESTS_DIR = Path("loadtests")  # under the output folder


def run(out_dir: Path, minutes: int, modes: list[str]) -> Path:
    machine_setup.ensure(gateways=(LOADTEST_GATEWAY,))
    endpoint = machine_setup.endpoint()
    cert_dir = machine_setup.CERTS_DIR / LOADTEST_GATEWAY

    def connect(n: int) -> PahoClient:
        return PahoClient(
            LOADTEST_GATEWAY, endpoint, cert_dir, f"{LOADTEST_GATEWAY}-{n:02d}"
        )

    a_clients = (
        [connect(n) for n in range(loadtest.A_CONNECTIONS)] if "a" in modes else []
    )
    b_client = connect(loadtest.A_CONNECTIONS) if "b" in modes else None
    name = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    manifest = loadtest.run(a_clients, b_client, name=name, seconds=minutes * 60)
    clients = {f"{LOADTEST_GATEWAY}-{n:02d}": c for n, c in enumerate(a_clients)}
    if b_client is not None:
        clients[f"{LOADTEST_GATEWAY}-{loadtest.A_CONNECTIONS:02d}"] = b_client
    loadtest.settle(manifest, clients, ACK_TIMEOUT)
    path = out_dir / LOADTESTS_DIR / f"{name}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(manifest.to_json(), encoding="utf-8")
    return path


def report(
    manifest_path: Path, cloudwatch=None, cloudformation=None
) -> dict[str, dict]:
    """Per mode: what was sent, and what IoT Core and Firehose counted."""
    cloudwatch = cloudwatch or boto3.client("cloudwatch")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    outputs = stack_outputs(STACK_NAME, cloudformation)
    start = datetime.fromisoformat(manifest["started_at"]).replace(
        second=0, microsecond=0
    )
    end = datetime.fromisoformat(manifest["finished_at"]) + timedelta(minutes=5)

    def total(namespace: str, metric: str, dimensions: dict[str, str]) -> float:
        points = cloudwatch.get_metric_statistics(
            Namespace=namespace,
            MetricName=metric,
            Dimensions=[{"Name": k, "Value": v} for k, v in dimensions.items()],
            StartTime=start,
            EndTime=end,
            Period=60,
            Statistics=["Sum"],
        )["Datapoints"]
        return sum(point["Sum"] for point in points)

    result = {}
    for mode, sent in manifest["sent"].items():
        rule = {"RuleName": LOADTEST_RULES[mode]}
        stream = {"DeliveryStreamName": outputs[f"StreamName{mode.upper()}"]}
        result[mode] = {
            "messages sent": sent["messages"],
            "bytes sent": sent["bytes"],
            "rule matched": total("AWS/IoT", "TopicMatch", rule),
            "actions succeeded": total(
                "AWS/IoT", "Success", rule | {"ActionType": "Firehose"}
            ),
            "actions failed": total(
                "AWS/IoT", "Failure", rule | {"ActionType": "Firehose"}
            ),
            "parse errors": total("AWS/IoT", "ParseError", rule),
            "firehose records in": total("AWS/Firehose", "IncomingRecords", stream),
            "firehose bytes in": total("AWS/Firehose", "IncomingBytes", stream),
            "firehose records to S3": total(
                "AWS/Firehose", "DeliveryToS3.Records", stream
            ),
        }
    return result


def cleanup(cloudformation=None, s3=None, iot=None) -> tuple[int, int]:
    """Delete the test's certificate and every object in its bucket."""
    s3 = s3 or boto3.client("s3")
    certificates = machine_setup.delete(iot=iot, gateways=(LOADTEST_GATEWAY,))[
        LOADTEST_GATEWAY
    ]
    bucket = stack_outputs(STACK_NAME, cloudformation)["BucketName"]
    deleted = 0
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket):
        keys = [{"Key": obj["Key"]} for obj in page.get("Contents", [])]
        if keys:
            s3.delete_objects(Bucket=bucket, Delete={"Objects": keys})
            deleted += len(keys)
    return certificates, deleted
