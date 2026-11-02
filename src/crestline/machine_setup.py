"""What MachineIngestStack needs after it deploys (M3 step 7): certificates and a schema.

- **A certificate per gateway.** Each gateway connects with its own X.509
  certificate. CloudFormation can make a certificate from a signing request
  but can't hand out a private key, so this makes them through the API
  instead: a key pair and certificate per gateway, attached to the
  gateway's thing and its policy (both in the stack). The private keys stay
  on this machine, in `.secrets/machines/<gateway>/` (gitignored,
  owner-only): only the local replay publisher uses them.
- **The Sparkplug B descriptor set**, put in the stack's schema bucket,
  where the Sparkplug rule's `decode()` reads it. Uploading it here rather
  than from the stack spares the stack a custom-resource Lambda, and the
  log group that would outlive it.

`delete()` undoes both, and must run before `cdk destroy`: IoT won't delete
a thing that still has a certificate attached, and CloudFormation won't
delete a bucket with something in it. It works from what AWS says is
attached to each thing, so a lost local folder can still be cleaned up.
Account and region come from the AWS environment (`AWS_PROFILE`,
`AWS_REGION`).
"""

import json
import os
from pathlib import Path

import boto3

from crestline.stack import stack_outputs
from crestline_machines.devices import (
    DESCRIPTOR_FILE,
    DESCRIPTOR_KEY,
    GATEWAYS,
    policy_name,
)

MACHINE_INGEST_STACK_NAME = "CrestlineMachineIngest"

CERTS_DIR = Path(".secrets/machines")
CERTIFICATE = "certificate.pem"
PRIVATE_KEY = "private.key"
DETAILS = "certificate.json"


def endpoint(iot=None) -> str:
    """The account's IoT Core data endpoint (ATS), where gateways connect."""
    iot = iot or boto3.client("iot")
    return iot.describe_endpoint(endpointType="iot:Data-ATS")["endpointAddress"]


def schema_bucket(cloudformation=None) -> str:
    return stack_outputs(MACHINE_INGEST_STACK_NAME, cloudformation)["SchemaBucketName"]


def put_schema(bucket: str, s3=None) -> None:
    """Upload the descriptor set (again: it is the same bytes every time)."""
    s3 = s3 or boto3.client("s3")
    s3.put_object(Bucket=bucket, Key=DESCRIPTOR_KEY, Body=DESCRIPTOR_FILE.read_bytes())


def delete_schema(bucket: str, s3=None) -> None:
    s3 = s3 or boto3.client("s3")
    s3.delete_object(Bucket=bucket, Key=DESCRIPTOR_KEY)


def ensure(
    certs_dir: Path = CERTS_DIR, iot=None, gateways: tuple[str, ...] = GATEWAYS
) -> dict[str, str]:
    """A certificate for every gateway that has none here; gateway -> "created" or "kept"."""
    iot = iot or boto3.client("iot")
    done = {}
    for gateway in gateways:
        folder = certs_dir / gateway
        if (folder / DETAILS).exists():
            done[gateway] = "kept"
            continue
        made = iot.create_keys_and_certificate(setAsActive=True)
        arn = made["certificateArn"]
        iot.attach_policy(policyName=policy_name(gateway), target=arn)
        iot.attach_thing_principal(thingName=gateway, principal=arn)
        folder.mkdir(parents=True, exist_ok=True)
        os.chmod(folder, 0o700)
        _write(folder / CERTIFICATE, made["certificatePem"])
        _write(folder / PRIVATE_KEY, made["keyPair"]["PrivateKey"])
        _write(
            folder / DETAILS,
            json.dumps({"certificateArn": arn, "certificateId": made["certificateId"]}),
        )
        done[gateway] = "created"
    return done


def delete(
    certs_dir: Path = CERTS_DIR, iot=None, gateways: tuple[str, ...] = GATEWAYS
) -> dict[str, int]:
    """Detach, deactivate, and delete every gateway's certificates; gateway -> how many."""
    iot = iot or boto3.client("iot")
    deleted = {}
    for gateway in gateways:
        try:
            principals = iot.list_thing_principals(thingName=gateway)["principals"]
        except iot.exceptions.ResourceNotFoundException:
            principals = []  # the stack is gone already
        for arn in principals:
            iot.detach_thing_principal(thingName=gateway, principal=arn)
            iot.detach_policy(policyName=policy_name(gateway), target=arn)
            certificate_id = arn.rsplit("/", 1)[1]
            iot.update_certificate(certificateId=certificate_id, newStatus="INACTIVE")
            iot.delete_certificate(certificateId=certificate_id)
        deleted[gateway] = len(principals)
        folder = certs_dir / gateway
        for name in (CERTIFICATE, PRIVATE_KEY, DETAILS):
            (folder / name).unlink(missing_ok=True)
        if folder.exists():
            folder.rmdir()
    return deleted


def _write(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")
    os.chmod(path, 0o600)
