"""What MachineIngestStack needs after deploying (crestline.machine_setup), against moto."""

import stat

import boto3
import pytest
from moto import mock_aws

from crestline import machine_setup
from crestline_machines.devices import GATEWAYS


@pytest.fixture
def iot():
    with mock_aws():
        client = boto3.client("iot", region_name="us-west-2")
        # What MachineIngestStack makes.
        for gateway in GATEWAYS:
            client.create_thing(thingName=gateway)
            client.create_policy(policyName=f"crestline-{gateway}", policyDocument="{}")
        yield client


def test_each_gateway_gets_a_certificate_attached_to_its_thing_and_policy(
    iot, tmp_path
):
    assert machine_setup.ensure(tmp_path, iot) == dict.fromkeys(GATEWAYS, "created")

    for gateway in GATEWAYS:
        [arn] = iot.list_thing_principals(thingName=gateway)["principals"]
        targets = iot.list_targets_for_policy(policyName=f"crestline-{gateway}")[
            "targets"
        ]
        assert targets == [arn]
        folder = tmp_path / gateway
        assert (
            (folder / "certificate.pem")
            .read_text()
            .startswith("-----BEGIN CERTIFICATE")
        )
        key = folder / "private.key"
        assert "PRIVATE KEY" in key.read_text()
        assert stat.S_IMODE(key.stat().st_mode) == 0o600
        assert stat.S_IMODE(folder.stat().st_mode) == 0o700


def test_running_it_again_keeps_what_is_there(iot, tmp_path):
    machine_setup.ensure(tmp_path, iot)
    assert machine_setup.ensure(tmp_path, iot) == dict.fromkeys(GATEWAYS, "kept")
    assert len(iot.list_certificates()["certificates"]) == len(GATEWAYS)


def test_delete_removes_every_certificate_even_with_the_local_files_gone(iot, tmp_path):
    machine_setup.ensure(tmp_path, iot)
    (tmp_path / "GW01" / "certificate.json").unlink()  # a half-lost folder

    assert machine_setup.delete(tmp_path, iot) == dict.fromkeys(GATEWAYS, 1)
    assert iot.list_certificates()["certificates"] == []
    for gateway in GATEWAYS:
        assert iot.list_thing_principals(thingName=gateway)["principals"] == []
        assert not (tmp_path / gateway).exists()
    # And after the stack is gone, there is nothing left to do.
    for gateway in GATEWAYS:
        iot.delete_thing(thingName=gateway)
    assert machine_setup.delete(tmp_path, iot) == dict.fromkeys(GATEWAYS, 0)


def test_the_schema_goes_up_and_comes_down(tmp_path):
    from crestline_machines.devices import DESCRIPTOR_FILE, DESCRIPTOR_KEY

    with mock_aws():
        s3 = boto3.client("s3", region_name="us-west-2")
        s3.create_bucket(
            Bucket="schema",
            CreateBucketConfiguration={"LocationConstraint": "us-west-2"},
        )
        machine_setup.put_schema("schema", s3)
        body = s3.get_object(Bucket="schema", Key=DESCRIPTOR_KEY)["Body"].read()
        assert body == DESCRIPTOR_FILE.read_bytes()
        machine_setup.delete_schema("schema", s3)
        assert s3.list_objects_v2(Bucket="schema").get("KeyCount") == 0
