"""The deployed API's secrets in Parameter Store, against moto (no AWS calls)."""

import boto3
import pytest
from moto import mock_aws

from crestline import erp_secrets

pytestmark = pytest.mark.usefixtures("fake_aws_credentials")


@pytest.fixture
def ssm():
    with mock_aws():
        yield boto3.client("ssm")


def test_both_are_created_as_tagged_secure_strings(ssm):
    assert erp_secrets.ensure(ssm) == {
        name: "created" for name in erp_secrets.PARAMETERS.values()
    }

    for name in erp_secrets.PARAMETERS.values():
        parameter = ssm.get_parameter(Name=name)["Parameter"]
        assert parameter["Type"] == "SecureString"
        tags = ssm.list_tags_for_resource(ResourceType="Parameter", ResourceId=name)
        assert {t["Key"]: t["Value"] for t in tags["TagList"]} == {
            "project": "crestline",
            "layer": "erp-api",
            "milestone": "M2",
        }
    values = erp_secrets.read(ssm)
    assert set(values) == {"client_secret", "signing_key"}
    assert len(set(values.values())) == 2 and all(len(v) >= 40 for v in values.values())


def test_running_it_again_keeps_the_values_and_rotate_replaces_them(ssm):
    erp_secrets.ensure(ssm)
    first = erp_secrets.read(ssm)

    assert set(erp_secrets.ensure(ssm).values()) == {"kept"}
    assert erp_secrets.read(ssm) == first
    assert set(erp_secrets.ensure(ssm, rotate=True).values()) == {"rotated"}
    rotated = erp_secrets.read(ssm)
    assert all(rotated[k] != first[k] for k in first)


def test_delete_removes_both_and_says_what_was_there(ssm):
    assert erp_secrets.delete(ssm) == []
    erp_secrets.ensure(ssm)
    assert erp_secrets.delete(ssm) == sorted(erp_secrets.PARAMETERS.values())
    assert (
        ssm.get_parameters(Names=list(erp_secrets.PARAMETERS.values()))["Parameters"]
        == []
    )
