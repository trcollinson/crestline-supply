"""FoundationStack, checked in the CloudFormation template it synthesizes.

No AWS calls: the app is built with the same feature flags the CDK CLI uses
(from `infra/cdk.json`), and the tests read what it would deploy.
"""

import json
import re
from pathlib import Path

import pytest
from aws_cdk import assertions

from app import build_app
from stacks.foundation import SCAN_LIMIT_BYTES

INFRA_DIR = Path(__file__).resolve().parent.parent / "infra"
CRESTLINE_TAGS = {"project": "crestline", "layer": "foundation", "milestone": "M1"}


@pytest.fixture(scope="module")
def app():
    context = json.loads((INFRA_DIR / "cdk.json").read_text())["context"]
    return build_app(context)


@pytest.fixture(scope="module")
def template(app) -> assertions.Template:
    return assertions.Template.from_stack(app.node.find_child("CrestlineFoundation"))


def bucket_id(template: assertions.Template, name: str) -> str:
    """The logical ID CDK gave the bucket construct called `name`."""
    [logical_id] = [
        logical_id
        for logical_id in template.find_resources("AWS::S3::Bucket")
        if logical_id.startswith(name)
    ]
    return logical_id


def bucket(template: assertions.Template, name: str) -> dict:
    return template.find_resources("AWS::S3::Bucket")[bucket_id(template, name)]


def test_raw_bucket_is_private_encrypted_and_versioned(template):
    props = bucket(template, "RawBucket")["Properties"]
    assert props["VersioningConfiguration"] == {"Status": "Enabled"}
    [rule] = props["BucketEncryption"]["ServerSideEncryptionConfiguration"]
    assert rule["ServerSideEncryptionByDefault"]["SSEAlgorithm"] == "AES256"
    assert props["PublicAccessBlockConfiguration"] == {
        "BlockPublicAcls": True,
        "BlockPublicPolicy": True,
        "IgnorePublicAcls": True,
        "RestrictPublicBuckets": True,
    }


def test_replaced_raw_files_expire_after_30_days(template):
    [rule] = bucket(template, "RawBucket")["Properties"]["LifecycleConfiguration"][
        "Rules"
    ]
    assert rule["NoncurrentVersionExpiration"] == {"NoncurrentDays": 30}
    assert "ExpirationInDays" not in rule  # current files are kept


def test_athena_results_expire_after_7_days(template):
    props = bucket(template, "AthenaResultsBucket")["Properties"]
    [rule] = props["LifecycleConfiguration"]["Rules"]
    assert rule["ExpirationInDays"] == 7
    assert props["PublicAccessBlockConfiguration"]["RestrictPublicBuckets"] is True


def test_both_buckets_refuse_plain_http(template):
    policies = template.find_resources("AWS::S3::BucketPolicy")
    https_only = {
        policy["Properties"]["Bucket"]["Ref"]
        for policy in policies.values()
        for statement in policy["Properties"]["PolicyDocument"]["Statement"]
        if statement["Effect"] == "Deny"
        and statement.get("Condition") == {"Bool": {"aws:SecureTransport": "false"}}
    }
    assert https_only == {
        bucket_id(template, "RawBucket"),
        bucket_id(template, "AthenaResultsBucket"),
    }


def test_cdk_destroy_empties_and_deletes_both_buckets(template):
    for name in ("RawBucket", "AthenaResultsBucket"):
        assert bucket(template, name)["DeletionPolicy"] == "Delete"
    template.resource_count_is("Custom::S3AutoDeleteObjects", 2)


def test_glue_database_is_crestline_raw(template):
    template.has_resource_properties(
        "AWS::Glue::Database", {"DatabaseInput": {"Name": "crestline_raw"}}
    )


def test_workgroup_caps_every_query_and_keeps_results_in_the_results_bucket(
    template,
):
    results_bucket = bucket_id(template, "AthenaResultsBucket")
    template.has_resource_properties(
        "AWS::Athena::WorkGroup",
        {
            "Name": "crestline",
            "RecursiveDeleteOption": True,
            "WorkGroupConfiguration": {
                "BytesScannedCutoffPerQuery": SCAN_LIMIT_BYTES,
                "EnforceWorkGroupConfiguration": True,
                "ResultConfiguration": {
                    "OutputLocation": {
                        "Fn::Join": ["", ["s3://", {"Ref": results_bucket}, "/"]]
                    }
                },
            },
        },
    )
    assert SCAN_LIMIT_BYTES == 1024**3


def test_stack_tags_reach_everything_cloudformation_deploys(app):
    # CloudFormation copies stack tags onto every resource that can carry
    # them, including the ones CDK adds itself (the bucket-emptying Lambda
    # and its role).
    stack = app.synth().get_stack_artifact("CrestlineFoundation")
    assert stack.tags == CRESTLINE_TAGS


def test_every_tagged_resource_in_the_template_has_all_three_tags(template):
    tagged = [
        resource
        for resource in template.to_json()["Resources"].values()
        if "Tags" in resource.get("Properties", {})
    ]
    assert {resource["Type"] for resource in tagged} == {
        "AWS::S3::Bucket",
        "AWS::Athena::WorkGroup",
    }
    for resource in tagged:
        tags = {tag["Key"]: tag["Value"] for tag in resource["Properties"]["Tags"]}
        assert CRESTLINE_TAGS.items() <= tags.items(), resource["Type"]


def test_outputs_name_the_buckets_database_and_workgroup(template):
    assert set(template.find_outputs("*")) == {
        "RawBucketName",
        "AthenaResultsBucketName",
        "GlueDatabaseName",
        "AthenaWorkGroupName",
    }


def test_template_names_no_account_or_region(template):
    text = json.dumps(template.to_json())
    assert not re.search(r"\b\d{12}\b", text)
    assert not re.search(r"\b[a-z]{2}(-gov)?-[a-z]+-\d\b", text)
