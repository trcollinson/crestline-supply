"""AgentStack (M4 step 3): the gateway, its tools, and what each may touch,
checked in the synthesized template."""

import json
from pathlib import Path

import pytest
from aws_cdk import assertions

from agent_tools.tools import TARGETS
from app import build_app

INFRA_DIR = Path(__file__).resolve().parent.parent / "infra"
TAGS = {"project": "crestline", "layer": "agent", "milestone": "M4"}


@pytest.fixture(scope="module")
def template() -> assertions.Template:
    base = json.loads((INFRA_DIR / "cdk.json").read_text())["context"]
    app = build_app(base | {"aws:cdk:bundling-stacks": []})
    return assertions.Template.from_stack(app.node.find_child("CrestlineAgent"))


def functions(template) -> dict[str, dict]:
    """Each tool Lambda by its handler (the stack's other Lambdas excluded)."""
    return {
        r["Properties"]["Handler"]: r
        for r in template.find_resources("AWS::Lambda::Function").values()
        if r["Properties"].get("Handler", "").startswith("agent_tools.")
    }


def role_statements(template, function: dict) -> list[dict]:
    """The statements in the policies attached to a function's role."""
    role = function["Properties"]["Role"]["Fn::GetAtt"][0]
    return [
        s
        for p in template.find_resources("AWS::IAM::Policy").values()
        if {"Ref": role} in p["Properties"]["Roles"]
        for s in p["Properties"]["PolicyDocument"]["Statement"]
    ]


def actions(statements: list[dict]) -> set[str]:
    return {
        a
        for s in statements
        for a in ([s["Action"]] if isinstance(s["Action"], str) else s["Action"])
    }


def test_one_gateway_checked_by_iam_speaking_mcp(template):
    (gateway,) = template.find_resources("AWS::BedrockAgentCore::Gateway").values()
    props = gateway["Properties"]
    assert props["Name"] == "crestline-agent"
    assert props["AuthorizerType"] == "AWS_IAM"
    assert props["ProtocolType"] == "MCP"
    assert props["ProtocolConfiguration"]["Mcp"]["SupportedVersions"] == ["2025-11-25"]
    assert props["PolicyEngineConfiguration"]["Mode"] == "ENFORCE"  # step 4


def test_four_targets_carry_every_tool(template):
    targets = {
        r["Properties"]["Name"]: [
            t["Name"]
            for t in r["Properties"]["TargetConfiguration"]["Mcp"]["Lambda"][
                "ToolSchema"
            ]["InlinePayload"]
        ]
        for r in template.find_resources(
            "AWS::BedrockAgentCore::GatewayTarget"
        ).values()
    }
    assert targets == {
        target: [t.name for t in tools] for target, tools in TARGETS.items()
    }
    assert targets["PlantScada"] == ["write_tag", "read_tag"]


def test_the_reads_run_athena_in_one_workgroup_and_write_nothing(template):
    statements = role_statements(
        template, functions(template)["agent_tools.plant_data.handler"]
    )
    assert actions(statements) <= {
        "athena:StartQueryExecution",
        "athena:GetQueryExecution",
        "athena:GetQueryResults",
        "glue:GetDatabase",
        "glue:GetTable",
        "glue:GetPartitions",
        "s3:GetObject*",
        "s3:GetBucket*",
        "s3:List*",
        "s3:PutObject",
        "s3:PutObjectLegalHold",
        "s3:PutObjectRetention",
        "s3:PutObjectTagging",
        "s3:PutObjectVersionTagging",
        "s3:Abort*",
    }
    text = json.dumps(statements)
    assert "workgroup/crestline" in text
    assert "truth" not in text  # the answer key never leaves the laptop
    assert "dynamodb" not in text and "bedrock" not in text


def test_each_write_may_do_one_thing(template):
    fns = functions(template)
    cmms = role_statements(template, fns["agent_tools.writes.maintenance_handler"])
    assert actions(cmms) == {"dynamodb:PutItem"}
    outbox = role_statements(template, fns["agent_tools.writes.report_handler"])
    assert actions(outbox) == {"s3:PutObject"}
    assert "outbox/*" in json.dumps(outbox)


def test_the_honeypot_can_write_its_logs_and_nothing_else(template):
    honeypot = functions(template)["agent_tools.scada.handler"]
    assert role_statements(template, honeypot) == []
    assert "VpcConfig" not in honeypot["Properties"]
    role = template.find_resources("AWS::IAM::Role")[
        honeypot["Properties"]["Role"]["Fn::GetAtt"][0]
    ]
    managed = json.dumps(role["Properties"].get("ManagedPolicyArns", []))
    assert "AWSLambdaBasicExecutionRole" in managed and "VPC" not in managed


def test_two_principal_roles_may_only_invoke_the_gateway(template):
    roles = {
        r["Properties"].get("RoleName"): (logical, r)
        for logical, r in template.find_resources("AWS::IAM::Role").items()
    }
    for name in ("crestline-ops-analyst", "crestline-maintenance-planner"):
        logical, _ = roles[name]
        statements = [
            s
            for p in template.find_resources("AWS::IAM::Policy").values()
            if {"Ref": logical} in p["Properties"]["Roles"]
            for s in p["Properties"]["PolicyDocument"]["Statement"]
        ]
        assert actions(statements) == {"bedrock-agentcore:InvokeGateway"}, name


def test_the_gateway_may_invoke_the_tool_lambdas_and_ask_the_engine(template):
    (gateway,) = template.find_resources("AWS::BedrockAgentCore::Gateway").values()
    role = gateway["Properties"]["RoleArn"]["Fn::GetAtt"][0]
    statements = [
        s
        for p in template.find_resources("AWS::IAM::Policy").values()
        if {"Ref": role} in p["Properties"]["Roles"]
        for s in p["Properties"]["PolicyDocument"]["Statement"]
    ]
    assert actions(statements) == {
        "lambda:InvokeFunction",
        # Asking the policy engine about each call (step 4).
        "bedrock-agentcore:AuthorizeAction",
        "bedrock-agentcore:PartiallyAuthorizeActions",
        "bedrock-agentcore:GetPolicyEngine",
    }


def test_the_cmms_is_a_stub_and_says_so(template):
    (table,) = template.find_resources("AWS::DynamoDB::Table").values()
    assert table["Properties"]["TableName"] == "crestline-cmms-stub"
    assert table["DeletionPolicy"] == "Delete"


def test_traces_go_to_xray_and_application_logs_to_cloudwatch(template):
    sources = {
        r["Properties"]["LogType"]
        for r in template.find_resources("AWS::Logs::DeliverySource").values()
    }
    assert sources == {"TRACES", "APPLICATION_LOGS"}
    destinations = {
        r["Properties"]["DeliveryDestinationType"]
        for r in template.find_resources("AWS::Logs::DeliveryDestination").values()
    }
    assert destinations == {"XRAY", "CWL"}


def test_every_resource_that_takes_tags_has_the_three(template):
    tagged = 0
    for logical, resource in template.to_json()["Resources"].items():
        tags = resource.get("Properties", {}).get("Tags")
        if tags is None:
            continue
        found = {t["Key"]: t["Value"] for t in tags} if isinstance(tags, list) else tags
        for key, value in TAGS.items():
            assert found.get(key) == value, (logical, key)
        tagged += 1
    assert tagged >= 10
