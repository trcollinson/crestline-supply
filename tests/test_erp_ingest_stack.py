"""ErpIngestStack: the scheduled ERP poller, checked in its synthesized template."""

import json
import re
from pathlib import Path

import pytest
from aws_cdk import assertions

from app import build_app
from erp_extract.entities import ENTITIES
from stacks import erp_api, erp_ingest

INFRA_DIR = Path(__file__).resolve().parent.parent / "infra"
TAGS = {"project": "crestline", "layer": "erp-ingest", "milestone": "M2"}


def synth(**context) -> assertions.Template:
    base = json.loads((INFRA_DIR / "cdk.json").read_text())["context"]
    app = build_app(base | {"aws:cdk:bundling-stacks": []} | context)
    return assertions.Template.from_stack(app.node.find_child("CrestlineErpIngest"))


@pytest.fixture(scope="module")
def template() -> assertions.Template:
    return synth()


def only(template, kind: str) -> dict:
    [resource] = template.find_resources(kind).values()
    return resource


def function(template, handler: str) -> tuple[str, dict]:
    """The Lambda with this handler: its logical ID and resource."""
    [(name, resource)] = [
        (name, resource)
        for name, resource in template.find_resources("AWS::Lambda::Function").items()
        if resource["Properties"]["Handler"] == handler
    ]
    return name, resource


def role_statements(template, handler: str) -> list[dict]:
    """The policy statements a Lambda's role was given."""
    role = function(template, handler)[1]["Properties"]["Role"]["Fn::GetAtt"][0]
    return [
        statement
        for policy in template.find_resources("AWS::IAM::Policy").values()
        if {"Ref": role} in policy["Properties"]["Roles"]
        for statement in policy["Properties"]["PolicyDocument"]["Statement"]
    ]


def workflow(template) -> dict:
    """The state machine's definition, with CloudFormation references stubbed."""
    parts = only(template, "AWS::StepFunctions::StateMachine")["Properties"][
        "DefinitionString"
    ]["Fn::Join"][1]
    return json.loads("".join(p if isinstance(p, str) else "REF" for p in parts))


def test_the_extractor_is_the_pipelines_package_and_nothing_else(template):
    props = function(template, "erp_extract.aws.handler")[1]["Properties"]
    assert (props["Runtime"], props["Architectures"]) == ("python3.12", ["arm64"])
    assert (props["MemorySize"], props["Timeout"]) == (256, 300)
    env = props["Environment"]["Variables"]
    assert env["CRESTLINE_ERP_URL_PARAMETER"] == erp_api.URL_PARAMETER
    assert (
        env["CRESTLINE_ERP_CLIENT_SECRET_PARAMETER"]
        == "/crestline/erp-api/client-secret"
    )
    assert env["CRESTLINE_ERP_CLIENT_ID"] == "crestline-poller"
    assert env["CRESTLINE_SWEEP_EVERY"] == "1"
    assert "Fn::GetStackOutput" in json.dumps(env["CRESTLINE_RAW_BUCKET"])


def test_it_may_write_raw_pages_and_its_own_state_and_read_two_parameters(template):
    statements = role_statements(template, "erp_extract.aws.handler")
    text = json.dumps(statements)
    assert "/source=erp-api/*" in text  # only under the poller's own prefix
    assert "s3:DeleteObject" not in text
    assert "dynamodb:PutItem" in text and "dynamodb:GetItem" in text
    assert "parameter/crestline/erp-api/url" in text
    assert "parameter/crestline/erp-api/client-secret" in text
    assert "signing-key" not in text  # the API's secret, not the client's


def test_state_is_an_on_demand_table_keyed_by_entity_that_goes_with_the_stack(template):
    table = only(template, "AWS::DynamoDB::Table")
    assert table["Properties"]["BillingMode"] == "PAY_PER_REQUEST"
    assert table["Properties"]["KeySchema"] == [
        {"AttributeName": "entity", "KeyType": "HASH"}
    ]
    assert table["DeletionPolicy"] == "Delete"


def test_a_run_maps_every_entity_three_at_a_time_retrying_failures(template):
    states = workflow(template)["States"]
    assert [
        item["entity"] for item in states["List the entities"]["Result"]["entities"]
    ] == list(ENTITIES)
    each = states["Each entity"]
    assert each["MaxConcurrency"] == erp_ingest.MAX_CONCURRENCY == 3
    task = each["ItemProcessor"]["States"]["Extract the entity"]
    [retry] = [r for r in task["Retry"] if "EntityFailed" in r["ErrorEquals"]]
    assert (retry["IntervalSeconds"], retry["MaxAttempts"], retry["BackoffRate"]) == (
        30,
        2,
        2,
    )
    [catch] = task["Catch"]
    assert catch["ErrorEquals"] == ["States.ALL"] and catch["ResultPath"] == "$.error"


def test_a_run_fails_naming_the_entities_that_failed(template):
    states = workflow(template)["States"]
    choice = states["Any failed?"]
    assert choice["QueryLanguage"] == "JSONata"
    assert "$exists(error)" in choice["Choices"][0]["Condition"]
    failed = states[choice["Choices"][0]["Next"]]
    assert failed["Type"] == "Fail" and failed["Error"] == "EntitiesFailed"
    assert states[choice["Default"]]["Type"] == "Succeed"
    assert (
        only(template, "AWS::StepFunctions::StateMachine")["Properties"][
            "StateMachineType"
        ]
        == "STANDARD"
    )


def test_the_schedule_starts_a_run_hourly_unless_told_otherwise(template):
    props = only(template, "AWS::Scheduler::Schedule")["Properties"]
    assert props["ScheduleExpression"] == "rate(1 hour)"
    assert props["State"] == "ENABLED"
    [machine] = template.find_resources("AWS::StepFunctions::StateMachine")
    assert props["Target"]["Arn"] == {"Ref": machine}

    other = only(
        synth(erpIngestSchedule="rate(10 minutes)"), "AWS::Scheduler::Schedule"
    )
    assert other["Properties"]["ScheduleExpression"] == "rate(10 minutes)"


def test_logs_are_kept_a_week_and_resources_carry_the_cost_tags(template):
    groups = template.find_resources("AWS::Logs::LogGroup").values()
    assert len(groups) == 2  # the extractor's and the curator's
    for group in groups:
        assert (group["Properties"]["RetentionInDays"], group["DeletionPolicy"]) == (
            7,
            "Delete",
        )
    for kind in (
        "AWS::Lambda::Function",
        "AWS::DynamoDB::Table",
        "AWS::StepFunctions::StateMachine",
        "AWS::Logs::LogGroup",
    ):
        for resource in template.find_resources(kind).values():
            tags = resource["Properties"].get("Tags") or resource["Properties"].get(
                "TagsEntry"
            )
            pairs = {t["Key"]: t["Value"] for t in tags}
            assert TAGS.items() <= pairs.items(), kind


def test_the_template_names_no_account_or_region(template):
    text = json.dumps(template.to_json())
    assert not re.search(r"\b\d{12}\b", text)
    assert not re.search(r"\b(?:us|eu|ap|ca|sa)-[a-z]+-\d\b", text)


def test_the_api_publishes_its_url_for_its_clients():
    base = json.loads((INFRA_DIR / "cdk.json").read_text())["context"]
    app = build_app(base | {"aws:cdk:bundling-stacks": []})
    api = assertions.Template.from_stack(app.node.find_child("CrestlineErpApi"))
    api.has_resource_properties(
        "AWS::SSM::Parameter", {"Name": erp_api.URL_PARAMETER, "Type": "String"}
    )
    # The poller's stack doesn't depend on the API's: deploying one never redeploys the other.
    ingest = app.node.find_child("CrestlineErpIngest")
    assert "CrestlineErpApi" not in [d.stack_name for d in ingest.dependencies]


def test_the_curator_is_the_same_package_with_its_own_handler(template):
    _, extractor = function(template, "erp_extract.aws.handler")
    _, curator = function(template, "erp_extract.curate.handler")
    props = curator["Properties"]
    assert props["Code"] == extractor["Properties"]["Code"]
    assert (props["Runtime"], props["Architectures"]) == ("python3.12", ["arm64"])
    env = props["Environment"]["Variables"]
    assert env["CRESTLINE_ATHENA_WORKGROUP"] == "crestline"
    assert env["CRESTLINE_RAW_DATABASE"] == "crestline_raw"
    assert env["CRESTLINE_CURATED_DATABASE"] == "crestline_curated"
    assert "Fn::GetStackOutput" in json.dumps(env["CRESTLINE_RAW_BUCKET"])


def test_the_curator_reads_raw_pages_and_writes_only_curated_tables(template):
    statements = role_statements(template, "erp_extract.curate.handler")
    text = json.dumps(statements)
    assert "workgroup/crestline" in text
    assert "table/crestline_raw/raw_erp_api" in text
    assert "table/crestline_curated/*" in text
    for statement in statements:
        actions = json.dumps(statement["Action"])
        resources = json.dumps(statement["Resource"])
        if "s3:PutObject" in actions or "s3:DeleteObject" in actions:
            # The results bucket (Athena's output) or the curated bucket, never raw.
            assert "RawBucket" not in resources, statement
        if "s3:DeleteObject" in actions:
            assert "CuratedBucket" in resources, statement


def test_each_entity_is_curated_right_after_it_is_extracted(template):
    steps = workflow(template)["States"]["Each entity"]["ItemProcessor"]["States"]
    assert steps["Extract the entity"]["Next"] == "Curate the entity"
    curate = steps["Curate the entity"]
    assert curate["ResultPath"] == "$.curated"  # beside the extractor's report
    [retry] = [r for r in curate["Retry"] if "CurateFailed" in r["ErrorEquals"]]
    assert (retry["IntervalSeconds"], retry["MaxAttempts"]) == (30, 2)
    [catch] = curate["Catch"]
    assert catch["ErrorEquals"] == ["States.ALL"] and catch["ResultPath"] == "$.error"
    # A failed extraction skips its curate step.
    [catch] = steps["Extract the entity"]["Catch"]
    assert steps[catch["Next"]]["Type"] == "Pass" and "Next" not in steps[catch["Next"]]


def test_raw_pages_are_a_projected_table_of_text_lines(template):
    [table] = template.find_resources("AWS::Glue::Table").values()
    props = table["Properties"]
    assert props["DatabaseName"] == "crestline_raw"
    table_input = props["TableInput"]
    assert table_input["Name"] == "raw_erp_api"
    assert [k["Name"] for k in table_input["PartitionKeys"]] == [
        "entity",
        "ingest_date",
    ]
    parameters = table_input["Parameters"]
    assert parameters["projection.enabled"] == "true"
    assert parameters["projection.entity.values"].split(",") == list(ENTITIES)
    assert parameters["projection.ingest_date.type"] == "date"
    storage = table_input["StorageDescriptor"]
    assert storage["Columns"] == [{"Name": "line", "Type": "string"}]
    assert "/source=erp-api/" in json.dumps(storage["Location"])
    assert storage["SerdeInfo"]["SerializationLibrary"].endswith("LazySimpleSerDe")
    # Wide: Athena sees no page filed outside the range, and the curate step
    # refuses a window that is (an unseen sweep would look empty).
    assert parameters["projection.ingest_date.range"] == "2020-01-01,2099-12-31"


def test_a_run_that_does_not_succeed_is_sent_to_an_alarm_topic(template):
    [rule] = template.find_resources("AWS::Events::Rule").values()
    pattern = rule["Properties"]["EventPattern"]
    assert pattern["source"] == ["aws.states"]
    assert pattern["detail-type"] == ["Step Functions Execution Status Change"]
    assert pattern["detail"]["status"] == ["FAILED", "TIMED_OUT", "ABORTED"]
    [(machine, _)] = template.find_resources("AWS::StepFunctions::StateMachine").items()
    assert pattern["detail"]["stateMachineArn"] == [{"Ref": machine}]
    [target] = rule["Properties"]["Targets"]
    [(topic, resource)] = template.find_resources("AWS::SNS::Topic").items()
    assert target["Arn"] == {"Ref": topic}
    pairs = {t["Key"]: t["Value"] for t in resource["Properties"]["Tags"]}
    assert TAGS.items() <= pairs.items()
    # No address unless one is given at deploy time.
    template.resource_count_is("AWS::SNS::Subscription", 0)


def test_an_alarm_email_given_at_deploy_time_is_subscribed():
    alarmed = synth(erpIngestAlarmEmail="ops@example.com")
    alarmed.has_resource_properties(
        "AWS::SNS::Subscription", {"Protocol": "email", "Endpoint": "ops@example.com"}
    )


def test_the_curator_writes_audits_to_the_curated_bucket_and_emails_the_alarm(
    template,
):
    _, curator = function(template, "erp_extract.curate.handler")
    env = curator["Properties"]["Environment"]["Variables"]
    [topic] = template.find_resources("AWS::SNS::Topic")
    assert env["CRESTLINE_ALARM_TOPIC"] == {"Ref": topic}
    assert "CuratedBucket" in json.dumps(env["CRESTLINE_CURATED_BUCKET"])
    statements = role_statements(template, "erp_extract.curate.handler")
    [publish] = [s for s in statements if s["Action"] == "sns:Publish"]
    assert publish["Resource"] == {"Ref": topic}
    # The snapshot ID comes from the table's metadata, which Glue points to.
    assert any("glue:GetTable" in json.dumps(s["Action"]) for s in statements)
