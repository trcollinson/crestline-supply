"""BriefStack (M5 step 6): the schedule, the function, and what it may touch,
checked in the synthesized template; and the package's import closure."""

import ast
import json
import sys
from pathlib import Path

import pytest
from aws_cdk import assertions

from app import build_app
from stacks.brief import MODULES, PACKAGES

REPO_ROOT = Path(__file__).resolve().parent.parent
INFRA_DIR = REPO_ROOT / "infra"
TAGS = {"project": "crestline", "layer": "brief", "milestone": "M5"}


def synth(**context: str) -> assertions.Template:
    base = json.loads((INFRA_DIR / "cdk.json").read_text())["context"]
    app = build_app(base | {"aws:cdk:bundling-stacks": []} | context)
    return assertions.Template.from_stack(app.node.find_child("CrestlineBrief"))


@pytest.fixture(scope="module")
def template() -> assertions.Template:
    return synth(
        briefEmail="plant.manager@crestline.example",
        briefClockStart="2026-08-12",
        briefClockAnchor="2026-11-23",
    )


def function(template) -> dict:
    (fn,) = [
        r
        for r in template.find_resources("AWS::Lambda::Function").values()
        if r["Properties"].get("Handler") == "crestline.brief.aws.handler"
    ]
    return fn


def statements(template) -> list[dict]:
    role = function(template)["Properties"]["Role"]["Fn::GetAtt"][0]
    return [
        s
        for p in template.find_resources("AWS::IAM::Policy").values()
        if {"Ref": role} in p["Properties"]["Roles"]
        for s in p["Properties"]["PolicyDocument"]["Statement"]
    ]


def actions_on(template, action: str) -> list:
    return [
        s["Resource"]
        for s in statements(template)
        if action in ([s["Action"]] if isinstance(s["Action"], str) else s["Action"])
    ]


def test_it_runs_at_0530_plant_time_on_weekdays(template):
    (schedule,) = template.find_resources("AWS::Scheduler::Schedule").values()
    props = schedule["Properties"]
    assert props["ScheduleExpression"] == "cron(30 5 ? * MON-FRI *)"
    assert props["ScheduleExpressionTimezone"] == "America/Denver"
    assert props["Target"]["RetryPolicy"]["MaximumRetryAttempts"] == 0
    assert props["Target"]["Input"] == "{}"


def test_a_test_schedule_replaces_it():
    (schedule,) = (
        synth(briefSchedule="cron(15 14 ? * FRI *)")
        .find_resources("AWS::Scheduler::Schedule")
        .values()
    )
    assert schedule["Properties"]["ScheduleExpression"] == "cron(15 14 ? * FRI *)"
    assert schedule["Properties"]["ScheduleExpressionTimezone"] == "America/Denver"


def test_the_function_carries_the_clock_and_the_model(template):
    env = function(template)["Properties"]["Environment"]["Variables"]
    assert env["CRESTLINE_BRIEF_CLOCK_START"] == "2026-08-12"
    assert env["CRESTLINE_BRIEF_CLOCK_ANCHOR"] == "2026-11-23"
    assert env["CRESTLINE_BRIEF_LAST_MORNING"] == "2026-10-01"
    assert env["CRESTLINE_BRIEF_MODEL"] == "sonnet"
    assert env["CRESTLINE_BRIEF_DATABASE"] == "crestline_brief"
    props = function(template)["Properties"]
    assert (props["Runtime"], props["Architectures"]) == ("python3.12", ["arm64"])
    assert props["Timeout"] == 900


def test_without_a_clock_there_is_none_and_half_a_clock_is_refused():
    env = function(synth())["Properties"]["Environment"]["Variables"]
    assert "CRESTLINE_BRIEF_CLOCK_START" not in env
    with pytest.raises(ValueError, match="go together"):
        synth(briefClockStart="2026-08-12")


def test_the_email_subscription_is_the_one_given(template):
    (sub,) = template.find_resources("AWS::SNS::Subscription").values()
    assert sub["Properties"]["Protocol"] == "email"
    assert sub["Properties"]["Endpoint"] == "plant.manager@crestline.example"


def test_it_changes_one_database_and_calls_two_models(template):
    [writes] = actions_on(template, "glue:CreateTable")
    assert "table/crestline_brief/*" in json.dumps(writes)
    assert not any("crestline_raw" in json.dumps(r) for r in writes)
    assert not any("crestline_curated" in json.dumps(r) for r in writes)
    [models] = actions_on(template, "bedrock:InvokeModel")
    text = json.dumps(models)
    assert "inference-profile/us.anthropic.claude-sonnet-4-6" in text
    assert "inference-profile/us.amazon.nova-2-lite-v1:0" in text
    assert text.count("foundation-model/") == 2


def test_every_resource_is_tagged(template):
    for resource in template.to_json()["Resources"].values():
        tags = resource.get("Properties", {}).get("Tags")
        if isinstance(tags, list) and tags and isinstance(tags[0], dict):
            found = {t["Key"]: t["Value"] for t in tags if "Key" in t}
            for key, value in TAGS.items():
                assert found.get(key) == value, resource["Type"]


# --- The package --------------------------------------------------------------

STDLIB_OR_RUNTIME = {"boto3", "botocore", "polars"}


def imports(path: Path) -> set[str]:
    """What a module imports when it loads: its top-level imports, not those
    inside a function (DuckDB's, in facts.py) or under TYPE_CHECKING."""
    found = set()
    for node in ast.parse(path.read_text()).body:
        if isinstance(node, ast.Import):
            found |= {a.name for a in node.names}
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            found.add(node.module)
    return found


def test_the_package_holds_everything_the_handler_imports_and_no_simulator():
    """Walk the imports from the handler: every `crestline` module it reaches
    must be one the bundle copies, and none is the simulator or DuckDB's."""
    packaged = {f"crestline.{m.removesuffix('.py')}" for m in MODULES} | {
        f"crestline.brief.{p.stem}"
        for p in (REPO_ROOT / "src/crestline/brief").glob("*.py")
    }
    packaged = {m.removesuffix(".__init__") for m in packaged}
    seen, todo = set(), ["crestline.brief.aws"]
    while todo:
        module = todo.pop()
        if module in seen:
            continue
        seen.add(module)
        path = REPO_ROOT / "src" / Path(*module.split("."))
        path = path / "__init__.py" if path.is_dir() else path.with_suffix(".py")
        for name in imports(path):
            if name.startswith("crestline"):
                todo.append(name)
            else:
                top = name.split(".")[0]
                assert top in STDLIB_OR_RUNTIME or top in sys.stdlib_module_names, (
                    f"{module} imports {name} at load, which the package doesn't carry"
                )
    assert seen <= packaged, seen - packaged
    assert not any(
        m.startswith("crestline.sim") or m == "crestline.query" for m in seen
    )
    assert set(PACKAGES) == {"polars", "polars-runtime-32"}
