"""The Cedar rules in infra/policies/ (M4 step 4), checked without AWS.

What can be checked before a deploy: each file is one readable rule that
names only real tools and the two agent roles, the machine list is the
Lambda's, and the stack creates one policy per file with the right
validation and mode. Whether AgentCore decides each call as the rules say
is test_policies_remote.py's job, against the deployed gateway.
"""

import json
import re
from pathlib import Path

import pytest
from aws_cdk import assertions

from agent_tools.tools import MACHINES, TARGETS
from app import build_app

REPO = Path(__file__).resolve().parent.parent
POLICIES = sorted((REPO / "infra" / "policies").glob("*.cedar"))
ACTIONS = {
    f"{target}___{tool.name}" for target, tools in TARGETS.items() for tool in tools
}
ROLES = {"crestline-ops-analyst", "crestline-maintenance-planner"}


def test_there_are_six_rules():
    assert [p.stem for p in POLICIES] == [
        "forbid_plant_scada",
        "permit_internal_reports",
        "permit_job_margin",
        "permit_job_status",
        "permit_machine_reads",
        "permit_maintenance_requests",
    ]


@pytest.mark.parametrize("path", POLICIES, ids=lambda p: p.stem)
def test_each_file_is_one_commented_rule(path):
    text = path.read_text()
    assert text.startswith("// "), "a rule opens with what it says, in words"
    code = "\n".join(line for line in text.splitlines() if not line.startswith("//"))
    effects = re.findall(r"^(permit|forbid) \(", code, re.MULTILINE)
    assert effects == [path.stem.split("_")[0]]
    assert code.rstrip().endswith(";") and code.count(";") == 1


@pytest.mark.parametrize("path", POLICIES, ids=lambda p: p.stem)
def test_each_rule_names_real_tools_and_only_the_agent_roles(path):
    text = path.read_text()
    actions = set(re.findall(r'AgentCore::Action::"([^"]+)"', text))
    assert actions and actions <= ACTIONS
    assert set(re.findall(r"assumed-role/([a-z-]+)", text)) <= ROLES
    # Only what Fn::Sub fills in: no account ID or gateway in the repo.
    assert set(re.findall(r"\$\{([^}]+)\}", text)) <= {"GatewayArn", "AWS::AccountId"}
    assert not re.search(r"\d{12}", text)


def test_the_machine_list_in_cedar_is_the_lambdas():
    text = (REPO / "infra" / "policies" / "permit_machine_reads.cedar").read_text()
    listed = re.search(r"\[([^\]]*)\]\.contains\(context\.input\.machine_id\)", text)
    assert tuple(json.loads(f"[{listed.group(1)}]")) == MACHINES


def test_every_tool_but_plant_scada_has_a_permit_and_plant_scada_a_forbid():
    permitted, forbidden = set(), set()
    for path in POLICIES:
        found = set(re.findall(r'AgentCore::Action::"([^"]+)"', path.read_text()))
        (forbidden if path.stem.startswith("forbid_") else permitted).update(found)
    scada = {a for a in ACTIONS if a.startswith("PlantScada___")}
    assert permitted == ACTIONS - scada
    assert forbidden == scada


def template(**context) -> assertions.Template:
    base = json.loads((REPO / "infra" / "cdk.json").read_text())["context"]
    app = build_app(base | {"aws:cdk:bundling-stacks": []} | context)
    return assertions.Template.from_stack(app.node.find_child("CrestlineAgent"))


def policies(t: assertions.Template) -> dict[str, dict]:
    return {
        r["Properties"]["Name"]: r["Properties"]
        for r in t.find_resources("AWS::BedrockAgentCore::Policy").values()
    }


def mode(t: assertions.Template) -> str:
    (gateway,) = t.find_resources("AWS::BedrockAgentCore::Gateway").values()
    return gateway["Properties"]["PolicyEngineConfiguration"]["Mode"]


@pytest.fixture(scope="module")
def default():
    return template()


def test_the_stack_creates_one_policy_per_file_and_only_the_forbid_skips_findings(
    default,
):
    found = policies(default)
    assert set(found) == {p.stem for p in POLICIES}
    assert {
        n for n, p in found.items() if p["ValidationMode"] == "IGNORE_ALL_FINDINGS"
    } == {"forbid_plant_scada"}
    sub = found["permit_maintenance_requests"]["Definition"]["Cedar"]["Statement"][
        "Fn::Sub"
    ]
    assert (
        sub[0]
        == (
            REPO / "infra" / "policies" / "permit_maintenance_requests.cedar"
        ).read_text()
    )
    assert "Overly" not in found["forbid_plant_scada"]["Description"]
    assert found["permit_job_status"]["Description"].startswith("Either agent role")


def test_the_engine_enforces_unless_told_otherwise(default):
    assert mode(default) == "ENFORCE"
    assert mode(template(agentPolicyMode="LOG_ONLY")) == "LOG_ONLY"
    with pytest.raises(ValueError, match="agentPolicyMode"):
        template(agentPolicyMode="AUDIT")


def test_the_scada_forbid_can_be_left_out_and_nothing_else_changes(default):
    without = policies(template(agentScadaForbid="false"))
    assert set(without) == set(policies(default)) - {"forbid_plant_scada"}


# --- The red-team eval's configurations (M4 step 6) ------------------------------


def guardrail_actions(t: assertions.Template) -> bool:
    return any(
        "bedrock:InvokeGuardrailChecks" in json.dumps(r["Properties"])
        for r in t.find_resources("AWS::IAM::Policy").values()
    )


def test_by_default_there_is_no_guardrail_and_the_rules_are_cedars(default):
    assert not any(name.startswith("guardrail_") for name in policies(default))
    assert "permit_everything" not in policies(default)
    assert not guardrail_actions(default)


def test_configuration_b_is_the_open_permit_and_the_guardrail_alone():
    t = template(agentPolicySet="open", agentGuardrail="true")
    got = policies(t)
    reads = {f"guardrail_{t.name}" for t in TARGETS["PlantData"]}
    assert set(got) == {"permit_everything"} | reads
    assert got["permit_everything"]["ValidationMode"] == "IGNORE_ALL_FINDINGS"
    for name in reads:  # one per read tool: a guardrail rule must name its action
        text = json.dumps(got[name]["Definition"]["Policy"]["Statement"])
        assert "suppressOutput" in text and "PromptAttack" in text
        assert f"PlantData___{name.removeprefix('guardrail_')}" in text
        assert "//" not in text  # the comment stays in the file
    assert guardrail_actions(t)
    assert mode(t) == "ENFORCE"


def test_configuration_d_is_the_six_rules_and_the_guardrail(default):
    t = template(agentGuardrail="true")
    reads = {f"guardrail_{t.name}" for t in TARGETS["PlantData"]}
    assert set(policies(t)) == set(policies(default)) | reads


def test_an_unknown_policy_set_is_refused():
    with pytest.raises(ValueError, match="policy_set"):
        template(agentPolicySet="lenient")


def test_the_eval_policies_name_only_the_agent_roles_and_threshold():
    open_rule = (REPO / "infra/policies/eval/permit_everything.cedar").read_text()
    assert open_rule.count("assumed-role/crestline-") == 2
    guardrail = (
        REPO / "infra/policies/eval/suppress_prompt_attacks.policy"
    ).read_text()
    assert 'greaterThan(decimal("0.5"))' in guardrail
    assert '"$..text"' in guardrail
    assert '["JAILBREAK"]' in guardrail and '["PROMPT_INJECTION"]' in guardrail
