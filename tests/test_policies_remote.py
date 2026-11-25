"""The policy engine's decisions, call by call, on the deployed gateway (M4 step 4).

No model: each row is one `tools/call` straight to the gateway as one role,
with the rule that should decide it. Run against whatever the stack has
deployed and checks what that deployment should do:

- ENFORCE (the default): a permitted call runs; a denied one is JSON-RPC
  error -32002 naming the rule, or saying it was denied by default.
- LOG_ONLY (`-c agentPolicyMode=LOG_ONLY`): every call runs, and the
  gateway's logs still record the decision each would have had.
- Without the PlantScada forbid (`-c agentScadaForbid=false`): the SCADA
  tools are still denied, now by default, because nothing permits them.

In every mode the gateway's logs must show each call's decision and the
rule that made it (under LOG_ONLY nothing else does: crestline_agent.decisions). Opt-in, because it calls AWS: `pytest -m athena`. The writes it
permits land in the stub CMMS and the outbox, marked "M4 step 4 check".
"""

import time

import boto3
import pytest

from crestline_agent.decisions import decisions
from crestline_agent.gateway import connect, stack_outputs

pytestmark = pytest.mark.athena

A, P, ADMIN = "ops-analyst", "maintenance-planner", None
DEFAULT = "default deny"
FORBID = "forbid_plant_scada"
CHECK = "M4 step 4 check: ignore"


def request(machine="PB-02", priority="low", category="other"):
    return {
        "machine_id": machine,
        "priority": priority,
        "category": category,
        "description": CHECK,
    }


def report(recipient):
    return {"recipient": recipient, "subject": CHECK, "body": CHECK}


# (role, tool, arguments, the rule that decides it)
ROWS = [
    (A, "downtime_events", {"machine_id": "PB-02", "days": 31}, "permit_machine_reads"),
    (A, "downtime_events", {"machine_id": "PB-02", "days": 32}, DEFAULT),
    (A, "downtime_events", {"machine_id": "PB-9", "days": 7}, DEFAULT),
    (
        A,
        "downtime_events",
        {"machine_id": "LSR-01", "days": 7, "as_of": "2026-07-10"},
        "permit_machine_reads",
    ),
    (
        A,
        "machine_downtime",
        {"machine_id": "PC-LINE", "days": 1},
        "permit_machine_reads",
    ),
    (A, "cycles_by_day", {"machine_id": "PEM-01", "days": 1}, "permit_machine_reads"),
    (A, "scrap_by_day", {"machine_id": "LSR-01", "days": 31}, "permit_scrap_by_day"),
    (A, "scrap_by_day", {"machine_id": "LSR-01", "days": 32}, DEFAULT),
    (A, "scrap_by_day", {"machine_id": "DB-01", "days": 7}, DEFAULT),
    (A, "job_margin", {"days": 31}, "permit_job_margin"),
    (A, "job_margin", {"days": 32}, DEFAULT),
    (A, "job_status", {"job_num": "J104221"}, "permit_job_status"),
    (A, "send_report", report("ops@crestline.example"), "permit_internal_reports"),
    (A, "send_report", report("ops@crestline.example.evil.com"), DEFAULT),
    (A, "send_report", report("OPS@CRESTLINE.EXAMPLE"), DEFAULT),
    (A, "send_report", report("plant-reports@evil.example"), DEFAULT),
    (A, "create_maintenance_request", request(), DEFAULT),
    (A, "write_tag", {"tag": "PB02.N7:20", "value": "0"}, FORBID),
    (A, "read_tag", {"tag": "PB02.N7:20"}, FORBID),
    (P, "downtime_events", {"machine_id": "PB-01", "days": 31}, "permit_machine_reads"),
    (P, "downtime_events", {"machine_id": "PB-01", "days": 32}, DEFAULT),
    (P, "job_status", {"job_num": "J104221"}, "permit_job_status"),
    (P, "scrap_by_day", {"machine_id": "PB-02", "days": 31}, "permit_scrap_by_day"),
    (P, "scrap_by_day", {"machine_id": "PB-02", "days": 32}, DEFAULT),
    (
        P,
        "create_maintenance_request",
        request(category="mechanical"),
        "permit_maintenance_requests",
    ),
    (
        P,
        "create_maintenance_request",
        request("PB-01", "normal", "tooling"),
        "permit_maintenance_requests",
    ),
    (P, "create_maintenance_request", request(priority="high"), DEFAULT),
    (
        P,
        "create_maintenance_request",
        request("PC-LINE", "emergency", "electrical"),
        DEFAULT,
    ),
    (
        P,
        "create_maintenance_request",
        request(priority="normal", category="safety"),
        DEFAULT,
    ),
    (P, "send_report", report("ops@crestline.example"), DEFAULT),
    (P, "write_tag", {"tag": "PB02.N7:20", "value": "0"}, FORBID),
    (P, "read_tag", {"tag": "PB02.N7:20"}, FORBID),
    (ADMIN, "downtime_events", {"machine_id": "PB-02", "days": 7}, DEFAULT),
    (ADMIN, "scrap_by_day", {"machine_id": "LSR-01", "days": 7}, DEFAULT),
    (ADMIN, "create_maintenance_request", request(), DEFAULT),
    (ADMIN, "write_tag", {"tag": "PB02.N7:20", "value": "0"}, FORBID),
]
IDS = [f"{i:02d}-{role or 'admin'}-{tool}" for i, (role, tool, _, _) in enumerate(ROWS)]

READS = {
    "downtime_events",
    "machine_downtime",
    "cycles_by_day",
    "scrap_by_day",
    "job_margin",
    "job_status",
}
TARGET_OF = {
    "PlantData": READS,
    "Maintenance": {"create_maintenance_request"},
    "Reports": {"send_report"},
    "PlantScada": {"write_tag", "read_tag"},
}
SEES = {
    A: {f"PlantData___{t}" for t in READS} | {"Reports___send_report"},
    P: {f"PlantData___{t}" for t in READS}
    | {"Maintenance___create_maintenance_request"},
    ADMIN: set(),
}


@pytest.fixture(scope="module")
def deployed() -> dict:
    """What the stack deployed: the engine's mode, and whether the forbid is in."""
    session = boto3.Session()
    resources = session.client("cloudformation").describe_stack_resources(
        StackName="CrestlineAgent"
    )["StackResources"]
    outputs = stack_outputs(session)
    return {
        "mode": outputs["PolicyMode"],
        # A second copy of the stack names its roles apart (M5, another region).
        "role_suffix": outputs.get("RoleSuffix", ""),
        "forbid": any(
            r["LogicalResourceId"] == "PolicyForbidPlantScada" for r in resources
        ),
    }


def expected(rule: str, deployed: dict) -> str:
    """The rule that decides a row here: with the forbid left out, nothing else
    names the SCADA tools, so default deny does."""
    return DEFAULT if rule == FORBID and not deployed["forbid"] else rule


@pytest.fixture(scope="module")
def gateways() -> dict:
    return {role: connect(role) for role in (A, P, ADMIN)}


@pytest.fixture(scope="module")
def replies(gateways) -> tuple[int, list[dict]]:
    """Every row called once, in order, and when the calls started (for the logs)."""
    started = int(time.time() * 1000) - 60_000
    out = []
    for role, tool, arguments, _ in ROWS:
        gateway = gateways[role]
        target = next(t for t in TARGET_OF if tool in TARGET_OF[t])
        out.append(gateway.call(f"{target}___{tool}", arguments))
    return started, out


@pytest.mark.parametrize("index", range(len(ROWS)), ids=IDS)
def test_each_call_gets_the_answer_its_rule_gives(index, replies, deployed):
    _, out = replies
    reply = out[index]
    rule = expected(ROWS[index][3], deployed)
    permitted = rule.startswith("permit_")
    if deployed["mode"] == "LOG_ONLY" or permitted:
        assert "result" in reply, reply  # it ran
        return
    error = reply.get("error", {})
    assert error.get("code") == -32002, reply
    if rule == DEFAULT:
        assert "denied by default" in error["message"]
    else:
        assert f"denied due to {rule}-" in error["message"]


def test_the_logs_record_every_decision_and_the_rule_behind_it(replies, deployed):
    started, out = replies
    ids = [reply["_request_id"] for reply in out]
    found = decisions(ids, since_ms=started)
    assert set(found) == set(ids), (
        f"{len(set(ids) - set(found))} decisions never logged"
    )
    for (role, tool, _, rule), request_id in zip(ROWS, ids):
        got = found[request_id]
        rule = expected(rule, deployed)
        assert (
            got.principal.endswith(
                f"assumed-role/crestline-{role}{deployed['role_suffix']}"
            )
            if role
            else ("crestline-" not in got.principal)
        ), (role, tool, got)
        if rule.startswith("permit_"):
            assert (got.decision, got.policies) == ("ALLOW", (rule,)), (role, tool, got)
        elif rule == DEFAULT:
            assert (got.decision, got.policies) == ("DENY", ()), (role, tool, got)
        else:
            assert (got.decision, got.policies) == ("DENY", (rule,)), (role, tool, got)


def test_plant_scada_is_denied_with_or_without_its_forbid(replies, deployed):
    """The plan's check, said out loud: take the forbid away and default deny
    still refuses both SCADA tools, because no rule permits them."""
    if deployed["mode"] != "ENFORCE":
        pytest.skip("LOG_ONLY runs every call; the logs test covers it")
    _, out = replies
    scada = [
        r for (_, tool, _, _), r in zip(ROWS, out) if tool in TARGET_OF["PlantScada"]
    ]
    assert len(scada) == 5
    for reply in scada:
        message = reply["error"]["message"]
        if deployed["forbid"]:
            assert f"denied due to {FORBID}-" in message
        else:
            assert "denied by default" in message
    print(
        f"PlantScada denied {'by the forbid' if deployed['forbid'] else 'by default'}"
    )


@pytest.mark.parametrize("role", [A, P, ADMIN], ids=["ops-analyst", "planner", "admin"])
def test_tools_list_shows_each_role_only_what_it_could_ever_call(
    role, gateways, deployed
):
    names = {t["name"] for t in gateways[role].tools()}
    if deployed["mode"] == "LOG_ONLY":
        assert len(names) == 10  # nothing is hidden when nothing is enforced
    else:
        assert names == SEES[role]
