"""The agent's tools through the real AgentCore Gateway (M4 step 3).

Each read tool, called from the laptop as `crestline-ops-analyst`, must give
the same rows as the same SQL file run in DuckDB over ./out with the same
parameters (or over CRESTLINE_OUT, the backfill the stacks hold: out-m5 in
M5's region). The writes must land in the stub CMMS and the outbox, each made
by the role the policies let make it (step 4). The honeypot must log every
call that reaches it under LOG_ONLY, and under ENFORCE never be invoked.
Which rule decides which call is test_policies_remote.py's subject.

Opt-in, because it runs in AWS: `pytest -m athena`. It needs every stack
deployed, the backfill uploaded, the ERP poller's first run curated, the
90-day machine replay curated, and `crestline athena setup` run.
"""

import json
import time

import boto3
import pytest
from test_agent_tools import LAST_DAY

from agent_tools import plant_data
from agent_tools.tools import TARGETS
from crestline_agent.gateway import connect, stack_outputs

pytestmark = pytest.mark.athena

# Within the policies' 31 days; `as_of` reaches the rest of the 90.
READS = [
    ("downtime_events", {"machine_id": "PB-02", "days": 31}),
    ("downtime_events", {"machine_id": "LSR-01", "days": 7, "as_of": "2026-07-10"}),
    ("machine_downtime", {"machine_id": "PB-01", "days": 14}),
    ("machine_downtime", {"machine_id": "PC-LINE", "days": 7, "as_of": "2026-08-14"}),
    ("cycles_by_day", {"machine_id": "PB-02", "days": 31}),
    ("cycles_by_day", {"machine_id": "PEM-01", "days": 31, "as_of": "2026-08-02"}),
    ("scrap_by_day", {"machine_id": "LSR-01", "days": 31, "as_of": "2026-08-31"}),
    ("scrap_by_day", {"machine_id": "PB-02", "days": 31}),
    ("job_margin", {"days": 31}),
    ("job_margin", {"days": 31, "as_of": "2026-08-15"}),
]

# Short name -> the gateway's `<Target>___<tool>`.
NAMES = {
    tool.name: f"{target}___{tool.name}"
    for target, tools in TARGETS.items()
    for tool in tools
}


@pytest.fixture(autouse=True)
def last_day(monkeypatch):
    """The DuckDB side's window ends where the deployed Lambdas' does."""
    monkeypatch.setenv("CRESTLINE_DATA_LAST_DAY", LAST_DAY)


@pytest.fixture(scope="module")
def gateway():
    return connect("ops-analyst")


@pytest.fixture(scope="module")
def planner():
    return connect("maintenance-planner")


@pytest.fixture(scope="module")
def mode() -> str:
    return stack_outputs(boto3.Session())["PolicyMode"]


def call(gateway, tool: str, arguments: dict) -> tuple[dict, dict]:
    reply = gateway.call(NAMES[tool], arguments)
    assert "result" in reply, reply
    (content,) = reply["result"]["content"]
    return json.loads(content["text"]), reply


@pytest.mark.parametrize("tool, arguments", READS)
def test_a_read_through_the_gateway_equals_duckdb(gateway, duckdb_run, tool, arguments):
    got, reply = call(gateway, tool, arguments)
    expected = plant_data.answer(tool, arguments, duckdb_run)
    assert got == expected
    assert got["rows"], (tool, arguments)
    print(f"{tool} {arguments}: {len(got['rows'])} rows in {reply['_ms']} ms")


def test_job_status_through_the_gateway_equals_duckdb(gateway, duckdb_run):
    job = duckdb_run.con.sql(
        "select JobNum from erp_jobs where JobComplete order by JobNum limit 1"
    ).fetchone()[0]
    got, _ = call(gateway, "job_status", {"job_num": job})
    assert got == plant_data.answer("job_status", {"job_num": job}, duckdb_run)


def test_a_bad_argument_the_policy_lets_through_is_refused_by_the_lambda(gateway):
    """Cedar checks the policy's limits; a malformed date is the Lambda's to catch."""
    got, _ = call(
        gateway,
        "cycles_by_day",
        {"machine_id": "PB-02", "days": 7, "as_of": "yesterday"},
    )
    assert "as_of" in got["error"]


def test_the_writes_land(gateway, planner):
    session = boto3.Session()
    outputs = stack_outputs(session)

    request, _ = call(
        planner,
        "create_maintenance_request",
        {
            "machine_id": "PB-02",
            "priority": "low",
            "category": "other",
            "description": "M4 step 3 check: ignore",
        },
    )
    item = (
        session.resource("dynamodb")
        .Table(outputs["CmmsTableName"])
        .get_item(Key={"request_id": request["request_id"]})["Item"]
    )
    assert item["description"] == "M4 step 3 check: ignore"
    assert item["gateway_request_id"]

    queued, _ = call(
        gateway,
        "send_report",
        {"recipient": "ops@crestline.example", "subject": "check", "body": "M4 step 3"},
    )
    keys = [
        o["Key"]
        for o in session.client("s3").list_objects_v2(
            Bucket=outputs["AgentBucketName"], Prefix="outbox/"
        )["Contents"]
    ]
    assert any(queued["message_id"] in k for k in keys)


def honeypot_attempts(started: int, seconds: int) -> list[dict]:
    """Every call the SCADA honeypot logged since `started`, polling `seconds`."""
    session = boto3.Session()
    group = stack_outputs(session)["PlantScadaLogGroup"]
    logs = session.client("logs")
    attempts: list[dict] = []
    for _ in range(seconds // 2):
        events = logs.filter_log_events(
            logGroupName=group, startTime=started, filterPattern='"honeypot"'
        )["events"]
        attempts = [
            json.loads(e["message"]) for e in events if e["message"].startswith("{")
        ]
        if {a["honeypot"] for a in attempts} >= {"write_tag", "read_tag"}:
            break
        time.sleep(2)
    return attempts


def test_the_honeypot_logs_what_reaches_it_and_enforce_lets_nothing_reach_it(
    gateway, mode
):
    started = int(time.time() * 1000)
    written = gateway.call(NAMES["write_tag"], {"tag": "PB02.N7:20", "value": "0"})
    read = gateway.call(NAMES["read_tag"], {"tag": "PB02.N7:20"})
    if mode == "ENFORCE":
        assert written["error"]["code"] == read["error"]["code"] == -32002
        assert honeypot_attempts(started, seconds=30) == []
        return
    assert json.loads(written["result"]["content"][0]["text"])["status"] == "ok"
    attempts = honeypot_attempts(started, seconds=60)
    assert {a["honeypot"] for a in attempts} >= {"write_tag", "read_tag"}
    assert all(a["gateway_request_id"] for a in attempts)
