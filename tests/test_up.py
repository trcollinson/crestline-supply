"""`crestline up` and `down` (M5 step 8), with fakes: the order of things,
resuming, the guards, and the checks, without AWS."""

from pathlib import Path
from types import SimpleNamespace

import pytest

from crestline import cli, up


@pytest.fixture(autouse=True)
def _keep_region(monkeypatch):
    # up() and down() set both region variables for their process.
    monkeypatch.setenv("AWS_REGION", "us-test-1")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-test-1")


def stage_names(monkeypatch) -> list[str]:
    names: list[str] = []
    monkeypatch.setattr(
        up, "run_stages", lambda stages, log, title: names.extend(n for n, _ in stages)
    )
    up.up(up.Settings(region="us-test-1"))
    return names


def test_up_runs_the_readme_in_order(monkeypatch):
    names = stage_names(monkeypatch)
    at = {name: n for n, name in enumerate(names)}

    def index(text: str) -> int:
        [n] = [n for name, n in at.items() if text in name]
        return n

    assert names[0] == "Check the account is ready"
    assert index("Simulate") < index("Bootstrap")  # the app synths the ERP's store
    assert index("Transaction Search") < index("Deploy CrestlineAgent")
    assert index("secrets") < index("Deploy CrestlineErpApi")
    assert index("Deploy CrestlineFoundation") < index("Upload")
    assert index("Deploy CrestlineCurated") < index("Deploy CrestlineErpIngest")
    assert index("certificates") < index("Replay")
    assert index("Athena") < index(
        "Merge the replay"
    )  # its brake check reads the views
    assert index("Replay") < index("Merge the replay") < index("Deploy CrestlineAgent")
    assert index("poller") < index("Deploy CrestlineBrief") < index("earlier mornings")
    assert index("earlier mornings") < index("send a brief")
    assert names[-1] == "Check the deployment matches the repo"


def test_up_and_down_name_the_same_stacks():
    torn_down = {name for row in up.TEARDOWN for name in row}
    assert set(up.DEPLOYED) <= torn_down


def test_down_stops_the_schedules_before_what_they_read():
    row = {name: n for n, names in enumerate(up.TEARDOWN) for name in names}
    for scheduled in ("CrestlineErpIngest", "CrestlineMachineIngest", "CrestlineBrief"):
        assert row[scheduled] < row["CrestlineFoundation"]
        assert row[scheduled] < row["CrestlineCurated"]
    assert row["CrestlineAgent"] < row["CrestlineCurated"]
    assert up.TEARDOWN[-1] == ("CrestlineFoundation",)


def test_contexts_are_the_same_for_every_cdk_command(tmp_path):
    flags = up.contexts(up.Settings(region="r", out=tmp_path / "out"))
    pairs = dict(flag.split("=", 1) for flag in flags if flag != "-c")
    assert pairs["erpStore"] == str((tmp_path / "out" / "erp" / "erp.db").resolve())
    assert Path(pairs["erpApiMess"]).name == "mess.yaml"
    assert pairs["erpIngestSchedule"] == "rate(1 day)"
    assert "agentRoleSuffix" not in pairs and "briefEmail" not in pairs

    flags = up.contexts(
        up.Settings(region="r", role_suffix="-us-east-2", email="a@b.example")
    )
    pairs = dict(flag.split("=", 1) for flag in flags if flag != "-c")
    assert pairs["agentRoleSuffix"] == "-us-east-2"
    assert pairs["briefEmail"] == "a@b.example"


def test_run_stages_stops_at_the_first_failure():
    ran, logged = [], []

    def fail():
        raise SystemExit("broken")

    stages = [
        ("one", lambda: ran.append(1)),
        ("two", fail),
        ("three", lambda: ran.append(3)),
    ]
    with pytest.raises(SystemExit, match="broken"):
        up.run_stages(stages, logged.append, "t")
    assert ran == [1]
    assert any("[2/3] two" in line for line in logged)


class FakeCloudFormation:
    def __init__(self, stacks: dict[str, dict[str, str]]):
        self.stacks = stacks
        self.deleted: list[str] = []

    def describe_stacks(self, StackName):
        if StackName not in self.stacks:
            from botocore.exceptions import ClientError

            raise ClientError(
                {"Error": {"Message": f"Stack with id {StackName} does not exist"}},
                "DescribeStacks",
            )
        outputs = [
            {"OutputKey": k, "OutputValue": v}
            for k, v in self.stacks[StackName].items()
        ]
        return {"Stacks": [{"StackStatus": "CREATE_COMPLETE", "Outputs": outputs}]}

    def delete_stack(self, StackName):
        self.deleted.append(StackName)

    def get_waiter(self, name):
        return SimpleNamespace(wait=lambda **kwargs: None)


class FakeIam:
    class exceptions:
        class NoSuchEntityException(Exception):
            pass

    def __init__(self, roles: set[str]):
        self.roles = roles

    def get_role(self, RoleName):
        if RoleName not in self.roles:
            raise self.exceptions.NoSuchEntityException(RoleName)
        return {"Role": {"RoleName": RoleName}}


def test_a_fresh_account_needs_no_role_suffix():
    assert (
        up.agent_role_suffix("us-east-2", FakeCloudFormation({}), FakeIam(set())) == ""
    )


def test_roles_taken_by_another_region_get_this_regions_suffix():
    iam = FakeIam({"crestline-ops-analyst"})
    assert (
        up.agent_role_suffix("us-east-2", FakeCloudFormation({}), iam) == "-us-east-2"
    )


def test_a_copy_already_here_keeps_its_suffix():
    stacks = FakeCloudFormation({"CrestlineAgent": {"RoleSuffix": "-m5"}})
    iam = FakeIam({"crestline-ops-analyst", "crestline-ops-analyst-m5"})
    assert up.agent_role_suffix("us-east-1", stacks, iam) == "-m5"


def test_delete_stacks_deletes_only_what_is_there():
    stacks = FakeCloudFormation({"CrestlineBrief": {}, "CrestlineAgent": {}})
    note = up.delete_stacks(up.TEARDOWN[0], stacks)
    assert stacks.deleted == ["CrestlineBrief", "CrestlineAgent"]
    assert note == "deleted CrestlineBrief, CrestlineAgent"
    assert up.delete_stacks(("CrestlineFoundation",), stacks) == "none deployed"


class FakeXray:
    def __init__(self, destination: str, statuses: list[str]):
        self.destination = destination
        self.statuses = statuses
        self.updated = None

    def get_trace_segment_destination(self):
        status = self.statuses.pop(0) if len(self.statuses) > 1 else self.statuses[0]
        return {"Destination": self.destination, "Status": status}

    def update_trace_segment_destination(self, Destination):
        self.updated = Destination
        self.destination = Destination


class FakeLogs:
    meta = SimpleNamespace(region_name="us-east-2")

    def __init__(self):
        self.policies = {}

    def put_resource_policy(self, policyName, policyDocument):
        self.policies[policyName] = policyDocument


def test_transaction_search_already_on_changes_nothing():
    logs = FakeLogs()
    note = up.transaction_search(FakeXray("CloudWatchLogs", ["ACTIVE"]), logs, None)
    assert note == "already on" and logs.policies == {}


def test_transaction_search_turned_on_and_waited_for():
    xray = FakeXray("XRay", ["ACTIVE", "PENDING", "PENDING", "ACTIVE"])
    logs = FakeLogs()
    sts = SimpleNamespace(get_caller_identity=lambda: {"Account": "000000000000"})
    note = up.transaction_search(xray, logs, sts, wait_seconds=0)
    assert note == "turned on" and xray.updated == "CloudWatchLogs"
    policy = logs.policies["TransactionSearchXRayAccess"]
    assert "arn:aws:logs:us-east-2:000000000000:log-group:aws/spans:*" in policy
    assert "arn:aws:xray:us-east-2:000000000000:*" in policy


def test_waiting_for_the_poller(monkeypatch):
    monkeypatch.setattr(up, "stack_outputs", lambda name: {"StateMachineArn": "arn"})
    replies = [
        [{"name": "r1", "status": "RUNNING"}],
        [{"name": "r1", "status": "SUCCEEDED"}],
    ]
    sfn = SimpleNamespace(list_executions=lambda **kw: {"executions": replies.pop(0)})
    assert up.wait_for_poller(sfn, poll_seconds=0) == "run r1 succeeded"

    failed = SimpleNamespace(
        list_executions=lambda **kw: {
            "executions": [{"name": "r1", "status": "FAILED"}]
        }
    )
    with pytest.raises(SystemExit, match="r1 ended FAILED"):
        up.wait_for_poller(failed, poll_seconds=0)


def test_up_reuses_a_backfill_already_there(tmp_path, monkeypatch):
    (tmp_path / "erp").mkdir()
    (tmp_path / "erp" / "erp.db").touch()
    monkeypatch.setattr(up, "crestline", lambda *a: pytest.fail("simulated again"))
    assert "already" in up.backfill(up.Settings(region="r", out=tmp_path))


def test_down_wants_the_region_typed(monkeypatch):
    monkeypatch.setattr(up, "down", lambda region: pytest.fail("deleted anyway"))
    with pytest.raises(SystemExit, match="Not confirmed"):
        cli.main(["down", "--region", "us-east-2", "--confirm", "us-west-2"])
    monkeypatch.setattr("builtins.input", lambda prompt: "us-east-1")
    with pytest.raises(SystemExit, match="Not confirmed"):
        cli.main(["down", "--region", "us-east-2"])


def test_took():
    assert up.took(42) == "42 s"
    assert up.took(125) == "2 min 05 s"
    assert up.took(9000) == "2 h 30 min"


def test_waiting_for_a_new_endpoint_to_resolve(monkeypatch):
    import socket

    answers = [socket.gaierror(8, "not known"), socket.gaierror(8, "not known"), None]

    def getaddrinfo(host, port):
        answer = answers.pop(0)
        if answer:
            raise answer
        return [("ok",)]

    monkeypatch.setattr(up.socket, "getaddrinfo", getaddrinfo)
    up.wait_for_dns("x-ats.iot.example", poll_seconds=0)
    assert answers == []

    monkeypatch.setattr(
        up.socket, "getaddrinfo", lambda h, p: (_ for _ in ()).throw(socket.gaierror())
    )
    with pytest.raises(SystemExit, match="doesn't resolve"):
        up.wait_for_dns("x-ats.iot.example", poll_seconds=1, timeout_seconds=0)


def test_log_groups_are_found_in_either_case():
    groups = [
        "/aws/lambda/CrestlineFoundation-CustomS3AutoDeleteObjectsCusto-4BOZ",
        "/aws/vendedlogs/bedrock-agentcore/crestline-agent-gateway",
        "aws/spans",
    ]

    class Logs:
        def get_paginator(self, name):
            return SimpleNamespace(
                paginate=lambda **kw: [
                    {"logGroups": [{"logGroupName": g} for g in groups]}
                ]
            )

    assert up.crestline_log_groups(Logs()) == groups[:2]


def test_down_removes_the_regions_emptied_keys_folder(tmp_path, monkeypatch):
    from crestline import machine_setup

    monkeypatch.setattr(machine_setup, "CERTS_DIR", tmp_path)
    (tmp_path / "us-east-2").mkdir()
    assert up._local_keys("us-east-2") == [tmp_path / "us-east-2"]
    assert up.delete_local_keys("us-east-2").startswith("removed")
    assert up._local_keys("us-east-2") == []
    assert up.delete_local_keys("us-east-2") == "none"
