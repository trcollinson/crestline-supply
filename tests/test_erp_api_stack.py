"""ErpApiStack: the ERP's API in Lambda, checked in the template it synthesizes.

No AWS calls and no package built: assets aren't bundled in these tests
(`aws:cdk:bundling-stacks` is empty), so they read only what would deploy.
"""

import json
import re
from pathlib import Path

import pytest
from aws_cdk import assertions

from app import build_app
from crestline import erp_secrets
from stacks import erp_api

INFRA_DIR = Path(__file__).resolve().parent.parent / "infra"
TAGS = {"project": "crestline", "layer": "erp-api", "milestone": "M2"}


def synth(**context) -> assertions.Template:
    base = json.loads((INFRA_DIR / "cdk.json").read_text())["context"]
    app = build_app(base | {"aws:cdk:bundling-stacks": []} | context)
    return assertions.Template.from_stack(app.node.find_child("CrestlineErpApi"))


@pytest.fixture(scope="module")
def template() -> assertions.Template:
    return synth()


def function(template) -> dict:
    [resource] = template.find_resources("AWS::Lambda::Function").values()
    return resource["Properties"]


def test_one_small_arm_function_runs_the_api(template):
    props = function(template)
    assert props["Runtime"] == "python3.12"
    assert props["Architectures"] == ["arm64"]
    assert props["Handler"] == "crestline_erp_api.lambda_handler.handler"
    assert (props["MemorySize"], props["Timeout"]) == (512, 30)


def test_the_environment_names_the_secrets_but_holds_none(template):
    env = function(template)["Environment"]["Variables"]
    assert env == {
        "CRESTLINE_ERP_DB": "/var/task/erp.db",
        "CRESTLINE_ERP_CLIENT_ID": "crestline-poller",
        "CRESTLINE_ERP_CLIENT_SECRET_PARAMETER": "/crestline/erp-api/client-secret",
        "CRESTLINE_ERP_SIGNING_KEY_PARAMETER": "/crestline/erp-api/signing-key",
    }  # a clean API, served as it stands: no mess, no clock


def test_the_function_may_read_its_two_parameters_and_nothing_else(template):
    [policy] = template.find_resources("AWS::IAM::Policy").values()
    statements = policy["Properties"]["PolicyDocument"]["Statement"]
    resources = json.dumps([s["Resource"] for s in statements])
    for name in erp_api.SECRET_PARAMETERS.values():
        assert f"parameter{name}" in resources
    actions = {a for s in statements for a in s["Action"]}
    assert "ssm:GetParameter" in actions
    assert all(a.startswith("ssm:") for a in actions)


def test_the_url_is_open_and_the_erps_own_tokens_guard_it(template):
    template.has_resource_properties("AWS::Lambda::Url", {"AuthType": "NONE"})
    outputs = template.find_outputs("*")
    assert set(outputs) == {"ErpApiUrl", "ErpApiClientId"}


def test_logs_are_kept_a_week_and_go_with_the_stack(template):
    [(logical_id, group)] = template.find_resources("AWS::Logs::LogGroup").items()
    assert group["Properties"]["RetentionInDays"] == 7
    assert group["DeletionPolicy"] == "Delete"
    assert logical_id in json.dumps(function(template)["LoggingConfig"])


def test_every_resource_carries_the_cost_tags(template):
    for kind in ("AWS::Lambda::Function", "AWS::Logs::LogGroup", "AWS::IAM::Role"):
        for resource in template.find_resources(kind).values():
            tags = {t["Key"]: t["Value"] for t in resource["Properties"]["Tags"]}
            assert TAGS.items() <= tags.items(), kind


def test_the_template_names_no_account_or_region(template):
    text = json.dumps(template.to_json())
    assert not re.search(r"\b\d{12}\b", text)
    assert not re.search(r"\b(?:us|eu|ap|ca|sa)-[a-z]+-\d\b", text)


def test_mess_and_clock_come_from_context_with_the_anchor_pinned():
    env = function(
        synth(
            erpApiMess="../mess.yaml",
            erpApiClockStart="2026-07-03T06:00:00Z",
            erpApiClockSpeed="3600",
            erpApiClockAnchor="2026-10-22T20:00:00Z",
        )
    )["Environment"]["Variables"]
    assert env["CRESTLINE_ERP_MESS"] == "/var/task/mess.yaml"
    assert env["CRESTLINE_ERP_CLOCK_START"] == "2026-07-03T06:00:00Z"
    assert env["CRESTLINE_ERP_CLOCK_SPEED"] == "3600"
    assert env["CRESTLINE_ERP_CLOCK_ANCHOR"] == "2026-10-22T20:00:00Z"

    unpinned = function(synth(erpApiClockStart="2026-07-03T06:00:00Z"))
    anchor = unpinned["Environment"]["Variables"]["CRESTLINE_ERP_CLOCK_ANCHOR"]
    assert re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ", anchor)  # now, at synth


def test_the_stack_and_the_secrets_command_name_the_same_parameters():
    assert erp_api.SECRET_PARAMETERS == erp_secrets.PARAMETERS


def test_the_package_holds_the_api_two_crestline_modules_the_store_and_the_mess(
    tmp_path, monkeypatch
):
    store = tmp_path / "erp.db"
    store.write_bytes(b"sqlite")
    mess = tmp_path / "mess.yaml"
    mess.write_text("mess_level: 2\n")
    calls = []
    monkeypatch.setattr(
        erp_api.UvBundle, "_uv", staticmethod(lambda *a: calls.append(a))
    )
    out = tmp_path / "package"
    out.mkdir()

    assert erp_api.UvBundle(store, mess).try_bundle(str(out))

    assert {p.name for p in (out / "crestline").iterdir()} == {
        "__init__.py",
        "mess.py",
        "rng.py",
    }  # not the simulator
    assert (out / "crestline_erp_api" / "lambda_handler.py").is_file()
    assert (out / "erp.db").read_bytes() == b"sqlite"
    assert (out / "mess.yaml").read_text() == "mess_level: 2\n"
    export, install = calls
    assert "--package=crestline-erp-api" in export and "--frozen" in export
    assert "--python-platform=aarch64-manylinux2014" in install
    assert f"--target={out}" in install


def test_packaging_needs_a_backfill_first(tmp_path):
    with pytest.raises(FileNotFoundError, match="crestline sim backfill"):
        erp_api.UvBundle(tmp_path / "missing.db", None).try_bundle(str(tmp_path))
