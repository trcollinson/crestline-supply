"""The Crestline CDK app: one app, one stack per layer (spec §7, §9).

Run it from this folder through the CDK CLI, e.g. `uv run cdk synth`. The
stacks are environment-agnostic: they deploy to whichever account and region
the AWS profile points at (`--profile` or `AWS_PROFILE`), so no account ID or
region is written into the code or the synthesized templates.
"""

from datetime import UTC, datetime
from pathlib import Path

import aws_cdk as cdk

from stacks.agent import AgentStack
from stacks.agent_spike import AgentSpikeStack
from stacks.curated import CuratedStack
from stacks.erp_api import ErpApiStack
from stacks.erp_ingest import ErpIngestStack
from stacks.foundation import FoundationStack
from stacks.machine_ingest import MachineIngestStack
from stacks.machine_loadtest import MachineLoadtestStack

INFRA_DIR = Path(__file__).resolve().parent


def build_app(context: dict | None = None) -> cdk.App:
    """The app with every stack in it.

    The CLI hands the app the feature flags in `cdk.json` by itself; tests
    pass them as `context` so they check the same template that deploys.
    """
    app = cdk.App(context=context)

    # The cost report groups the bill by these three tags, so they go on twice.
    # As stack tags, CloudFormation copies them onto every resource it
    # creates, including the Lambda and role CDK adds to empty the buckets on
    # delete. Through `Tags.of()`, they are written into the template on each
    # resource we define, so `cdk synth` shows them.
    tags = {"project": "crestline", "layer": "foundation", "milestone": "M1"}
    foundation = FoundationStack(app, "CrestlineFoundation", tags=tags)
    for key, value in tags.items():
        cdk.Tags.of(foundation).add(key, value)

    # The ERP's API. Paths in context are relative to infra/, where the CLI runs.
    tags = {"project": "crestline", "layer": "erp-api", "milestone": "M2"}
    mess = app.node.try_get_context("erpApiMess")
    erp_api = ErpApiStack(
        app,
        "CrestlineErpApi",
        store=INFRA_DIR / app.node.try_get_context("erpStore"),
        mess=INFRA_DIR / mess if mess else None,
        clock=_clock(app),
        tags=tags,
    )
    for key, value in tags.items():
        cdk.Tags.of(erp_api).add(key, value)

    # The curated layer: Iceberg tables the poller's curate step fills.
    tags = {"project": "crestline", "layer": "curated", "milestone": "M2"}
    curated = CuratedStack(app, "CrestlineCurated", tags=tags)
    for key, value in tags.items():
        cdk.Tags.of(curated).add(key, value)

    # The poller: the ERP API's address comes from Parameter Store, not from
    # the stack above, so deploying one never redeploys the other.
    tags = {"project": "crestline", "layer": "erp-ingest", "milestone": "M2"}
    erp_ingest = ErpIngestStack(
        app,
        "CrestlineErpIngest",
        raw_bucket=foundation.raw_bucket,
        results_bucket=foundation.results_bucket,
        curated_bucket=curated.bucket,
        schedule=app.node.try_get_context("erpIngestSchedule") or "rate(1 hour)",
        alarm_email=app.node.try_get_context("erpIngestAlarmEmail"),
        tags=tags,
    )
    for key, value in tags.items():
        cdk.Tags.of(erp_ingest).add(key, value)

    # The machines' gateways, through IoT Core into the raw bucket (M3).
    tags = {"project": "crestline", "layer": "machine-ingest", "milestone": "M3"}
    machine_ingest = MachineIngestStack(
        app,
        "CrestlineMachineIngest",
        raw_bucket=foundation.raw_bucket,
        results_bucket=foundation.results_bucket,
        curated_bucket=curated.bucket,
        alarm_email=app.node.try_get_context("machineIngestAlarmEmail"),
        tags=tags,
    )
    for key, value in tags.items():
        cdk.Tags.of(machine_ingest).add(key, value)

    # The load test (M3 step 9), only when asked: -c machineLoadtest=true.
    # `layer` isn't a stack tag here: CloudFormation would copy it onto every
    # resource, and each mode's resources carry their own value of it.
    if str(app.node.try_get_context("machineLoadtest")).lower() == "true":
        stack_tags = {"project": "crestline", "milestone": "M3"}
        loadtest = MachineLoadtestStack(
            app, "CrestlineMachineLoadtest", tags=stack_tags
        )
        for key, value in stack_tags.items():
            cdk.Tags.of(loadtest).add(key, value)
        cdk.Tags.of(loadtest).add("layer", "machine-loadtest")

    # The agent's tools behind an AgentCore Gateway (M4). The read tools'
    # windows end on the backfill's last day unless a call says otherwise;
    # the policy engine enforces unless -c agentPolicyMode=LOG_ONLY, and
    # -c agentScadaForbid=false leaves the PlantScada forbid out (step 4).
    # The red-team eval's configurations (step 6): -c agentPolicySet=open
    # permits both roles everything; -c agentGuardrail=true adds the
    # guardrail that withholds a tool reply scored as a prompt attack.
    tags = {"project": "crestline", "layer": "agent", "milestone": "M4"}
    agent = AgentStack(
        app,
        "CrestlineAgent",
        raw_bucket=foundation.raw_bucket,
        results_bucket=foundation.results_bucket,
        curated_bucket=curated.bucket,
        data_last_day=app.node.try_get_context("agentDataLastDay") or "2026-09-30",
        policy_mode=app.node.try_get_context("agentPolicyMode") or "ENFORCE",
        scada_forbid=str(app.node.try_get_context("agentScadaForbid")).lower()
        != "false",
        policy_set=app.node.try_get_context("agentPolicySet") or "cedar",
        guardrail=str(app.node.try_get_context("agentGuardrail")).lower() == "true",
        tags=tags,
    )
    for key, value in tags.items():
        cdk.Tags.of(agent).add(key, value)

    # M4 step 0's spike, only when asked: -c agentSpike=true.
    if str(app.node.try_get_context("agentSpike")).lower() == "true":
        tags = {"project": "crestline", "layer": "agent-spike", "milestone": "M4"}
        spike = AgentSpikeStack(
            app,
            "CrestlineAgentSpike",
            mode=app.node.try_get_context("agentSpikeMode") or "LOG_ONLY",
            tags=tags,
        )
        for key, value in tags.items():
            cdk.Tags.of(spike).add(key, value)
    return app


def _clock(app: cdk.App) -> dict[str, str] | None:
    """The ERP's clock from context, its anchor pinned now if not given.

    Every copy of the function reads the same anchor from its environment,
    so they agree on the ERP's time whenever each one started.
    """
    start = app.node.try_get_context("erpApiClockStart")
    if not start:
        return None
    return {
        "start": start,
        "speed": str(app.node.try_get_context("erpApiClockSpeed") or 1),
        "anchor": app.node.try_get_context("erpApiClockAnchor")
        or datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


if __name__ == "__main__":
    build_app().synth()
