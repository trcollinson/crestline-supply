"""`crestline up` and `crestline down`: the whole platform in one command (M5 step 8).

`up` is the README's AWS sequence written as code, in the same order. Each
stage either does its work or says why it doesn't need to, so running `up`
again after a failure picks up where it stopped (a `cdk deploy` with nothing
to change is quick). It adds the checks the README leaves to the reader:
that the models answer, that AgentCore exists in the region, and that
CloudWatch Transaction Search is on, which a region's first gateway needs
(M5 step 2 found that by rolling a stack back). It deploys M5's backfill
(the laser nozzle wearing out), because the morning brief is about it.

`down` deletes the stacks through CloudFormation in reverse order (the
schedules first, so nothing runs while the rest goes), the gateways'
certificates and the ERP's secrets, and the log groups the stacks leave
behind; then it looks for anything else and says what it found. It needs
no backfill and no synth, so it works from any checkout.

Every command runs in the region given: `up` and `down` set both
`AWS_REGION` (which the CDK CLI reads) and `AWS_DEFAULT_REGION` (which
boto3 reads) before anything else, because setting only one once sent a
command to the wrong region (M5 step 2).
"""

import json
import os
import shutil
import socket
import subprocess
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError, EndpointConnectionError

from crestline.stack import (
    BRIEF_STACK_NAME,
    CURATED_STACK_NAME,
    ERP_API_STACK_NAME,
    ERP_INGEST_STACK_NAME,
    STACK_NAME,
    stack_outputs,
)

REPO = Path(__file__).resolve().parents[2]
INFRA = REPO / "infra"

MACHINE_INGEST_STACK_NAME = "CrestlineMachineIngest"
AGENT_STACK_NAME = "CrestlineAgent"

# M5's backfill: the 90 days before Oct 1, the laser nozzle wearing out.
BACKFILL_DAYS = 90
BACKFILL_END = date(2026, 10, 1)  # also the newest morning a brief can be about
BACKFILL_SEED = 42
BACKFILL_SCENARIOS = REPO / "eval" / "scenarios_nozzle.yaml"
REPLAY_SPEED = 2000  # 90 days in about an hour and a half

# The models the brief and the agent call (brief/narrate.py, crestline_agent/loop.py).
MODELS = ("us.anthropic.claude-sonnet-4-6", "us.amazon.nova-2-lite-v1:0")
# The agent's principals (infra/stacks/agent.py); IAM role names are account-wide.
AGENT_ROLE = "crestline-ops-analyst"

# `down` deletes these a row at a time, the stacks in a row together. Opt-in
# stacks (the load test, M4's spike) go first if they are there.
TEARDOWN = (
    (
        "CrestlineMachineLoadtest",
        "CrestlineAgentSpike",
        BRIEF_STACK_NAME,
        AGENT_STACK_NAME,
    ),
    (ERP_INGEST_STACK_NAME, MACHINE_INGEST_STACK_NAME),
    (ERP_API_STACK_NAME, CURATED_STACK_NAME),
    (STACK_NAME,),
)
DEPLOYED = (
    STACK_NAME,
    CURATED_STACK_NAME,
    ERP_API_STACK_NAME,
    ERP_INGEST_STACK_NAME,
    MACHINE_INGEST_STACK_NAME,
    AGENT_STACK_NAME,
    BRIEF_STACK_NAME,
)

# What `down` leaves on purpose: shared by anything else in the region, or
# settings rather than resources.
LEFT_ON_PURPOSE = (
    "CDKToolkit (`cdk bootstrap`'s stack and its staging bucket), shared by every CDK app in the region",
    (
        "CloudWatch Transaction Search (the Logs resource policy TransactionSearchXRayAccess, "
        "X-Ray's segment destination, and the log groups aws/spans and /aws/application-signals/data), an account setting"
    ),
    "Bedrock model access, an account setting",
)


@dataclass
class Settings:
    region: str
    out: Path = Path("out")
    email: str | None = None
    replay: bool = True
    yes: bool = False  # deploy without CDK asking to approve IAM changes
    role_suffix: str = ""  # found by the preconditions
    log: Callable[[str], None] = field(default=print, repr=False)


def use_region(region: str) -> None:
    """Point the CDK CLI and boto3 at one region, for this process and its children."""
    os.environ["AWS_REGION"] = region
    os.environ["AWS_DEFAULT_REGION"] = region


def contexts(s: Settings) -> list[str]:
    """The `-c` flags every cdk command in `up` gets, so deploy and diff agree."""
    values = {
        "erpStore": str((s.out / "erp" / "erp.db").resolve()),
        "erpApiMess": str(REPO / "mess.yaml"),
        # No clock on the ERP: the poller's first run reads its whole
        # history, and a run a day after that is plenty for a finished one.
        "erpIngestSchedule": "rate(1 day)",
    }
    if s.role_suffix:
        values["agentRoleSuffix"] = s.role_suffix
    if s.email:
        values["briefEmail"] = s.email
    return [flag for key, value in values.items() for flag in ("-c", f"{key}={value}")]


# --- up ---------------------------------------------------------------------


def up(s: Settings) -> None:
    use_region(s.region)
    stages: list[tuple[str, Callable[[], str | None]]] = [
        ("Check the account is ready", lambda: preconditions(s)),
        ("Simulate the plant (90 days, the nozzle wearing out)", lambda: backfill(s)),
        ("Bootstrap the CDK in the region", lambda: bootstrap(s)),
        ("Turn on CloudWatch Transaction Search", transaction_search),
        ("Create the ERP API's secrets", lambda: crestline("erp-api", "secrets")),
        (f"Deploy {STACK_NAME}", lambda: deploy(s, STACK_NAME)),
        ("Upload the backfill to the raw bucket", lambda: upload(s)),
        (f"Deploy {CURATED_STACK_NAME}", lambda: deploy(s, CURATED_STACK_NAME)),
        (f"Deploy {ERP_API_STACK_NAME}", lambda: deploy(s, ERP_API_STACK_NAME)),
        (
            f"Deploy {ERP_INGEST_STACK_NAME} (its first run starts now)",
            lambda: deploy(s, ERP_INGEST_STACK_NAME),
        ),
        (
            f"Deploy {MACHINE_INGEST_STACK_NAME}",
            lambda: deploy(s, MACHINE_INGEST_STACK_NAME),
        ),
        ("Make the gateways' certificates", lambda: crestline("machines", "setup")),
        ("Create Athena's tables and views", lambda: crestline("athena", "setup")),
        ("Replay the machine messages through IoT Core", lambda: replay(s)),
        ("Wait for the ERP poller's first run", wait_for_poller),
        ("Merge the replay into the curated tables", curate_machines),
        (f"Deploy {AGENT_STACK_NAME}", lambda: deploy(s, AGENT_STACK_NAME)),
        (f"Deploy {BRIEF_STACK_NAME}", lambda: deploy(s, BRIEF_STACK_NAME)),
        ("Keep the brief's earlier mornings in its bucket", lambda: seed_brief(s)),
        ("Write and send a brief now", lambda: crestline("brief", "invoke")),
        ("Check the deployment matches the repo", lambda: diff(s)),
    ]
    run_stages(stages, s.log, f"crestline up in {s.region}")


def run_stages(
    stages: list[tuple[str, Callable[[], str | None]]],
    log: Callable[[str], None],
    title: str,
) -> None:
    """Run each stage in turn, timing it; a stage that fails stops the rest."""
    started = time.monotonic()
    for n, (name, stage) in enumerate(stages, 1):
        log(f"\n[{n}/{len(stages)}] {name}")
        began = time.monotonic()
        note = stage()
        log(f"    {note or 'done'} ({took(time.monotonic() - began)})")
    log(f"\n{title}: finished in {took(time.monotonic() - started)}")


def preconditions(s: Settings) -> str:
    """Fail before creating anything if the account or this machine isn't ready."""
    if shutil.which("cdk") is None:
        raise SystemExit("The AWS CDK CLI isn't installed: npm install -g aws-cdk")
    try:
        boto3.client("sts").get_caller_identity()
    except Exception as error:
        raise SystemExit(
            f"No AWS credentials ({error}): set AWS_PROFILE and sign in"
        ) from error

    bedrock = boto3.client("bedrock-runtime")
    for model in MODELS:
        try:
            bedrock.converse(
                modelId=model,
                messages=[{"role": "user", "content": [{"text": "Say OK."}]}],
                inferenceConfig={"maxTokens": 1},
            )
        except ClientError as error:
            raise SystemExit(
                f"{model} doesn't answer in {s.region}: {error.response['Error']['Message']}\n"
                "Turn on access to it in the Amazon Bedrock console (Model access), "
                "or pick a region whose `us.` inference profile reaches it."
            ) from error

    try:
        boto3.client("bedrock-agentcore-control").list_gateways(maxResults=1)
    except (ClientError, EndpointConnectionError) as error:
        raise SystemExit(
            f"Amazon Bedrock AgentCore isn't available in {s.region}: {error}"
        ) from error

    s.role_suffix = agent_role_suffix(s.region)
    roles = f"roles {AGENT_ROLE}{s.role_suffix}, ..."
    return f"credentials, both models, and AgentCore answer; the agent's {roles}"


def agent_role_suffix(region: str, cloudformation=None, iam=None) -> str:
    """'' unless the agent's role names are taken by a copy in another region.

    IAM role names are account-wide, so a second copy names its roles apart
    (`-c agentRoleSuffix`, M5 step 2). A copy already in this region keeps
    the suffix it was deployed with.
    """
    cloudformation = cloudformation or boto3.client("cloudformation")
    iam = iam or boto3.client("iam")
    if stack_exists(AGENT_STACK_NAME, cloudformation):
        return stack_outputs(AGENT_STACK_NAME, cloudformation).get("RoleSuffix", "")
    try:
        iam.get_role(RoleName=AGENT_ROLE)
    except iam.exceptions.NoSuchEntityException:
        return ""
    return f"-{region}"


def backfill(s: Settings) -> str | None:
    if (s.out / "erp" / "erp.db").exists():
        return f"using the backfill already in {s.out}/"
    crestline(
        "sim", "backfill",
        "--days", str(BACKFILL_DAYS),
        "--end", BACKFILL_END.isoformat(),
        "--seed", str(BACKFILL_SEED),
        "--scenarios", str(BACKFILL_SCENARIOS),
        "--mess", str(REPO / "mess.yaml"),
        "--out", str(s.out),
    )  # fmt: skip
    return None


def bootstrap(s: Settings) -> str | None:
    if stack_exists("CDKToolkit"):
        return "already bootstrapped"
    cdk(s, "bootstrap")
    return None


def transaction_search(xray=None, logs=None, sts=None, wait_seconds: int = 15) -> str:
    """Let X-Ray write spans to CloudWatch Logs, as the gateway's trace delivery needs."""
    xray = xray or boto3.client("xray")
    logs = logs or boto3.client("logs")
    sts = sts or boto3.client("sts")
    now = xray.get_trace_segment_destination()
    if now["Destination"] == "CloudWatchLogs" and now["Status"] == "ACTIVE":
        return "already on"
    account = sts.get_caller_identity()["Account"]
    region = logs.meta.region_name
    logs.put_resource_policy(
        policyName="TransactionSearchXRayAccess",
        policyDocument=json.dumps(transaction_search_policy(account, region)),
    )
    if now["Destination"] != "CloudWatchLogs":
        xray.update_trace_segment_destination(Destination="CloudWatchLogs")
    # The gateway's X-Ray delivery can't be created until this is ACTIVE
    # (about 3 minutes, M4 step 0).
    for _ in range(60):
        if xray.get_trace_segment_destination()["Status"] == "ACTIVE":
            return "turned on"
        time.sleep(wait_seconds)
    raise SystemExit("Transaction Search didn't become ACTIVE in 15 minutes")


def transaction_search_policy(account: str, region: str) -> dict:
    """The Logs resource policy AWS's Transaction Search setup asks for."""
    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "TransactionSearchXRayAccess",
                "Effect": "Allow",
                "Principal": {"Service": "xray.amazonaws.com"},
                "Action": "logs:PutLogEvents",
                "Resource": [
                    f"arn:aws:logs:{region}:{account}:log-group:aws/spans:*",
                    f"arn:aws:logs:{region}:{account}:log-group:/aws/application-signals/data:*",
                ],
                "Condition": {
                    "ArnLike": {"aws:SourceArn": f"arn:aws:xray:{region}:{account}:*"},
                    "StringEquals": {"aws:SourceAccount": account},
                },
            }
        ],
    }


def upload(s: Settings) -> str:
    from crestline import upload as uploading

    done = uploading.upload_backfill(
        s.out, ingest_date=BACKFILL_END, bucket=uploading.raw_bucket_name()
    )
    return f"{len(done.uploaded)} new or changed, {len(done.unchanged)} unchanged, {len(done.deleted)} deleted"


def replay(s: Settings) -> str:
    """Publish the backfill's machine files through IoT Core, then account for every one."""
    from crestline import machine_setup, replay_account
    from crestline.upload import raw_bucket_name

    if not s.replay:
        return "skipped (--no-replay): the machine tables, the brief's stops, and the agent's machine tools stay empty"
    manifests = sorted((s.out / replay_account.REPLAYS_DIR).glob("*.json"))
    if manifests and replay_account.account(manifests[-1], raw_bucket_name()).ok:
        return f"replay {manifests[-1].stem} is already accounted for"
    wait_for_dns(machine_setup.endpoint())
    first = BACKFILL_END - timedelta(days=BACKFILL_DAYS)
    last = BACKFILL_END - timedelta(days=1)
    crestline(
        "machines", "publish",
        "--from", first.isoformat(), "--to", last.isoformat(),
        "--speed", str(REPLAY_SPEED), "--out", str(s.out),
    )  # fmt: skip
    # Firehose holds its last buffer up to 90 s, so the newest messages land
    # a minute or two after the replay ends (M5 step 2).
    manifest = replay_account.latest_manifest(s.out)
    for attempt in range(4):
        result = replay_account.account(manifest, raw_bucket_name())
        if result.ok:
            s.log(result.report())
            return f"replay {manifest.stem} accounted for"
        if attempt < 3:
            time.sleep(60)
    s.log(result.report())
    raise SystemExit(
        "The replay isn't accounted for (IoT Core can lose a message, rarely): "
        "run `crestline up` again to replay"
    )


def wait_for_dns(
    host: str, poll_seconds: int = 30, timeout_seconds: int = 1200
) -> None:
    """Wait until this machine can resolve IoT Core's data endpoint.

    A region's endpoint is made the first time someone asks for it, and its
    name resolves a little later. A resolver that looked it up in between
    remembers that it didn't exist for a while (M5 step 8: a home router
    said NXDOMAIN for some minutes while public DNS answered).
    """
    waited = 0
    while True:
        try:
            socket.getaddrinfo(host, 8883)
            return
        except socket.gaierror:
            if waited >= timeout_seconds:
                raise SystemExit(
                    f"{host} doesn't resolve here after {timeout_seconds // 60} minutes"
                ) from None
            time.sleep(poll_seconds)
            waited += poll_seconds


def wait_for_poller(
    sfn=None, poll_seconds: int = 30, timeout_seconds: int = 1800
) -> str:
    """Wait until a run of the ERP poller has succeeded (its first starts at deploy)."""
    sfn = sfn or boto3.client("stepfunctions")
    arn = stack_outputs(ERP_INGEST_STACK_NAME)["StateMachineArn"]
    waited = 0
    while True:
        runs = sfn.list_executions(stateMachineArn=arn)["executions"]
        for run in runs:
            if run["status"] == "SUCCEEDED":
                return f"run {run['name']} succeeded"
        if runs and all(run["status"] != "RUNNING" for run in runs):
            raise SystemExit(
                f"The poller's run {runs[0]['name']} ended {runs[0]['status']}: "
                "see its execution in Step Functions"
            )
        if waited >= timeout_seconds:
            raise SystemExit(
                f"No poller run succeeded in {timeout_seconds // 60} minutes"
            )
        time.sleep(poll_seconds)
        waited += poll_seconds


def curate_machines(lambda_client=None, settle_seconds: int = 150) -> str:
    """Run the machine curator once now, rather than waiting up to an hour for it.

    The curator leaves a file younger than 2 minutes for its next run, so
    give Firehose's newest files time to age first. It runs one at a time;
    if its hourly run is going, wait for it.
    """
    lambda_client = lambda_client or boto3.client(
        "lambda", config=Config(read_timeout=900, retries={"max_attempts": 0})
    )
    name = stack_outputs(MACHINE_INGEST_STACK_NAME)["CuratorName"]
    time.sleep(settle_seconds)
    for _ in range(20):
        try:
            reply = lambda_client.invoke(FunctionName=name, Payload=b"{}")
        except lambda_client.exceptions.TooManyRequestsException:
            time.sleep(60)  # the hourly run is merging; ours follows it
            continue
        payload = json.loads(reply["Payload"].read())
        if reply.get("FunctionError"):
            raise SystemExit(
                f"The machine curator failed: {json.dumps(payload, indent=1)}"
            )
        return f"{len(payload)} devices merged and checked"
    raise SystemExit("The machine curator stayed busy for 20 minutes")


def seed_brief(s: Settings) -> str | None:
    """Compute the ledger's earlier mornings here, with DuckDB, and keep them in the bucket.

    The brief says how long an alert has been open by reading the briefs of
    earlier mornings. Computed in Athena that's about a minute each, up to
    20 of them on the first run; DuckDB's facts equal Athena's (M5 step 3).
    The newest morning is left for the Lambda to compute in Athena.
    """
    morning = BACKFILL_END.isoformat()
    crestline(
        "brief", "write", "--morning", morning, "--model", "none", "--out", str(s.out)
    )
    crestline("brief", "seed", "--out", str(s.out), "--before", morning)
    return None


def diff(s: Settings) -> str:
    """`cdk diff --method=template` on every stack; any difference fails `up`."""
    try:
        cdk(s, "diff", "--method=template", "--fail", *DEPLOYED)
    except subprocess.CalledProcessError as error:
        raise SystemExit("The deployed stacks differ from the repo (above)") from error
    return "no differences"


def deploy(s: Settings, stack: str) -> None:
    approval = ["--require-approval", "never"] if s.yes else []
    cdk(s, "deploy", stack, "--exclusively", *approval)


def cdk(s: Settings, *args: str) -> None:
    subprocess.run(["cdk", *args, *contexts(s)], cwd=INFRA, check=True)


def crestline(*args: str) -> None:
    """One `crestline` command, in this process (so it shares the region)."""
    from crestline.cli import main

    main(list(args))


# --- down -------------------------------------------------------------------


def down(region: str, log: Callable[[str], None] = print) -> None:
    use_region(region)
    found: dict[str, list[str]] = {}
    suffix = (
        stack_outputs(AGENT_STACK_NAME).get("RoleSuffix", "")
        if stack_exists(AGENT_STACK_NAME)
        else None
    )
    stages: list[tuple[str, Callable[[], str | None]]] = [
        (
            "Delete the gateways' certificates and the Sparkplug schema",
            delete_machine_keys,
        ),
    ]
    for row in TEARDOWN:
        stages.append((f"Delete {', '.join(row)}", lambda row=row: delete_stacks(row)))
    stages += [
        (
            "Delete the ERP API's secrets",
            lambda: crestline("erp-api", "secrets", "--delete"),
        ),
        ("Delete the log groups the stacks leave behind", delete_log_groups),
        ("Delete this region's empty keys folder", lambda: delete_local_keys(region)),
        (
            "Look for anything left",
            lambda: found.update(leftovers(region, suffix)) or None,
        ),
    ]
    run_stages(stages, log, f"crestline down in {region}")
    if found:
        log("\nStill there:")
        for kind, names in found.items():
            for name in names:
                log(f"  {kind}: {name}")
    else:
        log("\nNothing Crestline is left in the region.")
    log("Left on purpose:")
    for item in LEFT_ON_PURPOSE:
        log(f"  {item}")
    if found:
        raise SystemExit(1)


def delete_machine_keys() -> str | None:
    if not stack_exists(MACHINE_INGEST_STACK_NAME):
        return f"no {MACHINE_INGEST_STACK_NAME}"
    crestline("machines", "setup", "--delete")
    return None


def delete_local_keys(region: str) -> str:
    """The region's keys folder, once `machines setup --delete` has emptied it."""
    from crestline.machine_setup import certs_dir

    folder = certs_dir(region)
    if folder.exists() and not any(folder.iterdir()):
        folder.rmdir()
        return f"removed the empty {folder}"
    return "none"


def delete_stacks(names: tuple[str, ...], cloudformation=None) -> str:
    """Delete the stacks that exist, together, and wait for each to go."""
    cloudformation = cloudformation or boto3.client("cloudformation")
    present = [name for name in names if stack_exists(name, cloudformation)]
    for name in present:
        cloudformation.delete_stack(StackName=name)
    waiter = cloudformation.get_waiter("stack_delete_complete")
    for name in present:
        try:
            waiter.wait(StackName=name, WaiterConfig={"Delay": 15, "MaxAttempts": 240})
        except Exception as error:
            raise SystemExit(f"{name} didn't delete: {error}") from error
    return f"deleted {', '.join(present)}" if present else "none deployed"


def delete_log_groups(logs=None) -> str:
    """The log groups left when the stacks have gone: CDK's custom-resource
    Lambdas create theirs on first run, outside the stack (M1 step 8)."""
    logs = logs or boto3.client("logs")
    names = crestline_log_groups(logs)
    for name in names:
        logs.delete_log_group(logGroupName=name)
    return f"deleted {len(names)}: {', '.join(names)}" if names else "none"


def crestline_log_groups(logs) -> list[str]:
    """Every log group with Crestline in its name, in any case.

    `logGroupNamePattern` matches case-sensitively, and the names come both
    ways (`/aws/lambda/CrestlineFoundation-...`, `.../crestline-agent-gateway`),
    so this lists them all and matches here.
    """
    pages = logs.get_paginator("describe_log_groups").paginate()
    return [
        group["logGroupName"]
        for page in pages
        for group in page["logGroups"]
        if "crestline" in group["logGroupName"].lower()
    ]


def leftovers(region: str, role_suffix: str | None) -> dict[str, list[str]]:
    """Anything Crestline still in the region, by kind (empty when it's clean)."""
    cloudformation = boto3.client("cloudformation")
    found = {
        "stack": [
            stack["StackName"]
            for page in cloudformation.get_paginator("list_stacks").paginate(
                StackStatusFilter=[
                    status for status in _STACK_STATUSES if status != "DELETE_COMPLETE"
                ]
            )
            for stack in page["StackSummaries"]
            if stack["StackName"].startswith("Crestline")
        ],
        "tagged project=crestline": [
            resource["ResourceARN"]
            for page in boto3.client("resourcegroupstaggingapi")
            .get_paginator("get_resources")
            .paginate(TagFilters=[{"Key": "project", "Values": ["crestline"]}])
            for resource in page["ResourceTagMappingList"]
        ],
        "log group": crestline_log_groups(boto3.client("logs")),
        "parameter": [
            parameter["Name"]
            for page in boto3.client("ssm")
            .get_paginator("get_parameters_by_path")
            .paginate(Path="/crestline/", Recursive=True)
            for parameter in page["Parameters"]
        ],
        "Glue database": [
            database["Name"]
            for page in boto3.client("glue").get_paginator("get_databases").paginate()
            for database in page["DatabaseList"]
            if database["Name"].startswith("crestline")
        ],
        "bucket": [
            bucket["Name"]
            for bucket in boto3.client("s3")
            .list_buckets(Prefix="crestline", BucketRegion=region)
            .get("Buckets", [])
        ],
        "IoT thing": _iot_things(),
        "IAM role": _agent_roles(role_suffix),
        "local keys": [str(path) for path in _local_keys(region)],
    }
    return {kind: names for kind, names in found.items() if names}


_STACK_STATUSES = (
    "CREATE_IN_PROGRESS", "CREATE_FAILED", "CREATE_COMPLETE",
    "ROLLBACK_IN_PROGRESS", "ROLLBACK_FAILED", "ROLLBACK_COMPLETE",
    "DELETE_IN_PROGRESS", "DELETE_FAILED", "DELETE_COMPLETE",
    "UPDATE_IN_PROGRESS", "UPDATE_COMPLETE_CLEANUP_IN_PROGRESS", "UPDATE_COMPLETE",
    "UPDATE_FAILED", "UPDATE_ROLLBACK_IN_PROGRESS", "UPDATE_ROLLBACK_FAILED",
    "UPDATE_ROLLBACK_COMPLETE_CLEANUP_IN_PROGRESS", "UPDATE_ROLLBACK_COMPLETE",
    "REVIEW_IN_PROGRESS", "IMPORT_IN_PROGRESS", "IMPORT_COMPLETE",
    "IMPORT_ROLLBACK_IN_PROGRESS", "IMPORT_ROLLBACK_FAILED", "IMPORT_ROLLBACK_COMPLETE",
)  # fmt: skip


def _iot_things() -> list[str]:
    from crestline.machine_setup import GATEWAYS

    iot = boto3.client("iot")
    found = []
    for gateway in GATEWAYS:
        try:
            iot.describe_thing(thingName=gateway)
            found.append(gateway)
        except iot.exceptions.ResourceNotFoundException:
            pass
    return found


def _agent_roles(role_suffix: str | None) -> list[str]:
    """The agent's roles, if this region's stack named them (they are account-wide)."""
    if role_suffix is None:
        return []
    from crestline_agent.gateway import ROLES

    iam = boto3.client("iam")
    found = []
    for role in ROLES.values():
        try:
            iam.get_role(RoleName=role + role_suffix)
            found.append(role + role_suffix)
        except iam.exceptions.NoSuchEntityException:
            pass
    return found


def _local_keys(region: str) -> list[Path]:
    from crestline.machine_setup import certs_dir

    folder = certs_dir(region)
    return [folder, *sorted(folder.rglob("*"))] if folder.exists() else []


def stack_exists(name: str, cloudformation=None) -> bool:
    cloudformation = cloudformation or boto3.client("cloudformation")
    try:
        [stack] = cloudformation.describe_stacks(StackName=name)["Stacks"]
    except ClientError as error:
        if "does not exist" in error.response["Error"]["Message"]:
            return False
        raise
    return stack["StackStatus"] != "DELETE_COMPLETE"


def took(seconds: float) -> str:
    minutes, seconds = divmod(round(seconds), 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours} h {minutes:02d} min"
    return f"{minutes} min {seconds:02d} s" if minutes else f"{seconds} s"
