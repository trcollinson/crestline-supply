"""BriefStack: the morning brief, written and delivered at 05:30 (M5 step 6).

- **Scheduler**, every weekday at 05:30 America/Denver (or `-c briefSchedule`),
  invokes
- **the function** (Python 3.12, arm64), `crestline.brief.aws.handler`: the
  brief's facts from Athena's brief layer (`crestline_brief`, built by
  `crestline athena setup`), the model's prose through the number check, the
  template if it fails; it writes
- **the bucket**: `brief/<morning>.md` and `.html`, `brief/logs/`, and every
  morning's facts in `brief/facts/` (the ledger reads earlier mornings there);
  and publishes the text to
- **the topic**, which emails it (`-c briefEmail=...`; SNS asks the address to
  confirm first).

The package is polars (pinned by `uv.lock`, Linux arm64 wheels), the brief's
modules, the Athena helper, and the SQL the brief layer runs: not the
simulator, not DuckDB.

The function may run Athena in the one workgroup, read the raw, curated, and
brief databases and replace views in the brief database only (setting the
morning replaces `brief_cut`), read the MES prefix of the raw bucket and the
curated bucket (what the layer's sources read; the agent's tools read the
same), call the two brief models, and write its own bucket and topic.

The morning comes from a replay clock (crestline.brief.aws), set at deploy
time: `-c briefClockStart=2026-08-12 -c briefClockAnchor=2026-11-23` writes
Aug 12's brief on Nov 23 and moves on a plant weekday per real weekday. Both
are written into the function's environment, so `cdk diff` needs the same
flags; without them every run writes the last morning with data.

Nothing runs between mornings.
"""

import shutil
import subprocess
import tempfile
from pathlib import Path

import aws_cdk as cdk
import jsii
from aws_cdk import aws_iam as iam
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_logs as logs
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_scheduler as scheduler
from aws_cdk import aws_scheduler_targets as targets
from aws_cdk import aws_sns as sns
from aws_cdk import aws_sns_subscriptions as subscriptions
from constructs import Construct

from machine_curate.tables import CURATED_DATABASE
from stacks.foundation import ATHENA_WORKGROUP, BRIEF_DATABASE, GLUE_DATABASE

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
# brief/narrate.py's MODELS: the inference profiles and the models they route to.
MODELS = {
    "sonnet": ("us.anthropic.claude-sonnet-4-6", "anthropic.claude-sonnet-4-6"),
    "nova": ("us.amazon.nova-2-lite-v1:0", "amazon.nova-2-lite-v1:0"),
}
# What the package carries of the repo: the brief and what it imports, and
# the SQL its layer runs (crestline.brief.facts.LAYER_FILES).
MODULES = ("__init__.py", "athena.py", "stack.py")
QUERY_FILES = ("as_of", "machines", "margin_gap", "downtime", "scrap", "brief")
PACKAGES = ("polars", "polars-runtime-32")


class BriefStack(cdk.Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        raw_bucket: s3.IBucket,
        results_bucket: s3.IBucket,
        curated_bucket: s3.IBucket,
        email: str | None = None,
        schedule: str | None = None,
        clock_start: str | None = None,
        clock_anchor: str | None = None,
        last_morning: str = "2026-10-01",
        model: str = "sonnet",
        **kwargs,
    ) -> None:
        if bool(clock_start) != bool(clock_anchor):
            raise ValueError("briefClockStart and briefClockAnchor go together")
        super().__init__(
            scope,
            construct_id,
            description="Crestline morning brief: scheduled facts, prose, the number check, email",
            **kwargs,
        )

        bucket = s3.Bucket(
            self,
            "BriefBucket",
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
            removal_policy=cdk.RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )
        topic = sns.Topic(self, "BriefTopic", display_name="Crestline morning brief")
        if email:
            topic.add_subscription(subscriptions.EmailSubscription(email))

        environment = {
            "CRESTLINE_BRIEF_BUCKET": bucket.bucket_name,
            "CRESTLINE_BRIEF_TOPIC_ARN": topic.topic_arn,
            "CRESTLINE_ATHENA_WORKGROUP": ATHENA_WORKGROUP,
            "CRESTLINE_RAW_DATABASE": GLUE_DATABASE,
            "CRESTLINE_BRIEF_DATABASE": BRIEF_DATABASE,
            "CRESTLINE_BRIEF_MODEL": model,
            "CRESTLINE_BRIEF_LAST_MORNING": last_morning,
        }
        if clock_start and clock_anchor:
            environment["CRESTLINE_BRIEF_CLOCK_START"] = clock_start
            environment["CRESTLINE_BRIEF_CLOCK_ANCHOR"] = clock_anchor

        function = lambda_.Function(
            self,
            "Function",
            description="Write and deliver the Crestline morning brief (M5)",
            runtime=lambda_.Runtime.PYTHON_3_12,
            architecture=lambda_.Architecture.ARM_64,
            handler="crestline.brief.aws.handler",
            code=lambda_.Code.from_asset(
                str(REPO_ROOT / "src" / "crestline" / "brief"),
                asset_hash_type=cdk.AssetHashType.OUTPUT,
                bundling=cdk.BundlingOptions(
                    image=lambda_.Runtime.PYTHON_3_12.bundling_image,
                    local=BriefBundle(),
                    command=["false"],  # bundled on the host with uv; see BriefBundle
                ),
            ),
            # polars wants the memory; Athena takes most of the time (about a
            # minute a morning), and a cold ledger store would take longer.
            memory_size=1024,
            timeout=cdk.Duration.minutes(15),
            environment=environment,
            log_group=logs.LogGroup(
                self,
                "Logs",
                retention=logs.RetentionDays.ONE_WEEK,
                removal_policy=cdk.RemovalPolicy.DESTROY,
            ),
        )
        self._grant_athena(function, raw_bucket, results_bucket, curated_bucket)
        self._grant_models(function)
        bucket.grant_read_write(function)
        topic.grant_publish(function)

        denver = cdk.TimeZone.AMERICA_DENVER
        scheduler.Schedule(
            self,
            "Schedule",
            description="Write the Crestline morning brief before first shift",
            schedule=(
                scheduler.ScheduleExpression.expression(schedule, denver)
                if schedule
                else scheduler.ScheduleExpression.cron(
                    minute="30", hour="5", week_day="MON-FRI", time_zone=denver
                )
            ),
            target=targets.LambdaInvoke(
                function,
                input=scheduler.ScheduleTargetInput.from_object({}),
                retry_attempts=0,  # a second brief would be a second email
            ),
        )

        cdk.CfnOutput(self, "BriefBucketName", value=bucket.bucket_name)
        cdk.CfnOutput(self, "BriefTopicArn", value=topic.topic_arn)
        cdk.CfnOutput(self, "BriefFunctionName", value=function.function_name)

    def _grant_athena(
        self,
        fn: lambda_.Function,
        raw_bucket: s3.IBucket,
        results_bucket: s3.IBucket,
        curated_bucket: s3.IBucket,
    ) -> None:
        arn = f"arn:{self.partition}"
        where = f"{self.region}:{self.account}"
        fn.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "athena:StartQueryExecution",
                    "athena:GetQueryExecution",
                    "athena:GetQueryResults",
                ],
                resources=[f"{arn}:athena:{where}:workgroup/{ATHENA_WORKGROUP}"],
            )
        )
        databases = (GLUE_DATABASE, CURATED_DATABASE, BRIEF_DATABASE)
        fn.add_to_role_policy(
            iam.PolicyStatement(
                actions=["glue:GetDatabase", "glue:GetTable", "glue:GetPartitions"],
                resources=[f"{arn}:glue:{where}:catalog"]
                + [f"{arn}:glue:{where}:database/{d}" for d in databases]
                + [f"{arn}:glue:{where}:table/{d}/*" for d in databases],
            )
        )
        # Setting the morning replaces one view (brief_cut) in the brief's
        # own database: the only thing the function changes in the catalog.
        fn.add_to_role_policy(
            iam.PolicyStatement(
                actions=["glue:CreateTable", "glue:UpdateTable", "glue:DeleteTable"],
                resources=[
                    f"{arn}:glue:{where}:catalog",
                    f"{arn}:glue:{where}:database/{BRIEF_DATABASE}",
                    f"{arn}:glue:{where}:table/{BRIEF_DATABASE}/*",
                ],
            )
        )
        raw_bucket.grant_read(fn, "source=mes/*")
        curated_bucket.grant_read(fn)
        results_bucket.grant_read(fn)
        results_bucket.grant_put(fn)

    def _grant_models(self, fn: lambda_.Function) -> None:
        """Converse through the `us.` inference profiles, which route each
        call to a model in one of several US regions."""
        arn = f"arn:{self.partition}"
        resources = []
        for profile, model in MODELS.values():
            resources.append(
                f"{arn}:bedrock:{self.region}:{self.account}:inference-profile/{profile}"
            )
            resources.append(f"{arn}:bedrock:*::foundation-model/{model}")
        fn.add_to_role_policy(
            iam.PolicyStatement(actions=["bedrock:InvokeModel"], resources=resources)
        )


@jsii.implements(cdk.ILocalBundling)
class BriefBundle:
    """Build the function's package on this machine with uv, no Docker needed:
    polars as Linux arm64 wheels at the versions `uv.lock` pins, the brief's
    modules and the two of `crestline` they import (never the simulator), and
    the brief layer's SQL."""

    def try_bundle(self, output_dir: str, *args, **kwargs) -> bool:
        out = Path(output_dir)
        with tempfile.TemporaryDirectory() as scratch:
            exported = subprocess.run(
                ["uv", "export", "--frozen", "--no-dev", "--no-hashes",
                 "--all-packages", "--no-emit-workspace", "--quiet"],
                cwd=REPO_ROOT, check=True, capture_output=True, text=True,
            ).stdout  # fmt: skip
            pinned = [
                line.split(";")[0].strip()
                for line in exported.splitlines()
                if line.split("==")[0] in PACKAGES
            ]
            if len(pinned) != len(PACKAGES):
                raise RuntimeError(f"uv.lock doesn't pin all of {PACKAGES}: {pinned}")
            requirements = Path(scratch) / "requirements.txt"
            requirements.write_text("\n".join(pinned) + "\n")
            subprocess.run(
                ["uv", "pip", "install", f"--requirements={requirements}",
                 f"--target={out}", "--python-platform=aarch64-manylinux2014",
                 "--python-version=3.12", "--only-binary=:all:", "--quiet"],
                cwd=REPO_ROOT, check=True,
            )  # fmt: skip
        # Paths of this checkout end up in console scripts and RECORD; neither
        # runs in Lambda, and both would change the asset hash (M5 step 2).
        shutil.rmtree(out / "bin", ignore_errors=True)
        for record in out.glob("*.dist-info/RECORD"):
            record.unlink()
        package = out / "crestline"
        package.mkdir()
        for module in MODULES:
            shutil.copy(REPO_ROOT / "src/crestline" / module, package / module)
        shutil.copytree(
            REPO_ROOT / "src/crestline/brief",
            package / "brief",
            ignore=shutil.ignore_patterns("__pycache__"),
        )
        (out / "queries").mkdir()
        for name in QUERY_FILES:
            shutil.copy(REPO_ROOT / "queries" / f"{name}.sql", out / "queries")
        return True
