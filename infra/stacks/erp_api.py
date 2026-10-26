"""ErpApiStack: the simulated ERP's REST API, in Lambda behind a function URL (M2 step 6).

The ERP poller (M2 step 7) runs in AWS and needs an ERP to call. This stack
puts the same FastAPI app that runs on a laptop into one Lambda function:

- the **function** (Python 3.12, arm64) serves the API through Mangum, with
  the ERP's store (`erp.db`, from a backfill) packaged alongside the code and
  opened read-only;
- its **function URL** is open to the internet: the ERP's own OAuth token
  endpoint is the authentication, as it would be for a real ERP's API;
- its **log group** keeps a week of logs and goes when the stack goes.

Nothing runs between requests, so idle it costs nothing.

The client secret and the token signing key are SecureString parameters in
Parameter Store, which CloudFormation cannot create (it would have to write
the value into the template). `crestline erp-api secrets` creates them with
random values before the first deploy; this stack only lets the function
read them, and `cdk destroy` leaves them (`crestline erp-api secrets
--delete` removes them).

How the deployed API behaves is chosen at deploy time with CDK context: no
mess and no clock serves the store as it stands (step 1's clean API);
`-c erpApiMess=../mess.yaml` adds the imperfections, and `-c
erpApiClockStart=... -c erpApiClockSpeed=...` replays the ERP's history. The
clock's anchor is pinned in the environment at synth time, so every copy of
the function agrees on what time it is in the ERP.
"""

import shutil
import subprocess
import tempfile
from pathlib import Path

import aws_cdk as cdk
import jsii
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_logs as logs
from aws_cdk import aws_ssm as ssm
from constructs import Construct

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
CLIENT_ID = "crestline-poller"
# Setting -> its SecureString parameter (see `crestline erp-api secrets`).
SECRET_PARAMETERS = {
    "client_secret": "/crestline/erp-api/client-secret",
    "signing_key": "/crestline/erp-api/signing-key",
}
# The API's URL, published for its clients (ErpIngestStack's extractor), so a
# client stack never depends on this one and can't redeploy it by accident.
URL_PARAMETER = "/crestline/erp-api/url"
TASK_ROOT = "/var/task"  # where Lambda unpacks the package


class ErpApiStack(cdk.Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        store: Path,
        mess: Path | None = None,
        clock: dict[str, str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        environment = {
            "CRESTLINE_ERP_DB": f"{TASK_ROOT}/erp.db",
            "CRESTLINE_ERP_CLIENT_ID": CLIENT_ID,
            "CRESTLINE_ERP_CLIENT_SECRET_PARAMETER": SECRET_PARAMETERS["client_secret"],
            "CRESTLINE_ERP_SIGNING_KEY_PARAMETER": SECRET_PARAMETERS["signing_key"],
        }
        if mess is not None:
            environment["CRESTLINE_ERP_MESS"] = f"{TASK_ROOT}/mess.yaml"
        for setting, value in (clock or {}).items():
            environment[f"CRESTLINE_ERP_CLOCK_{setting.upper()}"] = value

        function = lambda_.Function(
            self,
            "Function",
            description="The simulated ERP's REST API (Crestline Supply)",
            runtime=lambda_.Runtime.PYTHON_3_12,
            architecture=lambda_.Architecture.ARM_64,
            handler="crestline_erp_api.lambda_handler.handler",
            code=lambda_.Code.from_asset(
                str(REPO_ROOT / "interfaces" / "erp-api"),
                asset_hash_type=cdk.AssetHashType.OUTPUT,
                bundling=cdk.BundlingOptions(
                    image=lambda_.Runtime.PYTHON_3_12.bundling_image,
                    local=UvBundle(store, mess),
                    command=["false"],  # bundled on the host with uv; see UvBundle
                ),
            ),
            memory_size=512,
            timeout=cdk.Duration.seconds(30),
            environment=environment,
            log_group=logs.LogGroup(
                self,
                "Logs",
                retention=logs.RetentionDays.ONE_WEEK,
                removal_policy=cdk.RemovalPolicy.DESTROY,
            ),
        )
        for setting, name in SECRET_PARAMETERS.items():
            ssm.StringParameter.from_secure_string_parameter_attributes(
                self,
                f"{setting.title().replace('_', '')}Parameter",
                parameter_name=name,
            ).grant_read(function)

        url = function.add_function_url(auth_type=lambda_.FunctionUrlAuthType.NONE)
        ssm.StringParameter(
            self,
            "UrlParameter",
            parameter_name=URL_PARAMETER,
            string_value=url.url,
            description="The simulated ERP API's URL (Crestline Supply)",
        )
        cdk.CfnOutput(self, "ErpApiUrl", value=url.url)
        cdk.CfnOutput(self, "ErpApiClientId", value=CLIENT_ID)


@jsii.implements(cdk.ILocalBundling)
class UvBundle:
    """Build the function's package on this machine with uv, no Docker needed.

    The package is: the API's dependencies as Linux arm64 wheels, pinned by
    `uv.lock`; the API itself; the two modules of `crestline` it imports
    (`mess` and `rng`, which never import the simulator), not the whole
    package with its simulator and data libraries; the store; and the mess
    file, if any.
    """

    def __init__(self, store: Path, mess: Path | None) -> None:
        self.store = store
        self.mess = mess

    def try_bundle(self, output_dir: str, *args, **kwargs) -> bool:
        if not self.store.is_file():
            raise FileNotFoundError(
                f"no ERP store at {self.store}; run `crestline sim backfill` first"
            )
        out = Path(output_dir)
        with tempfile.TemporaryDirectory() as scratch:
            requirements = Path(scratch) / "requirements.txt"
            self._uv(
                "export",
                "--package=crestline-erp-api",
                "--frozen",
                "--no-dev",
                "--no-hashes",
                "--no-emit-workspace",
                "--quiet",
                f"--output-file={requirements}",
            )
            self._uv(
                "pip",
                "install",
                f"--requirements={requirements}",
                f"--target={out}",
                "--python-platform=aarch64-manylinux2014",
                "--python-version=3.12",
                "--only-binary=:all:",
                "--quiet",
            )
        shutil.copytree(
            REPO_ROOT / "interfaces/erp-api/src/crestline_erp_api",
            out / "crestline_erp_api",
            ignore=shutil.ignore_patterns("__pycache__"),
        )
        (out / "crestline").mkdir()
        for module in ("__init__.py", "mess.py", "rng.py"):
            shutil.copy(
                REPO_ROOT / "src/crestline" / module, out / "crestline" / module
            )
        shutil.copy(self.store, out / "erp.db")
        if self.mess is not None:
            shutil.copy(self.mess, out / "mess.yaml")
        return True

    @staticmethod
    def _uv(*arguments: str) -> None:
        subprocess.run(["uv", *arguments], cwd=REPO_ROOT, check=True)
