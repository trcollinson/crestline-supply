"""AgentStack: the agent's tools behind an AgentCore Gateway (spec §9, M4 step 3).

    agent (SigV4, as an IAM role) --MCP--> AgentCore Gateway (AWS_IAM)
        PlantData___downtime_events, machine_downtime,   Lambda -> Athena views (read)
                    cycles_by_day, job_margin, job_status
        Maintenance___create_maintenance_request          Lambda -> DynamoDB crestline-cmms-stub
        Reports___send_report                             Lambda -> S3 outbox/ (sends nothing)
        PlantScada___write_tag, read_tag                  Lambda honeypot: logs, reaches nothing

What each Lambda may touch is the first line of defense, and it is narrow:
the reads can run Athena in the crestline workgroup over the raw and
curated databases and nothing else (the answer key never leaves the
laptop); the CMMS write can put items in one table; the report can put
objects under one prefix; the SCADA honeypot can write its own logs and no
more, outside any VPC.

The gateway checks who is calling (IAM), and a policy engine decides what
each caller may call (M4 step 4): the Cedar rules in infra/policies/, one a
file, created as they are written. Two roles are the principals the rules
name: `crestline-ops-analyst` and `crestline-maintenance-planner`, each
allowed only to invoke this gateway. The engine's mode is a deploy-time
choice, `-c agentPolicyMode=ENFORCE|LOG_ONLY` (ENFORCE unless given), and
`-c agentScadaForbid=false` leaves the PlantScada forbid out, to show that
default deny refuses those tools anyway.
"""

from pathlib import Path

import aws_cdk as cdk
from aws_cdk import aws_bedrockagentcore as agentcore
from aws_cdk import aws_dynamodb as dynamodb
from aws_cdk import aws_iam as iam
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_logs as logs
from aws_cdk import aws_s3 as s3
from constructs import Construct

from agent_tools.tools import TARGETS, Tool
from machine_curate.tables import CURATED_DATABASE
from stacks.erp_api import REPO_ROOT
from stacks.foundation import ATHENA_WORKGROUP, GLUE_DATABASE

GATEWAY_NAME = "crestline-agent"
POLICY_ENGINE_NAME = "crestline_agent"
POLICIES_DIR = REPO_ROOT / "infra" / "policies"
POLICY_MODES = ("ENFORCE", "LOG_ONLY")
SCADA_FORBID = "forbid_plant_scada"
# The red-team eval's configurations (M4 step 6): `open` swaps the rules for
# one that permits both roles everything (configuration B, a guardrail
# alone); the guardrail withholds a tool reply scored as a prompt attack.
EVAL_POLICIES = POLICIES_DIR / "eval"
POLICY_SETS = ("cedar", "open")
OPEN_POLICY = EVAL_POLICIES / "permit_everything.cedar"
GUARDRAIL_POLICY = EVAL_POLICIES / "suppress_prompt_attacks.policy"
CMMS_TABLE = "crestline-cmms-stub"
ROLES = ("crestline-ops-analyst", "crestline-maintenance-planner")
# Which Lambda serves each target (pipelines/agent_tools/).
HANDLERS = {
    "PlantData": "agent_tools.plant_data.handler",
    "Maintenance": "agent_tools.writes.maintenance_handler",
    "Reports": "agent_tools.writes.report_handler",
    "PlantScada": "agent_tools.scada.handler",
}


class AgentStack(cdk.Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        raw_bucket: s3.IBucket,
        results_bucket: s3.IBucket,
        curated_bucket: s3.IBucket,
        data_last_day: str,
        policy_mode: str = "ENFORCE",
        scada_forbid: bool = True,
        policy_set: str = "cedar",
        guardrail: bool = False,
        **kwargs,
    ) -> None:
        if policy_set not in POLICY_SETS:
            raise ValueError(f"policy_set must be one of {POLICY_SETS}")
        if policy_mode not in POLICY_MODES:
            raise ValueError(f"agentPolicyMode must be one of {POLICY_MODES}")
        super().__init__(
            scope,
            construct_id,
            description="Crestline agent: AgentCore Gateway, tool Lambdas, stub CMMS, outbox",
            **kwargs,
        )

        cmms = dynamodb.Table(
            self,
            "CmmsStub",
            table_name=CMMS_TABLE,
            partition_key=dynamodb.Attribute(
                name="request_id", type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=cdk.RemovalPolicy.DESTROY,
        )
        outbox = s3.Bucket(
            self,
            "AgentBucket",
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
            removal_policy=cdk.RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        functions = {
            target: self._function(target, HANDLERS[target]) for target in TARGETS
        }

        reads = functions["PlantData"]
        reads.add_environment("CRESTLINE_ATHENA_WORKGROUP", ATHENA_WORKGROUP)
        reads.add_environment("CRESTLINE_RAW_DATABASE", GLUE_DATABASE)
        reads.add_environment("CRESTLINE_DATA_LAST_DAY", data_last_day)
        self._grant_athena_reads(reads, raw_bucket, results_bucket, curated_bucket)

        maintenance = functions["Maintenance"]
        maintenance.add_environment("CRESTLINE_CMMS_TABLE", cmms.table_name)
        maintenance.add_to_role_policy(
            iam.PolicyStatement(
                actions=["dynamodb:PutItem"], resources=[cmms.table_arn]
            )
        )

        reports = functions["Reports"]
        reports.add_environment("CRESTLINE_AGENT_BUCKET", outbox.bucket_name)
        reports.add_to_role_policy(
            iam.PolicyStatement(
                actions=["s3:PutObject"], resources=[outbox.arn_for_objects("outbox/*")]
            )
        )
        # PlantScada gets nothing beyond the logs every function has.

        gateway_role = iam.Role(
            self,
            "GatewayRole",
            assumed_by=iam.ServicePrincipal("bedrock-agentcore.amazonaws.com"),
        )
        for fn in functions.values():
            fn.grant_invoke(gateway_role)

        engine = agentcore.CfnPolicyEngine(
            self,
            "PolicyEngine",
            name=POLICY_ENGINE_NAME,
            description="Who may call which Crestline agent tool (infra/policies/)",
        )
        # The gateway asks the engine about every call: AuthorizeAction for a
        # tools/call, PartiallyAuthorizeActions for a tools/list (it hides the
        # tools a caller could never be permitted). Without GetPolicyEngine,
        # LOG_ONLY fails silently (M4 step 0). The gateway's own ARN would be
        # a cycle (it needs this role first), so any gateway in the account.
        any_gateway = self.format_arn(
            service="bedrock-agentcore", resource="gateway", resource_name="*"
        )
        gateway_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "bedrock-agentcore:AuthorizeAction",
                    "bedrock-agentcore:PartiallyAuthorizeActions",
                ],
                resources=[engine.attr_policy_engine_arn, any_gateway],
            )
        )
        gateway_role.add_to_policy(
            iam.PolicyStatement(
                actions=["bedrock-agentcore:GetPolicyEngine"],
                resources=[engine.attr_policy_engine_arn],
            )
        )

        gateway = agentcore.CfnGateway(
            self,
            "Gateway",
            name=GATEWAY_NAME,
            description="Crestline plant data tools for the M4 agent",
            role_arn=gateway_role.role_arn,
            authorizer_type="AWS_IAM",
            protocol_type="MCP",
            protocol_configuration=agentcore.CfnGateway.GatewayProtocolConfigurationProperty(
                mcp=agentcore.CfnGateway.MCPGatewayConfigurationProperty(
                    supported_versions=["2025-11-25"]
                )
            ),
            exception_level="DEBUG",
            policy_engine_configuration=agentcore.CfnGateway.GatewayPolicyEngineConfigurationProperty(
                arn=engine.attr_policy_engine_arn, mode=policy_mode
            ),
        )
        gateway.node.add_dependency(gateway_role)
        self.gateway = gateway
        self.targets = {
            target: self._target(gateway, target, functions[target], tools)
            for target, tools in TARGETS.items()
        }
        rules = (
            [OPEN_POLICY]
            if policy_set == "open"
            else [
                path
                for path in sorted(POLICIES_DIR.glob("*.cedar"))
                if scada_forbid or path.stem != SCADA_FORBID
            ]
        )
        self.policies = {
            path.stem: self._policy(engine, gateway, path) for path in rules
        }
        if guardrail:
            # A guardrail in a policy runs with the gateway's role (M4 step 0).
            gateway_role.add_to_policy(
                iam.PolicyStatement(
                    actions=["bedrock:InvokeGuardrailChecks"], resources=["*"]
                )
            )
            # A guardrail rule must name its action, so one per read tool:
            # the reads are where text anyone typed reaches the agent.
            for tool in TARGETS["PlantData"]:
                name = f"guardrail_{tool.name}"
                self.policies[name] = self._policy(
                    engine,
                    gateway,
                    GUARDRAIL_POLICY,
                    name=name,
                    values={"ReadTool": f"PlantData___{tool.name}"},
                )

        # Traces: the per-call record of what the gateway (and from step 4,
        # the policy engine) decided. Spans land in the account's `aws/spans`
        # log group; that needs CloudWatch Transaction Search (M4 step 0).
        traces_source = logs.CfnDeliverySource(
            self,
            "TracesSource",
            name="crestline-agent-gateway-traces",
            log_type="TRACES",
            resource_arn=gateway.attr_gateway_arn,
        )
        traces_destination = logs.CfnDeliveryDestination(
            self,
            "TracesDestination",
            name="crestline-agent-gateway-xray",
            delivery_destination_type="XRAY",
        )
        traces = logs.CfnDelivery(
            self,
            "Traces",
            delivery_source_name=traces_source.name,
            delivery_destination_arn=traces_destination.attr_arn,
        )
        traces.add_resource_dependency(traces_source)

        # The gateway's application logs. Under LOG_ONLY the engine decides
        # each call asynchronously and the trace spans don't record it (M4
        # step 4: the call's span says only LOG_ONLY_ASYNC); this delivery is
        # where a per-call record of a would-be denial could be.
        gateway_logs = logs.LogGroup(
            self,
            "GatewayLogs",
            log_group_name="/aws/vendedlogs/bedrock-agentcore/crestline-agent-gateway",
            retention=logs.RetentionDays.ONE_WEEK,
            removal_policy=cdk.RemovalPolicy.DESTROY,
        )
        logs_source = logs.CfnDeliverySource(
            self,
            "GatewayLogsSource",
            name="crestline-agent-gateway-logs",
            log_type="APPLICATION_LOGS",
            resource_arn=gateway.attr_gateway_arn,
        )
        logs_destination = logs.CfnDeliveryDestination(
            self,
            "GatewayLogsDestination",
            name="crestline-agent-gateway-logs",
            delivery_destination_type="CWL",
            destination_resource_arn=gateway_logs.log_group_arn,
        )
        delivery = logs.CfnDelivery(
            self,
            "GatewayLogsDelivery",
            delivery_source_name=logs_source.name,
            delivery_destination_arn=logs_destination.attr_arn,
        )
        delivery.add_resource_dependency(logs_source)

        for role_name in ROLES:
            role = iam.Role(
                self,
                role_name.removeprefix("crestline-").title().replace("-", ""),
                role_name=role_name,
                description="An M4 agent principal: may invoke the Crestline gateway, nothing else",
                assumed_by=iam.AccountRootPrincipal(),
                max_session_duration=cdk.Duration.hours(1),
            )
            role.add_to_policy(
                iam.PolicyStatement(
                    actions=["bedrock-agentcore:InvokeGateway"],
                    resources=[gateway.attr_gateway_arn],
                )
            )

        cdk.CfnOutput(self, "GatewayUrl", value=gateway.attr_gateway_url)
        cdk.CfnOutput(self, "GatewayId", value=gateway.attr_gateway_identifier)
        cdk.CfnOutput(self, "GatewayArn", value=gateway.attr_gateway_arn)
        cdk.CfnOutput(self, "CmmsTableName", value=cmms.table_name)
        cdk.CfnOutput(self, "AgentBucketName", value=outbox.bucket_name)
        cdk.CfnOutput(self, "PolicyEngineId", value=engine.attr_policy_engine_id)
        cdk.CfnOutput(self, "PolicyMode", value=policy_mode)
        cdk.CfnOutput(self, "PolicySet", value=policy_set)
        cdk.CfnOutput(self, "Guardrail", value=str(guardrail).lower())
        cdk.CfnOutput(self, "GatewayLogGroup", value=gateway_logs.log_group_name)
        for target, fn in functions.items():
            cdk.CfnOutput(self, f"{target}LogGroup", value=fn.log_group.log_group_name)

    def _function(self, target: str, handler: str) -> lambda_.Function:
        return lambda_.Function(
            self,
            f"{target}Tools",
            description=f"Crestline agent tools: {target} (M4)",
            runtime=lambda_.Runtime.PYTHON_3_12,
            architecture=lambda_.Architecture.ARM_64,
            handler=handler,
            code=lambda_.Code.from_asset(
                str(REPO_ROOT / "pipelines"),
                exclude=["**/__pycache__", "pyproject.toml"],
            ),
            memory_size=256,
            # A read over the machine views can take Athena a while.
            timeout=cdk.Duration.minutes(2),
            log_group=logs.LogGroup(
                self,
                f"{target}ToolsLogs",
                retention=logs.RetentionDays.ONE_WEEK,
                removal_policy=cdk.RemovalPolicy.DESTROY,
            ),
        )

    def _grant_athena_reads(
        self,
        fn: lambda_.Function,
        raw_bucket: s3.IBucket,
        results_bucket: s3.IBucket,
        curated_bucket: s3.IBucket,
    ) -> None:
        """Athena in one workgroup, over the raw and curated databases only."""
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
        fn.add_to_role_policy(
            iam.PolicyStatement(
                actions=["glue:GetDatabase", "glue:GetTable", "glue:GetPartitions"],
                resources=[
                    f"{arn}:glue:{where}:catalog",
                    f"{arn}:glue:{where}:database/{GLUE_DATABASE}",
                    f"{arn}:glue:{where}:table/{GLUE_DATABASE}/*",
                    f"{arn}:glue:{where}:database/{CURATED_DATABASE}",
                    f"{arn}:glue:{where}:table/{CURATED_DATABASE}/*",
                ],
            )
        )
        # The views read the MES files in raw and the Iceberg tables in curated.
        raw_bucket.grant_read(fn, "source=mes/*")
        curated_bucket.grant_read(fn)
        results_bucket.grant_read(fn)
        results_bucket.grant_put(fn)

    def _target(
        self,
        gateway: agentcore.CfnGateway,
        target: str,
        fn: lambda_.Function,
        tools: list[Tool],
    ) -> agentcore.CfnGatewayTarget:
        schema = agentcore.CfnGatewayTarget
        return schema(
            self,
            f"{target}Target",
            gateway_identifier=gateway.attr_gateway_identifier,
            name=target,
            credential_provider_configurations=[
                schema.CredentialProviderConfigurationProperty(
                    credential_provider_type="GATEWAY_IAM_ROLE"
                )
            ],
            target_configuration=schema.TargetConfigurationProperty(
                mcp=schema.McpTargetConfigurationProperty(
                    lambda_=schema.McpLambdaTargetConfigurationProperty(
                        lambda_arn=fn.function_arn,
                        tool_schema=schema.ToolSchemaProperty(
                            inline_payload=[
                                schema.ToolDefinitionProperty(
                                    name=tool.name,
                                    description=tool.description,
                                    input_schema=schema.SchemaDefinitionProperty(
                                        type="object",
                                        properties={
                                            key: schema.SchemaDefinitionProperty(
                                                **value
                                            )
                                            for key, value in tool.properties.items()
                                        },
                                        required=tool.required,
                                    ),
                                )
                                for tool in tools
                            ]
                        ),
                    )
                )
            ),
        )

    def _policy(
        self,
        engine: agentcore.CfnPolicyEngine,
        gateway: agentcore.CfnGateway,
        path: Path,
        name: str | None = None,
        values: dict[str, str] | None = None,
    ) -> agentcore.CfnPolicy:
        """One policy file as one policy, named for the file (or `name`, when
        one template makes several; `values` fill its other variables).

        `${GatewayArn}` and `${AWS::AccountId}` are filled in by Fn::Sub, so
        the files name no account. A forbid is created without the
        analyzer's findings: one that denies every call is the point, and the
        analyzer calls it "Overly Restrictive", which would fail the stack
        (M4 step 0). The permits must pass it.
        """
        name = name or path.stem
        variables = {"GatewayArn": gateway.attr_gateway_arn, **(values or {})}
        statement = cdk.Fn.sub(path.read_text(), variables)
        if path.suffix == ".policy":
            # A guardrail rule (`suppressOutput ... when guardrails`) is not
            # Cedar: it goes in the definition's `policy` statement, without
            # its comment (M4 step 0 tested it bare).
            body = "\n".join(
                line
                for line in path.read_text().splitlines()
                if not line.startswith("//")
            )
            definition = agentcore.CfnPolicy.PolicyDefinitionProperty(
                policy=agentcore.CfnPolicy.PolicyStatementProperty(
                    statement=cdk.Fn.sub(body, variables)
                )
            )
        else:
            definition = agentcore.CfnPolicy.PolicyDefinitionProperty(
                cedar=agentcore.CfnPolicy.CedarPolicyProperty(statement=statement)
            )
        policy = agentcore.CfnPolicy(
            self,
            "Policy" + name.title().replace("_", ""),
            name=name,
            description=_first_comment(path),
            policy_engine_id=engine.attr_policy_engine_id,
            # Permits must pass the analyzer. A forbid that denies every call
            # is "Overly Restrictive" to it, and the eval's open permit and
            # guardrail are deliberate too (M4 steps 0 and 6).
            validation_mode=(
                "FAIL_ON_ANY_FINDINGS"
                if path.stem.startswith("permit_") and path != OPEN_POLICY
                else "IGNORE_ALL_FINDINGS"
            ),
            definition=definition,
        )
        # Cedar is validated against the gateway's tools, so they exist first.
        for target in self.targets.values():
            policy.add_resource_dependency(target)
        return policy


def _first_comment(path: Path) -> str:
    """The rule's opening comment, as one line: what it says, in words."""
    lines = []
    for line in path.read_text().splitlines():
        if not line.startswith("//"):
            break
        lines.append(line.removeprefix("//").strip())
    return " ".join(lines)
