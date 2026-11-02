"""MachineLoadtestStack: what 1,000 tags cost, measured from the bill (M3 step 9).

Opt-in (`-c machineLoadtest=true`), deployed for one test and destroyed
after it. The same path as MachineIngestStack, for a synthetic gateway
(`crestline_machines.loadtest`):

    LOADTEST-GW --MQTT 5/TLS--> IoT Core Basic Ingest --> rule A --> Firehose A --> S3
                                                      \\-> rule B --> Firehose B --> S3

One rule and one Firehose stream per mode, each tagged with its own
`layer` (`machine-loadtest-a`, `machine-loadtest-b`), so Cost Explorer
splits the modes though they run in the same hour; the thing, the policy,
the role, and the bucket they share are `machine-loadtest`. The `layer`
key is already a cost allocation tag, so new values need no activation.
The bucket is the stack's own, so storage and requests bill to the test
too; `crestline machines loadtest --cleanup` empties it (and deletes the
certificate) before `cdk destroy`.
"""

import aws_cdk as cdk
from aws_cdk import aws_iam as iam
from aws_cdk import aws_iot as iot
from aws_cdk import aws_kinesisfirehose as firehose
from aws_cdk import aws_logs as logs
from aws_cdk import aws_s3 as s3
from constructs import Construct

from crestline_machines.devices import (
    LOADTEST_FILTERS,
    LOADTEST_GATEWAY,
    LOADTEST_RULES,
    policy_name,
)

LAYER = "machine-loadtest"
BUFFER = cdk.Duration.seconds(60)


class MachineLoadtestStack(cdk.Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        bucket = s3.Bucket(
            self,
            "Landing",
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
            removal_policy=cdk.RemovalPolicy.DESTROY,
            lifecycle_rules=[s3.LifecycleRule(expiration=cdk.Duration.days(1))],
        )
        iot.CfnThing(self, "Thing", thing_name=LOADTEST_GATEWAY)
        iot.CfnPolicy(
            self,
            "Policy",
            policy_name=policy_name(LOADTEST_GATEWAY),
            policy_document={
                "Version": "2012-10-17",
                "Statement": [
                    {
                        # Several connections, all named for this gateway. No
                        # IsAttached condition, unlike the plant's gateways: it
                        # holds only when the client ID is the thing's own name,
                        # and these are LOADTEST-GW-00, -01, ... The policy is
                        # attached to this gateway's certificate alone.
                        "Effect": "Allow",
                        "Action": "iot:Connect",
                        "Resource": self._iot_arn("client", f"{LOADTEST_GATEWAY}-*"),
                    },
                    {
                        "Effect": "Allow",
                        "Action": "iot:Publish",
                        "Resource": [
                            self._iot_arn("topic", f"$aws/rules/{rule}/*")
                            for rule in LOADTEST_RULES.values()
                        ],
                    },
                ],
            },
        )
        role = iam.Role(
            self,
            "RulesRole",
            assumed_by=iam.ServicePrincipal("iot.amazonaws.com"),
            description="Crestline load test rules: deliver to Firehose, write errors",
        )
        bucket.grant_put(role, "errors/*")

        for mode, rule_name in LOADTEST_RULES.items():
            scope = Construct(self, f"Mode{mode.upper()}")
            cdk.Tags.of(scope).add("layer", f"{LAYER}-{mode}", priority=200)
            stream = firehose.DeliveryStream(
                scope,
                "Stream",
                destination=firehose.S3Bucket(
                    bucket,
                    data_output_prefix=f"mode={mode}/!{{timestamp:yyyy-MM-dd-HH}}/",
                    error_output_prefix=f"firehose-errors/mode={mode}/!{{firehose:error-output-type}}/",
                    file_extension=".jsonl",
                    buffering_interval=BUFFER,
                    logging_config=firehose.EnableLogging(
                        logs.LogGroup(
                            scope,
                            "StreamLogs",
                            retention=logs.RetentionDays.ONE_DAY,
                            removal_policy=cdk.RemovalPolicy.DESTROY,
                        )
                    ),
                ),
            )
            stream.grant_put_records(role)
            iot.CfnTopicRule(
                scope,
                "Rule",
                rule_name=rule_name,
                topic_rule_payload=iot.CfnTopicRule.TopicRulePayloadProperty(
                    description=f"Crestline load test, mode {mode.upper()}",
                    sql=f"SELECT *, timestamp() AS received_at_utc FROM '{LOADTEST_FILTERS[mode]}'",
                    aws_iot_sql_version="2016-03-23",
                    rule_disabled=False,
                    actions=[
                        iot.CfnTopicRule.ActionProperty(
                            firehose=iot.CfnTopicRule.FirehoseActionProperty(
                                delivery_stream_name=stream.delivery_stream_name,
                                role_arn=role.role_arn,
                                separator="\n",
                            )
                        )
                    ],
                    error_action=iot.CfnTopicRule.ActionProperty(
                        s3=iot.CfnTopicRule.S3ActionProperty(
                            bucket_name=bucket.bucket_name,
                            key=f"errors/mode={mode}/${{newuuid()}}.json",
                            role_arn=role.role_arn,
                        )
                    ),
                ),
            )
            cdk.CfnOutput(
                self, f"StreamName{mode.upper()}", value=stream.delivery_stream_name
            )

        cdk.CfnOutput(self, "BucketName", value=bucket.bucket_name)

    def _iot_arn(self, kind: str, name: str) -> str:
        return self.format_arn(service="iot", resource=kind, resource_name=name)
