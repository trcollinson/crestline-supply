"""MachineIngestStack: the plant's gateways into AWS (spec §9, M3 step 7).

    gateway --MQTT 5/TLS--> IoT Core Basic Ingest --> rule --> Firehose --> S3 raw
                                                         \\--(error)--> S3 raw errors/

- An **IoT thing per gateway** (EDGE-LASER, GW01, GW02, GW03), each with a
  policy that lets it connect only as itself and publish only to its own
  rules' Basic Ingest topics (`$aws/rules/<rule>/...`). Basic Ingest skips
  the message broker, so there is no messaging charge; rules triggered and
  actions run still bill. Certificates aren't here: CloudFormation can't
  hand out a private key, so `crestline machines setup` makes them.
- A **rule per device** adds the topic, IoT Core's arrival time
  (`received_at_utc`, epoch milliseconds), and two MQTT 5 user properties
  the replay publisher sets: `published_at` (when the message left the
  gateway, in plant time: drift correction needs it, and in a replay the
  arrival time is the replay's own clock) and `replay`. The Sparkplug rule
  keeps the payload's bytes as base64 and also decodes them against the
  Sparkplug B schema: a descriptor set in this stack's schema bucket, put
  there by `crestline machines setup` (which `--delete` empties again before
  `cdk destroy`), so no custom-resource Lambda is needed to upload it.
- A **Firehose stream per device** writes newline-separated records to
  `source=machines-iot/entity=<device>/ingest_date=<UTC date it arrived>/`
  in the raw bucket (FoundationStack's), with a plain timestamp prefix: no
  dynamic partitioning, which bills extra.
- Each rule's **error action** writes a message its action couldn't
  deliver to `source=machines-iot/errors/entity=<device>/`, one object
  each.

One kind of message reaches neither: JSON that won't parse (GW01 corrupts
a few in transit). The rules engine parses a JSON-looking payload before
any SQL runs, whatever the SQL says (`SELECT *` and `encode(*, 'base64')`
alike, measured in M3 step 7), and drops one that fails, counting it only
in the rule's `ParseError` metric. So a message published is in raw, in
errors, or in that metric, and `crestline machines account` counts all
three.

Every log group goes with the stack. The raw bucket keeps what landed.
"""

import aws_cdk as cdk
from aws_cdk import aws_cloudwatch as cloudwatch
from aws_cdk import aws_cloudwatch_actions as cloudwatch_actions
from aws_cdk import aws_dynamodb as dynamodb
from aws_cdk import aws_glue as glue
from aws_cdk import aws_iam as iam
from aws_cdk import aws_iot as iot
from aws_cdk import aws_kinesisfirehose as firehose
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_logs as logs
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_scheduler as scheduler
from aws_cdk import aws_scheduler_targets as targets
from aws_cdk import aws_sns as sns
from aws_cdk import aws_sns_subscriptions as subscriptions
from constructs import Construct

from crestline_machines.devices import (
    DESCRIPTOR_KEY,
    DEVICES,
    ERRORS_PREFIX,
    FIREHOSE_ERRORS_PREFIX,
    GATEWAYS,
    PUBLISHED_AT,
    RAW_SOURCE,
    REPLAY,
    Device,
    policy_name,
)
from erp_extract.curated import RAW_INGEST_DATES
from machine_curate.tables import CURATED_DATABASE, RAW_TABLE, TABLES
from stacks.erp_api import REPO_ROOT
from stacks.foundation import ATHENA_WORKGROUP, GLUE_DATABASE

BUFFER = cdk.Duration.seconds(
    60
)  # how long Firehose holds records before writing a file


class MachineIngestStack(cdk.Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        raw_bucket: s3.IBucket,
        results_bucket: s3.IBucket,
        curated_bucket: s3.IBucket,
        alarm_email: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        schema = self._schema_bucket()
        for gateway in GATEWAYS:
            self._gateway(gateway)

        # One role for every rule: put records into its stream, and write
        # what fails to the errors prefix.
        rules_role = iam.Role(
            self,
            "RulesRole",
            assumed_by=iam.ServicePrincipal("iot.amazonaws.com"),
            description="Crestline machine rules: deliver to Firehose, write errors to raw",
        )
        raw_bucket.grant_put(rules_role, f"{ERRORS_PREFIX}/*")
        for device in DEVICES:
            stream = self._stream(device, raw_bucket)
            stream.grant_put_records(rules_role)
            self._rule(device, stream, raw_bucket, schema, rules_role)

        self._raw_table(raw_bucket)
        self._curator(raw_bucket, results_bucket, curated_bucket, alarm_email)

        cdk.CfnOutput(self, "SchemaBucketName", value=schema.bucket_name)

    # -- the curated layer (M3 step 8) -----------------------------------------

    def _raw_table(self, raw_bucket: s3.IBucket) -> None:
        """`raw_machines_iot`: Firehose's records as Athena reads them, one line of text each.

        Read whole and taken apart with JSON functions, as the ERP's raw pages
        are: the devices' fields differ, and PB-01's keys (`N7:12`) aren't
        column names. Partitions are projected, by device and arrival date.
        """
        glue.CfnTable(
            self,
            "RawMachinesTable",
            catalog_id=self.account,
            database_name=GLUE_DATABASE,
            table_input=glue.CfnTable.TableInputProperty(
                name=RAW_TABLE,
                description="Machine messages as IoT Core's rules landed them, one per line",
                table_type="EXTERNAL_TABLE",
                partition_keys=[
                    glue.CfnTable.ColumnProperty(name="entity", type="string"),
                    glue.CfnTable.ColumnProperty(name="ingest_date", type="string"),
                ],
                parameters={
                    "projection.enabled": "true",
                    "projection.entity.type": "enum",
                    "projection.entity.values": ",".join(d.name for d in DEVICES),
                    "projection.ingest_date.type": "date",
                    "projection.ingest_date.format": "yyyy-MM-dd",
                    "projection.ingest_date.range": ",".join(RAW_INGEST_DATES),
                },
                storage_descriptor=glue.CfnTable.StorageDescriptorProperty(
                    columns=[glue.CfnTable.ColumnProperty(name="line", type="string")],
                    location=f"s3://{raw_bucket.bucket_name}/{RAW_SOURCE}/",
                    input_format="org.apache.hadoop.mapred.TextInputFormat",
                    output_format="org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                    serde_info=glue.CfnTable.SerdeInfoProperty(
                        serialization_library="org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                    ),
                ),
            ),
        )

    def _curator(
        self,
        raw_bucket: s3.IBucket,
        results_bucket: s3.IBucket,
        curated_bucket: s3.IBucket,
        alarm_email: str | None,
    ) -> None:
        """The hourly curate step, its state, and the alarm when it fails."""
        state = dynamodb.Table(
            self,
            "CurateState",
            partition_key=dynamodb.Attribute(
                name="entity", type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=cdk.RemovalPolicy.DESTROY,
        )
        topic = sns.Topic(self, "Alarm", display_name="Crestline machines")
        if alarm_email:
            topic.add_subscription(subscriptions.EmailSubscription(alarm_email))
        curator = lambda_.Function(
            self,
            "Curator",
            description="Merge new machine messages into the curated tables (Crestline Supply)",
            runtime=lambda_.Runtime.PYTHON_3_12,
            architecture=lambda_.Architecture.ARM_64,
            handler="machine_curate.curate.handler",
            code=lambda_.Code.from_asset(
                str(REPO_ROOT / "pipelines"),
                exclude=["**/__pycache__", "pyproject.toml"],
            ),
            memory_size=256,
            timeout=cdk.Duration.minutes(10),
            # One run at a time: two would read the same window. The keyed
            # MERGE keeps the table right even then (seen on the first deploy:
            # the schedule's first run overlapped a manual one, and the second
            # inserted 0 of the 3,422 lines it read), but one is enough.
            reserved_concurrent_executions=1,
            environment={
                "CRESTLINE_RAW_BUCKET": raw_bucket.bucket_name,
                "CRESTLINE_MACHINE_STATE_TABLE": state.table_name,
                "CRESTLINE_ATHENA_WORKGROUP": ATHENA_WORKGROUP,
                "CRESTLINE_RAW_DATABASE": GLUE_DATABASE,
                "CRESTLINE_ALARM_TOPIC": topic.topic_arn,
            },
            log_group=logs.LogGroup(
                self,
                "CuratorLogs",
                retention=logs.RetentionDays.ONE_WEEK,
                removal_policy=cdk.RemovalPolicy.DESTROY,
            ),
        )
        state.grant_read_write_data(curator)
        topic.grant_publish(curator)
        raw_bucket.grant_read(curator, f"{RAW_SOURCE}/*")
        # The brake reconciliation reads the MES labor files too (checks).
        raw_bucket.grant_read(curator, "source=mes/*")
        results_bucket.grant_read(curator)
        results_bucket.grant_put(curator)
        curated_bucket.grant_read_write(curator)
        arn = f"arn:{self.partition}"
        where = f"{self.region}:{self.account}"
        curator.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "athena:StartQueryExecution",
                    "athena:GetQueryExecution",
                    "athena:GetQueryResults",
                ],
                resources=[f"{arn}:athena:{where}:workgroup/{ATHENA_WORKGROUP}"],
            )
        )
        curator.add_to_role_policy(
            iam.PolicyStatement(
                # GetPartitions: the checks read raw_mes_labor, which is partitioned.
                actions=[
                    "glue:GetDatabase",
                    "glue:GetTable",
                    "glue:GetPartitions",
                    "glue:UpdateTable",
                ],
                resources=[
                    f"{arn}:glue:{where}:catalog",
                    f"{arn}:glue:{where}:database/{GLUE_DATABASE}",
                    # The raw table, and the views the checks read (machines.sql's
                    # and the MES source over raw_mes_labor).
                    f"{arn}:glue:{where}:table/{GLUE_DATABASE}/*",
                    f"{arn}:glue:{where}:database/{CURATED_DATABASE}",
                    *(
                        f"{arn}:glue:{where}:table/{CURATED_DATABASE}/{table}"
                        for table in TABLES.values()
                    ),
                ],
            )
        )
        scheduler.Schedule(
            self,
            "CurateSchedule",
            description="Merge new machine messages into the curated tables (Crestline Supply)",
            schedule=scheduler.ScheduleExpression.expression("rate(1 hour)"),
            target=targets.LambdaInvoke(
                curator, input=scheduler.ScheduleTargetInput.from_object({})
            ),
        )
        # A run that raises is an error; tell someone (the curator also emails
        # the topic itself when a merge would shrink a table).
        cloudwatch.Alarm(
            self,
            "CurateFailed",
            alarm_description="A Crestline machine curate run failed",
            metric=curator.metric_errors(period=cdk.Duration.hours(1)),
            threshold=1,
            evaluation_periods=1,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
            treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
        ).add_alarm_action(cloudwatch_actions.SnsAction(topic))
        cdk.CfnOutput(self, "CuratorName", value=curator.function_name)
        cdk.CfnOutput(self, "CurateStateTableName", value=state.table_name)
        cdk.CfnOutput(self, "AlarmTopicArn", value=topic.topic_arn)

    # -- the Sparkplug B schema, where the rule's decode() can read it -------

    def _schema_bucket(self) -> s3.Bucket:
        bucket = s3.Bucket(
            self,
            "Schema",
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
            removal_policy=cdk.RemovalPolicy.DESTROY,
        )
        # decode() reads the descriptor set as IoT Core, not as the rule's role.
        bucket.add_to_resource_policy(
            iam.PolicyStatement(
                actions=["s3:GetObject"],
                principals=[iam.ServicePrincipal("iot.amazonaws.com")],
                resources=[bucket.arn_for_objects("descriptors/*")],
                conditions={"StringEquals": {"aws:SourceAccount": self.account}},
            )
        )
        return bucket

    # -- a gateway: its thing, and what it may do -----------------------------

    def _gateway(self, gateway: str) -> None:
        iot.CfnThing(self, f"Thing{_id(gateway)}", thing_name=gateway)
        rules = [d.rule for d in DEVICES if d.gateway == gateway]
        iot.CfnPolicy(
            self,
            f"Policy{_id(gateway)}",
            policy_name=policy_name(gateway),
            policy_document={
                "Version": "2012-10-17",
                "Statement": [
                    {
                        # Only as itself: its own name, or its name and a number.
                        # A real gateway holds one connection; the replay opens a
                        # few for its busiest gateways, because IoT Core takes 100
                        # publishes a second per connection and a 90-day replay
                        # needs more (the pipeline orders by published_at, never
                        # by arrival). That rules out IsAttached, which holds only
                        # when the client ID is the thing's own name; the policy
                        # is attached to this gateway's certificate alone.
                        "Effect": "Allow",
                        "Action": "iot:Connect",
                        "Resource": [
                            self._iot_arn("client", gateway),
                            self._iot_arn("client", f"{gateway}-*"),
                        ],
                    },
                    {
                        # Only to its own rules, through Basic Ingest.
                        "Effect": "Allow",
                        "Action": "iot:Publish",
                        "Resource": [
                            self._iot_arn("topic", f"$aws/rules/{rule}/*")
                            for rule in rules
                        ],
                    },
                ],
            },
        )

    def _iot_arn(self, kind: str, name: str) -> str:
        return self.format_arn(service="iot", resource=kind, resource_name=name)

    # -- a device: its stream and its rule -------------------------------------

    def _stream(
        self, device: Device, raw_bucket: s3.IBucket
    ) -> firehose.DeliveryStream:
        return firehose.DeliveryStream(
            self,
            f"Stream{_id(device.name)}",
            destination=firehose.S3Bucket(
                raw_bucket,
                data_output_prefix=(
                    f"{RAW_SOURCE}/entity={device.name}/ingest_date=!{{timestamp:yyyy-MM-dd}}/"
                ),
                error_output_prefix=(
                    f"{FIREHOSE_ERRORS_PREFIX}/entity={device.name}/"
                    "ingest_date=!{timestamp:yyyy-MM-dd}/!{firehose:error-output-type}/"
                ),
                file_extension=".jsonl",
                buffering_interval=BUFFER,
                logging_config=firehose.EnableLogging(
                    logs.LogGroup(
                        self,
                        f"StreamLogs{_id(device.name)}",
                        retention=logs.RetentionDays.ONE_WEEK,
                        removal_policy=cdk.RemovalPolicy.DESTROY,
                    )
                ),
            ),
        )

    def _rule(
        self,
        device: Device,
        stream: firehose.DeliveryStream,
        raw_bucket: s3.IBucket,
        schema: s3.IBucket,
        role: iam.IRole,
    ) -> None:
        iot.CfnTopicRule(
            self,
            f"Rule{_id(device.name)}",
            rule_name=device.rule,
            topic_rule_payload=iot.CfnTopicRule.TopicRulePayloadProperty(
                description=f"Crestline {device.name} via {device.gateway}",
                sql=rule_sql(device, schema.bucket_name),
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
                        bucket_name=raw_bucket.bucket_name,
                        key=(
                            f"{ERRORS_PREFIX}/entity={device.name}/"
                            "ingest_date=${parse_time('yyyy-MM-dd', timestamp(), 'UTC')}/"
                            "${newuuid()}.json"
                        ),
                        role_arn=role.role_arn,
                    )
                ),
            ),
        )


def rule_sql(device: Device, schema_bucket: str) -> str:
    """What a device's rule keeps of each message."""
    envelope = (
        "topic() AS topic, "
        f"get(get_user_properties('{PUBLISHED_AT}'), 0) AS published_at, "
        f"get(get_user_properties('{REPLAY}'), 0) AS replay, "
        "timestamp() AS received_at_utc"
    )
    if device.sparkplug:
        # The bytes as they came, and the same bytes decoded: protobuf's JSON
        # mapping (camelCase names, 64-bit integers as strings).
        keep = (
            "encode(*, 'base64') AS payload_b64, "
            f"decode(*, 'proto', '{schema_bucket}', '{DESCRIPTOR_KEY}', 'sparkplug_b', 'Payload') AS decoded"
        )
    else:
        keep = "*"  # the gateway's JSON fields, as sent
    return f"SELECT {keep}, {envelope} FROM '{device.topic_filter}'"


def _id(name: str) -> str:
    """A construct ID part: EDGE-LASER -> EdgeLaser, pcline -> Pcline."""
    return "".join(part.capitalize() for part in name.replace("_", "-").split("-"))
