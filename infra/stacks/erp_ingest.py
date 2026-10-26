"""ErpIngestStack: the ERP poller, on a schedule (spec §9, M2 steps 7 and 8).

    EventBridge Scheduler --> Step Functions --> per entity: extract --> S3 raw
                                                             curate  --> Iceberg (Glue)

- A **schedule** starts a run (hourly by default; `-c erpIngestSchedule=...`).
- A **Step Functions** workflow maps over the ERP's entities, three at a
  time, calling the extractor once per entity. An entity that fails is tried
  again twice, 30 s then 60 s later; if it still fails, the others carry on
  and the run ends Failed, naming it. Every retry and failure is in the
  execution's history.
- The **extractor** (`pipelines/erp_extract/aws.py`) is the robust one from
  step 5, reading one entity: it signs in to the ERP API (URL and client
  secret from Parameter Store, published by ErpApiStack and `crestline
  erp-api secrets`), writes raw pages to the raw bucket under
  `source=erp-api/`, and keeps the entity's watermark, sequence number, and
  run count in a **DynamoDB** table, one item per entity.
- The **curate step** (`pipelines/erp_extract/curate.py`) follows each
  entity's extraction: it lists the entity's raw reads not merged yet, runs
  one Athena MERGE that folds them into the entity's Iceberg table in
  CuratedStack, and notes the last read merged on the same
  DynamoDB item. Athena reads the raw pages through **`raw_erp_api`**, a Glue
  table over `source=erp-api/` defined here: one line of text per page,
  partitioned by entity and ingest date, with partition projection, so a
  new day's pages need no `MSCK REPAIR`.
- An **alarm**: an EventBridge rule sends every run that fails, times out,
  or is stopped to an SNS topic (`-c erpIngestAlarmEmail=...` subscribes an
  address).

Nothing runs between runs: idle it costs nothing, and a run costs about a
tenth of a cent in Step Functions transitions, plus Athena's 10 MB minimum
per query (a check and a MERGE per entity, 22 a run: about a tenth of a cent). The table and the log group
go with the stack; raw pages stay in the raw bucket, which belongs to
FoundationStack.
"""

import aws_cdk as cdk
from aws_cdk import aws_dynamodb as dynamodb
from aws_cdk import aws_events as events
from aws_cdk import aws_events_targets as events_targets
from aws_cdk import aws_glue as glue
from aws_cdk import aws_iam as iam
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_logs as logs
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_scheduler as scheduler
from aws_cdk import aws_scheduler_targets as targets
from aws_cdk import aws_sns as sns
from aws_cdk import aws_sns_subscriptions as subscriptions
from aws_cdk import aws_ssm as ssm
from aws_cdk import aws_stepfunctions as sfn
from aws_cdk import aws_stepfunctions_tasks as tasks
from constructs import Construct

from erp_extract.curated import CURATED_DATABASE, RAW_INGEST_DATES, RAW_PAGES_TABLE
from erp_extract.entities import ENTITIES
from erp_extract.raw import SOURCE
from stacks.erp_api import CLIENT_ID, REPO_ROOT, SECRET_PARAMETERS, URL_PARAMETER
from stacks.foundation import ATHENA_WORKGROUP, GLUE_DATABASE

MAX_CONCURRENCY = 3  # entities extracted at once
SWEEP_EVERY = 1  # sweep the job tables on every run: one run is one ERP day


class ErpIngestStack(cdk.Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        raw_bucket: s3.IBucket,
        results_bucket: s3.IBucket,
        curated_bucket: s3.IBucket,
        schedule: str = "rate(1 hour)",
        alarm_email: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        state = dynamodb.Table(
            self,
            "State",
            partition_key=dynamodb.Attribute(
                name="entity", type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=cdk.RemovalPolicy.DESTROY,
        )

        # The standard library and the runtime's boto3 are all either needs.
        code = lambda_.Code.from_asset(
            str(REPO_ROOT / "pipelines"),
            exclude=["**/__pycache__", "pyproject.toml"],
        )
        extractor = lambda_.Function(
            self,
            "Extractor",
            description="Extract one ERP entity to the raw bucket (Crestline Supply)",
            runtime=lambda_.Runtime.PYTHON_3_12,
            architecture=lambda_.Architecture.ARM_64,
            handler="erp_extract.aws.handler",
            code=code,
            memory_size=256,
            timeout=cdk.Duration.minutes(5),
            environment={
                "CRESTLINE_ERP_URL_PARAMETER": URL_PARAMETER,
                "CRESTLINE_ERP_CLIENT_ID": CLIENT_ID,
                "CRESTLINE_ERP_CLIENT_SECRET_PARAMETER": SECRET_PARAMETERS[
                    "client_secret"
                ],
                "CRESTLINE_RAW_BUCKET": raw_bucket.bucket_name,
                "CRESTLINE_STATE_TABLE": state.table_name,
                "CRESTLINE_SWEEP_EVERY": str(SWEEP_EVERY),
            },
            log_group=logs.LogGroup(
                self,
                "Logs",
                retention=logs.RetentionDays.ONE_WEEK,
                removal_policy=cdk.RemovalPolicy.DESTROY,
            ),
        )
        raw_bucket.grant_put(extractor, f"{SOURCE}/*")
        state.grant_read_write_data(extractor)
        ssm.StringParameter.from_string_parameter_name(
            self, "UrlParameter", URL_PARAMETER
        ).grant_read(extractor)
        ssm.StringParameter.from_secure_string_parameter_attributes(
            self,
            "ClientSecretParameter",
            parameter_name=SECRET_PARAMETERS["client_secret"],
        ).grant_read(extractor)

        self._raw_pages_table(raw_bucket)
        curator = lambda_.Function(
            self,
            "Curator",
            description="Merge one ERP entity's new raw pages into its Iceberg table (Crestline Supply)",
            runtime=lambda_.Runtime.PYTHON_3_12,
            architecture=lambda_.Architecture.ARM_64,
            handler="erp_extract.curate.handler",
            code=code,
            memory_size=256,
            # A MERGE takes seconds; the rest is headroom for a slow Athena queue.
            timeout=cdk.Duration.minutes(5),
            environment={
                "CRESTLINE_RAW_BUCKET": raw_bucket.bucket_name,
                "CRESTLINE_STATE_TABLE": state.table_name,
                "CRESTLINE_ATHENA_WORKGROUP": ATHENA_WORKGROUP,
                "CRESTLINE_RAW_DATABASE": GLUE_DATABASE,
                "CRESTLINE_CURATED_DATABASE": CURATED_DATABASE,
            },
            log_group=logs.LogGroup(
                self,
                "CuratorLogs",
                retention=logs.RetentionDays.ONE_WEEK,
                removal_policy=cdk.RemovalPolicy.DESTROY,
            ),
        )
        state.grant_read_write_data(curator)
        self._let_curate(curator, raw_bucket, results_bucket, curated_bucket)

        run = sfn.StateMachine(
            self,
            "Run",
            definition_body=sfn.DefinitionBody.from_chainable(
                self._workflow(extractor, curator)
            ),
            state_machine_type=sfn.StateMachineType.STANDARD,
            timeout=cdk.Duration.minutes(30),
        )

        scheduler.Schedule(
            self,
            "Schedule",
            description="Start an ERP extraction run (Crestline Supply)",
            schedule=scheduler.ScheduleExpression.expression(schedule),
            target=targets.StepFunctionsStartExecution(
                run, input=scheduler.ScheduleTargetInput.from_object({})
            ),
        )

        self._alarm(run, alarm_email)

        cdk.CfnOutput(self, "StateMachineArn", value=run.state_machine_arn)
        cdk.CfnOutput(self, "StateTableName", value=state.table_name)

    def _alarm(self, run: sfn.IStateMachine, email: str | None) -> None:
        """Tell someone when a run doesn't succeed.

        A failed run says so only in its own execution history, and nobody
        reads that: in M2 the curate step failed every hour for three days
        before anyone looked. Step Functions reports every run's end to
        EventBridge; this rule sends the ones that didn't succeed to an SNS
        topic. `-c erpIngestAlarmEmail=...` subscribes an address (SNS emails
        it once to confirm); without it, subscribe to the topic yourself.
        """
        topic = sns.Topic(self, "Alarm", display_name="Crestline ERP poller")
        if email:
            topic.add_subscription(subscriptions.EmailSubscription(email))
        detail = events.EventField.from_path
        events.Rule(
            self,
            "RunDidNotSucceed",
            description="A Crestline ERP extraction run failed, timed out, or was stopped",
            event_pattern=events.EventPattern(
                source=["aws.states"],
                detail_type=["Step Functions Execution Status Change"],
                detail={
                    "stateMachineArn": [run.state_machine_arn],
                    "status": ["FAILED", "TIMED_OUT", "ABORTED"],
                },
            ),
            targets=[
                events_targets.SnsTopic(
                    topic,
                    message=events.RuleTargetInput.from_text(
                        f"Crestline ERP poller run {detail('$.detail.name')} ended "
                        f"{detail('$.detail.status')}: {detail('$.detail.error')} "
                        f"{detail('$.detail.cause')}"
                    ),
                )
            ],
        )
        cdk.CfnOutput(self, "AlarmTopicArn", value=topic.topic_arn)

    def _raw_pages_table(self, raw_bucket: s3.IBucket) -> None:
        """`raw_erp_api`: the raw pages as Athena reads them, one line of text each.

        Each line is read whole and taken apart with JSON functions: Athena's
        JSON reader would lowercase every key in a nested object (M1 found
        that out on PB-01), and the body's fields differ by entity anyway.
        Partitions are projected, not registered: entity is one of the ERP's,
        ingest_date any day in the range, so a new day needs no repair. The
        MERGE always names its dates, so a wide range costs nothing, and a
        narrow one is a trap: pages filed past its end are invisible. (In M2
        the replayed ERP's clock ran past a range ending 2027-12-31, its
        sweeps read as empty, and the curated tables emptied themselves.)
        """
        glue.CfnTable(
            self,
            "RawPagesTable",
            catalog_id=self.account,
            database_name=GLUE_DATABASE,
            table_input=glue.CfnTable.TableInputProperty(
                name=RAW_PAGES_TABLE,
                description="ERP API pages as the poller wrote them, one per line",
                table_type="EXTERNAL_TABLE",
                partition_keys=[
                    glue.CfnTable.ColumnProperty(name="entity", type="string"),
                    glue.CfnTable.ColumnProperty(name="ingest_date", type="string"),
                ],
                parameters={
                    "projection.enabled": "true",
                    "projection.entity.type": "enum",
                    "projection.entity.values": ",".join(ENTITIES),
                    "projection.ingest_date.type": "date",
                    "projection.ingest_date.format": "yyyy-MM-dd",
                    "projection.ingest_date.range": ",".join(RAW_INGEST_DATES),
                },
                storage_descriptor=glue.CfnTable.StorageDescriptorProperty(
                    columns=[glue.CfnTable.ColumnProperty(name="line", type="string")],
                    location=f"s3://{raw_bucket.bucket_name}/{SOURCE}/",
                    input_format="org.apache.hadoop.mapred.TextInputFormat",
                    output_format="org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                    # Fields split at \001 by default, a character no JSON
                    # line holds, so each line is one column. (CloudFormation
                    # won't take the character itself in a template.)
                    serde_info=glue.CfnTable.SerdeInfoProperty(
                        serialization_library="org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                    ),
                ),
            ),
        )

    def _let_curate(
        self,
        curator: lambda_.IFunction,
        raw_bucket: s3.IBucket,
        results_bucket: s3.IBucket,
        curated_bucket: s3.IBucket,
    ) -> None:
        """What a MERGE needs: Athena runs it with the curator's own permissions."""
        # List the raw pages and read them (Athena reads them as the curator).
        raw_bucket.grant_read(curator, f"{SOURCE}/*")
        # Athena writes each query's result file to the workgroup's bucket.
        results_bucket.grant_read(curator)
        results_bucket.grant_put(curator)
        # The curated tables' files: written by MERGE and OPTIMIZE, deleted by VACUUM.
        curated_bucket.grant_read_write(curator)
        arn = f"arn:{self.partition}"
        where = f"{self.region}:{self.account}"
        curator.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "athena:StartQueryExecution",
                    "athena:GetQueryExecution",
                    "athena:GetQueryResults",  # the check before each MERGE
                ],
                resources=[f"{arn}:athena:{where}:workgroup/{ATHENA_WORKGROUP}"],
            )
        )
        # The tables' definitions: raw_erp_api to read, the curated tables to
        # read and commit to (an Iceberg commit points the table at new metadata).
        curator.add_to_role_policy(
            iam.PolicyStatement(
                actions=["glue:GetDatabase", "glue:GetTable", "glue:UpdateTable"],
                resources=[
                    f"{arn}:glue:{where}:catalog",
                    f"{arn}:glue:{where}:database/{GLUE_DATABASE}",
                    f"{arn}:glue:{where}:table/{GLUE_DATABASE}/{RAW_PAGES_TABLE}",
                    f"{arn}:glue:{where}:database/{CURATED_DATABASE}",
                    f"{arn}:glue:{where}:table/{CURATED_DATABASE}/*",
                ],
            )
        )

    def _workflow(
        self, extractor: lambda_.IFunction, curator: lambda_.IFunction
    ) -> sfn.IChainable:
        """Every entity, a few at a time: extract, then curate.

        A run fails if any entity does, at either step.
        """
        entities = sfn.Pass(
            self,
            "List the entities",
            result=sfn.Result.from_object(
                {"entities": [{"entity": entity} for entity in ENTITIES]}
            ),
        )
        extract = tasks.LambdaInvoke(
            self,
            "Extract the entity",
            lambda_function=extractor,
            payload_response_only=True,  # the extractor's report is the output
            retry_on_service_exceptions=True,
        )
        extract.add_retry(
            errors=["EntityFailed", "States.Timeout"],
            interval=cdk.Duration.seconds(30),
            max_attempts=2,
            backoff_rate=2,
        )
        extract.add_catch(
            sfn.Pass(self, "Note the failure"),
            errors=["States.ALL"],
            result_path="$.error",
        )
        each = sfn.Map(
            self,
            "Each entity",
            items_path="$.entities",
            max_concurrency=MAX_CONCURRENCY,
        )
        curate = tasks.LambdaInvoke(
            self,
            "Curate the entity",
            lambda_function=curator,
            payload_response_only=True,
            retry_on_service_exceptions=True,
            result_path="$.curated",  # next to the extractor's report
        )
        # Another writer's commit to the same table can make a MERGE fail;
        # the reads are still there, so trying again is safe.
        curate.add_retry(
            errors=["CurateFailed", "States.Timeout"],
            interval=cdk.Duration.seconds(30),
            max_attempts=2,
            backoff_rate=2,
        )
        curate.add_catch(
            sfn.Pass(self, "Note the curate failure"),
            errors=["States.ALL"],
            result_path="$.error",
        )
        each.item_processor(extract.next(curate))

        failed = sfn.Fail(
            self,
            "Some entities failed",
            query_language=sfn.QueryLanguage.JSONATA,
            error="EntitiesFailed",
            cause="{% $join($states.input[$exists(error)].entity, ', ') %}",
        )
        verdict = (
            sfn.Choice(self, "Any failed?", query_language=sfn.QueryLanguage.JSONATA)
            .when(
                sfn.Condition.jsonata(
                    "{% $count($states.input[$exists(error)]) > 0 %}"
                ),
                failed,
            )
            .otherwise(sfn.Succeed(self, "Every entity extracted"))
        )
        return entities.next(each).next(verdict)
