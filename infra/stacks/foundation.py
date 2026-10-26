"""FoundationStack: where raw data lands and how it gets queried (spec §9).

M1 needs four things, and this stack is all of them:

- a **raw bucket** that holds each system's output exactly as extracted,
  under `source=<erp|mes|machines>/entity=<name>/ingest_date=YYYY-MM-DD/`;
- an **Athena results bucket** that clears out old query results by itself;
- the **Glue database** `crestline_raw`, home of the external tables that
  Athena reads the raw files through;
- an **Athena workgroup** that refuses any query scanning more than 1 GB.

Idle, this costs next to nothing: two small buckets, a workgroup, and a
catalog entry. Athena is the one piece that bills by use, and the scan limit
caps it.

The stack is built to be torn down: `cdk destroy` empties both buckets and
deletes them. A production raw bucket would keep its data on delete
(`RemovalPolicy.RETAIN`); this one holds a simulated plant that one command
can regenerate. One thing outlives the stack: the log group of the Lambda
that CDK adds to empty the buckets. Lambda creates it on the function's first
run, outside CloudFormation, so after a destroy it has to be deleted by hand
(`aws logs delete-log-group`).
"""

import aws_cdk as cdk
from aws_cdk import aws_athena as athena
from aws_cdk import aws_glue as glue
from aws_cdk import aws_s3 as s3
from constructs import Construct

GLUE_DATABASE = "crestline_raw"
ATHENA_WORKGROUP = "crestline"

# Athena bills $5 per TB scanned. The 90-day margin query reads about 44 MB,
# so a 1 GB cutoff never gets in its way, and a runaway query stops at half
# a cent.
SCAN_LIMIT_BYTES = 1024**3


class FoundationStack(cdk.Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(
            scope,
            construct_id,
            description=(
                "Crestline data platform foundation: raw bucket, Athena "
                "results bucket, Glue database, Athena workgroup"
            ),
            **kwargs,
        )

        self.raw_bucket = raw_bucket = s3.Bucket(
            self,
            "RawBucket",
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
            # Uploads are idempotent: a re-run overwrites the same keys.
            # Versioning keeps the copy it replaced, which is worth having
            # for a month, not forever.
            versioned=True,
            lifecycle_rules=[
                s3.LifecycleRule(
                    id="ExpireReplacedVersions",
                    noncurrent_version_expiration=cdk.Duration.days(30),
                    abort_incomplete_multipart_upload_after=cdk.Duration.days(7),
                )
            ],
            removal_policy=cdk.RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        self.results_bucket = results_bucket = s3.Bucket(
            self,
            "AthenaResultsBucket",
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
            # Query results are scratch output; nobody needs last week's.
            lifecycle_rules=[
                s3.LifecycleRule(
                    id="ExpireQueryResults",
                    expiration=cdk.Duration.days(7),
                    abort_incomplete_multipart_upload_after=cdk.Duration.days(1),
                )
            ],
            removal_policy=cdk.RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        # CloudFormation can't tag a Glue database, so this is the one
        # resource the cost allocation tags don't reach. The Data Catalog is
        # free at this size, so the cost report misses nothing.
        database = glue.CfnDatabase(
            self,
            "RawDatabase",
            catalog_id=self.account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name=GLUE_DATABASE,
                description="External tables over the raw bucket, one per entity",
            ),
        )

        workgroup = athena.CfnWorkGroup(
            self,
            "WorkGroup",
            name=ATHENA_WORKGROUP,
            description="Crestline queries, with a per-query scan limit",
            # Delete saved queries along with the workgroup, so that
            # `cdk destroy` doesn't stall on them.
            recursive_delete_option=True,
            work_group_configuration=athena.CfnWorkGroup.WorkGroupConfigurationProperty(
                bytes_scanned_cutoff_per_query=SCAN_LIMIT_BYTES,
                # Clients can't lift the limit or send results elsewhere.
                enforce_work_group_configuration=True,
                engine_version=athena.CfnWorkGroup.EngineVersionProperty(
                    selected_engine_version="Athena engine version 3"
                ),
                result_configuration=athena.CfnWorkGroup.ResultConfigurationProperty(
                    output_location=f"s3://{results_bucket.bucket_name}/",
                    encryption_configuration=athena.CfnWorkGroup.EncryptionConfigurationProperty(
                        encryption_option="SSE_S3"
                    ),
                ),
            ),
        )

        # The upload step and the Athena DDL read these instead of
        # guessing names.
        cdk.CfnOutput(self, "RawBucketName", value=raw_bucket.bucket_name)
        cdk.CfnOutput(self, "AthenaResultsBucketName", value=results_bucket.bucket_name)
        cdk.CfnOutput(self, "GlueDatabaseName", value=database.ref)
        cdk.CfnOutput(self, "AthenaWorkGroupName", value=workgroup.ref)
