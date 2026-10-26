"""CuratedStack: the curated layer's Iceberg tables (spec §9, M2 step 8).

- A **curated bucket** holding the tables' data and metadata files.
- The Glue database **`crestline_curated`** with one **Iceberg table** per
  ERP entity (`erp_jobs`, `erp_job_operations`, ...), its columns typed from
  `erp_extract.curated.COLUMNS`. The tables are empty until the curate step
  in ErpIngestStack merges raw pages into them; Athena reads and writes them
  like any other table in the account's Glue catalog, views included.

Why not S3 Tables, which compacts and expires snapshots by itself: Athena
reaches a table bucket through a federated catalog (`s3tablescatalog/...`)
and won't create a view over it, and the margin query reads every
source through views (queries/athena/sources.sql). Plain Iceberg in Glue
keeps the query layer as it is; the curate step does the upkeep instead,
with a daily OPTIMIZE and VACUUM.

The tables are created here rather than with Athena DDL so that `cdk
destroy` removes them with the bucket; the curate step can rebuild their
data from the raw bucket. Like the raw database, a Glue database or table
can't carry tags in CloudFormation; the bucket, where the storage cost is,
does.
"""

import aws_cdk as cdk
from aws_cdk import aws_glue as glue
from aws_cdk import aws_s3 as s3
from constructs import Construct

from erp_extract.curated import COLUMNS, CURATED_DATABASE, TABLES, column_name
from erp_extract.entities import ENTITIES


class CuratedStack(cdk.Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(
            scope,
            construct_id,
            description="Crestline curated layer: Iceberg tables in Glue",
            **kwargs,
        )

        self.bucket = bucket = s3.Bucket(
            self,
            "CuratedBucket",
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
            # Iceberg keeps its own history (snapshots), so no S3 versioning.
            lifecycle_rules=[
                s3.LifecycleRule(
                    id="AbortIncompleteUploads",
                    abort_incomplete_multipart_upload_after=cdk.Duration.days(1),
                )
            ],
            removal_policy=cdk.RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        database = glue.CfnDatabase(
            self,
            "CuratedDatabase",
            catalog_id=self.account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name=CURATED_DATABASE,
                description="Curated Iceberg tables, one per entity, typed and merged",
                location_uri=f"s3://{bucket.bucket_name}/",
            ),
        )

        for entity, table_name in TABLES.items():
            keys = ENTITIES[entity][0]
            table = glue.CfnTable(
                self,
                f"{entity}IcebergTable",
                catalog_id=self.account,
                database_name=CURATED_DATABASE,
                name=table_name,
                open_table_format_input=glue.CfnTable.OpenTableFormatInputProperty(
                    iceberg_input=glue.CfnTable.IcebergInputProperty(
                        metadata_operation="CREATE",
                        version="2",  # row-level deletes, which MERGE needs
                        iceberg_table_input=glue.CfnTable.IcebergTableInputProperty(
                            location=f"s3://{bucket.bucket_name}/{table_name}/",
                            schema=glue.CfnTable.IcebergSchemaProperty(
                                type="struct",
                                fields=[
                                    glue.CfnTable.IcebergStructFieldProperty(
                                        id=number,
                                        name=column_name(field),
                                        type=kind,
                                        required=field in keys,
                                    )
                                    for number, (field, kind) in enumerate(
                                        COLUMNS[entity], start=1
                                    )
                                ],
                            ),
                        ),
                    )
                ),
            )
            table.node.add_dependency(database)

        cdk.CfnOutput(self, "CuratedBucketName", value=bucket.bucket_name)
        cdk.CfnOutput(self, "CuratedDatabaseName", value=CURATED_DATABASE)
