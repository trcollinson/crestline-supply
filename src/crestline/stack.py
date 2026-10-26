"""What the stacks created, read from their CloudFormation outputs.

FoundationStack (infra/stacks/foundation.py) publishes the names of the raw
bucket, the Glue database, and the Athena workgroup; ErpApiStack the ERP
API's URL and client ID; CuratedStack the curated tables' database; and
ErpIngestStack the poller's state table. So nothing here guesses a name or
hardcodes an account. Account and region come from the AWS environment
(`AWS_PROFILE`, `AWS_REGION`), like the CDK CLI's.
"""

import boto3

STACK_NAME = "CrestlineFoundation"  # infra/app.py
ERP_API_STACK_NAME = "CrestlineErpApi"
CURATED_STACK_NAME = "CrestlineCurated"
ERP_INGEST_STACK_NAME = "CrestlineErpIngest"


def foundation_outputs(cloudformation=None) -> dict[str, str]:
    """Output name -> value, e.g. {"RawBucketName": "crestlinefoundation-raw..."}."""
    return stack_outputs(STACK_NAME, cloudformation)


def stack_outputs(stack_name: str, cloudformation=None) -> dict[str, str]:
    """A stack's outputs, name -> value."""
    cloudformation = cloudformation or boto3.client("cloudformation")
    [stack] = cloudformation.describe_stacks(StackName=stack_name)["Stacks"]
    return {output["OutputKey"]: output["OutputValue"] for output in stack["Outputs"]}


def curate_setup(cloudformation=None) -> dict:
    """What `erp_extract.curate.curate` needs, for the deployed stacks."""
    foundation = foundation_outputs(cloudformation)
    curated = stack_outputs(CURATED_STACK_NAME, cloudformation)
    ingest = stack_outputs(ERP_INGEST_STACK_NAME, cloudformation)
    return {
        "bucket": foundation["RawBucketName"],
        "workgroup": foundation["AthenaWorkGroupName"],
        "raw_database": foundation["GlueDatabaseName"],
        "curated_database": curated["CuratedDatabaseName"],
        "s3": boto3.client("s3"),
        "athena": boto3.client("athena"),
        "table": boto3.resource("dynamodb").Table(ingest["StateTableName"]),
    }
