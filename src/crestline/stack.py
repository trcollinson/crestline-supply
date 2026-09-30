"""What FoundationStack created, read from its CloudFormation outputs.

The stack (infra/stacks/foundation.py) publishes the names of the raw bucket,
the Glue database, and the Athena workgroup, so nothing here guesses a name
or hardcodes an account. Account and region come from the AWS environment
(`AWS_PROFILE`, `AWS_REGION`), like the CDK CLI's.
"""

import boto3

STACK_NAME = "CrestlineFoundation"  # infra/app.py


def foundation_outputs(cloudformation=None) -> dict[str, str]:
    """Output name -> value, e.g. {"RawBucketName": "crestlinefoundation-raw..."}."""
    cloudformation = cloudformation or boto3.client("cloudformation")
    [stack] = cloudformation.describe_stacks(StackName=STACK_NAME)["Stacks"]
    return {output["OutputKey"]: output["OutputValue"] for output in stack["Outputs"]}
