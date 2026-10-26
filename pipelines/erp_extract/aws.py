"""The robust extractor in AWS Lambda: one entity per invocation (M2 step 7).

Step Functions calls this once per entity per run (`ErpIngestStack`); each
invocation:

1. loads that entity's state (watermark, sequence, run count) from DynamoDB;
2. signs in to the ERP API, whose URL and client secret are in Parameter
   Store, and reads the entity the way it changes (`robust.py`);
3. writes each completed read's raw pages to the raw bucket under
   `source=erp-api/` (an S3 PUT appears whole or not at all, so a file still
   means a completed read);
4. saves the entity's state, then reports what it read, or raises
   `EntityFailed` if a read had to be dropped, so Step Functions retries the
   entity and, if that fails too, shows the failure.

boto3 comes with the Lambda runtime, so the package is this folder and
nothing else. The backoff jitter is seeded with the invocation's request ID,
so a run can be replayed from its logs.
"""

import os
import random
import time
from collections.abc import Callable
from functools import cache

from .client import ErpClient, Transport, http_transport
from .entities import ENTITIES
from .raw import Page, encode, raw_key
from .robust import RobustExtractor, State

# Setting -> environment variable (the stack sets them all).
ENV = {
    "url_parameter": "CRESTLINE_ERP_URL_PARAMETER",
    "client_id": "CRESTLINE_ERP_CLIENT_ID",
    "client_secret_parameter": "CRESTLINE_ERP_CLIENT_SECRET_PARAMETER",
    "bucket": "CRESTLINE_RAW_BUCKET",
    "table": "CRESTLINE_STATE_TABLE",
    "sweep_every": "CRESTLINE_SWEEP_EVERY",
}


class EntityFailed(Exception):
    """A read kept failing; the entity's other reads are written and saved."""


class S3RawWriter:
    """Raw reads into the raw bucket, at the same keys a local folder would use."""

    def __init__(self, s3, bucket: str) -> None:
        self.s3 = s3
        self.bucket = bucket

    def write(self, pages: list[Page], ordinal: int, ingest_date: str) -> str:
        key = raw_key(pages, ordinal, ingest_date)
        self.s3.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=encode(pages),
            ContentType="application/x-ndjson",
        )
        return key


class StateTable:
    """An entity's state as one DynamoDB item: entity, runs, watermark, sequence, last run.

    The curate step keeps its own attribute on the same item
    (`merged_through`), so saving sets these attributes and leaves the rest.
    """

    def __init__(self, table) -> None:
        self.table = table  # a boto3 DynamoDB Table

    def load(self, entity: str) -> State:
        item = self.table.get_item(Key={"entity": entity}).get("Item", {})
        state = State()
        if "runs" in item:
            state.runs[entity] = int(item["runs"])
        if "watermark" in item:
            state.watermarks[entity] = item["watermark"]
        if "sequence" in item:
            state.sequences[entity] = int(item["sequence"])
        if "last_run" in item:
            state.last_runs[entity] = item["last_run"]
        return state

    def save(self, entity: str, state: State) -> None:
        values = {"runs": state.runs.get(entity, 0)}
        if entity in state.watermarks:
            values["watermark"] = state.watermarks[entity]
        if entity in state.sequences:
            values["sequence"] = state.sequences[entity]
        if entity in state.last_runs:
            values["last_run"] = state.last_runs[entity]
        self.table.update_item(
            Key={"entity": entity},
            UpdateExpression="SET "
            + ", ".join(f"#{name} = :{name}" for name in values),
            ExpressionAttributeNames={f"#{name}": name for name in values},
            ExpressionAttributeValues={
                f":{name}": value for name, value in values.items()
            },
        )


@cache
def _setup() -> dict:
    """AWS clients and the ERP's address and secret, once per cold start."""
    import boto3

    settings = {name: os.environ[variable] for name, variable in ENV.items()}
    ssm = boto3.client("ssm")

    def parameter(name: str) -> str:
        return ssm.get_parameter(Name=name, WithDecryption=True)["Parameter"]["Value"]

    return {
        "url": parameter(settings["url_parameter"]).rstrip("/"),
        "client_id": settings["client_id"],
        "client_secret": parameter(settings["client_secret_parameter"]),
        "s3": boto3.client("s3"),
        "bucket": settings["bucket"],
        "table": boto3.resource("dynamodb").Table(settings["table"]),
        "sweep_every": int(settings["sweep_every"]),
    }


def handler(event: dict, context: object) -> dict:
    """Extract one entity: `{"entity": "Jobs"}` -> what was read."""
    entity = event["entity"]
    if entity not in ENTITIES:
        raise ValueError(f"no entity {entity}")
    return extract(entity, _setup(), seed=getattr(context, "aws_request_id", entity))


def extract(
    entity: str,
    setup: dict,
    seed: str,
    transport: Transport | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> dict:
    """Read one entity into the bucket and save its state.

    `transport` and `sleep` are for tests: an API in-process, and waits that
    don't take real time.
    """
    states = StateTable(setup["table"])
    client = ErpClient(
        transport or http_transport(setup["url"]),
        setup["client_id"],
        setup["client_secret"],
    )
    extractor = RobustExtractor(
        client,
        S3RawWriter(setup["s3"], setup["bucket"]),
        states.load(entity),
        rng=random.Random(seed),
        sleep=sleep,
        sweep_every=setup["sweep_every"],
    )
    extractor.run([entity])
    # Completed reads moved the state on; save it even if another read failed.
    states.save(entity, extractor.state)

    tally = extractor.tally
    report = {
        "entity": entity,
        "run": extractor.state.runs.get(entity, 0),
        # The ERP's clock hadn't moved since the last run: nothing was read.
        "skipped": bool(tally.skipped),
        "reads": dict(tally.reads),
        "rows": dict(tally.rows),
        "requests": {str(status): n for status, n in sorted(tally.requests.items())},
        "logins": tally.logins,
        "waited_seconds": round(tally.waited_seconds, 1),
    }
    if tally.abandoned:
        raise EntityFailed(
            f"{entity}: {tally.abandoned[entity]} read(s) dropped; {report}"
        )
    return report
