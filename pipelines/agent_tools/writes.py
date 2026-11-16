"""The agent's two writes: one into a stub CMMS, one into an outbox.

`create_maintenance_request` (target `Maintenance`) puts a request in a
DynamoDB table named for what it is, `crestline-cmms-stub`. It takes any
priority and category a real CMMS would; which ones an agent may file is
the policy's decision (step 4), not this table's.

`send_report` (target `Reports`) writes the email it would have sent to the
agent bucket's `outbox/` prefix, and sends nothing. It takes any address;
which ones an agent may write to is, again, the policy's decision.

Both record the gateway's request ID, so the red-team eval can tie what
was executed to the call the gateway decided on.
"""

import json
import os
import uuid
from datetime import UTC, datetime

import boto3

from agent_tools import tools


def maintenance_handler(event: dict, context: object) -> dict:
    return _handle(event, context, _create_maintenance_request)


def report_handler(event: dict, context: object) -> dict:
    return _handle(event, context, _send_report)


def _handle(event: dict, context: object, write) -> dict:
    name = tools.tool_name(context)
    request_id = tools.gateway_request_id(context)
    print(
        json.dumps({"tool": name, "arguments": event, "gateway_request_id": request_id})
    )
    try:
        return write(event, request_id)
    except tools.ToolInputError as error:
        return {"error": str(error)}


def _create_maintenance_request(
    arguments: dict, gateway_request_id: str | None
) -> dict:
    item = {
        "request_id": f"MR-{uuid.uuid4().hex[:10].upper()}",
        "created_at_utc": _now(),
        "machine_id": tools.machine(arguments),
        "priority": tools.one_of(arguments, "priority", tools.PRIORITIES),
        "category": tools.one_of(arguments, "category", tools.CATEGORIES),
        "description": tools.text(arguments, "description", tools.MAX_DESCRIPTION),
        "gateway_request_id": gateway_request_id or "",
    }
    boto3.resource("dynamodb").Table(os.environ["CRESTLINE_CMMS_TABLE"]).put_item(
        Item=item
    )
    return {"status": "created", "request_id": item["request_id"]}


def _send_report(arguments: dict, gateway_request_id: str | None) -> dict:
    message = {
        "message_id": uuid.uuid4().hex,
        "queued_at_utc": _now(),
        "recipient": tools.text(arguments, "recipient", 254, tools.EMAIL),
        "subject": tools.text(arguments, "subject", tools.MAX_SUBJECT),
        "body": tools.text(arguments, "body", tools.MAX_BODY),
        "gateway_request_id": gateway_request_id or "",
    }
    key = f"outbox/{message['queued_at_utc'][:10]}/{message['message_id']}.json"
    boto3.client("s3").put_object(
        Bucket=os.environ["CRESTLINE_AGENT_BUCKET"],
        Key=key,
        Body=json.dumps(message).encode(),
        ContentType="application/json",
    )
    return {"status": "queued", "message_id": message["message_id"]}


def _now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
