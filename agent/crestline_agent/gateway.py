"""Just enough MCP to talk to the AgentCore Gateway, signed with SigV4.

MCP over HTTP is JSON-RPC in a POST: `initialize` once, then `tools/list`
and `tools/call`. The gateway's inbound auth is `AWS_IAM`, so each request
is signed for the `bedrock-agentcore` service with whatever credentials
the session holds. That is the whole identity story: the caller is an IAM
role (`crestline-ops-analyst`, `crestline-maintenance-planner`), and the
policy engine sees that role as the principal.

Standard library plus botocore's signer, on purpose (M4 step 0 chose it):
every byte the agent sends is visible here, and a reader can see there is
no safety logic in it.
"""

import json
import time
import urllib.error
import urllib.request

import boto3
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest

SERVICE = "bedrock-agentcore"
PROTOCOL = "2025-11-25"
STACK = "CrestlineAgent"
# A read can take a minute (spec §9); the read tools' Lambdas stop at two.
TIMEOUT_S = 300
ROLES = {
    "ops-analyst": "crestline-ops-analyst",
    "maintenance-planner": "crestline-maintenance-planner",
}


def stack_outputs(session: boto3.Session, stack: str = STACK) -> dict[str, str]:
    described = session.client("cloudformation").describe_stacks(StackName=stack)
    return {o["OutputKey"]: o["OutputValue"] for o in described["Stacks"][0]["Outputs"]}


def as_role(session: boto3.Session, role: str, suffix: str = "") -> boto3.Session:
    """A session as one of the agent's roles, by its short name.

    `suffix` is the stack's `RoleSuffix` output: a second copy of the stack
    in the same account (another region) names its roles apart.
    """
    account = session.client("sts").get_caller_identity()["Account"]
    credentials = session.client("sts").assume_role(
        RoleArn=f"arn:aws:iam::{account}:role/{ROLES[role]}{suffix}",
        RoleSessionName=f"crestline-{role}",
    )["Credentials"]
    return boto3.Session(
        aws_access_key_id=credentials["AccessKeyId"],
        aws_secret_access_key=credentials["SecretAccessKey"],
        aws_session_token=credentials["SessionToken"],
        region_name=session.region_name,
    )


class Gateway:
    """An MCP session with the gateway: `tools()` and `call()`.

    Every reply carries `_request_id` (the gateway's `x-amzn-RequestId`, the
    key its traces and the tools' logs share) and `_ms`, the round trip.
    """

    def __init__(
        self, url: str, session: boto3.Session, client_name: str = "crestline-agent"
    ):
        self.url = url
        self.session = session
        self.session_id: str | None = None
        self.next_id = 0
        self.rpc(
            "initialize",
            {
                "protocolVersion": PROTOCOL,
                "capabilities": {},
                "clientInfo": {"name": client_name, "version": "0.1.0"},
            },
        )

    def tools(self) -> list[dict]:
        return self.rpc("tools/list", {}).get("result", {}).get("tools", [])

    def call(self, name: str, arguments: dict) -> dict:
        return self.rpc("tools/call", {"name": name, "arguments": arguments})

    def rpc(self, method: str, params: dict) -> dict:
        self.next_id += 1
        body = json.dumps(
            {"jsonrpc": "2.0", "id": self.next_id, "method": method, "params": params}
        ).encode()
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
            "MCP-Protocol-Version": PROTOCOL,
        }
        if self.session_id:
            headers["Mcp-Session-Id"] = self.session_id
        signed = AWSRequest(method="POST", url=self.url, data=body, headers=headers)
        SigV4Auth(
            self.session.get_credentials().get_frozen_credentials(),
            SERVICE,
            self.session.region_name,
        ).add_auth(signed)
        request = urllib.request.Request(
            self.url, data=body, headers=dict(signed.headers), method="POST"
        )
        started = time.perf_counter()
        try:
            with urllib.request.urlopen(request, timeout=TIMEOUT_S) as response:
                self.session_id = response.headers.get(
                    "Mcp-Session-Id", self.session_id
                )
                request_id = response.headers.get("x-amzn-RequestId")
                if response.headers.get("Content-Type", "").startswith(
                    "text/event-stream"
                ):
                    reply = first_reply(response, self.next_id)
                else:
                    reply = json.loads(response.read().decode())
        except urllib.error.HTTPError as error:
            return {
                "http_error": error.code,
                "body": error.read().decode(),
                "_request_id": error.headers.get("x-amzn-RequestId"),
            }
        except (TimeoutError, urllib.error.URLError) as error:
            # No reply at all: the caller sees a failed call, as for any error.
            return {
                "transport_error": str(error),
                "_request_id": None,
                "_ms": round((time.perf_counter() - started) * 1000),
            }
        elapsed_ms = round((time.perf_counter() - started) * 1000)
        reply["_request_id"] = request_id
        reply["_ms"] = elapsed_ms
        return reply


def first_reply(stream, request_id: int) -> dict:
    """The JSON-RPC reply to `request_id` from a server-sent event stream.

    MCP's streamable HTTP lets a server answer a POST with a stream of
    events (progress notices, then the reply) rather than one JSON body, and
    the stream may stay open after the reply. So read events as they come
    and stop at the reply rather than at the end of the stream. (The gateway
    has answered with plain JSON in every call measured so far.)
    """
    data: list[str] = []
    for raw in stream:
        line = raw.decode().rstrip("\r\n")
        if line.startswith("data:"):
            data.append(line[5:].strip())
        elif not line and data:  # a blank line ends an event
            message = json.loads("\n".join(data))
            data = []
            if message.get("id") == request_id and (
                "result" in message or "error" in message
            ):
                return message
    if data:  # the stream closed without a final blank line
        message = json.loads("\n".join(data))
        if message.get("id") == request_id:
            return message
    return {"transport_error": "the event stream ended without a reply"}


def connect(role: str | None = None, session: boto3.Session | None = None) -> Gateway:
    """A gateway session, as `role` if given (else as the laptop's own identity)."""
    session = session or boto3.Session()
    outputs = stack_outputs(session)
    suffix = outputs.get("RoleSuffix", "")
    return Gateway(
        outputs["GatewayUrl"], as_role(session, role, suffix) if role else session
    )
