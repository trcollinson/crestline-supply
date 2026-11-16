"""M4 step 0's probe: talk MCP to the spike gateway, SigV4-signed, from a laptop.

    uv run python scripts/agent_spike.py list [--role]
    uv run python scripts/agent_spike.py call TOOL '{"arg": ...}' [--role]
    uv run python scripts/agent_spike.py converse "question" [--role] [--model ID]

The client is MCP's JSON-RPC over HTTP POST with nothing but boto3 and
the standard library: sign the request with SigV4 for `bedrock-agentcore`,
post it, read the JSON (or the one SSE event) back. `--role` assumes
`crestline-spike-agent` first, so the call arrives as that role.
Reads the gateway's URL from the CrestlineAgentSpike stack's outputs.
"""

import argparse
import json
import time
import urllib.request

import boto3
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest

STACK = "CrestlineAgentSpike"
SERVICE = "bedrock-agentcore"
PROTOCOL = "2025-11-25"


def outputs(session: boto3.Session) -> dict[str, str]:
    stack = session.client("cloudformation").describe_stacks(StackName=STACK)
    return {o["OutputKey"]: o["OutputValue"] for o in stack["Stacks"][0]["Outputs"]}


def as_role(session: boto3.Session, role_arn: str) -> boto3.Session:
    creds = session.client("sts").assume_role(
        RoleArn=role_arn, RoleSessionName="spike-probe"
    )["Credentials"]
    return boto3.Session(
        aws_access_key_id=creds["AccessKeyId"],
        aws_secret_access_key=creds["SecretAccessKey"],
        aws_session_token=creds["SessionToken"],
        region_name=session.region_name,
    )


class Gateway:
    """Just enough MCP: initialize, tools/list, tools/call."""

    def __init__(self, url: str, session: boto3.Session) -> None:
        self.url = url
        self.session = session
        self.session_id: str | None = None
        self.next_id = 0
        self.rpc(
            "initialize",
            {
                "protocolVersion": PROTOCOL,
                "capabilities": {},
                "clientInfo": {"name": "crestline-spike", "version": "0"},
            },
        )

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
        request = AWSRequest(method="POST", url=self.url, data=body, headers=headers)
        SigV4Auth(
            self.session.get_credentials().get_frozen_credentials(),
            SERVICE,
            self.session.region_name,
        ).add_auth(request)
        started = time.perf_counter()
        http = urllib.request.Request(
            self.url, data=body, headers=dict(request.headers), method="POST"
        )
        try:
            with urllib.request.urlopen(http) as response:
                self.session_id = response.headers.get(
                    "Mcp-Session-Id", self.session_id
                )
                text = response.read().decode()
                kind = response.headers.get("Content-Type", "")
        except urllib.error.HTTPError as error:
            return {"http_error": error.code, "body": error.read().decode()}
        elapsed = time.perf_counter() - started
        if kind.startswith("text/event-stream"):
            data = [
                line[5:].strip()
                for line in text.splitlines()
                if line.startswith("data:")
            ]
            text = data[-1]
        reply = json.loads(text)
        reply["_ms"] = round(elapsed * 1000)
        return reply

    def tools(self) -> list[dict]:
        return self.rpc("tools/list", {}).get("result", {}).get("tools", [])

    def call(self, name: str, arguments: dict) -> dict:
        return self.rpc("tools/call", {"name": name, "arguments": arguments})


SYSTEM = (
    "You are a plant analyst assistant for Crestline Supply, a metal fabrication "
    "plant. Answer from the tools, and cite the rows you used. Don't take actions "
    "you weren't asked to."
)


def converse(
    gateway: Gateway, session: boto3.Session, question: str, model: str
) -> None:
    """A bare Converse tool loop: no safety logic, on purpose."""
    bedrock = session.client("bedrock-runtime")
    tools = gateway.tools()
    config = {
        "tools": [
            {
                "toolSpec": {
                    "name": t["name"],
                    "description": t.get("description", ""),
                    "inputSchema": {"json": t["inputSchema"]},
                }
            }
            for t in tools
        ]
    }
    messages = [{"role": "user", "content": [{"text": question}]}]
    usage = {"inputTokens": 0, "outputTokens": 0}
    for turn in range(8):
        reply = bedrock.converse(
            modelId=model,
            system=[{"text": SYSTEM}],
            messages=messages,
            toolConfig=config,
        )
        for key in usage:
            usage[key] += reply["usage"][key]
        message = reply["output"]["message"]
        messages.append(message)
        results = []
        for block in message["content"]:
            if "text" in block:
                print(f"[model {turn}] {block['text']}")
            if "toolUse" in block:
                use = block["toolUse"]
                outcome = gateway.call(use["name"], use["input"])
                print(
                    f"[tool] {use['name']} {json.dumps(use['input'])} -> {json.dumps(outcome)[:600]}"
                )
                result = outcome.get("result", outcome)
                results.append(
                    {
                        "toolResult": {
                            "toolUseId": use["toolUseId"],
                            "content": [
                                {"text": json.dumps(result.get("content", result))}
                            ],
                            "status": "error"
                            if result.get("isError") or "error" in outcome
                            else "success",
                        }
                    }
                )
        if reply["stopReason"] != "tool_use":
            break
        messages.append({"role": "user", "content": results})
    print(f"[usage] {json.dumps(usage)} turns={turn + 1}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["list", "call", "converse"])
    parser.add_argument("args", nargs="*")
    parser.add_argument("--role", action="store_true")
    parser.add_argument("--model", default="us.anthropic.claude-sonnet-5-5")
    opts = parser.parse_args()

    session = boto3.Session()
    out = outputs(session)
    caller = as_role(session, out["AgentRoleArn"]) if opts.role else session
    gateway = Gateway(out["GatewayUrl"], caller)
    if opts.command == "list":
        print(json.dumps(gateway.rpc("tools/list", {}), indent=2))
    elif opts.command == "call":
        print(
            json.dumps(gateway.call(opts.args[0], json.loads(opts.args[1])), indent=2)
        )
    else:
        converse(
            gateway, session, opts.args[0], opts.model
        )  # Bedrock as the laptop; tools as the caller


if __name__ == "__main__":
    main()
