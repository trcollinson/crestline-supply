"""What the policy engine decided about each call, from the gateway's logs.

Under ENFORCE a denied call comes back as a JSON-RPC error that names the
policy, so the caller knows. Under LOG_ONLY the call runs either way and
the reply says nothing. The engine decides asynchronously there, and the
gateway's trace spans don't carry the decision either: the call's span says
only `LOG_ONLY_ASYNC` (M4 step 4; step 0 had assumed the spans held it).

The gateway's application logs do, in both modes (delivered to CloudWatch
Logs by AgentStack, output `GatewayLogGroup`). Each `tools/call` gets one
line with a `policy` object: the decision, the reason, the policies that
decided it, and the principal; its `request_id` is the `x-amzn-RequestId`
the gateway sent back with the reply (`Gateway.call` keeps it as
`_request_id`). So a list of request IDs is enough to look up every
decision. Lines arrive a minute or so after the call, so `decisions` waits.

A reply a guardrail policy withholds (`suppressOutput`, M4 step 6) gets no
`policy` object: the call's only line about it is an error whose text names
the policy ("Output blocked by policy: Policy evaluation denied due to
guardrail_downtime_events-fb5i7rucs6"), and no line records the decision
that let the call in. `decision` reads it as SUPPRESS.
"""

import json
import time
from dataclasses import dataclass

import boto3

from crestline_agent.gateway import stack_outputs


@dataclass(frozen=True)
class Decision:
    request_id: str
    decision: str  # ALLOW, DENY, or SUPPRESS (the reply withheld)
    reason: str  # empty for an ALLOW
    policies: tuple[str, ...]  # the determining policies, by name; () = default deny
    principal: str


SUPPRESSED = "Output blocked by policy: "


def policy_name(policy_id: str) -> str:
    """`forbid_plant_scada-cr9022wjzo` -> `forbid_plant_scada`: the engine
    adds a suffix to the name a policy was created with."""
    return policy_id.rsplit("-", 1)[0]


def decision(line: dict) -> Decision | None:
    """The decision in one gateway log line, or None if it holds none."""
    body = line.get("body", {})
    policy = body.get("policy")
    text = body.get("log", "")
    if not policy and text.startswith(SUPPRESSED):
        return Decision(
            request_id=line["request_id"],
            decision="SUPPRESS",
            reason=text,
            policies=(policy_name(text.rsplit(" ", 1)[-1]),),
            principal="",
        )
    if not policy:
        return None
    return Decision(
        request_id=line["request_id"],
        decision=policy["decision"],
        reason=policy.get("reason", ""),
        policies=tuple(policy_name(p) for p in policy.get("determiningPolicies", [])),
        principal=policy.get("principal", {}).get("entityId", ""),
    )


def decisions(
    request_ids: list[str],
    since_ms: int,
    session: boto3.Session | None = None,
    wait_s: float = 300,
) -> dict[str, Decision]:
    """The engine's decision for each request ID, waiting up to `wait_s` for
    the log lines to land. Missing IDs are left out; the caller decides
    whether that is a failure."""
    session = session or boto3.Session()
    group = stack_outputs(session)["GatewayLogGroup"]
    logs = session.client("logs")
    wanted = set(request_ids)
    found: dict[str, Decision] = {}
    deadline = time.monotonic() + wait_s
    while True:
        for page in logs.get_paginator("filter_log_events").paginate(
            logGroupName=group,
            startTime=since_ms,
            filterPattern='"Policy evaluation"',
        ):
            for event in page["events"]:
                got = decision(json.loads(event["message"]))
                if got and got.request_id in wanted:
                    found[got.request_id] = got
        if wanted <= set(found) or time.monotonic() > deadline:
            return found
        time.sleep(15)
