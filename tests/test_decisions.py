"""Reading the policy engine's decision out of a gateway log line (M4 step 4)."""

from crestline_agent.decisions import decision, policy_name

ROLE = "arn:aws:sts::123456789012:assumed-role/crestline-ops-analyst"

# Two lines from the gateway's application logs under LOG_ONLY, trimmed, with
# an example account: a denial by the forbid, and the line before it.
DENIED = {
    "request_id": "c3a22ffe-a1ed-402d-b205-35d6dc2ae0a9",
    "body": {
        "isError": True,
        "log": "Policy evaluation denied request",
        "policy": {
            "decision": "DENY",
            "reason": "Policy evaluation denied due to forbid_plant_scada-wkoqat2049",
            "determiningPolicies": ["forbid_plant_scada-wkoqat2049"],
            "principal": {"entityType": "AgentCore::IamEntity", "entityId": ROLE},
            "latencyMs": 49,
        },
    },
}
EXECUTING = {
    "request_id": "c3a22ffe-a1ed-402d-b205-35d6dc2ae0a9",
    "body": {"log": "Executing tool PlantScada___write_tag from target S3RJEJ55VI"},
}


def test_a_log_line_gives_the_decision_the_rule_and_the_caller():
    got = decision(DENIED)
    assert got.request_id == "c3a22ffe-a1ed-402d-b205-35d6dc2ae0a9"
    assert (got.decision, got.policies, got.principal) == (
        "DENY",
        ("forbid_plant_scada",),
        ROLE,
    )


def test_default_deny_has_no_determining_policy():
    policy = DENIED["body"]["policy"] | {
        "reason": "No policy applies to the request (denied by default).",
        "determiningPolicies": [],
    }
    assert decision(DENIED | {"body": {"policy": policy}}).policies == ()


def test_a_line_without_a_decision_is_none():
    assert decision(EXECUTING) is None


def test_policy_names_lose_only_the_engines_suffix():
    assert policy_name("permit_machine_reads-ab12cd34ef") == "permit_machine_reads"


def test_a_withheld_reply_is_read_as_suppressed():
    """Configuration B and D's guardrail: no `policy` object, only this line."""
    line = {
        "request_id": "932cc5db-a3af-42ae-8025-6314617e96c1",
        "body": {
            "isError": "True",
            "log": "Output blocked by policy: Policy evaluation denied due to"
            " guardrail_downtime_events-fb5i7rucs6",
        },
    }
    got = decision(line)
    assert (got.decision, got.policies) == ("SUPPRESS", ("guardrail_downtime_events",))
