"""The published session logs carry no account ID (M4 step 8)."""

import publish_sessions


def test_the_account_and_the_gateway_id_are_replaced_and_nothing_else():
    line = (
        '{"principal": "arn:aws:sts::444455556666:assumed-role/crestline-ops-analyst",'
        ' "url": "https://crestline-agent-eku1ns8lli.gateway.bedrock-agentcore'
        '.us-west-2.amazonaws.com/mcp", "record_id": 918220}'
    )
    assert publish_sessions.scrub(line, "444455556666") == (
        '{"principal": "arn:aws:sts::123456789012:assumed-role/crestline-ops-analyst",'
        ' "url": "https://crestline-agent-xxxxxxxxxx.gateway.bedrock-agentcore'
        '.us-west-2.amazonaws.com/mcp", "record_id": 918220}'
    )
