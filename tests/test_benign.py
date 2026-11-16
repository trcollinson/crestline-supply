"""The benign question set and how a session is checked (M4 steps 5 and 7)."""

import json

import pytest

import benign
from agent_tools.tools import TARGETS

TOOLS = {tool.name: target for target, tools in TARGETS.items() for tool in tools}


def test_twenty_questions_with_real_tools_and_roles():
    questions = benign.load()
    assert len(questions) == 20
    assert len({q["id"] for q in questions}) == 20
    for q in questions:
        assert q["role"] in {"both", "ops-analyst", "maintenance-planner"}
        assert q["question"].strip()
        assert q["tools"] and all(q["tools"])
        for group in q["tools"]:
            for name in group:
                assert name in TOOLS and TOOLS[name] != "PlantScada", name


def test_only_the_role_specific_questions_write():
    for q in benign.load():
        names = {name for group in q["tools"] for name in group}
        writes = names & {"send_report", "create_maintenance_request"}
        assert bool(writes) == (q["role"] != "both"), q["id"]


def records(*calls, stop="end_turn", answer="PB-02 jammed.", decisions=()):
    out = [{"type": "session"}]
    for name, text, status in calls:
        out.append(
            {
                "type": "tool_call",
                "name": f"PlantData___{name}",
                "status": status,
                "result_block": {"toolResult": {"content": [{"text": text}]}},
            }
        )
    out.append({"type": "end", "stop": stop, "answer": answer})
    for name, decision, policies in decisions:
        out.append(
            {
                "type": "decision",
                "name": f"PlantData___{name}",
                "decision": decision,
                "policies": policies,
            }
        )
    return out


ROWS = json.dumps({"rows": []})
QUESTION = {"tools": [["downtime_events", "machine_downtime"], ["cycles_by_day"]]}


def test_a_session_that_used_one_tool_from_each_group_passes():
    got = records(
        ("machine_downtime", ROWS, "success"), ("cycles_by_day", ROWS, "success")
    )
    assert benign.check(QUESTION, got) == []


@pytest.mark.parametrize(
    "calls, problem",
    [
        ([("machine_downtime", ROWS, "success")], "no answer from cycles_by_day"),
        (
            [
                ("machine_downtime", ROWS, "success"),
                ("cycles_by_day", json.dumps({"error": "days"}), "success"),
            ],
            "no answer from cycles_by_day",
        ),
        (
            [("downtime_events", "{}", "error"), ("cycles_by_day", ROWS, "success")],
            "no answer from downtime_events or machine_downtime",
        ),
    ],
)
def test_a_missing_or_refused_tool_fails(calls, problem):
    assert benign.check(QUESTION, records(*calls)) == [problem]


def test_no_answer_and_a_denial_fail():
    calls = [("machine_downtime", ROWS, "success"), ("cycles_by_day", ROWS, "success")]
    got = benign.check(QUESTION, records(*calls, stop="max_turns", answer=""))
    assert got == ["no answer (stopped: max_turns)"]
    denied = records(
        *calls,
        decisions=[("cycles_by_day", "DENY", []), ("machine_downtime", None, None)],
    )
    assert benign.check(QUESTION, denied) == [
        "denied cycles_by_day: default",
        "no decision logged for machine_downtime",
    ]
