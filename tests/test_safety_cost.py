"""The cost-of-safety measurements' arithmetic, offline (M4 step 7)."""

import json
from pathlib import Path

import pytest

import safety_cost


@pytest.mark.parametrize(
    "q, expected", [(0.5, 3), (0.95, 5), (0.2, 1), (0.21, 2), (1.0, 5), (0.0, 1)]
)
def test_percentile_is_the_nearest_rank(q, expected):
    assert safety_cost.percentile([5, 1, 4, 2, 3], q) == expected


def test_a_requests_time_before_the_tool_and_in_all():
    """As the gateway logs an allowed read (M4 step 7, a C session)."""
    lines = [
        (1794436593361, "Successfully processed request"),
        (1794436591323, "Started processing request"),
        (1794436591328, "Received request for tools/call method"),
        (1794436591399, "Executing tool PlantData___downtime_events from target X"),
        (1794436593360, "Policy evaluation completed"),
    ]
    assert safety_cost.timing(lines) == {"before_tool_ms": 71, "gateway_ms": 2038}


def test_a_denied_call_never_reaches_the_tool():
    lines = [
        (100, "Started processing request"),
        (102, "Received request for tools/call method"),
        (150, "Policy evaluation denied request"),
    ]
    assert safety_cost.timing(lines) == {"before_tool_ms": None, "gateway_ms": 50}
    assert safety_cost.timing([(1, "Started processing request")]) is None


def write_log(path: Path, request_id: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    records = [
        {"type": "session", "at": "2026-11-11T22:00:00Z"},
        {
            "type": "tool_call",
            "at": "2026-11-11T22:00:01Z",
            "name": "PlantData___downtime_events",
            "input": {"machine_id": "PB-01", "days": 1},
            "request_id": request_id,
            "ms": 2100,
        },
    ]
    path.write_text("".join(json.dumps(r) + "\n" for r in records))


def test_each_call_carries_the_configuration_it_ran_under(tmp_path: Path):
    write_log(tmp_path / "injection/D/sonnet/direct-01/r1/a.jsonl", "r-1")
    write_log(tmp_path / "benign/20261111T230936Z-sonnet-ops-analyst-C/b.jsonl", "r-2")
    write_log(tmp_path / "benign/20261111T160514Z-sonnet/c.jsonl", "r-3")  # unknown
    got = {
        c["request_id"]: (c["config"], c["model"], c["tool"])
        for c in safety_cost.calls(tmp_path)
    }
    assert got == {
        "r-1": ("D", "sonnet", "downtime_events"),
        "r-2": ("C", "sonnet", "downtime_events"),
    }
