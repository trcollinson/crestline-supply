"""eval/brief_passed.py: alert labels by script, and readings refused unless they quote the log."""

from pathlib import Path

import pytest
import yaml

import brief_passed as bp


def brief(text: str, statuses: dict[str, str], model: str = "nova-v2") -> bp.Brief:
    facts = [
        {"id": alert, "value": "5.1", "unit": "points", "status": s}
        for alert, s in statuses.items()
    ]
    return bp.Brief(
        model=model,
        morning="2026-08-18",
        log=Path(f"20261120T000000Z-2026-08-18-{model}.jsonl"),
        text=text,
        given={
            "morning": "Tuesday 2026-08-18",
            "plant_day": "Monday 2026-08-17",
            "facts": facts,
        },
    )


MARGIN = "anomaly:PB-02 2nd:erp_margin_above_rebuilt"
SCRAP = "anomaly:LSR-01:scrap_above_baseline"


@pytest.mark.parametrize(
    ("line", "status", "verdict"),
    [
        (f"- **New:** PB-02 2nd gap 5.1 points [{MARGIN}]", "New", "right"),
        (
            f"- Still open since 2026-07-10: PB-02 2nd [{MARGIN}]",
            "Still open since 2026-07-10",
            "right",
        ),
        (
            f"- Open since at least Jul 10: PB-02 2nd [{MARGIN}]",
            "Open since at least 2026-07-10",
            "right",
        ),
        (
            f"- Still open: PB-02 2nd [{MARGIN}]",
            "Still open since 2026-07-10",
            "right, no date",
        ),
        (
            f"- **New:** PB-02 2nd gap 5.1 points [{MARGIN}]",
            "Still open since 2026-07-10",
            "mislabelled",
        ),
        (
            f"- Still open since 2026-07-20 [{MARGIN}]",
            "Still open since 2026-07-10",
            "wrong date",
        ),
        (f"- PB-02 2nd gap 5.1 points [{MARGIN}]", "New", "unclear"),
    ],
)
def test_an_alerts_label_is_read_against_the_ledger(line, status, verdict):
    [row] = bp.alert_labels(brief(f"## Alerts\n\n{line}\n", {MARGIN: status}))
    assert row["verdict"] == verdict


def test_the_label_can_come_from_a_heading_above():
    text = f"## Alerts\n\n**Still open:**\n- PB-02 2nd gap 5.1 points [{MARGIN}]\n"
    [row] = bp.alert_labels(brief(text, {MARGIN: "Still open since 2026-07-10"}))
    assert row["verdict"] == "right, no date"


def test_no_new_alerts_is_not_a_new_label():
    text = f"No new alerts.\n- Still open since 2026-07-10 [{MARGIN}]\n"
    [row] = bp.alert_labels(brief(text, {MARGIN: "Still open since 2026-07-10"}))
    assert row["verdict"] == "right"


def test_an_alert_never_named():
    [row] = bp.alert_labels(brief("## Output\n\nLASER: 700 pieces.\n", {SCRAP: "New"}))
    assert row["verdict"] == "not named"


def readings(tmp_path: Path, entries: list[dict]) -> Path:
    path = tmp_path / "review.yaml"
    path.write_text(yaml.safe_dump({"out": "out-m5", "briefs": entries}))
    return path


@pytest.fixture
def two_briefs(monkeypatch):
    found = [
        brief("PB-02 margin was 21.7 percent.", {}, "nova-v2"),
        brief("All work centers ran.", {}, "sonnet-v2"),
    ]
    monkeypatch.setattr(bp, "passed_briefs", lambda out_dir: found)
    return found


def entry(b: bp.Brief, findings: list[dict]) -> dict:
    return {
        "model": b.model,
        "morning": b.morning,
        "log": b.log.name,
        "findings": findings,
    }


def test_readings_that_quote_the_logs_are_counted(tmp_path, two_briefs):
    nova, sonnet = two_briefs
    wrong = {
        "quote": "PB-02 margin was 21.7 percent",
        "verdict": "wrong",
        "fact": "x",
        "note": "y",
    }
    path = readings(tmp_path, [entry(nova, [wrong]), entry(sonnet, [])])
    _, entries = bp.checked(Path("out-m5"), path)
    assert [len(bp.misleading(e)) for e in entries] == [1, 0]


@pytest.mark.parametrize(
    "problem",
    [
        "a quote the brief doesn't hold",
        "a brief not read",
        "a brief read twice",
        "another log",
    ],
)
def test_readings_that_drift_from_the_logs_are_refused(tmp_path, two_briefs, problem):
    nova, sonnet = two_briefs
    entries = [entry(nova, []), entry(sonnet, [])]
    if problem == "a quote the brief doesn't hold":
        entries[0]["findings"] = [
            {
                "quote": "PB-02 margin was 12.7",
                "verdict": "wrong",
                "fact": "x",
                "note": "y",
            }
        ]
    elif problem == "a brief not read":
        entries.pop()
    elif problem == "a brief read twice":
        entries.append(entry(sonnet, []))
    else:
        entries[0]["log"] = "20261119T000000Z-2026-08-18-nova.jsonl"
    with pytest.raises(SystemExit):
        bp.checked(Path("out-m5"), readings(tmp_path, entries))
