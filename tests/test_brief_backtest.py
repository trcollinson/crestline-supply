"""The brief's backtest scoring (M5 step 5), on planted mornings and a planted
answer key; and the Python rules against the SQL's own alerts on real ones.

The backtest re-applies each rule in Python to the rows the SQL's rules
read, so it can sweep a threshold without a query. That is only honest if
the two agree, so one test runs the SQL on real mornings and compares.
"""

import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import polars as pl
import pytest

import brief_backtest
import brief_scoring as bs

REPO_ROOT = Path(__file__).resolve().parent.parent

T = bs.Thresholds(3, 2, 60, 0.5, 3, 5)
MORNINGS = [date(2026, 8, 10) + timedelta(days=d) for d in range(12) if d % 7 < 5]


def day_before(morning: date) -> date:
    return morning - timedelta(days=3 if morning.weekday() == 0 else 1)


def backtest(**rows: pl.DataFrame) -> bs.Backtest:
    """A backtest of planted rows; every view is present, most of them empty."""
    empty = {
        "brief_scrap": {"machine_id": pl.String, "pieces": pl.Int64, "scrapped": pl.Int64,
                        "rate": pl.Float64, "baseline_rate": pl.Float64,
                        "standard_errors": pl.Float64},
        "brief_stops": {"machine_id": pl.String, "stop_minutes": pl.Float64,
                        "usual_stop_minutes": pl.Float64},
        "brief_output": {"work_center": pl.String, "pieces": pl.Int64,
                         "usual_pieces": pl.Float64},
        "brief_margin": {"subject": pl.String, "jobs": pl.Int64, "gap_points": pl.Float64},
        "brief_margin_jobs": {"job_num": pl.String, "brake": pl.String,
                              "setup_shift": pl.String, "revenue": pl.Float64,
                              "erp_cost": pl.Float64},
        "brief_machine_stops": {"machine_id": pl.String,
                                "started_at_utc": pl.Datetime("us", "UTC"),
                                "ended_at_utc": pl.Datetime("us", "UTC")},
    }  # fmt: skip
    frames = {}
    for view, schema in empty.items():
        frame = rows.get(view, pl.DataFrame(schema={**schema, "morning": pl.Date}))
        frames[view] = frame.with_columns(
            plant_day=pl.col("morning").map_elements(day_before, return_dtype=pl.Date)
        )
    return bs.Backtest(
        Path("."), MORNINGS, {m: day_before(m) for m in MORNINGS}, {}, frames
    )


# --- Issues -----------------------------------------------------------------------


def test_consecutive_mornings_are_one_issue_and_a_gap_starts_another():
    found = pl.DataFrame(
        {
            "morning": [MORNINGS[i] for i in (0, 1, 2, 4, 5)],
            "plant_day": [day_before(MORNINGS[i]) for i in (0, 1, 2, 4, 5)],
            "fact_id": ["anomaly:LSR-01:scrap_above_baseline"] * 5,
            "family": ["scrap"] * 5,
            "subject": ["LSR-01"] * 5,
            "value": [4.0] * 5,
        }
    )
    grouped = bs.issues(found, MORNINGS)
    assert grouped["mornings"].to_list() == [3, 2]
    assert grouped["opened_morning"].to_list() == [MORNINGS[0], MORNINGS[4]]
    # Over a weekend is still consecutive: Monday's brief follows Friday's.
    assert (MORNINGS[4].weekday(), MORNINGS[5].weekday()) == (4, 0)


# --- The scrap rule, as built and frozen ------------------------------------------


def laser_rows(
    rates: list[float], baselines: list[float], pieces: int = 900
) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "morning": MORNINGS[: len(rates)],
            "machine_id": ["LSR-01"] * len(rates),
            "pieces": [pieces] * len(rates),
            "scrapped": [round(r * pieces) for r in rates],
            "rate": rates,
            "baseline_rate": baselines,
            "standard_errors": [
                bs.standard_errors(r, b, pieces)
                for r, b in zip(rates, baselines, strict=True)
            ],
        }
    )


def test_a_frozen_baseline_keeps_an_alert_open_that_the_trailing_one_swallows():
    # A rate that holds at 3.5% while the trailing baseline climbs toward it.
    rates = [0.016, 0.035, 0.035, 0.035, 0.035, 0.016]
    baselines = [0.016, 0.016, 0.022, 0.028, 0.031, 0.032]
    bt = backtest(brief_scrap=laser_rows(rates, baselines))

    built = bs.alerts(bt, T).filter(pl.col("family") == "scrap")
    frozen = bs.alerts(bt, T, frozen_scrap=True).filter(pl.col("family") == "scrap")
    assert built["morning"].to_list() == MORNINGS[1:2]  # 3.5% on 2.2% is 2.7 SE
    assert frozen["morning"].to_list() == MORNINGS[1:5]
    # The first alert is the same either way: freezing only acts once one is open.
    assert built["morning"][0] == frozen["morning"][0]


def test_the_other_rules_fire_as_the_sql_has_them():
    bt = backtest(
        brief_stops=pl.DataFrame(
            {"morning": MORNINGS[:3], "machine_id": ["PEM-01"] * 3,
             "stop_minutes": [130.0, 59.0, 80.0], "usual_stop_minutes": [20.0, 20.0, 40.0]}
        ),
        brief_output=pl.DataFrame(
            {"morning": MORNINGS[:2], "work_center": ["PEM"] * 2,
             "pieces": [100, 200], "usual_pieces": [400.0, 400.0]}
        ),
        brief_margin=pl.DataFrame(
            {"morning": MORNINGS[:1] * 3, "subject": ["PB-02 2nd", "PB-01 1st", "all jobs"],
             "jobs": [40, 4, 300], "gap_points": [6.1, 5.0, 4.0]}
        ),
    )  # fmt: skip
    found = bs.alerts(bt, T)
    assert found.select("morning", "fact_id").rows() == [
        (MORNINGS[0], "anomaly:PB-02 2nd:erp_margin_above_rebuilt"),
        (MORNINGS[0], "anomaly:PEM-01:stop_minutes_above_usual"),
        (MORNINGS[0], "anomaly:PEM:output_below_usual"),
    ]  # 59 minutes is under the floor; 80 isn't over twice 40; 200 isn't under half


# --- Verdicts against a planted answer key ----------------------------------------

DAY = day_before(MORNINGS[1])  # a Tuesday


def utc(hour: int, minute: int = 0, day: date = DAY) -> datetime:
    return datetime(day.year, day.month, day.day, hour, minute, tzinfo=UTC)


def planted_truth(**parts: pl.DataFrame) -> bs.Truth:
    def frame(name: str, schema: dict) -> pl.DataFrame:
        return parts.get(name, pl.DataFrame(schema=schema))

    states = frame(
        "states",
        {"machine_id": pl.String, "state": pl.String, "reason": pl.String,
         "started_at": pl.Datetime("us", "UTC"), "ended_at": pl.Datetime("us", "UTC"),
         "job_num": pl.String, "op_seq": pl.Int64},
    ).with_columns(
        minutes=(pl.col("ended_at") - pl.col("started_at")).dt.total_seconds() / 60,
        day=bs._local_day("started_at"),
    )  # fmt: skip
    return bs.Truth(
        scrap=frame("scrap", {"machine_id": pl.String, "day": pl.Date, "pieces": pl.Int64,
                              "scrapped": pl.Int64, "wear": pl.Int64, "setup": pl.Int64,
                              "random": pl.Int64}),
        pieces=frame("pieces", {"work_center": pl.String, "day": pl.Date,
                                "pieces": pl.Int64}),
        down=bs._true_down(states),
        states=states,
        operations=frame("operations", {"job_num": pl.String, "work_center": pl.String,
                                        "machine_id": pl.String, "setup_hours": pl.Float64,
                                        "std_setup_min": pl.Float64}),
        costs=frame("costs", {"job_num": pl.String, "total_cost": pl.Float64}),
        cycles=pl.DataFrame(),
        events=pl.DataFrame(),
        labor_files=frame("labor_files", {"file_day": pl.Date, "work_center": pl.String,
                                          "scan_day": pl.Date, "pieces": pl.Int64}),
    )  # fmt: skip


def judge(family: str, subject: str, bt: bs.Backtest, tr: bs.Truth) -> bs.Verdict:
    issue = {"family": family, "subject": subject, "opened_day": DAY,
             "opened_morning": MORNINGS[1]}  # fmt: skip
    return bs.verdict(issue, T, bt, tr)


@pytest.mark.parametrize(
    ("true_row", "expected", "words"),
    [
        ((900, 60, 40, 5, 15), "scenario", "40 of 900 pieces scrapped for nozzle wear"),
        ((900, 60, 0, 45, 15), "false", "chance: the answer key agrees"),
        (
            (900, 15, 0, 5, 10),
            "false",
            "data: platform 6.7% of 900, answer key 1.7% of 900",
        ),
    ],
    ids=["nozzle", "chance", "data"],
)
def test_a_scrap_alert_is_the_nozzle_ordinary_scrap_or_a_wrong_number(
    true_row, expected, words
):
    bt = backtest(brief_scrap=laser_rows([0.016, 0.0667], [0.016, 0.016]))
    pieces, scrapped, wear, setup, random = true_row
    tr = planted_truth(
        scrap=pl.DataFrame(
            {"machine_id": ["LSR-01"], "day": [DAY], "pieces": [pieces],
             "scrapped": [scrapped], "wear": [wear], "setup": [setup], "random": [random]}
        )
    )  # fmt: skip
    found = judge("scrap", "LSR-01", bt, tr)
    assert found.verdict == expected
    assert words in found.reason


def stop_backtest(minutes: float) -> bs.Backtest:
    return backtest(
        brief_stops=pl.DataFrame(
            {"morning": [MORNINGS[1]], "machine_id": ["PC-LINE"],
             "stop_minutes": [minutes], "usual_stop_minutes": [20.0]}
        ),
        brief_machine_stops=pl.DataFrame(
            {"morning": [MORNINGS[1]], "machine_id": ["PC-LINE"],
             "started_at_utc": [utc(16)], "ended_at_utc": [utc(17, 30)]}
        ),
    )  # fmt: skip


def test_a_stop_alert_is_real_when_the_answer_key_was_down_that_long():
    tr = planted_truth(
        states=pl.DataFrame(
            {"machine_id": ["PC-LINE"] * 3, "state": ["DOWN", "PLANNED_STOP", "DOWN"],
             "reason": ["JAM", "BREAK", "JAM"],
             "started_at": [utc(16), utc(17), utc(17, 10)],
             "ended_at": [utc(17), utc(17, 10), utc(17, 40)],
             "job_num": ["J1", None, "J1"], "op_seq": [50, None, 50]}
        )
    )  # fmt: skip
    found = judge("stops", "PC-LINE", stop_backtest(90), tr)
    assert found == bs.Verdict(
        "real", "90 minutes down in the answer key (usual 20): JAM 90"
    )


def test_a_stop_alert_the_machine_wasnt_down_for_names_what_it_was_doing():
    tr = planted_truth(
        states=pl.DataFrame(
            {"machine_id": ["PC-LINE"] * 2, "state": ["IDLE", "PLANNED_STOP"],
             "reason": [None, "MEAL"], "started_at": [utc(16), utc(17)],
             "ended_at": [utc(17), utc(17, 30)], "job_num": [None, None],
             "op_seq": [None, None]}
        )
    )  # fmt: skip
    found = judge("stops", "PC-LINE", stop_backtest(90), tr)
    assert found.verdict == "false"
    assert "were, in the answer key, IDLE 60, PLANNED_STOP MEAL 30" in found.reason


def test_an_output_alert_counts_the_pieces_still_open_at_0530():
    bt = backtest(
        brief_output=pl.DataFrame(
            {"morning": [MORNINGS[1]], "work_center": ["PEM"], "pieces": [150],
             "usual_pieces": [400.0]}
        )
    )  # fmt: skip
    tr = planted_truth(
        pieces=pl.DataFrame({"work_center": ["PEM"], "day": [DAY], "pieces": [390]}),
        labor_files=pl.DataFrame(
            {"file_day": [DAY, MORNINGS[1]], "work_center": ["PEM", "PEM"],
             "scan_day": [DAY, DAY], "pieces": [150, 240]}
        ),
    )  # fmt: skip
    found = judge("output", "PEM", bt, tr)
    assert found == bs.Verdict(
        "false",
        "data: the platform counted 150, the answer key 390 (usual 400); "
        "240 pieces in scans still open at 05:30",
    )


def test_pb02_second_shift_is_the_scenario_and_another_brake_is_judged_on_its_gap():
    jobs = pl.DataFrame(
        {"morning": [MORNINGS[1]] * 2, "job_num": ["J1", "J2"], "brake": ["PB-02", "PB-01"],
         "setup_shift": ["2nd", "1st"], "revenue": [1000.0, 1000.0],
         "erp_cost": [800.0, 800.0]}
    )  # fmt: skip
    tr = planted_truth(
        costs=pl.DataFrame({"job_num": ["J1", "J2"], "total_cost": [860.0, 810.0]}),
        operations=pl.DataFrame(
            {"job_num": ["J1", "J2"], "work_center": ["BRAKE"] * 2,
             "machine_id": ["PB-02", "PB-01"], "setup_hours": [1.4, 0.5],
             "std_setup_min": [30.0, 30.0]}
        ),
    )  # fmt: skip
    bt = backtest(brief_margin_jobs=jobs)
    assert judge("margin", "PB-02 2nd", bt, tr) == bs.Verdict(
        "scenario",
        "pb02_second_shift_setups: answer key's gap 6.0 points on 1 jobs, setups 2.80x standard",
    )
    assert judge("margin", "PB-01 1st", bt, tr).verdict == "false"


def test_a_saturday_is_one_shift_whatever_the_rule():
    issue = {"family": "output", "subject": "ASSY", "opened_day": date(2026, 8, 15),
             "opened_morning": date(2026, 8, 17)}  # fmt: skip
    assert bs.verdict(issue, T, backtest(), planted_truth()).reason.startswith(
        "calendar"
    )


# --- The nozzle: lead time and what the wait cost ---------------------------------


def test_the_wait_is_priced_from_the_brief_morning_to_the_change():
    first = MORNINGS[2]  # Wednesday; the change is the next Monday
    changed = datetime(2026, 8, 17, 13, 46, tzinfo=UTC)
    at_six = datetime(
        first.year, first.month, first.day, 12, 0, tzinfo=UTC
    )  # 06:00 MDT
    cycles = pl.DataFrame(
        {
            "job_num": ["J1"] * 4,
            "completed_at": [
                at_six - timedelta(minutes=1),
                at_six,
                changed - timedelta(minutes=1),
                changed,
            ],
            "scrap_cause": [bs.WEAR] * 4,
        }
    )
    tr = planted_truth(
        operations=pl.DataFrame(
            {"job_num": ["J1"], "work_center": ["LASER"], "machine_id": ["LSR-01"],
             "setup_hours": [0.2], "std_setup_min": [15.0]}
        ),
        costs=pl.DataFrame({"job_num": ["J1"], "total_cost": [0.0]}),
    )  # fmt: skip
    tr.operations = tr.operations.with_columns(
        qty_good=pl.lit(90),
        qty_scrap=pl.lit(10),
        labor_cost=pl.lit(40.0),
        burden_cost=pl.lit(160.0),
    )
    tr.costs = tr.costs.with_columns(material_cost=pl.lit(300.0))
    tr.cycles = cycles
    tr.events = pl.DataFrame(
        {"event": ["ramp_start", "nozzle_changed"],
         "local_date": [MORNINGS[0], date(2026, 8, 17)], "at_utc": [None, changed]}
    )  # fmt: skip
    tr.scrap = pl.DataFrame(
        {"machine_id": ["LSR-01"], "day": [MORNINGS[1]], "pieces": [100], "scrapped": [6],
         "wear": [4], "setup": [1], "random": [1]}
    )  # fmt: skip
    scored = pl.DataFrame(
        {"family": ["scrap"], "subject": ["LSR-01"], "verdict": ["scenario"],
         "opened_morning": [first], "opened_day": [day_before(first)]}
    )  # fmt: skip
    found = bs.nozzle("as built", scored, backtest(), tr)
    # $5 a piece: (40 + 160) labor and burden plus 300 material, over 100 pieces cut.
    assert (found.wear_pieces, found.wear_pieces_after_alert) == (4, 2)
    assert (found.wear_cost, found.wear_cost_after_alert) == (20.0, 10.0)
    assert (
        found.early_working_days == 3
    )  # Wed, Thu, Fri mornings' plant days before Monday
    assert found.over_5_pct == MORNINGS[1]


# --- The Python rules equal the SQL's, on real mornings ---------------------------


def test_the_python_rules_raise_the_sqls_alerts(m5_out, monkeypatch):
    """Three real mornings: one quiet, the nozzle open, a stop on PC-LINE."""
    monkeypatch.chdir(REPO_ROOT)
    picked = [date(2026, 7, 28), date(2026, 8, 18), date(2026, 8, 20)]
    brief_backtest._run_chunk(m5_out, picked)
    monkeypatch.setattr(bs, "mornings", lambda out: picked)
    bt = bs.load(m5_out)
    assert bs.rule_mismatches(bt, bs.shipped()) == []
    assert "anomaly:LSR-01:scrap_above_baseline" in bt.sql_alerts[date(2026, 8, 20)]


def test_the_backtests_mornings_are_the_weekdays_after_the_first_day(m5_out):
    every = brief_backtest.mornings(m5_out)
    assert every[0] == date(2026, 7, 3) and every[1] == date(2026, 7, 6)
    assert every[-1] == date(2026, 10, 1)
    assert all(m.weekday() < 5 for m in every)


# --- Choosing a threshold ---------------------------------------------------------


def test_the_choice_keeps_the_find_then_drops_false_alerts_then_changes_least():
    nozzle_on = date(2026, 8, 14)
    table = pl.DataFrame(
        {
            "scrap_standard_errors": [2.0, 3.0, 3.5, 4.0, 5.0, 6.0],
            "baseline": ["as built"] * 6,
            # At 5 the trailing baseline splits the nozzle's alert in two: still
            # one scenario found, not two. At 6 it is found a day later.
            "scenario": [1, 1, 1, 1, 2, 1],
            "scenarios_found": [1, 1, 1, 1, 1, 1],
            "false": [4, 1, 0, 0, 0, 0],
            "first_nozzle_morning": [nozzle_on] * 5 + [nozzle_on + timedelta(days=1)],
        }
    )
    assert bs.choose("scrap", table, T) == {"scrap_standard_errors": 3.5}


def test_a_rule_with_two_settings_moves_the_fewest_steps():
    rows = [
        {"stop_minutes_factor": f, "stop_minutes_floor": m, "scenarios_found": 0,
         "false": 0 if (f, m) in {(2, 90), (4, 30), (3, 30)} else 2}
        for f in (1.5, 2, 3, 4) for m in (30, 60, 90, 120)
    ]  # fmt: skip
    found = bs.choose("stops", pl.DataFrame(rows), T)
    assert found == {"stop_minutes_factor": 2.0, "stop_minutes_floor": 90.0}


# --- Narration: the hand readings must match the logs -----------------------------


def narration_table(orphans: list[dict]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "morning": ["2026-08-14", "2026-08-17"],
            "version": ["template", "model"],
            "reason": [
                "the model's brief failed the number check (1 orphans)",
                "passed",
            ],
            "stop_reason": ["end_turn", "end_turn"],
            "input_tokens": [5000, 5000],
            "output_tokens": [2000, 2000],
            "ms": [15000, 15000],
            "tables": [0, 0],
            "alerts_left_out": ["[]", '["anomaly:PEM-01:stop_minutes_above_usual"]'],
            "to_read": [json.dumps(orphans), "[]"],
            "orphans": ['["1.9% (misattributed)"]', "[]"],
        }
    )


ORPHAN = {"text": "1.9%", "reason": "misattributed", "block": "BRAKE: 17 of 872, 1.9%."}


def test_a_reading_counts_the_brief_as_misleading_and_a_missing_one_is_listed():
    read = [{"model": "sonnet", "morning": "2026-08-14", "number": "1.9%",
             "verdict": "computed", "note": "17 of 872 as a rate"}]  # fmt: skip
    text = "\n".join(bs.narration_summary("sonnet", narration_table([ORPHAN]), read))
    assert "1 would have stated a wrong, worked-out, or invented number" in text
    assert "Orphans still to read by hand: none" in text
    assert "2026-08-17: anomaly:PEM-01:stop_minutes_above_usual" in text  # left out

    unread = "\n".join(bs.narration_summary("sonnet", narration_table([ORPHAN]), []))
    assert "Orphans still to read by hand: 1" in unread


def test_a_reading_with_no_orphan_in_the_logs_fails_the_score():
    read = [{"model": "sonnet", "morning": "2026-08-14", "number": "2.0%",
             "verdict": "wrong", "note": ""}]  # fmt: skip
    with pytest.raises(SystemExit, match="no such sonnet orphan"):
        bs.narration_summary("sonnet", narration_table([ORPHAN]), read)
