"""The data-shape checks (pipelines/checks): each passes on good data and trips on a planted breach."""

import json

import pytest

from checks import shape
from checks.aws import ChecksFailed, run_checks

JOBS = next(s for s in shape.ERP_SHAPES if s.table == "erp_jobs")


class Script:
    """A fake Athena: answers each statement by the first pattern it contains."""

    def __init__(self, answers: dict[str, list[list[str]]]) -> None:
        self.answers = answers
        self.statements: list[str] = []

    def __call__(self, sql: str) -> list[list[str]]:
        self.statements.append(sql)
        for pattern, rows in self.answers.items():
            if pattern in sql:
                return rows
        raise AssertionError(f"unexpected statement: {sql}")


class FakeState:
    def __init__(self) -> None:
        self.items: dict[str, dict] = {}

    def get_item(self, Key):
        return {"Item": self.items.get(Key["entity"], {})}

    def update_item(self, Key, UpdateExpression, ExpressionAttributeValues):
        self.items.setdefault(Key["entity"], {})["baselines"] = (
            ExpressionAttributeValues[":b"]
        )


def nulls(rows: int, *counts: int) -> list[list[str]]:
    return [[str(rows), *(str(c) for c in counts)]]


def test_the_first_run_records_a_baseline_and_breaches_nothing():
    breaches, baseline = shape.check_null_rates(JOBS, nulls(100, 0, 0, 5, 0)[0], {})
    assert breaches == []
    assert baseline["prodqty"] == [0.05]


def test_a_null_rate_jump_over_the_trailing_maximum_breaches():
    past = {c: [0.0, 0.02] for c in JOBS.key_columns}
    quiet, _ = shape.check_null_rates(JOBS, nulls(100, 0, 0, 3, 0)[0], past)
    assert quiet == []  # 3% is within a point of the past 2%
    [breach], updated = shape.check_null_rates(JOBS, nulls(100, 0, 0, 40, 0)[0], past)
    assert breach.check == "null rate" and "prodqty is null in 40.0%" in breach.detail
    assert updated["prodqty"] == [0.0, 0.02, 0.4]


def test_the_baseline_keeps_the_last_seven_runs():
    past = {c: [0.0] * 7 for c in JOBS.key_columns}
    _, updated = shape.check_null_rates(JOBS, nulls(10, 0, 0, 0, 0)[0], past)
    assert all(len(v) == shape.BASELINE_RUNS for v in updated.values())


def test_a_key_held_twice_breaches():
    run = Script({"count_if": nulls(10, 0, 0, 0, 0), "having count(*) > 1": [["2"]]})
    [breach], _ = shape.check_table(run, JOBS, {})
    assert breach.check == "primary key" and "2 keys" in breach.detail
    assert "group by jobnum having count(*) > 1" in run.statements[1]


def test_labor_that_does_not_cost_out_breaches():
    assert shape.check_erp_labor(Script({"laborrate": [["1309", "0", "0.0"]]})) == []
    [breach] = shape.check_erp_labor(Script({"laborrate": [["1309", "3", "41.5"]]}))
    assert "3 of 1309 jobs" in breach.detail and "$41.50" in breach.detail


def test_brake_strokes_that_drift_from_mes_pieces_breach():
    ok = Script({"machine_counts": [["64", "0", "0"]]})
    assert shape.check_brake_strokes(ok, "crestline_raw") == []
    assert "cast(scan_on_local as date)" in ok.statements[0]
    [breach] = shape.check_brake_strokes(
        Script({"machine_counts": [["64", "1", "57"]]}), "x"
    )
    assert "on 1 of 64 days" in breach.detail


def test_a_breach_fails_the_run_and_the_baseline_is_still_saved():
    state = FakeState()
    good = Script({"count_if": nulls(10, 0, 0, 0, 0), "having count(*) > 1": [["0"]]})
    run_checks(good, state, [JOBS], [])
    bad = Script({"count_if": nulls(10, 0, 0, 0, 0), "having count(*) > 1": [["1"]]})
    with pytest.raises(ChecksFailed, match="erp_jobs: primary key"):
        run_checks(bad, state, [JOBS], [])
    saved = json.loads(state.items["_checks"]["baselines"])
    assert saved["erp_jobs"]["prodqty"] == [0.0, 0.0]


def test_every_checked_column_exists_in_its_table():
    from erp_extract.curated import COLUMNS, TABLES, column_name
    from machine_curate import tables as machine

    by_table = {
        TABLES[e]: {column_name(f) for f, _ in cols} for e, cols in COLUMNS.items()
    } | {
        machine.TABLES[d]: {n for n, _, _ in cols}
        for d, cols in machine.COLUMNS.items()
    }
    for s in shape.ERP_SHAPES + shape.MACHINE_SHAPES:
        assert set(s.keys) | set(s.key_columns) <= by_table[s.table], s.table
