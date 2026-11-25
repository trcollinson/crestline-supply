"""The data-shape checks (pipelines/checks): each passes on good data and trips on a planted breach."""

import json
from pathlib import Path

import duckdb
import pytest

from checks import shape
from checks.aws import ChecksFailed, run_checks
from crestline.athena import split_statements

REPO_ROOT = Path(__file__).resolve().parent.parent

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


def brake_check_on(strokes: dict[str, int], pieces: dict[str, int]) -> tuple:
    """brake_strokes_sql on DuckDB over planted rows: strokes and pieces per
    plant day (noon, plant time)."""
    con = duckdb.connect()
    sources = (REPO_ROOT / "queries" / "sources.sql").read_text()
    [macro] = [s for s in split_statements(sources) if "macro with_timezone" in s]
    con.execute(macro)
    con.execute(
        "create table machine_counts as select * from (values ('PB-01', timestamp '2000-01-01', 0))"
        " as t (machine_id, counted_at_utc, cycles) where false"
    )
    con.execute(
        "create table mes_labor as select * from (values ('BRAKE', timestamp '2000-01-01', 0, 0))"
        " as t (work_center, scan_on_local, qty_good, qty_scrap) where false"
    )
    for day, count in strokes.items():
        # 18:00 UTC is noon in Denver (daylight time).
        con.execute(
            "insert into machine_counts values ('PB-02', cast(? as timestamp) + interval 18 hour, ?)",
            [day, count],
        )
    for day, count in pieces.items():
        con.execute(
            "insert into mes_labor values ('BRAKE', cast(? as timestamp) + interval 7 hour, ?, 0)",
            [day, count],
        )
    return con.sql(shape.brake_strokes_sql("main")).fetchone()


def test_the_newest_day_of_scans_is_not_checked():
    """Its scans left open arrive tomorrow, its strokes already have (M5 step 2)."""
    days, off, _worst = brake_check_on(
        strokes={"2026-09-29": 400, "2026-09-30": 392},
        pieces={"2026-09-29": 400, "2026-09-30": 361},
    )
    assert (days, off) == (1, 0)


def test_an_earlier_day_off_still_breaches():
    days, off, worst = brake_check_on(
        strokes={"2026-09-28": 400, "2026-09-29": 400, "2026-09-30": 392},
        pieces={"2026-09-28": 350, "2026-09-29": 400, "2026-09-30": 361},
    )
    assert (days, off, worst) == (2, 1, 50)


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
