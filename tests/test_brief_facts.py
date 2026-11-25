"""The morning brief's facts (M5 step 3): no look-ahead, and each family works.

The look-ahead test compares two ways of seeing the plant on a morning: the
brief's own cut (queries/as_of.sql, over the whole backfill) and a copy of
the backfill holding only the files that had arrived by 05:30, with the ERP
rebuilt as it stood then from its history (eval/backfill_as_of.py). The
facts must be the same, cell for cell.

Then a planted case per family, on a copy as of one morning: scans taken
away, scrap typed in, a conveyor stopped, a log row deleted. And the two
stories M5's backfill holds: the nozzle and PB-02's second shift.
"""

import csv
import re
import shutil
from datetime import date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from backfill_as_of import cut_backfill
from crestline.brief import facts
from look_ahead_leak import leaked_facts

REPO_ROOT = Path(__file__).resolve().parent.parent
PLANT_TZ = ZoneInfo("America/Denver")

MORNINGS = [
    date(2026, 7, 13),  # a Monday: Friday's scans left open close this morning
    date(2026, 8, 13),  # the morning after the nozzle's ramp starts (Wed Aug 12)
    date(2026, 8, 17),  # a Monday, mid-ramp
    date(2026, 8, 24),  # the Monday the plant notices and changes the nozzle
    date(2026, 10, 1),  # the extract's own morning: the whole backfill
]
# The planted cases' morning, a Wednesday before the ramp; its plant day.
PLANTED_MORNING = date(2026, 7, 29)
PLANT_DAY = date(2026, 7, 28)


# m5_out, M5's 90-day backfill with the nozzle on, is in conftest.py.


@pytest.fixture(scope="module")
def full(m5_out):
    """The brief's layer over the whole backfill; tests move its morning."""
    with pytest.MonkeyPatch.context() as mp:
        mp.chdir(REPO_ROOT)
        return facts.connect(m5_out, MORNINGS[-1])


def facts_for(con, morning: date) -> pl.DataFrame:
    with pytest.MonkeyPatch.context() as mp:
        mp.chdir(REPO_ROOT)
        return facts.facts(con, morning)


def facts_over(out: Path, morning: date) -> pl.DataFrame:
    with pytest.MonkeyPatch.context() as mp:
        mp.chdir(REPO_ROOT)
        return facts.facts(facts.connect(out, morning))


def by_id(frame: pl.DataFrame) -> dict[str, float]:
    return dict(frame.select("fact_id", "value").iter_rows())


# --- No look-ahead ----------------------------------------------------------------


@pytest.mark.parametrize("morning", MORNINGS, ids=str)
def test_facts_see_only_what_had_arrived(full, m5_out, tmp_path, morning):
    cut_backfill(m5_out, tmp_path, morning)
    as_it_stood = facts_over(tmp_path, morning)
    assert set(as_it_stood["family"]) == {
        "output",
        "margin",
        "stops",
        "scrap",
        "anomaly",
    }
    assert as_it_stood["fact_id"].is_unique().all()
    assert as_it_stood["morning"].unique().to_list() == [morning]
    assert_frame_equal(facts_for(full, morning), as_it_stood)


def test_mondays_brief_is_about_friday(full):
    found = facts_for(full, date(2026, 8, 24))
    assert found["plant_day"].unique().to_list() == [date(2026, 8, 21)]


def test_reading_past_the_morning_changes_the_facts(m5_out, tmp_path):
    """The check above can fail. Let in the scans opened before Monday's
    morning that closed after it (eval/look_ahead_leak.py): the four Friday's
    shifts left open, closed at 06:00 Monday, landing in Tuesday's file. The
    plant day stays Friday, Friday's output grows, and no alert changes, so
    nothing in the brief would look wrong."""
    monday = date(2026, 8, 17)
    cut_backfill(m5_out, tmp_path / "as-of", monday)
    as_it_stood = facts_over(tmp_path / "as-of", monday)

    with pytest.MonkeyPatch.context() as mp:
        mp.chdir(REPO_ROOT)
        honest, leaked, scans = leaked_facts(m5_out, monday)

    assert_frame_equal(honest, as_it_stood)
    assert scans.height == 4
    assert leaked["plant_day"].unique().to_list() == [date(2026, 8, 14)]
    before, after = by_id(as_it_stood), by_id(leaked)
    changed = {
        key for key in before.keys() | after.keys() if before.get(key) != after.get(key)
    }
    assert {
        "output:PEM:pieces",
        "output:BRAKE:pieces",
        "output:POWDER:pieces",
    } <= changed
    assert after["output:PEM:pieces"] - before["output:PEM:pieces"] == 77
    assert not {key for key in changed if key.startswith("anomaly:")}


def test_six_weeks_of_machine_data_is_as_good_as_all_of_it(full, m5_out):
    """The layer reads the machines from six weeks before the morning
    (as_of.sql's machines_from_utc). On the window's last morning, where
    that cuts the most, the facts are the same as from the whole history."""
    morning = MORNINGS[-1]
    with pytest.MonkeyPatch.context() as mp:
        mp.chdir(REPO_ROOT)
        whole = facts.connect(m5_out, morning)
        for view in MACHINE_SOURCES:
            whole.execute(
                f"create or replace view {view} as select * from crestline_raw.{view}"
                " where published_at_utc < (select cut_utc from brief_cut)"
            )
        from_the_start = facts.facts(whole)
    assert_frame_equal(facts_for(full, morning), from_the_start)


MACHINE_SOURCES = [
    "pb01_data",
    "pb02_counts",
    "pem01_counts",
    "pcline_process",
    "lsr01_messages",
    "lsr01_metrics",
]


# --- A planted case per family ----------------------------------------------------


@pytest.fixture(scope="module")
def as_of_planted_morning(m5_out, tmp_path_factory) -> Path:
    base = tmp_path_factory.mktemp("as-of")
    cut_backfill(m5_out, base, PLANTED_MORNING)
    return base


@pytest.fixture(scope="module")
def unplanted(as_of_planted_morning) -> pl.DataFrame:
    return facts_over(as_of_planted_morning, PLANTED_MORNING)


def planted_copy(base: Path, target: Path, *edited: str) -> Path:
    """`base`, linked file by file, except `edited` (relative paths), copied
    so a test can change them."""
    for path in base.rglob("*"):
        if path.is_dir() or path.parts[len(base.parts)] == "decoded":
            continue
        relative = path.relative_to(base)
        (target / relative).parent.mkdir(parents=True, exist_ok=True)
        if str(relative) in edited:
            shutil.copy2(path, target / relative)
        else:
            (target / relative).symlink_to(path)
    return target


def rewrite_labor(path: Path, change) -> None:
    """Apply `change(row) -> row | None` to every scan in a labor file."""
    with path.open(newline="") as f:
        rows = list(csv.DictReader(f))
    kept = [row for row in (change(dict(row)) for row in rows) if row is not None]
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]), lineterminator="\n")
        writer.writeheader()
        writer.writerows(kept)


def opened_on_plant_day(row: dict) -> bool:
    return row["scan_on"].startswith(PLANT_DAY.isoformat())


LABOR_FILE = f"mes/outbound/labor_{PLANT_DAY:%Y%m%d}.csv"
DOWNTIME_FILE = f"mes/outbound/downtime_{PLANT_DAY:%Y%m%d}.csv"


def test_output_finds_a_work_center_that_made_nothing(
    as_of_planted_morning, unplanted, tmp_path
):
    """Take away every laser scan from the plant day: the laser made 0
    pieces, and the output rule says so."""
    planted = planted_copy(as_of_planted_morning, tmp_path, LABOR_FILE)
    rewrite_labor(
        planted / LABOR_FILE,
        lambda row: (
            None if row["work_center"] == "LASER" and opened_on_plant_day(row) else row
        ),
    )
    before, after = by_id(unplanted), by_id(facts_over(planted, PLANTED_MORNING))

    assert before["output:LASER:pieces"] > 0
    assert "anomaly:LASER:output_below_usual" not in before
    assert after["output:LASER:pieces"] == 0
    assert after["anomaly:LASER:output_below_usual"] == 0
    assert after["output:BRAKE:pieces"] == before["output:BRAKE:pieces"]


def test_jobs_finished_are_the_ones_the_answer_key_finished(
    m5_out, as_of_planted_morning, unplanted
):
    """Every job the answer key finished on the plant day, except one whose
    last scan was left open: it closes this morning, and arrives tomorrow."""
    truth = pl.read_parquet(m5_out / "truth" / "truth_jobs.parquet")
    finished = set(
        truth.filter(
            pl.col("completed_at").dt.convert_time_zone("America/Denver").dt.date()
            == PLANT_DAY
        )["job_num"]
    )
    with pytest.MonkeyPatch.context() as mp:
        mp.chdir(REPO_ROOT)
        con = facts.connect(as_of_planted_morning, PLANTED_MORNING)
        found = {
            job
            for (job,) in con.sql(
                "select job_num from brief_jobs_finished where finished_on = ?",
                params=[PLANT_DAY],
            ).fetchall()
        }
    left_open = set()
    with (
        m5_out / "mes" / "outbound" / f"labor_{PLANTED_MORNING:%Y%m%d}.csv"
    ).open() as f:
        for row in csv.DictReader(f):
            if opened_on_plant_day(row):
                left_open.add(row["job_num"])

    assert finished
    assert found == finished - left_open
    assert by_id(unplanted)["output:plant:jobs_finished"] == len(found)


def test_scrap_finds_a_machine_scrapping_more_than_usual(
    as_of_planted_morning, unplanted, tmp_path
):
    """Type 25 more scrapped pieces into PEM-01's biggest scan of the day."""
    with (as_of_planted_morning / LABOR_FILE).open() as f:
        biggest = max(
            (
                row
                for row in csv.DictReader(f)
                if row["work_center"] == "PEM" and opened_on_plant_day(row)
            ),
            key=lambda row: int(row["qty_good"]),
        )
    assert int(biggest["qty_good"]) >= 25

    def scrap_25(row: dict) -> dict:
        if row["record_id"] == biggest["record_id"]:
            row["qty_good"] = str(int(row["qty_good"]) - 25)
            row["qty_scrap"] = str(int(row["qty_scrap"]) + 25)
        return row

    planted = planted_copy(as_of_planted_morning, tmp_path, LABOR_FILE)
    rewrite_labor(planted / LABOR_FILE, scrap_25)
    before, after = by_id(unplanted), by_id(facts_over(planted, PLANTED_MORNING))

    assert "anomaly:PEM-01:scrap_above_baseline" not in before
    assert after["scrap:PEM-01:scrapped"] == before["scrap:PEM-01:scrapped"] + 25
    assert after["scrap:PEM-01:pieces"] == before["scrap:PEM-01:pieces"]
    assert after["anomaly:PEM-01:scrap_above_baseline"] > 3


def test_stops_find_a_long_stop_nobody_wrote_down(
    as_of_planted_morning, unplanted, tmp_path
):
    """Stop the powder line's conveyor from 10:40 to 12:20 on the plant day
    (inside one working window, between the meal and the afternoon break),
    with nothing in the operators' log: 100 minutes, over the 90-minute
    floor the backtest picked (M5 step 5)."""
    pcline = "machines/pcline.jsonl"
    planted = planted_copy(as_of_planted_morning, tmp_path, pcline)
    start = datetime.combine(PLANT_DAY, time(10, 40), tzinfo=PLANT_TZ)
    end = start + timedelta(minutes=100)
    published_at = re.compile(r'"published_at":"([^"]+)"')
    lines = (planted / pcline).read_text().splitlines(keepends=True)
    with (planted / pcline).open("w") as f:
        for line in lines:
            moment = datetime.fromisoformat(published_at.search(line)[1])
            if start <= moment < end:
                line = re.sub(r'"spd":[0-9.]+', '"spd":0.0', line)
            f.write(line)
    before, after = by_id(unplanted), by_id(facts_over(planted, PLANTED_MORNING))

    assert "anomaly:PC-LINE:stop_minutes_above_usual" not in before
    assert after["stops:PC-LINE:stops"] == before["stops:PC-LINE:stops"] + 1
    assert (
        after["stops:PC-LINE:stop_minutes"] >= before["stops:PC-LINE:stop_minutes"] + 99
    )
    assert (
        after["stops:PC-LINE:stops_not_logged"]
        == before["stops:PC-LINE:stops_not_logged"] + 1
    )
    assert after["anomaly:PC-LINE:stop_minutes_above_usual"] >= 100


def test_stops_count_a_deleted_log_row_as_not_logged(
    as_of_planted_morning, unplanted, tmp_path
):
    """Delete every row the operators wrote for the plant day: each stop the
    machines found is now one nobody logged."""
    planted = planted_copy(as_of_planted_morning, tmp_path, DOWNTIME_FILE)
    header = (planted / DOWNTIME_FILE).read_text().splitlines(keepends=True)[0]
    (planted / DOWNTIME_FILE).write_text(header)
    before, after = by_id(unplanted), by_id(facts_over(planted, PLANTED_MORNING))

    machines = ["LSR-01", "PB-01", "PB-02", "PEM-01", "PC-LINE"]
    assert sum(before[f"stops:{m}:logged_stops"] for m in machines) > 0
    for machine in machines:
        assert after[f"stops:{machine}:logged_stops"] == 0
        assert after[f"stops:{machine}:stops"] == before[f"stops:{machine}:stops"]
        assert (
            after[f"stops:{machine}:stops_not_logged"]
            == after[f"stops:{machine}:stops"]
        )


@pytest.fixture(scope="module")
def mid_ramp(full) -> dict[str, float]:
    """Thursday Aug 20's facts: the nozzle at its worst, PB-02 as always."""
    return by_id(facts_for(full, date(2026, 8, 20)))


def test_margin_finds_pb02_second_shift(mid_ramp):
    """The simulator's planted case: PB-02's 2nd-shift setups take 2.8 times
    standard, and the ERP costs them at standard."""
    assert mid_ramp["margin:PB-02 2nd:setup_vs_standard"] > 2.5
    assert mid_ramp["anomaly:PB-02 2nd:erp_margin_above_rebuilt"] > 3
    for other in ("PB-01 1st", "PB-01 2nd", "PB-02 1st"):
        assert f"anomaly:{other}:erp_margin_above_rebuilt" not in mid_ramp
        assert mid_ramp[f"margin:{other}:setup_vs_standard"] < 1.3


def test_scrap_finds_the_nozzle_once_it_wears(full, mid_ramp):
    """The other planted case: the laser's scrap climbs from Wed Aug 12."""
    before_the_ramp = by_id(facts_for(full, date(2026, 8, 12)))

    assert "anomaly:LSR-01:scrap_above_baseline" not in before_the_ramp
    assert mid_ramp["scrap:LSR-01:scrap_pct"] > 8
    assert mid_ramp["anomaly:LSR-01:scrap_above_baseline"] > 3
