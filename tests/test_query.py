import contextlib
import io
import re
from datetime import UTC, datetime, time
from pathlib import Path
from zoneinfo import ZoneInfo

import duckdb
import polars as pl
import pytest

from crestline import query
from crestline.cli import main
from crestline.sim.config import PlantConfig

REPO_ROOT = Path(__file__).resolve().parent.parent
PLANT_TZ = ZoneInfo("America/Denver")
UNGROUPED = {"no brake op", "unplaced"}


@pytest.fixture(scope="module")
def out(tmp_path_factory) -> Path:
    """Four weeks of plant history: enough PB-02 2nd-shift jobs to see the gap."""
    out = tmp_path_factory.mktemp("out")
    with pytest.MonkeyPatch.context() as mp, contextlib.redirect_stdout(io.StringIO()):
        mp.chdir(REPO_ROOT)
        main(
            [
                "sim",
                "backfill",
                "--days",
                "28",
                "--end",
                "2026-08-03",
                "--out",
                str(out),
            ]
        )
    return out


@pytest.fixture(scope="module")
def con(out) -> duckdb.DuckDBPyConnection:
    """The sources and every view in margin_gap.sql, plus its answer as a table."""
    with pytest.MonkeyPatch.context() as mp:
        mp.chdir(REPO_ROOT)
        con = query.connect(out)
        query.run(con, "margin-gap").to_table("margin_gap")
    return con


def by_group(con) -> dict[tuple[str, str], dict]:
    rows = con.sql("select * from margin_gap").pl().iter_rows(named=True)
    return {(row["brake"], row["shift"]): row for row in rows}


def test_pb02_second_shift_stands_out(con):
    groups = by_group(con)
    pb02_2nd = groups.pop(("PB-02", "2nd"))

    assert {("PB-01", "1st"), ("PB-01", "2nd"), ("PB-02", "1st")} <= set(groups)
    others = [g for key, g in groups.items() if key[0] not in UNGROUPED]
    # The ERP flatters PB-02's 2nd shift, and only PB-02's 2nd shift...
    assert (
        pb02_2nd["erp_overstates_pts"]
        >= max(g["erp_overstates_pts"] for g in others) + 3
    )
    # ...because its setups run long.
    assert pb02_2nd["setup_vs_standard"] > 2
    assert all(g["setup_vs_standard"] < 1.5 for g in others)


def test_brake_operations_land_on_the_right_brake(con, out):
    truth = pl.read_parquet(out / "truth" / "truth_operations.parquet")
    setup_shift = (
        pl.read_parquet(out / "truth" / "truth_machine_states.parquet")
        .filter(pl.col("state") == "SETUP")
        .sort("started_at")
        .group_by("job_num", "op_seq")
        .agg(pl.col("shift").first().alias("true_shift"))
    )
    placed = (
        con.sql("select * from brake_operations")
        .pl()
        .join(truth, on=["job_num", "op_seq"])
        .join(setup_shift, on=["job_num", "op_seq"])
    )

    assert placed.height > 100
    assert (placed["brake"] == placed["machine_id"]).mean() >= 0.99
    assert (placed["setup_shift"] == placed["true_shift"]).mean() >= 0.99


def test_forgotten_scan_offs_are_cut_back(con):
    scans = con.sql(
        """
        select *,
               lead(scan_on) over (partition by employee_id order by scan_on) as next_scan_on
        from scans
        """
    ).pl()

    assert scans["was_left_open"].sum() > 0
    shift_end = {"1st": "14:30:00", "2nd": "23:00:00"}

    def local(utc: datetime) -> datetime:
        return utc.replace(tzinfo=UTC).astimezone(PLANT_TZ)

    for scan in scans.iter_rows(named=True):
        scan_on, scan_off = local(scan["scan_on"]), local(scan["scan_off"])
        assert str(scan_off.time()) <= shift_end[scan["shift"]]
        assert scan_off.date() == scan_on.date() == scan["work_date"]
        if scan["next_scan_on"] is not None:
            assert scan["scan_off"] <= scan["next_scan_on"]


def test_work_windows_match_the_plant_schedule(con):
    """The SQL's copy of the shift schedule agrees with the simulator's."""

    def minutes(clock: time) -> int:
        return clock.hour * 60 + clock.minute

    expected = []  # (shift, start, end) in minutes after midnight
    for shift in PlantConfig().calendar.shifts:
        cursor = minutes(shift.start)
        for brk in shift.breaks:
            expected.append((shift.name, cursor, minutes(brk.start)))
            cursor = minutes(brk.start) + brk.minutes
        expected.append((shift.name, cursor, minutes(shift.end)))

    windows = con.sql(
        "select shift, start_time, end_time from plant_work_windows order by 1, 2"
    ).fetchall()
    assert [
        (shift, minutes(time.fromisoformat(start)), minutes(time.fromisoformat(end)))
        for shift, start, end in windows
    ] == expected


def test_both_engines_define_the_same_sources():
    """margin_gap.sql runs on either engine, so both must offer the same names."""

    def views(path: Path) -> set[str]:
        sql = (REPO_ROOT / path).read_text(encoding="utf-8")
        return set(re.findall(r"create or replace view (\w+)", sql))

    duckdb_sources = views(Path("queries/sources.sql"))
    assert "mes_labor" in duckdb_sources
    assert views(Path("queries/athena/sources.sql")) == duckdb_sources


def test_cli_prints_the_margin_gap(out, capsys):
    with pytest.MonkeyPatch.context() as mp:
        mp.chdir(REPO_ROOT)
        main(["query", "margin-gap", "--out", str(out)])

    printed = capsys.readouterr().out
    assert "PB-02" in printed
    assert "erp_overstates_pts" in printed


def test_revenue_is_the_order_not_the_overs(con):
    rows = con.sql(
        """
        select m.revenue, j.CompletedQty, l.OrderQty, l.UnitPrice
        from job_margins as m
        join erp_jobs as j on j.JobNum = m.job_num
        join erp_sales_order_lines as l
          on l.OrderNum = j.OrderNum and l.OrderLine = j.OrderLine
        """
    ).pl()

    assert (rows["revenue"] <= rows["OrderQty"] * rows["UnitPrice"] + 1e-6).all()
    assert (rows["CompletedQty"] > rows["OrderQty"]).any()  # overs go to stock...
    assert (rows["CompletedQty"] < rows["OrderQty"]).any()  # ...and shorts ship short
