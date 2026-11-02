import contextlib
import io
import re
from datetime import UTC, datetime, time, timedelta
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
    assert pb02_2nd["gap_pts"] >= max(g["gap_pts"] for g in others) + 3
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
               lead(scan_on_utc) over (partition by employee_id order by scan_on_utc) as next_scan_on_utc
        from scans
        """
    ).pl()

    assert scans["was_left_open"].sum() > 0
    shift_end = {"1st": "14:30:00", "2nd": "23:00:00"}

    def local(utc: datetime) -> datetime:
        return utc.replace(tzinfo=UTC).astimezone(PLANT_TZ)

    for scan in scans.iter_rows(named=True):
        scan_on, scan_off = local(scan["scan_on_utc"]), local(scan["scan_off_utc"])
        assert str(scan_off.time()) <= shift_end[scan["shift"]]
        assert scan_off.date() == scan_on.date() == scan["work_date"]
        if scan["next_scan_on_utc"] is not None:
            assert scan["scan_off_utc"] <= scan["next_scan_on_utc"]


def test_every_timestamp_column_says_its_clock(con):
    """Spec §10, M3: a timestamp has no zone in either engine, so its name says it."""
    columns = con.sql(
        """
        select table_name, column_name, data_type
        from information_schema.columns
        """
    ).fetchall()
    looks_like_a_time = re.compile(r"(^ts$|_ts$|_at$|time)")
    times = [
        (table, column, kind)
        for table, column, kind in columns
        if kind.startswith("TIMESTAMP") or looks_like_a_time.search(column)
    ]
    assert len(times) > 10  # the check sees the views
    unnamed = [t for t in times if not t[1].endswith(("_utc", "_local"))]
    assert unnamed == []


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
        "select shift, start_time_local, end_time_local from plant_work_windows order by 1, 2"
    ).fetchall()
    assert [
        (shift, minutes(time.fromisoformat(start)), minutes(time.fromisoformat(end)))
        for shift, start, end in windows
    ] == expected


def test_both_engines_define_the_same_sources():
    """The query files run on either engine, so both must offer the same names."""

    def views(path: Path) -> set[str]:
        sql = (REPO_ROOT / path).read_text(encoding="utf-8")
        return set(re.findall(r"create or replace view (\w+)", sql))

    duckdb_sources = views(Path("queries/sources.sql"))
    assert "mes_labor" in duckdb_sources
    assert views(Path("queries/athena/sources.sql")) == duckdb_sources


def test_rebuilt_cycles_add_up_to_every_piece_each_machine_made(con, out):
    """Every message once, counters unwrapped: the totals are exact."""
    truth = (
        pl.read_parquet(out / "truth" / "truth_cycles.parquet")
        .group_by("machine_id")
        .len()
    )
    rebuilt = con.sql(
        "select machine_id, sum(cycles) as cycles from machine_cycles_by_day group by 1"
    ).pl()
    both = truth.join(rebuilt, on="machine_id")
    assert set(both["machine_id"]) == {"LSR-01", "PB-01", "PB-02", "PEM-01", "PC-LINE"}
    assert (both["len"] == both["cycles"]).all()


def test_the_backfill_holds_rollovers_and_resets_and_both_are_told_apart(con):
    counted_as = dict(
        con.sql(
            "select counted_as, count(*) from machine_counts where machine_id in ('PB-02', 'PEM-01') group by 1"
        ).fetchall()
    )
    assert counted_as["rollover"] >= 1
    assert counted_as["reset"] >= 1


@pytest.fixture(scope="module")
def downtime(con) -> pl.DataFrame:
    with pytest.MonkeyPatch.context() as mp:
        mp.chdir(REPO_ROOT)
        query.run(con, "downtime")
    return con.sql("select * from machine_downtime").pl()


def test_downtime_is_only_ever_inside_working_windows(con, downtime):
    # Every second of every stop is working time. (A stop can run from one
    # window into the next: 1st shift ends at 14:30, when 2nd starts.)
    windows = con.sql("select starts_at_utc, ends_at_utc from work_windows").fetchall()
    for stop in downtime.iter_rows(named=True):
        start, end = stop["started_at_utc"], stop["ended_at_utc"]
        assert start < end
        working = sum(
            (
                max(timedelta(0), min(end, w_end) - max(start, w_start))
                for w_start, w_end in windows
            ),
            timedelta(0),
        )
        assert working == end - start


def test_downtime_stops_never_overlap_on_one_machine(downtime):
    for _, stops in downtime.sort("started_at_utc").group_by("machine_id"):
        starts, ends = stops["started_at_utc"], stops["ended_at_utc"]
        assert (starts[1:] >= ends[:-1]).all()


def test_each_generation_finds_downtime_its_own_way(downtime):
    found = {
        (machine, how)
        for machine, how in downtime.select("machine_id", "found_by").iter_rows()
    }
    assert {m for m, _ in found} == {"LSR-01", "PB-01", "PB-02", "PEM-01", "PC-LINE"}
    # The laser and the powder line say when they stop; the legacy presses
    # only stop counting.
    assert {how for m, how in found if m in ("LSR-01", "PC-LINE")} == {"state"}
    assert {how for m, how in found if m in ("PB-02", "PEM-01")} == {"stroke gap"}
    assert "stroke gap" in {how for m, how in found if m == "PB-01"}


def test_cli_prints_the_downtime(out, capsys):
    with pytest.MonkeyPatch.context() as mp:
        mp.chdir(REPO_ROOT)
        main(["query", "downtime", "--out", str(out)])

    printed = capsys.readouterr().out
    assert "stroke gap" in printed
    assert "PEM-01" in printed


def test_cli_prints_the_margin_gap(out, capsys):
    with pytest.MonkeyPatch.context() as mp:
        mp.chdir(REPO_ROOT)
        main(["query", "margin-gap", "--out", str(out)])

    printed = capsys.readouterr().out
    assert "PB-02" in printed
    assert "gap_pts" in printed


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
