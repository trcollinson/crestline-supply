"""Athena's time zones, tested on Athena itself (spec §10, M3).

DuckDB and Athena read a timestamp with no zone differently: DuckDB takes a
bare `06:00 at time zone ...` as plant time, Athena as UTC. A view that gets
this wrong passes every DuckDB test and is off by hours in Athena, so these
tests send the plant's local timestamps through the real Athena views and
check the exact UTC instants that come back.

Opt-in, because it runs in AWS: `pytest -m athena` (plain `pytest` skips it).
It needs the stacks deployed, the backfill in ./out uploaded, and the
poller's first run curated (README). It replaces the views in the Glue
database from the SQL files in this checkout first, so it tests the SQL as
it is now, not as it was last set up. No release tag until it passes.
"""

from datetime import UTC, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import polars as pl
import pytest

from crestline import query
from crestline.athena import Athena

pytestmark = pytest.mark.athena

REPO_ROOT = Path(__file__).resolve().parent.parent
OUT = REPO_ROOT / "out"
PLANT_TZ = ZoneInfo("America/Denver")


@pytest.fixture(scope="module")
def athena() -> Athena:
    """Athena with the source views and margin_gap.sql's views as in this checkout."""
    athena = Athena.from_stack()
    athena.setup(REPO_ROOT / "queries")
    athena.run_file(REPO_ROOT / "queries" / "margin_gap.sql")
    return athena


@pytest.fixture(scope="module")
def duckdb():
    con = query.connect(OUT, REPO_ROOT / "queries")
    query.run(con, "margin-gap", REPO_ROOT / "queries").fetchall()
    return con


def athena_rows(athena: Athena, sql: str) -> pl.DataFrame:
    query_id, _ = athena.execute(sql)
    return athena.fetch(query_id)


def instant(text: str | None) -> datetime | None:
    """Athena's timestamp text ('2026-07-06 12:00:00.000') as a UTC instant."""
    if text is None:
        return None
    return datetime.fromisoformat(text).replace(tzinfo=UTC)


def utc_from_plant(local: datetime) -> datetime:
    """What a plant-local wall clock reading is in UTC, by Python's zone rules."""
    return local.replace(tzinfo=PLANT_TZ).astimezone(UTC)


def test_every_working_window_starts_and_ends_at_the_right_utc_instant(athena):
    windows = athena_rows(
        athena,
        "select starts_local, ends_local, starts_at_utc, ends_at_utc from work_windows",
    )
    assert windows.height > 400  # every day the MES recorded, 8 windows each
    for row in windows.iter_rows(named=True):
        for local, utc in (
            ("starts_local", "starts_at_utc"),
            ("ends_local", "ends_at_utc"),
        ):
            expected = utc_from_plant(datetime.fromisoformat(row[local]))
            assert instant(row[utc]) == expected, row


def test_a_known_shift_start_is_noon_utc_in_summer(athena):
    # 1st shift starts at 06:00 plant time; in July Denver is UTC-6.
    starts = athena_rows(
        athena,
        "select min(starts_at_utc) as first_start from work_windows"
        " where work_date = date '2026-07-06'",
    )
    assert instant(starts["first_start"][0]) == datetime(2026, 7, 6, 12, 0, tzinfo=UTC)


def test_every_scan_arrives_in_utc_exactly_as_duckdb_has_it(athena, duckdb):
    columns = "record_id, scan_on_utc, scan_off_utc, was_left_open"
    in_athena = athena_rows(athena, f"select {columns} from scans").sort("record_id")
    in_duckdb = duckdb.sql(f"select {columns} from scans order by record_id").pl()
    assert in_athena.height == in_duckdb.height > 5_000
    assert in_athena["record_id"].to_list() == in_duckdb["record_id"].to_list()
    for column in ("scan_on_utc", "scan_off_utc"):
        assert [instant(t) for t in in_athena[column]] == [
            t.replace(tzinfo=UTC) for t in in_duckdb[column]
        ], column
    # Forgotten scans are the ones whose times were rewritten in SQL.
    assert in_athena["was_left_open"].to_list() == in_duckdb["was_left_open"].to_list()
    assert in_athena["was_left_open"].sum() > 0


def test_every_scan_on_is_its_mes_time_converted_by_the_zone_rules(athena, duckdb):
    """Not just the engines agreeing: both against Python's zoneinfo."""
    local = dict(
        duckdb.sql("select record_id, scan_on_local from mes_labor").fetchall()
    )
    scans = athena_rows(athena, "select record_id, scan_on_utc from scans")
    for record_id, scan_on in scans.iter_rows():
        assert instant(scan_on) == utc_from_plant(local[record_id]), record_id


def test_machine_and_erp_timestamps_match_duckdb(athena, duckdb):
    """UTC in, UTC out: the same instants on both engines, counted and summed.

    Summed in whole milliseconds, as integers: a sum of 90 days of epoch
    seconds as doubles depends on the order it adds them in.
    """
    checks = {
        "pb01_data": "device_ts_utc",
        "pb02_counts": "gateway_ts_utc",
        "erp_jobs": "lastmodified_utc",
        "erp_job_operations": "lastmodified_utc",
        "erp_job_costs": "lastmodified_utc",
    }
    for view, column in checks.items():
        in_athena = athena_rows(
            athena,
            f"select count(*) as n, cast(min({column}) as varchar) as first,"
            f" cast(max({column}) as varchar) as last,"
            f" sum(cast(round(to_unixtime({column}) * 1000) as bigint)) as ms from {view}",
        ).row(0)
        n, first, last, ms = duckdb.sql(
            f"select count(*), min({column}), max({column}),"
            f" sum(epoch_ms({column})) from {view}"
        ).fetchone()
        assert in_athena[0] == n > 0, view
        assert (instant(in_athena[1]), instant(in_athena[2])) == (
            first.replace(tzinfo=UTC),
            last.replace(tzinfo=UTC),
        ), view
        assert in_athena[3] == ms, view
