"""The hour that happens twice: plant-local times across the clock changes.

The MES writes local time with no offset (spec §4.2), so a few readings
each year are tricky: on the night the clocks fall back, 01:00 to 02:00
happens twice, and on the night they spring forward, 02:00 to 03:00 never
happens. The fixture (tests/fixtures/dst/) plants scans at those edges and
runs them through the same views the margin query uses, on DuckDB here and
on Athena under `pytest -m athena`. Both must give the same UTC instants,
and those instants must be the ones written below.

The rule for an ambiguous time (spec §4.2): it's the earlier occurrence.
"""

import dataclasses
import re
from datetime import UTC, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import boto3
import duckdb
import pytest

from crestline.athena import Athena, render, split_statements
from crestline.stack import foundation_outputs

REPO_ROOT = Path(__file__).resolve().parent.parent
QUERIES = REPO_ROOT / "queries"
FIXTURE = Path(__file__).resolve().parent / "fixtures" / "dst"
PLANT_TZ = ZoneInfo("America/Denver")

# The views the fixture feeds: the MES source and margin_gap.sql's views
# that read nothing else.
VIEWS = ["mes_labor", "plant_work_windows", "work_windows", "scans", "work_segments"]


def utc(text: str) -> datetime:
    return datetime.fromisoformat(text).replace(tzinfo=UTC)


# Every fixture scan in UTC, as Crestline's rule reads it, and how long it
# really ran. `naive_hours` is what subtracting the local times gives.
SCANS = {
    # A normal Thursday 2nd-shift scan: daylight time, UTC-6.
    9900001: ("2026-10-29 20:35", "2026-10-29 22:10", 1 + 35 / 60, 1 + 35 / 60),
    # Left open Thursday night, closed 06:00 Friday: crosses midnight.
    9900002: ("2026-10-30 03:40", "2026-10-30 12:00", 8 + 20 / 60, 8 + 20 / 60),
    # Left open Friday night, closed 06:00 Monday, over the fall-back: the
    # weekend had 49 hours in it, so the scan ran an hour longer than its
    # local times say.
    9900003: ("2026-10-31 03:30", "2026-11-02 13:00", 57.5, 56.5),
    # A weekend callout inside the repeated hour: both ends are ambiguous,
    # and both read as the first 01:xx (still daylight time).
    9900004: ("2026-11-01 07:15", "2026-11-01 07:45", 0.5, 0.5),
    # One that starts in the repeated hour and ends after it. Read by the
    # rule it ran 1 h 40; if it really started in the second 01:40, it ran
    # 40 minutes. The data can't say which: that's the cost of the rule.
    9900005: ("2026-11-01 07:40", "2026-11-01 09:20", 1 + 40 / 60, 40 / 60),
    # Left open over the spring-forward weekend: an hour shorter than its
    # local times say.
    9900006: ("2027-03-13 05:00", "2027-03-15 12:00", 55.0, 56.0),
}

# What the margin query's `scans` view makes of them: forgotten scans cut
# back to the end of their shift, and the Sunday callouts dropped (no shift
# runs at 1 AM on a Sunday, so they belong to no working window).
SCANS_VIEW = {
    9900001: ("2026-10-29 20:35", "2026-10-29 22:10", False),
    9900002: ("2026-10-30 03:40", "2026-10-30 05:00", True),
    9900003: ("2026-10-31 03:30", "2026-10-31 05:00", True),
    9900006: ("2027-03-13 05:00", "2027-03-13 06:00", True),
}


def statements_for(path: Path, views: list[str]) -> list[str]:
    """The macro and `create view` statements in `path` for the named views."""
    wanted = []
    for statement in split_statements(path.read_text(encoding="utf-8")):
        match = re.match(r"create or replace (?:view|macro) (\w+)", statement)
        if match and (match[1] in views or match[1] == "with_timezone"):
            wanted.append(statement)
    return wanted


def view_statements() -> list[str]:
    return statements_for(QUERIES / "sources.sql", VIEWS) + statements_for(
        QUERIES / "margin_gap.sql", VIEWS
    )


# --- DuckDB -------------------------------------------------------------------


@pytest.fixture(scope="module")
def con() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    con.execute("SET file_search_path = ?", [str(FIXTURE)])
    for statement in view_statements():
        con.execute(statement)
    return con


def mes_rows(rows) -> dict[int, tuple[datetime, datetime, float]]:
    """{record_id: (scan_on_utc, scan_off_utc, hours)} from any engine's rows."""
    return {
        int(record_id): (on, off, (off - on).total_seconds() / 3600)
        for record_id, on, off in rows
    }


def expected_mes() -> dict[int, tuple[datetime, datetime, float]]:
    return {
        record_id: (utc(on), utc(off), pytest.approx(hours))
        for record_id, (on, off, hours, _) in SCANS.items()
    }


def test_every_scan_reads_by_the_rule_in_duckdb(con):
    rows = con.sql(
        "select record_id, scan_on_utc, scan_off_utc from mes_labor"
    ).fetchall()
    got = mes_rows(
        (r, on.replace(tzinfo=UTC), off.replace(tzinfo=UTC)) for r, on, off in rows
    )
    assert got == expected_mes()


def test_naive_local_subtraction_is_an_hour_off_over_a_clock_change(con):
    naive = dict(
        con.sql(
            "select record_id,"
            " date_diff('second', scan_on_local, scan_off_local) / 3600.0 from mes_labor"
        ).fetchall()
    )
    assert naive == {r: pytest.approx(s[3]) for r, s in SCANS.items()}
    assert naive[9900003] == pytest.approx(56.5)  # fall back: an hour short
    assert naive[9900006] == pytest.approx(56.0)  # spring forward: an hour long


def test_the_scans_view_cuts_and_converts_them_in_duckdb(con):
    rows = con.sql(
        "select record_id, scan_on_utc, scan_off_utc, was_left_open from scans"
    ).fetchall()
    assert {
        r: (on.replace(tzinfo=UTC), off.replace(tzinfo=UTC), left_open)
        for r, on, off, left_open in rows
    } == {r: (utc(on), utc(off), left) for r, (on, off, left) in SCANS_VIEW.items()}


def test_the_rule_matches_python_for_every_quarter_hour_of_two_years(con):
    """The macro against zoneinfo's fold=0 (the earlier occurrence), both clock
    changes of 2026 and 2027 included."""
    start = datetime.fromisoformat("2026-01-01 00:00")  # plant local, no zone
    steps = 2 * 365 * 24 * 4
    rows = con.sql(
        f"""
        select local,
               cast(with_timezone(local, 'America/Denver') at time zone 'UTC' as timestamp)
        from (select timestamp '{start}' + to_minutes(cast(15 * i as bigint)) as local
              from range({steps}) as t(i))
        """
    ).fetchall()
    assert len(rows) == steps
    wrong = [
        (local, got)
        for local, got in rows
        if got != local.replace(tzinfo=PLANT_TZ).astimezone(UTC).replace(tzinfo=None)
    ]
    assert wrong == []


# --- Athena -------------------------------------------------------------------


@pytest.fixture(scope="module")
def athena() -> Athena:
    """The fixture's table and the same views, in the fixture database.

    Uploads the fixture under the raw bucket's fixtures/dst/ prefix (the
    backfill's prune never lists it) and points the raw table's own DDL there.
    """
    outputs = foundation_outputs()
    athena = dataclasses.replace(
        Athena.from_stack(), database=outputs["FixtureDatabaseName"]
    )
    prefix = "fixtures/dst"
    csv = FIXTURE / "mes" / "outbound" / "labor_dst.csv"
    boto3.client("s3").put_object(
        Bucket=athena.raw_bucket,
        Key=f"{prefix}/source=mes/entity=labor/ingest_date=2026-11-02/{csv.name}",
        Body=csv.read_bytes(),
    )
    tables = render(
        (QUERIES / "athena" / "tables.sql").read_text(encoding="utf-8"),
        raw_bucket=f"{athena.raw_bucket}/{prefix}",
    )
    for statement in split_statements(tables):
        if re.search(r"\braw_mes_labor\b", statement):
            athena.execute(statement)
    for statement in statements_for(QUERIES / "athena" / "sources.sql", VIEWS):
        athena.execute(statement)
    for statement in statements_for(QUERIES / "margin_gap.sql", VIEWS):
        athena.execute(statement)
    return athena


def athena_rows(athena: Athena, sql: str):
    query_id, _ = athena.execute(sql)
    return athena.fetch(query_id).iter_rows()


def instant(text: str) -> datetime:
    return datetime.fromisoformat(text).replace(tzinfo=UTC)


@pytest.mark.athena
def test_every_scan_reads_by_the_rule_in_athena(athena):
    rows = athena_rows(
        athena, "select record_id, scan_on_utc, scan_off_utc from mes_labor"
    )
    assert (
        mes_rows((r, instant(on), instant(off)) for r, on, off in rows)
        == expected_mes()
    )


@pytest.mark.athena
def test_the_scans_view_cuts_and_converts_them_in_athena(athena):
    rows = athena_rows(
        athena, "select record_id, scan_on_utc, scan_off_utc, was_left_open from scans"
    )
    assert {
        r: (instant(on), instant(off), left_open) for r, on, off, left_open in rows
    } == {r: (utc(on), utc(off), left) for r, (on, off, left) in SCANS_VIEW.items()}


@pytest.mark.athena
def test_both_engines_give_the_same_instant_for_every_row_of_every_view(athena, con):
    for view, columns in {
        "mes_labor": "record_id, scan_on_local, scan_off_local, scan_on_utc, scan_off_utc",
        "work_windows": "work_date, shift, starts_local, ends_local, starts_at_utc, ends_at_utc",
        "scans": "record_id, work_date, shift, scan_on_utc, scan_off_utc, was_left_open",
        "work_segments": "record_id, starts_at_utc, ends_at_utc",
    }.items():
        order = ", ".join(columns.split(", ")[:3])
        in_duckdb = [
            tuple(str(v) for v in row)
            for row in con.sql(
                f"select {columns} from {view} order by {order}"
            ).fetchall()
        ]
        in_athena = [
            tuple(_as_duckdb_text(v) for v in row)
            for row in athena_rows(
                athena, f"select {columns} from {view} order by {order}"
            )
        ]
        assert in_athena == in_duckdb, view
        assert in_duckdb, view


def _as_duckdb_text(value) -> str:
    """Athena's '2026-10-29 20:35:00.000' as DuckDB prints it; other values as is."""
    if isinstance(value, str) and re.fullmatch(
        r"\d{4}-\d\d-\d\d \d\d:\d\d:\d\d\.000", value
    ):
        return value[: -len(".000")]
    return str(value)


def test_the_fixture_spans_both_clock_changes():
    """Guard the fixture itself: its edges are where the clocks change."""
    fall = datetime(2026, 11, 1, 1, 30, tzinfo=PLANT_TZ)
    assert fall.utcoffset() != fall.replace(fold=1).utcoffset()  # 01:30 happens twice
    spring = datetime(2027, 3, 14, 2, 30, tzinfo=PLANT_TZ)
    assert spring.astimezone(UTC).astimezone(PLANT_TZ).hour == 3  # 02:30 never happens
