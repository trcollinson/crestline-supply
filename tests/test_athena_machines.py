"""Athena equals DuckDB on the machine data, cell for cell (M3 step 8).

The machine SQL (queries/machines.sql, margin_gap.sql, downtime.sql) runs
unchanged on both engines. DuckDB reads the backfill's machine files in
./out; Athena reads the same messages after they went through AWS IoT Core,
Firehose, and the curate step into the curated tables (the newest replay of
each device). Every row of every result below must be the same, null
matching null (the margin table's "no brake op" row has no shift).

Opt-in, because it runs in AWS: `pytest -m athena`. It needs the stacks
deployed, the 90-day replay published and curated, and `crestline athena
setup` run; it rebuilds the views from this checkout first.
"""

from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import pytest

from crestline import query
from crestline.athena import Athena

pytestmark = pytest.mark.athena

REPO_ROOT = Path(__file__).resolve().parent.parent
OUT = REPO_ROOT / "out"
QUERIES = REPO_ROOT / "queries"

# Each result compared, and the columns that order it.
RESULTS = {
    "machine_cycles_by_day": "select machine_id, cast(work_date as varchar) as work_date, cycles from machine_cycles_by_day",
    "machine_counts_summary": """
        select machine_id, counted_as, count(*) as readings, sum(cycles) as cycles,
               cast(min(counted_at_utc) as varchar) as first_utc,
               cast(max(counted_at_utc) as varchar) as last_utc
        from machine_counts group by 1, 2""",
    "machine_states_summary": """
        select machine_id, state_code, count(*) as intervals,
               sum(date_diff('millisecond', started_at_utc, ended_at_utc)) as ms
        from machine_states group by 1, 2""",
    "brake_operations": """
        select job_num, op_seq, brake, setup_shift, pieces, strokes,
               cast(first_stroke_at_utc as varchar) as first_stroke_utc
        from brake_operations""",
    "machine_downtime": """
        select machine_id, generation, found_by,
               cast(started_at_utc as varchar) as started_utc,
               cast(ended_at_utc as varchar) as ended_utc
        from machine_downtime""",
}


def normalized(frame: pl.DataFrame) -> pl.DataFrame:
    """Same types and order on both sides: text for times, floats rounded, rows sorted."""
    frame = frame.with_columns(
        [
            pl.col(c).cast(pl.Float64).round(6)
            for c, t in frame.schema.items()
            if t.is_numeric()
        ]
    )
    times = [c for c in frame.columns if c.endswith(("_utc", "work_date"))]
    frame = frame.with_columns(
        [
            pl.col(c)
            .cast(pl.String)
            .str.replace(r"(\.\d*?)0+$", "$1")
            .str.replace(r"\.$", "")
            for c in times
        ]
    )
    return frame.sort(frame.columns)


@pytest.fixture(scope="module")
def athena() -> Athena:
    athena = Athena.from_stack()
    athena.setup(QUERIES)
    for name in ("margin_gap", "downtime"):
        athena.run_file(QUERIES / f"{name}.sql")
    return athena


@pytest.fixture(scope="module")
def duckdb():
    con = query.connect(OUT, QUERIES)
    query.run(con, "downtime", QUERIES).fetchall()
    return con


@pytest.mark.parametrize("name", RESULTS)
def test_athena_equals_duckdb(athena, duckdb, name):
    sql = RESULTS[name]
    query_id, _ = athena.execute(sql)
    in_athena = normalized(athena.fetch(query_id))
    in_duckdb = normalized(duckdb.sql(sql).pl())
    assert in_athena.height == in_duckdb.height > 0, name
    assert in_athena.columns == in_duckdb.columns
    differ = in_athena.join(
        in_duckdb, on=in_athena.columns, how="anti", nulls_equal=True
    )
    assert differ.height == 0, differ.head(10)


def test_the_margin_answer_is_the_same_on_both_engines(athena):
    """The margin query's answer table, from machine data that went through IoT Core."""
    in_athena = normalized(athena.run_query("margin-gap", QUERIES).frame)
    in_duckdb = normalized(
        query.run(query.connect(OUT, QUERIES), "margin-gap", QUERIES).pl()
    )
    assert in_athena.height == in_duckdb.height == 5
    assert in_athena.columns == in_duckdb.columns
    assert (
        in_athena.join(
            in_duckdb, on=in_athena.columns, how="anti", nulls_equal=True
        ).height
        == 0
    )


def test_the_replay_athena_reads_is_the_whole_backfill(athena):
    query_id, _ = athena.execute(
        "select cast(min(published_at_utc) as varchar), cast(max(published_at_utc) as varchar) from pb02_counts"
    )
    first, last = athena.fetch(query_id).row(0)
    assert datetime.fromisoformat(first).replace(tzinfo=UTC) <= datetime(
        2026, 7, 3, 7, tzinfo=UTC
    )
    assert datetime.fromisoformat(last).replace(tzinfo=UTC) >= datetime(
        2026, 10, 1, 5, tzinfo=UTC
    )
