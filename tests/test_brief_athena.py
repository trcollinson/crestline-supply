"""The brief's facts have no look-ahead on Athena either (M5 step 3).

Athena holds the whole backfill, as the ERP poller, the upload, and the IoT
replay delivered it: the curated ERP tables (each row's newest version), the
MES files, and the machines' messages. The brief's layer (queries/as_of.sql
in the `crestline_brief` database) cuts them at 05:30 on each morning. What
it must equal is DuckDB over a copy of the same backfill holding only what
had arrived by then, the ERP rebuilt as it stood from its history
(eval/backfill_as_of.py): Athena has no history to rebuild it from.

Each morning's time in Athena is printed (`-s` shows it): decision 9 of the
M5 plan filters before cleaning only if one morning takes over 60 s.

Opt-in, because it runs in AWS: `pytest -m athena`. It needs the stacks, the
upload, the replay curated, and CrestlineFoundation's brief database; it
rebuilds the brief's views from this checkout first.
"""

import os
from datetime import date
from pathlib import Path

import polars as pl
import pytest

from backfill_as_of import cut_backfill
from crestline.athena import Athena
from crestline.brief import facts

pytestmark = pytest.mark.athena

REPO_ROOT = Path(__file__).resolve().parent.parent
# The backfill the stacks hold: CRESTLINE_OUT=out-m5 for M5's, in us-east-1.
OUT = REPO_ROOT / os.environ.get("CRESTLINE_OUT", "out")
QUERIES = REPO_ROOT / "queries"

MORNINGS = [
    date(2026, 7, 13),  # a Monday
    date(2026, 8, 13),  # the morning after the nozzle's ramp starts
    date(2026, 8, 17),  # a Monday, mid-ramp
    date(2026, 8, 24),  # the Monday the nozzle is changed
    date(2026, 10, 1),  # the whole backfill
]
COLUMNS = [
    "fact_id",
    "morning",
    "plant_day",
    "value",
    "unit",
    "baseline",
    "source_rows",
]


@pytest.fixture(scope="module")
def athena() -> Athena:
    athena = Athena.from_stack()
    if athena.brief_database is None:
        pytest.skip("the foundation stack has no brief database")
    facts.setup_athena(athena, MORNINGS[0], QUERIES)
    return athena


def comparable(frame: pl.DataFrame) -> pl.DataFrame:
    """Dates as text (Athena returns them so), sorted by fact."""
    return (
        frame.select(COLUMNS)
        .with_columns(pl.col("morning", "plant_day").cast(pl.String))
        .sort("fact_id")
    )


@pytest.mark.parametrize("morning", MORNINGS, ids=str)
def test_athena_facts_equal_the_data_as_it_stood(athena, tmp_path, morning):
    found = facts.athena_facts(athena, morning, QUERIES)
    print(
        f"\n{morning}: {found.frame.height} facts in {found.seconds:.1f} s, "
        f"{found.bytes_scanned / 1e6:.0f} MB scanned"
    )

    cut_backfill(OUT, tmp_path, morning)
    expected = facts.facts(
        facts.connect(tmp_path, morning, QUERIES), queries_dir=QUERIES
    )

    athena_rows, duckdb_rows = comparable(found.frame), comparable(expected)
    assert athena_rows["fact_id"].to_list() == duckdb_rows["fact_id"].to_list()
    for column in ("morning", "plant_day", "unit", "source_rows"):
        assert athena_rows[column].to_list() == duckdb_rows[column].to_list(), column
    for column in ("value", "baseline"):
        apart = (pl.col(column) - pl.col("other")).abs() > 1e-9
        one_missing = pl.col(column).is_null() != pl.col("other").is_null()
        differs = athena_rows.with_columns(other=duckdb_rows[column]).filter(
            apart | one_missing
        )
        assert differs.is_empty(), differs
