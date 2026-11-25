"""The brief's facts as of a morning, on DuckDB or Athena (M5 step 3).

A brief for morning D may only use what had arrived by 05:30 on D. The
sources over the full data can't promise that, so the brief runs in a layer
of its own: `queries/as_of.sql` redefines every source, cut at that moment,
over the real ones in `crestline_raw`, and every query file the brief needs
runs on top, unchanged. In DuckDB the real sources sit in a schema called
`crestline_raw` and the layer in `main`; in Athena the layer is its own Glue
database (FoundationStack's `crestline_brief`).

Every view in the layer reads the morning from one view, `brief_cut`, so
moving to another morning replaces that one view and nothing else. One brief
at a time per layer, then: two runs for two mornings would share it.

Building the layer creates views only; nothing is computed until the facts
are asked for, so a backtest builds it once and moves the morning.

DuckDB is imported only by the functions that use it: the brief's Lambda
(M5 step 6) runs on Athena and doesn't carry it.
"""

from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl

from crestline.athena import Athena, render, split_statements

if TYPE_CHECKING:
    import duckdb

QUERIES_DIR = Path("queries")
RAW_SCHEMA = "crestline_raw"  # the sources' home, in both engines

# The layer, in the order its files build on each other.
LAYER_FILES = ("as_of", "machines", "margin_gap", "downtime", "scrap", "brief")
# brief.sql's facts, one view per family, and the order a reader takes them.
FAMILY_VIEWS = (
    "brief_output_facts",
    "brief_margin_facts",
    "brief_stop_facts",
    "brief_scrap_facts",
)
FAMILY_ORDER = {"anomaly": 0, "output": 1, "margin": 2, "stops": 3, "scrap": 4}


def view_statements(path: Path, **values: str) -> list[str]:
    """The `create ... view` statements in a query file, placeholders filled."""
    sql = render(path.read_text(encoding="utf-8"), **values)
    return [
        statement
        for statement in split_statements(sql)
        if statement.startswith("create or replace view")
    ]


def cut_statement(morning: date, queries_dir: Path = QUERIES_DIR) -> str:
    """The statement that sets the layer's morning (as_of.sql's `brief_cut`)."""
    [statement] = [
        statement
        for statement in view_statements(
            queries_dir / "as_of.sql", morning=morning.isoformat()
        )
        if statement.startswith("create or replace view brief_cut ")
    ]
    return statement


def facts_statement(queries_dir: Path = QUERIES_DIR) -> str:
    """brief.sql's last statement: the facts, in reading order."""
    return split_statements((queries_dir / "brief.sql").read_text(encoding="utf-8"))[-1]


# --- DuckDB ---------------------------------------------------------------------


def connect(
    out_dir: Path, morning: date, queries_dir: Path = QUERIES_DIR
) -> duckdb.DuckDBPyConnection:
    """A DuckDB connection with the brief's layer over a backfill's output.

    The sources go into the `crestline_raw` schema, as `crestline query`
    defines them; the layer into `main`, found first. The search path also
    reaches `crestline_raw`, where the time zone macro lives.
    """
    from crestline import query  # DuckDB: the Lambda, on Athena, has none

    con = query.connect_sources(out_dir, queries_dir, schema=RAW_SCHEMA)
    # (Setting the search path also makes `main` where views are created.)
    con.execute(f"SET search_path = 'main,{RAW_SCHEMA}'")
    for name in LAYER_FILES:
        for statement in view_statements(
            queries_dir / f"{name}.sql", morning=morning.isoformat()
        ):
            con.execute(statement)
    return con


def facts(
    con: duckdb.DuckDBPyConnection,
    morning: date | None = None,
    queries_dir: Path = QUERIES_DIR,
) -> pl.DataFrame:
    """The facts for `morning` (the layer's current one, if not given)."""
    if morning is not None:
        con.execute(cut_statement(morning, queries_dir))
    return con.sql(facts_statement(queries_dir)).pl()


# --- Athena ---------------------------------------------------------------------


@dataclass(frozen=True)
class AthenaFacts:
    frame: pl.DataFrame
    seconds: float  # wall time to set the morning and fetch the facts
    bytes_scanned: int


def setup_athena(athena: Athena, morning: date, queries_dir: Path = QUERIES_DIR) -> int:
    """Create the layer's views in the brief database; returns bytes scanned."""
    if athena.brief_database is None:
        raise RuntimeError(
            "the foundation stack has no brief database; deploy CrestlineFoundation"
        )
    scanned = 0
    for name in LAYER_FILES:
        scanned += athena.run_file(
            queries_dir / f"{name}.sql",
            views_only=True,
            database=athena.brief_database,
            morning=morning.isoformat(),
        )[1]
    return scanned


def athena_facts(
    athena: Athena, morning: date, queries_dir: Path = QUERIES_DIR
) -> AthenaFacts:
    """The facts for `morning`, from the brief database (built by setup_athena).

    Each family's view is its own query, the four run at once: asked for
    in one query, Athena spent half the time planning it (brief.sql). The
    rows come back in brief.sql's order.
    """
    started = time.monotonic()
    athena.execute(cut_statement(morning, queries_dir), athena.brief_database)
    with ThreadPoolExecutor(len(FAMILY_VIEWS)) as pool:
        results = list(
            pool.map(
                lambda view: athena.execute(
                    f"select * from {view}", athena.brief_database
                ),
                FAMILY_VIEWS,
            )
        )
    frame = pl.concat([athena.fetch(query_id) for query_id, _ in results])
    frame = frame.sort(
        pl.col("family").replace_strict(FAMILY_ORDER, return_dtype=pl.Int64),
        "subject",
        "metric",
    )
    scanned = sum(bytes_scanned for _, bytes_scanned in results)
    return AthenaFacts(frame, time.monotonic() - started, scanned)
