"""Run the SQL in `queries/` with DuckDB against one backfill's output.

`queries/sources.sql` defines the inputs as views over the files the plant's
systems wrote. Every other file there is plain SQL on top of those names,
which is what lets the same query run in Athena later.

This is data-platform code: it must never read `out/truth/` or import the
simulator. The eval (`eval/`) is the only place allowed to compare against
the answer key.
"""

from pathlib import Path

import duckdb

QUERIES_DIR = Path("queries")


def connect(
    out_dir: Path, queries_dir: Path = QUERIES_DIR
) -> duckdb.DuckDBPyConnection:
    """A DuckDB connection with the sources defined over `out_dir`."""
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")  # display only; the SQL is explicit
    con.execute("SET file_search_path = ?", [str(out_dir.resolve())])
    con.execute((queries_dir / "sources.sql").read_text(encoding="utf-8"))
    return con


def run(
    con: duckdb.DuckDBPyConnection, name: str, queries_dir: Path = QUERIES_DIR
) -> duckdb.DuckDBPyRelation:
    """Run `queries/<name>.sql` and return its final statement's result.

    Views the file creates stay on `con`, so callers can look inside them.
    """
    sql = (queries_dir / f"{name.replace('-', '_')}.sql").read_text(encoding="utf-8")
    return con.sql(sql)
