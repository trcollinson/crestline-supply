"""Run the SQL in `queries/` with DuckDB against one backfill's output.

`queries/sources.sql` defines the inputs as views over the files the plant's
systems wrote, and `queries/machines.sql` cleans the machine data on top of
them (M3). Every other file there is plain SQL on top of those names,
which is what lets the same query run in Athena later.

LSR-01 speaks Sparkplug B, protobuf that SQL can't read. In AWS the IoT rule
decodes it on the way in; here `connect()` does the same first, writing
`decoded/lsr01.jsonl` next to the backfill's files (`crestline_machines.iot_rule`).

This is data-platform code: it must never read `out/truth/` or import the
simulator. The eval (`eval/`) is the only place allowed to compare against
the answer key.
"""

from pathlib import Path

import duckdb

from crestline_machines.iot_rule import write_rule_records

QUERIES_DIR = Path("queries")

# Query files that build on another's views, and the files to run first.
PREREQUISITES = {"downtime": ("margin_gap",)}


def connect(
    out_dir: Path, queries_dir: Path = QUERIES_DIR
) -> duckdb.DuckDBPyConnection:
    """A DuckDB connection with the sources and clean machine data over `out_dir`."""
    laser = out_dir / "machines" / "lsr01.jsonl"
    if laser.exists():
        write_rule_records(laser, out_dir / "decoded" / "lsr01.jsonl")
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")  # display only; the SQL is explicit
    con.execute("SET file_search_path = ?", [str(out_dir.resolve())])
    con.execute((queries_dir / "sources.sql").read_text(encoding="utf-8"))
    con.execute((queries_dir / "machines.sql").read_text(encoding="utf-8"))
    return con


def run(
    con: duckdb.DuckDBPyConnection, name: str, queries_dir: Path = QUERIES_DIR
) -> duckdb.DuckDBPyRelation:
    """Run `queries/<name>.sql` and return its final statement's result.

    Views the file creates stay on `con`, so callers can look inside them.
    """
    name = name.replace("-", "_")
    for prerequisite in PREREQUISITES.get(name, ()):
        con.execute((queries_dir / f"{prerequisite}.sql").read_text(encoding="utf-8"))
    sql = (queries_dir / f"{name}.sql").read_text(encoding="utf-8")
    return con.sql(sql)
