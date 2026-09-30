"""Run the SQL in `queries/` on Athena, over the raw bucket.

Two steps, mirroring what `crestline query` does with DuckDB:

- `setup()` creates the raw tables (queries/athena/tables.sql) and the source
  views (queries/athena/sources.sql) in the Glue database. Run it after
  deploying the stack, and again after uploading a new extract.
- `run_query(name)` runs queries/<name>.sql, the same file DuckDB runs, and
  returns the result of its last statement.

Athena takes one statement per request, so a file is split at each `;` and
its statements run in order. The views a query file creates stay in the
Glue database, where they can be queried from the Athena console afterwards.

The database, workgroup, and bucket come from FoundationStack's outputs. The
workgroup caps every query at 1 GB scanned.

This is data-platform code: it never imports the simulator.
"""

import re
import time
from dataclasses import dataclass, field
from pathlib import Path

import boto3
import polars as pl

from crestline.stack import foundation_outputs

QUERIES_DIR = Path("queries")
SQL_COMMENT = re.compile(r"--[^\n]*")
PLACEHOLDER = re.compile(r"\$\{(\w+)\}")
POLL_SECONDS = 1.0
DONE = {"SUCCEEDED", "FAILED", "CANCELLED"}

# How to read Athena's result values, which all arrive as text.
INTEGER_TYPES = {"tinyint", "smallint", "integer", "bigint"}
DECIMAL_TYPES = {"real", "float", "double", "decimal"}


class AthenaQueryError(RuntimeError):
    pass


@dataclass(frozen=True)
class QueryResult:
    frame: pl.DataFrame
    bytes_scanned: int


@dataclass
class Athena:
    database: str
    workgroup: str
    raw_bucket: str
    client: object = field(default_factory=lambda: boto3.client("athena"))

    @classmethod
    def from_stack(cls, cloudformation=None) -> "Athena":
        outputs = foundation_outputs(cloudformation)
        return cls(
            database=outputs["GlueDatabaseName"],
            workgroup=outputs["AthenaWorkGroupName"],
            raw_bucket=outputs["RawBucketName"],
        )

    def setup(self, queries_dir: Path = QUERIES_DIR) -> int:
        """Create the raw tables and source views; returns bytes scanned."""
        scanned = self.run_file(queries_dir / "athena" / "tables.sql")[1]
        scanned += self.run_file(queries_dir / "athena" / "sources.sql")[1]
        return scanned

    def run_query(self, name: str, queries_dir: Path = QUERIES_DIR) -> QueryResult:
        """Run queries/<name>.sql and return its final statement's result."""
        path = queries_dir / f"{name.replace('-', '_')}.sql"
        last_query_id, scanned = self.run_file(path)
        return QueryResult(self.fetch(last_query_id), scanned)

    def run_file(self, path: Path) -> tuple[str, int]:
        """Run every statement in `path` in order.

        Returns the last statement's query ID and the bytes all of them scanned.
        """
        sql = render(path.read_text(encoding="utf-8"), raw_bucket=self.raw_bucket)
        query_id, scanned = "", 0
        for statement in split_statements(sql):
            query_id, bytes_scanned = self.execute(statement)
            scanned += bytes_scanned
        return query_id, scanned

    def execute(self, statement: str) -> tuple[str, int]:
        """Run one statement and wait for it; returns its ID and bytes scanned."""
        query_id = self.client.start_query_execution(
            QueryString=statement,
            QueryExecutionContext={"Database": self.database},
            WorkGroup=self.workgroup,
        )["QueryExecutionId"]
        while True:
            execution = self.client.get_query_execution(QueryExecutionId=query_id)
            status = execution["QueryExecution"]["Status"]
            if status["State"] in DONE:
                break
            time.sleep(POLL_SECONDS)
        if status["State"] != "SUCCEEDED":
            reason = status.get("StateChangeReason", status["State"])
            raise AthenaQueryError(f"{reason}\n\n{statement}")
        statistics = execution["QueryExecution"].get("Statistics", {})
        return query_id, statistics.get("DataScannedInBytes", 0)

    def fetch(self, query_id: str) -> pl.DataFrame:
        """A finished query's rows, typed from Athena's result metadata."""
        pages = self.client.get_paginator("get_query_results").paginate(
            QueryExecutionId=query_id
        )
        columns, rows = [], []
        for page in pages:
            result_set = page["ResultSet"]
            if not columns:
                columns = result_set["ResultSetMetadata"]["ColumnInfo"]
                data = result_set["Rows"][1:]  # the first row repeats the header
            else:
                data = result_set["Rows"]
            for row in data:
                texts = [cell.get("VarCharValue") for cell in row["Data"]]
                rows.append(
                    [
                        _typed(text, column["Type"])
                        for text, column in zip(texts, columns)
                    ]
                )
        schema = {column["Name"]: _polars_type(column["Type"]) for column in columns}
        return pl.DataFrame(rows, schema=schema, orient="row")


def split_statements(sql: str) -> list[str]:
    """The statements in a SQL file, without comments.

    Splits at every `;`, which is safe for the files in queries/: none has a
    semicolon inside a string.
    """
    statements = SQL_COMMENT.sub("", sql).split(";")
    return [statement.strip() for statement in statements if statement.strip()]


def render(sql: str, **values: str) -> str:
    """Fill in ${name} placeholders; a placeholder with no value is an error."""
    missing = set(PLACEHOLDER.findall(sql)) - values.keys()
    if missing:
        raise KeyError(f"no value for {sorted(missing)}")
    return PLACEHOLDER.sub(lambda match: values[match.group(1)], sql)


def _polars_type(athena_type: str) -> pl.DataType:
    if athena_type in INTEGER_TYPES:
        return pl.Int64
    if athena_type in DECIMAL_TYPES:
        return pl.Float64
    if athena_type == "boolean":
        return pl.Boolean
    return pl.String


def _typed(text: str | None, athena_type: str) -> int | float | bool | str | None:
    if text is None:
        return None
    if athena_type in INTEGER_TYPES:
        return int(text)
    if athena_type in DECIMAL_TYPES:
        return float(text)
    if athena_type == "boolean":
        return text == "true"
    return text
