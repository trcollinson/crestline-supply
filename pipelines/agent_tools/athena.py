"""Run one of the tools' SQL files in Athena, with its arguments as parameters.

The arguments never go into the SQL text. Each `?` in a file is filled by
Athena from `ExecutionParameters`, so whatever a caller passes is a value,
not SQL. Athena takes each parameter as a literal, so a text value is
quoted here (and any quote in it doubled); the tools only pass values
they have already checked, but the quoting doesn't rely on that.
"""

import os
import re
import time
from datetime import date
from pathlib import Path

import boto3

SQL_DIR = Path(__file__).resolve().parent / "sql"
POLL_SECONDS = 0.5
NUMBERS = {"tinyint", "smallint", "integer", "bigint"}
DECIMALS = {"double", "float", "real", "decimal"}


def sql(name: str) -> str:
    return (SQL_DIR / f"{name}.sql").read_text(encoding="utf-8")


def literal(value: str | int | date) -> str:
    """A parameter as Athena wants it: a number as is, text in single quotes."""
    if isinstance(value, bool):
        raise TypeError("no booleans")
    if isinstance(value, int):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


def run(name: str, parameters: list[str | int | date], client=None) -> list[dict]:
    """The rows `sql/<name>.sql` returns, typed, as dictionaries."""
    client = client or boto3.client("athena")
    query_id = client.start_query_execution(
        QueryString=sql(name),
        QueryExecutionContext={"Database": os.environ["CRESTLINE_RAW_DATABASE"]},
        WorkGroup=os.environ["CRESTLINE_ATHENA_WORKGROUP"],
        ExecutionParameters=[literal(p) for p in parameters],
    )["QueryExecutionId"]
    while True:
        status = client.get_query_execution(QueryExecutionId=query_id)[
            "QueryExecution"
        ]["Status"]
        if status["State"] in {"SUCCEEDED", "FAILED", "CANCELLED"}:
            break
        time.sleep(POLL_SECONDS)
    if status["State"] != "SUCCEEDED":
        raise RuntimeError(status.get("StateChangeReason", status["State"]))
    rows, columns = [], []
    for page in client.get_paginator("get_query_results").paginate(
        QueryExecutionId=query_id
    ):
        result = page["ResultSet"]
        data = result["Rows"]
        if not columns:
            columns = result["ResultSetMetadata"]["ColumnInfo"]
            data = data[1:]  # the first row repeats the header
        for row in data:
            values = [cell.get("VarCharValue") for cell in row["Data"]]
            rows.append(
                {c["Name"]: typed(v, c["Type"]) for c, v in zip(columns, values)}
            )
    return rows


ZERO_FRACTION = re.compile(r"\.0+$")


def typed(text: str | None, athena_type: str) -> object:
    """Athena's text as JSON-friendly values; timestamps without a zero fraction.

    A view over the raw tables prints `.000` (milliseconds), one over the
    curated Iceberg tables `.000000` (microseconds); DuckDB prints neither."""
    if text is None:
        return None
    if athena_type in NUMBERS:
        return int(text)
    if athena_type in DECIMALS:
        return float(text)
    if athena_type == "boolean":
        return text == "true"
    if athena_type.startswith("timestamp"):
        return ZERO_FRACTION.sub("", text)
    return text
