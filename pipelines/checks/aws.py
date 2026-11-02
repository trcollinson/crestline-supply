"""Running the checks in AWS: Athena for the SQL, DynamoDB for the baselines.

`run_checks()` is called at the end of every machine curate run, and
`erp_handler` is the ERP poller's last step, after every entity has been
extracted and merged (the ERP's reconciliation needs `LaborDetails` and
`JobCosts` merged to the same run, so it can't run after either alone).
A breach raises `ChecksFailed`, which fails the run and fires its alarm.
"""

import json
import os
import time
from collections.abc import Callable
from functools import cache

from erp_extract.curate import CurateFailed, run_athena

from .shape import (
    ERP_SHAPES,
    MACHINE_SHAPES,
    Breach,
    RunSql,
    TableShape,
    check_brake_strokes,
    check_erp_labor,
    run_all,
)

BASELINE_ITEM = "_checks"  # the state table item holding the null-rate baselines


class ChecksFailed(CurateFailed):
    """The data merged, but its shape is wrong."""


def athena_sql(
    athena, workgroup: str, sleep: Callable[[float], None] = time.sleep
) -> RunSql:
    def run(sql: str) -> list[list[str | None]]:
        query_id, _ = run_athena(athena, workgroup, sql, sleep)
        rows = athena.get_query_results(QueryExecutionId=query_id)["ResultSet"]["Rows"]
        return [[cell.get("VarCharValue") for cell in row["Data"]] for row in rows[1:]]

    return run


def run_checks(
    run_sql: RunSql,
    state,
    shapes: list[TableShape],
    reconciliations: list[Callable[[RunSql], list[Breach]]],
) -> list[Breach]:
    """Every check; saves the new baselines; raises ChecksFailed on any breach."""
    item = state.get_item(Key={"entity": BASELINE_ITEM}).get("Item", {})
    baselines = json.loads(item.get("baselines", "{}"))
    breaches, updated = run_all(run_sql, shapes, baselines, reconciliations)
    state.update_item(
        Key={"entity": BASELINE_ITEM},
        UpdateExpression="SET baselines = :b",
        ExpressionAttributeValues={":b": json.dumps(baselines | updated)},
    )
    if breaches:
        raise ChecksFailed("; ".join(str(b) for b in breaches))
    return breaches


def machine_checks(run_sql: RunSql, state, raw_database: str) -> list[Breach]:
    return run_checks(
        run_sql,
        state,
        MACHINE_SHAPES,
        [lambda run: check_brake_strokes(run, raw_database)],
    )


@cache
def _erp_setup() -> dict:
    import boto3

    return {
        "athena": boto3.client("athena"),
        "workgroup": os.environ["CRESTLINE_ATHENA_WORKGROUP"],
        "table": boto3.resource("dynamodb").Table(os.environ["CRESTLINE_STATE_TABLE"]),
    }


def erp_handler(event: dict, context: object) -> dict:
    """The ERP poller's last step: null rates and keys of its tables, and the labor total."""
    setup = _erp_setup()
    run_sql = athena_sql(setup["athena"], setup["workgroup"])
    run_checks(run_sql, setup["table"], ERP_SHAPES, [check_erp_labor])
    return {"checked": [s.table for s in ERP_SHAPES], "breaches": 0}
