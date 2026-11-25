"""The `PlantData` target: the agent's read tools, one Lambda.

Each tool is a SQL file in sql/ run in Athena over the same views the
questions in queries/ read (queries/athena/sources.sql and the views built on it),
so a tool's answer is the query's answer. The Lambda checks the arguments
(tools.py), runs the file with them as parameters, and returns the rows.

Text from the plant's systems comes back as it is. The MES downtime log's
reason_text is whatever someone typed into the MES, and that is what M4
measures: what an agent does with it. Sanitizing it here would make the
measurement dishonest.
"""

import json
import os
from datetime import UTC, date, datetime
from zoneinfo import ZoneInfo

from agent_tools import athena, tools

PLANT_TZ = ZoneInfo("America/Denver")
MAX_ROWS = 200  # each file asks for one more, to know when it was cut short


def handler(event: dict, context: object) -> dict:
    name = tools.tool_name(context)
    print(
        json.dumps(
            {
                "tool": name,
                "arguments": event,
                "gateway_request_id": tools.gateway_request_id(context),
            }
        )
    )
    try:
        return answer(name, event, athena.run)
    except tools.ToolInputError as error:
        return {"error": str(error)}


def answer(name: str, arguments: dict, run) -> dict:
    """What tool `name` returns; `run(sql_name, parameters)` gives the rows."""
    last_day = date.fromisoformat(os.environ["CRESTLINE_DATA_LAST_DAY"])
    if name == "job_status":
        return _job_status(run("job_status", [tools.job_num(arguments)]))
    if name == "job_margin":
        first, last = tools.window(arguments, last_day)
        return _rows(
            run("job_margin", [first.isoformat(), last.isoformat()]), first, last
        )
    if name in {"downtime_events", "machine_downtime", "cycles_by_day", "scrap_by_day"}:
        machine = tools.machine(arguments)
        first, last = tools.window(arguments, last_day)
        rows = run(name, [machine, first.isoformat(), last.isoformat()])
        if name == "machine_downtime":
            rows = [_with_local_times(row) for row in rows]
        return _rows(rows, first, last)
    raise tools.ToolInputError(f"no tool named {name!r}")


def _rows(rows: list[dict], first: date, last: date) -> dict:
    return {
        "from": first.isoformat(),
        "to": last.isoformat(),
        "rows": rows[:MAX_ROWS],
        "truncated": len(rows) > MAX_ROWS,
    }


def _with_local_times(row: dict) -> dict:
    """UTC from the SQL, plant time added here: the engines disagree on how
    to print a zoned timestamp, and Python doesn't."""
    for prefix in ("started_at", "ended_at"):
        utc = datetime.fromisoformat(row[f"{prefix}_utc"]).replace(tzinfo=UTC)
        row[f"{prefix}_local"] = utc.astimezone(PLANT_TZ).strftime("%Y-%m-%d %H:%M:%S")
    return row


def _job_status(rows: list[dict]) -> dict:
    if not rows:
        return {"error": "no such job"}
    operation_fields = {"work_center"} | {k for k in rows[0] if k.startswith("op_")}
    job = {k: v for k, v in rows[0].items() if k not in operation_fields}
    job["operations"] = [
        {
            "op_seq": row["op_seq"],
            "work_center": row["work_center"],
            "qty_completed": row["op_qty_completed"],
            "scrap_qty": row["op_scrap_qty"],
            "complete": row["op_complete"],
        }
        for row in rows
        if row["op_seq"] is not None
    ]
    return job
