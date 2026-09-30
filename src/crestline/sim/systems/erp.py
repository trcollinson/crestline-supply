"""The ERP: orders, jobs, and standard costing (spec §4.1).

The ERP knows its own sales orders and jobs, the standard routing for every
part, and the rates in spec §2.4. Everything it knows about the shop floor
arrives as labor the MES posts to it: hours, pieces, and "operation
complete". This module never sees the plant simulation, only those inputs.

How a mid-market ERP costs a job, and why it hides the PB-02 problem:

- **Standard cost** is the routing: standard setup + run hours for the job
  quantity, × the labor and burden rates, plus standard material.
- **Actual labor** is the MES labor, rounded to 0.1 hr and posted the next
  day (both settings in `mess.yaml`). Before it posts, a supervisor works
  the daily labor exception report: a scan that runs past the end of its
  shift was left open, so it is cut back to the shift end. Every plant does
  this by hand; it stops a forgotten scan-off from posting a 16-hour run,
  but it still over-charges the job for the rest of that shift.
- **Actual burden is applied at standard hours**: each operation earns its
  standard setup plus standard run time for the pieces reported. Machine
  time beyond standard never shows up, so a setup that takes 2.8× as long
  costs the same burden as one that runs to standard.
- **Actual material** is issued at standard cost when the job is released.
- **Released quantity**: the planner releases the order plus a 5% scrap
  allowance, so `ProdQty` on the job is a little over `OrderQty` on the
  order line. Standard cost and material are for `ProdQty`; the invoice is
  for `OrderQty`.

Output is a SQLite database (what the M2 ERP API will serve) and the same
rows as one JSONL file per entity, which is what the M1 backfill uploads,
since Athena cannot read SQLite.
"""

import json
import math
import sqlite3
from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import polars as pl

from crestline.mess import ErpMess
from crestline.reference import ReferenceData

from ..clock import PLANT_TZ
from ..config import ShiftCalendarConfig
from ..jobs import GeneratedJobs

COMPANY = "CRE"
# ERP numeric fields carry 8 decimal places (spec §4.1).
DECIMALS = 8
# The ERP has been in use for years; its sequence numbers did not start at 1.
FIRST_LABOR_DTL_SEQ = 1_207_001

Row = dict[str, object]

# Entity -> column -> SQLite type. Column order is the order in both outputs.
ENTITIES: dict[str, dict[str, str]] = {
    # The rates behind standard costing (spec §2.4), per work center.
    "WorkCenters": {
        "Company": "TEXT",
        "WorkCenter": "TEXT",
        "LaborRate": "REAL",  # $/hr
        "BurdenRate": "REAL",  # machine burden $/hr
    },
    "SalesOrders": {
        "Company": "TEXT",
        "OrderNum": "TEXT",
        "CustomerCode": "TEXT",
        "OrderDate": "TEXT",
        "LastModified": "TEXT",
    },
    "SalesOrderLines": {
        "Company": "TEXT",
        "OrderNum": "TEXT",
        "OrderLine": "INTEGER",
        "PartNum": "TEXT",
        "RevisionNum": "TEXT",
        "OrderQty": "INTEGER",
        "UnitPrice": "REAL",
        "LastModified": "TEXT",
    },
    "Jobs": {
        "Company": "TEXT",
        "JobNum": "TEXT",
        "OrderNum": "TEXT",
        "OrderLine": "INTEGER",
        "PartNum": "TEXT",
        "RevisionNum": "TEXT",
        "ProdQty": "INTEGER",  # released: OrderQty plus the scrap allowance
        "ReleaseDate": "TEXT",
        "ReqDueDate": "TEXT",
        "JobReleased": "INTEGER",
        "JobComplete": "INTEGER",
        "CompletedQty": "INTEGER",  # posted only when the job completes
        "LastModified": "TEXT",
    },
    "JobOperations": {
        "Company": "TEXT",
        "JobNum": "TEXT",
        "OprSeq": "INTEGER",
        "WorkCenter": "TEXT",  # BRAKE, not which brake: the ERP never knows
        "StdSetupHours": "REAL",
        "StdRunHoursPerPiece": "REAL",
        "QtyCompleted": "INTEGER",
        "ScrapQty": "INTEGER",
        "OpComplete": "INTEGER",
        "LastModified": "TEXT",
    },
    "LaborDetails": {
        "Company": "TEXT",
        "LaborDtlSeq": "INTEGER",
        "EmployeeNum": "TEXT",
        "JobNum": "TEXT",
        "OprSeq": "INTEGER",
        "WorkCenter": "TEXT",
        "PayrollDate": "TEXT",
        "LaborHrs": "REAL",
        "LaborQty": "INTEGER",
        "ScrapQty": "INTEGER",
        "LastModified": "TEXT",  # when it posted
    },
    "JobCosts": {
        "Company": "TEXT",
        "JobNum": "TEXT",
        "StdLaborCost": "REAL",
        "StdBurdenCost": "REAL",
        "StdMaterialCost": "REAL",
        "ActLaborCost": "REAL",
        "ActBurdenCost": "REAL",
        "ActMaterialCost": "REAL",
        "LastModified": "TEXT",
    },
}

PRIMARY_KEYS = {
    "WorkCenters": ["WorkCenter"],
    "SalesOrders": ["OrderNum"],
    "SalesOrderLines": ["OrderNum", "OrderLine"],
    "Jobs": ["JobNum"],
    "JobOperations": ["JobNum", "OprSeq"],
    "LaborDetails": ["LaborDtlSeq"],
    "JobCosts": ["JobNum"],
}


@dataclass(frozen=True, slots=True)
class PostedLabor:
    """One MES scan as it landed in the ERP."""

    employee_id: str
    job_num: str
    op_seq: int
    work_center: str
    payroll_date: str
    hours: float
    qty_good: int
    qty_scrap: int
    op_complete: bool
    posted_at: datetime


def build_erp(
    generated: GeneratedJobs,
    released_at: Mapping[str, datetime],
    mes_scans: pl.DataFrame,
    reference: ReferenceData,
    mess: ErpMess,
    extracted_at: datetime,
    calendar: ShiftCalendarConfig | None = None,
) -> dict[str, list[Row]]:
    """The ERP's tables as they stand at `extracted_at`.

    `released_at` is when each job was released in the ERP; the plant model
    decides when during 1st shift the planner gets to it.
    """
    parts = {(p.part_id, p.rev): p for p in reference.parts}
    rates = {r.work_center: r for r in reference.rates}
    cost_per_lb = {m.material_id: m.cost_per_lb for m in reference.materials}
    customers = {o.order_num: o.customer_code for o in generated.sales_orders}
    prices = {(p.part_id, p.rev): p.price for p in reference.parts}

    posted = _posted_labor(
        mes_scans, mess, extracted_at, calendar or ShiftCalendarConfig()
    )
    labor_by_op: dict[tuple[str, int], list[PostedLabor]] = defaultdict(list)
    for labor in posted:
        labor_by_op[(labor.job_num, labor.op_seq)].append(labor)

    tables: dict[str, list[Row]] = {name: [] for name in ENTITIES}
    tables["WorkCenters"] = [
        {
            "Company": COMPANY,
            "WorkCenter": r.work_center,
            "LaborRate": r.labor_per_hr,
            "BurdenRate": r.machine_burden_per_hr,
        }
        for r in reference.rates
    ]
    for seq, labor in enumerate(posted, start=FIRST_LABOR_DTL_SEQ):
        tables["LaborDetails"].append(
            {
                "Company": COMPANY,
                "LaborDtlSeq": seq,
                "EmployeeNum": labor.employee_id,
                "JobNum": labor.job_num,
                "OprSeq": labor.op_seq,
                "WorkCenter": labor.work_center,
                "PayrollDate": labor.payroll_date,
                "LaborHrs": labor.hours,
                "LaborQty": labor.qty_good,
                "ScrapQty": labor.qty_scrap,
                "LastModified": _timestamp(labor.posted_at),
            }
        )

    for job in generated.jobs:
        part = parts[(job.part_id, job.rev)]
        released = released_at[job.job_num]
        job_modified = released
        std_labor = std_burden = act_labor = act_burden = 0.0
        final_op: Row = {}

        for op in part.routing:
            rate = rates[op.work_center]
            labor = labor_by_op[(job.job_num, op.op_seq)]
            qty_completed = sum(entry.qty_good for entry in labor)
            scrap_qty = sum(entry.qty_scrap for entry in labor)
            op_modified = max([released, *(entry.posted_at for entry in labor)])
            job_modified = max(job_modified, op_modified)

            std_hours = (op.setup_min + op.run_min_per_piece * job.qty_released) / 60
            # Burden is applied at standard hours earned: the standard setup
            # once the operation has started, plus standard run time for
            # every piece reported. How long it really took never enters.
            earned_hours = (op.setup_min if labor else 0) / 60 + (
                op.run_min_per_piece * (qty_completed + scrap_qty) / 60
            )
            std_labor += std_hours * rate.labor_per_hr
            std_burden += std_hours * rate.machine_burden_per_hr
            act_labor += sum(entry.hours for entry in labor) * rate.labor_per_hr
            act_burden += earned_hours * rate.machine_burden_per_hr

            final_op = {
                "Company": COMPANY,
                "JobNum": job.job_num,
                "OprSeq": op.op_seq,
                "WorkCenter": op.work_center,
                "StdSetupHours": round(op.setup_min / 60, DECIMALS),
                "StdRunHoursPerPiece": round(op.run_min_per_piece / 60, DECIMALS),
                "QtyCompleted": qty_completed,
                "ScrapQty": scrap_qty,
                "OpComplete": any(entry.op_complete for entry in labor),
                "LastModified": _timestamp(op_modified),
            }
            tables["JobOperations"].append(final_op)

        job_complete = bool(final_op["OpComplete"])
        std_material = job.qty_released * part.weight_lb * cost_per_lb[part.material_id]
        tables["Jobs"].append(
            {
                "Company": COMPANY,
                "JobNum": job.job_num,
                "OrderNum": job.order_num,
                "OrderLine": job.line_num,
                "PartNum": job.part_id,
                "RevisionNum": job.rev,
                "ProdQty": job.qty_released,
                "ReleaseDate": job.release_date.isoformat(),
                "ReqDueDate": job.due_date.isoformat(),
                "JobReleased": True,
                "JobComplete": job_complete,
                "CompletedQty": final_op["QtyCompleted"] if job_complete else 0,
                "LastModified": _timestamp(job_modified),
            }
        )
        tables["JobCosts"].append(
            {
                "Company": COMPANY,
                "JobNum": job.job_num,
                "StdLaborCost": round(std_labor, 2),
                "StdBurdenCost": round(std_burden, 2),
                "StdMaterialCost": round(std_material, 2),
                "ActLaborCost": round(act_labor, 2),
                "ActBurdenCost": round(act_burden, 2),
                "ActMaterialCost": round(std_material, 2),  # issued at release
                "LastModified": _timestamp(job_modified),
            }
        )
        tables["SalesOrders"].append(
            {
                "Company": COMPANY,
                "OrderNum": job.order_num,
                "CustomerCode": customers[job.order_num],
                "OrderDate": job.release_date.isoformat(),
                "LastModified": _timestamp(released),
            }
        )
        tables["SalesOrderLines"].append(
            {
                "Company": COMPANY,
                "OrderNum": job.order_num,
                "OrderLine": job.line_num,
                "PartNum": job.part_id,
                "RevisionNum": job.rev,
                "OrderQty": job.qty_ordered,
                "UnitPrice": prices[(job.part_id, job.rev)],
                "LastModified": _timestamp(released),
            }
        )
    return tables


def write_erp(tables: Mapping[str, list[Row]], out_dir: Path) -> dict[Path, int]:
    """Write `erp.db` and `export/<Entity>.jsonl` from the same rows."""
    export_dir = out_dir / "export"
    export_dir.mkdir(parents=True, exist_ok=True)
    db_path = out_dir / "erp.db"
    db_path.unlink(missing_ok=True)

    written: dict[Path, int] = {}
    with sqlite3.connect(db_path) as db:
        for entity, columns in ENTITIES.items():
            rows = tables[entity]
            column_defs = ", ".join(f"{c} {t}" for c, t in columns.items())
            key = ", ".join(PRIMARY_KEYS[entity])
            db.execute(f"CREATE TABLE {entity} ({column_defs}, PRIMARY KEY ({key}))")
            placeholders = ", ".join("?" for _ in columns)
            db.executemany(
                f"INSERT INTO {entity} VALUES ({placeholders})",
                [tuple(row[c] for c in columns) for row in rows],
            )
            path = export_dir / f"{entity}.jsonl"
            with path.open("w", encoding="utf-8") as f:
                for row in rows:
                    f.write(json.dumps(row, separators=(",", ":")) + "\n")
            written[path] = len(rows)
    db.close()
    written[db_path] = sum(len(rows) for rows in tables.values())
    return written


def _posted_labor(
    mes_scans: pl.DataFrame,
    mess: ErpMess,
    extracted_at: datetime,
    calendar: ShiftCalendarConfig,
) -> list[PostedLabor]:
    """MES scans that have posted to the ERP by `extracted_at`, in posting order."""
    delay = timedelta(hours=mess.labor_post_delay_hours)
    posted: list[PostedLabor] = []
    for scan in mes_scans.sort("scan_off", "record_id").iter_rows(named=True):
        posted_at = scan["scan_off"] + delay
        if posted_at > extracted_at:
            continue
        # The labor exception report: nobody works past the end of their shift.
        worked_until = min(scan["scan_off"], _shift_end(scan["scan_on"], calendar))
        hours = (worked_until - scan["scan_on"]).total_seconds() / 3600
        posted.append(
            PostedLabor(
                employee_id=scan["employee_id"],
                job_num=scan["job_num"],
                op_seq=scan["op_seq"],
                work_center=scan["work_center"],
                payroll_date=scan["scan_on"].astimezone(PLANT_TZ).date().isoformat(),
                hours=_round_hours(hours, mess.labor_rounding_hours),
                qty_good=scan["qty_good"],
                qty_scrap=scan["qty_scrap"],
                op_complete=scan["op_complete"],
                posted_at=posted_at,
            )
        )
    return posted


def _shift_end(scan_on: datetime, calendar: ShiftCalendarConfig) -> datetime:
    """The end of the shift a scan started in."""
    local = scan_on.astimezone(PLANT_TZ)
    shift = next(s for s in calendar.shifts if s.start <= local.time() < s.end)
    return datetime.combine(local.date(), shift.end, tzinfo=PLANT_TZ)


def _round_hours(hours: float, increment: float) -> float:
    """Round to the nearest `increment` (halves up); 0 keeps exact hours."""
    if increment > 0:
        hours = math.floor(hours / increment + 0.5) * increment
    return round(hours, DECIMALS)


def _timestamp(moment: datetime) -> str:
    return moment.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
