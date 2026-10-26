"""The ERP: orders, jobs, and standard costing (spec §4.1).

The ERP knows its own sales orders and jobs, the standard routing for every
part, and the rates in spec §2.4. Everything it knows about the shop floor
arrives as labor the MES posts to it: hours, pieces, and "operation
complete". This module never sees the plant simulation, only those inputs.

How a mid-market ERP costs a job, and why it hides the PB-02 problem:

- **Standard cost** is the routing: standard setup + run hours for the job
  quantity, × the labor and burden rates, plus standard material.
- **Actual labor** is the MES labor, one row per scan, rounded to 0.1 hr
  and posted by a nightly batch on the next working day (both settings in
  `mess.yaml`). Before it posts, a supervisor works the daily labor
  exception report: a scan that runs past the end of its shift was left
  open, so it is cut back to the shift end. Every plant does this by hand;
  it stops a forgotten scan-off from posting a 16-hour run, but it still
  over-charges the job for the rest of that shift.
- **Actual burden is applied at standard hours**: each operation earns its
  standard setup plus standard run time for the pieces reported. Machine
  time beyond standard never shows up, so a setup that takes 2.8× as long
  costs the same burden as one that runs to standard.
- **Actual material** is issued at standard cost when the job is released.
- **Released quantity**: the planner releases the order plus a 5% scrap
  allowance, so `ProdQty` on the job is a little over `OrderQty` on the
  order line. Standard cost and material are for `ProdQty`; the invoice is
  for `OrderQty`.

What changes in the ERP, and when (its change log):

- **Order entry.** Sales enters the order a few working days before the
  planner gets to it. The order line creates the job: its routing is copied
  from the part, its standard cost is known, and its sheet stock is required
  but not yet issued.
- **Job release.** The planner releases the job and issues its material at
  standard cost.
- **The nightly labor batch.** Each working night the ERP posts the closed
  scans of earlier days: one `LaborDetails` row per scan, then new versions
  of the operations, job, and costs those scans touched.

Master data (work centers, customers, parts, revisions) was set up before
the window and never changes in it.

What the ERP gets wrong (each a knob in `mess.yaml`, so each can be off):

- **Duplicate orders.** A clerk enters an order twice. Next morning the
  copy is cancelled: its order line is soft-deleted (`Deleted`), and its
  unreleased job, operations, material, and costs are deleted outright.
- **Job splits.** After release the planner splits a job into J and J-1 and
  shares the quantity out. The floor keeps running the lot under J, so all
  the labor is reported there and the child is left open with none.
- **Stale `LastModified`.** An operation's quantities can change, and a
  labor row can post, without the stamp moving. A load by `LastModified`
  misses both. The store's own `SysValidFrom` still says when they changed.

`build_erp` returns that log in time order. The ERP as it stood at any
instant is the log replayed up to then, which is what the API serves as of
its clock. The store keeps every version of every row: `erp.db` has the
current rows per entity (what the ERP lists today) and a history table of
superseded versions, the way system-versioned tables keep them. The current
rows are also written as one JSONL file per entity, which is what the
backfill uploads, since Athena cannot read SQLite. True/false columns are
declared `BOOLEAN` in SQLite: it still stores 0 and 1, but the declared type
tells the API to serve `true`, so the API reads the store's own schema and
never this module.
"""

import itertools
import json
import math
import sqlite3
from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path

import polars as pl

from crestline.mess import ErpMess
from crestline.reference import ReferenceData
from crestline.reference.models import Part, Rate
from crestline.rng import make_rng

from ..clock import PLANT_TZ
from ..config import ShiftCalendarConfig
from ..jobs import GeneratedJobs, Job, SalesOrder, SalesOrderLine

COMPANY = "CRE"
# ERP numeric fields carry 8 decimal places (spec §4.1).
DECIMALS = 8
# The ERP has been in use for years; its sequence numbers did not start at 1.
FIRST_LABOR_DTL_SEQ = 1_207_001
# Every job issues one material, its sheet stock.
MATERIAL_SEQ = 10
FAMILY_DESCRIPTIONS = {
    "ENC": "Electrical enclosure",
    "BRK": "Mounting bracket",
    "PNL": "Custom panel",
}
# Duplicate entries are numbered from a block of their own, so the order and
# job numbers of real work don't move when the knob does.
DUPLICATE_FIRST_SEQ = 900_001
# A split-off job is numbered after its parent.
SPLIT_SUFFIX = "-1"
# The store's own columns, not part of the API: when a version of a row
# began, and (history tables only) when the next version replaced it.
SYS_VALID_FROM = "SysValidFrom"
SYS_VALID_TO = "SysValidTo"
HISTORY_SUFFIX = "_History"

Row = dict[str, object]
Key = tuple[object, ...]

# Entity -> column -> SQLite type. Column order is the order in both outputs.
# DATE and TIMESTAMP columns hold ISO 8601 text (SQLite keeps it as text); the
# declared type tells the API which values it may format as dates.
# Master data (work centers, customers, parts) carries no LastModified: it was
# set up before the window and never changes in it.
ENTITIES: dict[str, dict[str, str]] = {
    # The rates behind standard costing (spec §2.4), per work center.
    "WorkCenters": {
        "Company": "TEXT",
        "WorkCenter": "TEXT",
        "LaborRate": "REAL",  # $/hr
        "BurdenRate": "REAL",  # machine burden $/hr
    },
    "Customers": {
        "Company": "TEXT",
        "CustomerCode": "TEXT",
        "Name": "TEXT",
    },
    # What the plant makes (type M, sold by the piece) and the sheet stock it
    # buys (type P, by the pound), in one table as most ERPs keep them.
    "Parts": {
        "Company": "TEXT",
        "PartNum": "TEXT",
        "Description": "TEXT",
        "PartType": "TEXT",  # M = manufactured, P = purchased
        "Family": "TEXT",  # ENC, BRK, PNL; none for purchased
        "UOM": "TEXT",  # EA or LB
        "UnitPrice": "REAL",  # selling price, manufactured parts
        "UnitCost": "REAL",  # standard $/lb, purchased parts
    },
    "PartRevisions": {
        "Company": "TEXT",
        "PartNum": "TEXT",
        "RevisionNum": "TEXT",
        "Approved": "BOOLEAN",
        "MtlPartNum": "TEXT",  # the sheet stock it is cut from
        "MtlQtyPerPiece": "REAL",  # lb
    },
    "SalesOrders": {
        "Company": "TEXT",
        "OrderNum": "TEXT",
        "CustomerCode": "TEXT",
        "OrderDate": "DATE",
        "LastModified": "TIMESTAMP",
    },
    "SalesOrderLines": {
        "Company": "TEXT",
        "OrderNum": "TEXT",
        "OrderLine": "INTEGER",
        "PartNum": "TEXT",
        "RevisionNum": "TEXT",
        "OrderQty": "INTEGER",
        "UnitPrice": "REAL",
        "Deleted": "BOOLEAN",  # soft delete: the line was cancelled
        "LastModified": "TIMESTAMP",
    },
    "Jobs": {
        "Company": "TEXT",
        "JobNum": "TEXT",
        "OrderNum": "TEXT",
        "OrderLine": "INTEGER",
        "PartNum": "TEXT",
        "RevisionNum": "TEXT",
        "ProdQty": "INTEGER",  # released: OrderQty plus the scrap allowance
        "ReleaseDate": "DATE",
        "ReqDueDate": "DATE",
        "JobReleased": "BOOLEAN",  # false from order entry until the planner releases it
        "JobComplete": "BOOLEAN",
        "CompletedQty": "INTEGER",  # posted only when the job completes
        "LastModified": "TIMESTAMP",
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
        "OpComplete": "BOOLEAN",
        "LastModified": "TIMESTAMP",
    },
    # Material required by the job from order entry, issued at standard cost
    # when it is released.
    "JobMaterials": {
        "Company": "TEXT",
        "JobNum": "TEXT",
        "MtlSeq": "INTEGER",
        "MtlPartNum": "TEXT",
        "RequiredQty": "REAL",  # lb, for ProdQty
        "IssuedQty": "REAL",  # lb
        "UnitCost": "REAL",  # standard $/lb
        "IssuedComplete": "BOOLEAN",
        "LastModified": "TIMESTAMP",
    },
    "LaborDetails": {
        "Company": "TEXT",
        "LaborDtlSeq": "INTEGER",
        "EmployeeNum": "TEXT",
        "JobNum": "TEXT",
        "OprSeq": "INTEGER",
        "WorkCenter": "TEXT",
        "PayrollDate": "DATE",
        "LaborHrs": "REAL",
        "LaborQty": "INTEGER",
        "ScrapQty": "INTEGER",
        "LastModified": "TIMESTAMP",  # when it posted
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
        "LastModified": "TIMESTAMP",
    },
}

PRIMARY_KEYS = {
    "WorkCenters": ["WorkCenter"],
    "Customers": ["CustomerCode"],
    "Parts": ["PartNum"],
    "PartRevisions": ["PartNum", "RevisionNum"],
    "SalesOrders": ["OrderNum"],
    "SalesOrderLines": ["OrderNum", "OrderLine"],
    "Jobs": ["JobNum"],
    "JobOperations": ["JobNum", "OprSeq"],
    "JobMaterials": ["JobNum", "MtlSeq"],
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
    closed_at: datetime  # when the scan closed, a day or so before it posted


@dataclass(frozen=True, slots=True)
class Change:
    """One change the ERP made: the whole row as it stood afterwards.

    A deleted change removes the row; `row` is the version it removes, which
    is all the key needs.
    """

    at: datetime | None  # UTC; None for master data, in the ERP since go-live
    entity: str
    row: Row
    deleted: bool = False


@dataclass(frozen=True)
class ErpChanges:
    """The ERP's change log: master data first, then every change in time order.

    Changes at the same instant (a batch) keep the order they were made in.
    """

    changes: list[Change]

    def as_of(self, at: datetime | None = None) -> dict[str, list[Row]]:
        """The ERP's tables after every change up to and including `at` (None: all).

        Rows come in the order the ERP created them.
        """
        current, _ = _replay(
            c for c in self.changes if at is None or c.at is None or c.at <= at
        )
        return {
            entity: [change.row for change in versions.values()]
            for entity, versions in current.items()
        }


@dataclass(frozen=True)
class _Rules:
    """The reference data costing needs, and how this ERP stamps its changes."""

    parts: Mapping[tuple[str, str], Part]
    rates: Mapping[str, Rate]
    cost_per_lb: Mapping[str, float]
    # False: the stamp stays put when that change is made (`lastmodified_skips`).
    stamp_ops_on_qty: bool = True
    stamp_labor_on_post: bool = True


@dataclass(frozen=True)
class _Split:
    """A job the planner split after release: the parent keeps part of the quantity."""

    at: datetime
    parent: Job  # with the quantity it keeps
    child: Job


def build_erp(
    generated: GeneratedJobs,
    released_at: Mapping[str, datetime],
    mes_scans: pl.DataFrame,
    reference: ReferenceData,
    mess: ErpMess,
    extracted_at: datetime,
    calendar: ShiftCalendarConfig | None = None,
    *,
    seed: int,
) -> ErpChanges:
    """The ERP's change log up to `extracted_at`; nothing later has happened yet.

    `released_at` is when each job was released in the ERP; the plant model
    decides when during 1st shift the planner gets to it. `seed` drives the
    ERP's own mistakes (duplicate orders, splits), each on a stream of its
    own, so turning one on or off moves nothing else.
    """
    rules = _Rules(
        parts={(p.part_id, p.rev): p for p in reference.parts},
        rates={r.work_center: r for r in reference.rates},
        cost_per_lb={m.material_id: m.cost_per_lb for m in reference.materials},
        stamp_ops_on_qty="JobOperations.qty" not in mess.lastmodified_skips,
        stamp_labor_on_post="LaborDetails.post" not in mess.lastmodified_skips,
    )
    orders = {o.order_num: o for o in generated.sales_orders}
    entered_at = {
        job.job_num: orders[job.order_num].entered_at for job in generated.jobs
    }
    posted = _posted_labor(mes_scans, mess, calendar or ShiftCalendarConfig())
    splits = _split_plans(generated, released_at, mess.job_split_rate, seed)

    # Each kind of change is built on its own, then the kinds are merged by
    # time. The sort is stable, so a batch's rows stay together, in order.
    timed = (
        _order_entries(generated, rules)
        + _duplicate_orders(generated, rules, mess.soft_delete_rate, seed)
        + _releases(generated, entered_at, released_at, rules)
        + _split_changes(splits, entered_at, posted, rules)
        + _labor_batches(posted, generated, entered_at, splits, rules)
    )
    timed = [change for change in timed if change.at <= extracted_at]
    timed.sort(key=lambda change: change.at)
    return ErpChanges(_master_data(reference) + timed)


def write_erp(changes: ErpChanges, out_dir: Path) -> dict[Path, int]:
    """Write `erp.db` (every version of every row) and `export/<Entity>.jsonl`.

    Each entity is two tables. `Jobs` holds the current rows, each with
    `SysValidFrom`, the moment that version began; `Jobs_History` holds every
    superseded version with `SysValidFrom` and `SysValidTo`, the moment the
    next version replaced it. Master data has `SysValidFrom` NULL (in the
    ERP since go-live) and an empty history. The export is the current rows
    without the `Sys` columns. The count reported for `erp.db` is the number
    of versions.
    """
    export_dir = out_dir / "export"
    export_dir.mkdir(parents=True, exist_ok=True)
    db_path = out_dir / "erp.db"
    db_path.unlink(missing_ok=True)

    current, superseded = _replay(changes.changes)
    history: dict[str, list[tuple]] = {entity: [] for entity in ENTITIES}
    for change, replaced_at in superseded:
        history[change.entity].append((*_values(change), _stamp(replaced_at)))

    written: dict[Path, int] = {}
    with sqlite3.connect(db_path) as db:
        for entity, columns in ENTITIES.items():
            rows = list(current[entity].values())
            column_defs = ", ".join(f"{c} {t}" for c, t in columns.items())
            key = ", ".join(PRIMARY_KEYS[entity])
            db.execute(
                f"CREATE TABLE {entity} ({column_defs}, {SYS_VALID_FROM} TEXT,"
                f" PRIMARY KEY ({key}))"
            )
            placeholders = ", ".join("?" for _ in range(len(columns) + 1))
            db.executemany(
                f"INSERT INTO {entity} VALUES ({placeholders})",
                [_values(change) for change in rows],
            )
            history_table = f"{entity}{HISTORY_SUFFIX}"
            db.execute(
                f"CREATE TABLE {history_table} ({column_defs},"
                f" {SYS_VALID_FROM} TEXT, {SYS_VALID_TO} TEXT)"
            )
            db.execute(
                f"CREATE INDEX {history_table}_Key"
                f" ON {history_table} ({key}, {SYS_VALID_FROM})"
            )
            db.executemany(
                f"INSERT INTO {history_table} VALUES ({placeholders}, ?)",
                history[entity],
            )
            path = export_dir / f"{entity}.jsonl"
            with path.open("w", encoding="utf-8") as f:
                for change in rows:
                    f.write(json.dumps(change.row, separators=(",", ":")) + "\n")
            written[path] = len(rows)
    db.close()
    written[db_path] = sum(not change.deleted for change in changes.changes)
    return written


def _replay(
    changes: Iterable[Change],
) -> tuple[dict[str, dict[Key, Change]], list[tuple[Change, datetime | None]]]:
    """Apply changes in order.

    Returns the current version of every row (per entity, in the order the
    rows were first created) and every superseded or deleted version with
    the moment the next one replaced it, or it was deleted.
    """
    current: dict[str, dict[Key, Change]] = {entity: {} for entity in ENTITIES}
    superseded: list[tuple[Change, datetime | None]] = []
    for change in changes:
        versions = current[change.entity]
        key = tuple(change.row[k] for k in PRIMARY_KEYS[change.entity])
        if change.deleted:
            superseded.append((versions.pop(key), change.at))
            continue
        if key in versions:
            superseded.append((versions[key], change.at))
        versions[key] = change  # a dict keeps the key's first position
    return current, superseded


def _master_data(reference: ReferenceData) -> list[Change]:
    """What was in the ERP before the window: rates, customers, parts."""
    rows: dict[str, list[Row]] = {
        "WorkCenters": [
            {
                "Company": COMPANY,
                "WorkCenter": r.work_center,
                "LaborRate": r.labor_per_hr,
                "BurdenRate": r.machine_burden_per_hr,
            }
            for r in reference.rates
        ],
        "Customers": [
            {"Company": COMPANY, "CustomerCode": c.code, "Name": c.name}
            for c in reference.customers
        ],
        "Parts": _parts(reference),
        "PartRevisions": [
            {
                "Company": COMPANY,
                "PartNum": p.part_id,
                "RevisionNum": p.rev,
                "Approved": True,
                "MtlPartNum": p.material_id,
                "MtlQtyPerPiece": p.weight_lb,
            }
            for p in reference.parts
        ],
    }
    return [Change(None, entity, row) for entity, rows in rows.items() for row in rows]


def _order_entries(generated: GeneratedJobs, rules: _Rules) -> list[Change]:
    """Sales enters each order; its line creates the job, routed and costed, unreleased."""
    lines = {line.order_num: line for line in generated.sales_order_lines}
    jobs = {job.order_num: job for job in generated.jobs}
    return [
        change
        for order in generated.sales_orders
        for change in _entry(
            order, lines[order.order_num], jobs[order.order_num], rules
        )
    ]


def _entry(
    order: SalesOrder, line: SalesOrderLine, job: Job, rules: _Rules
) -> list[Change]:
    """An order entered at `order.entered_at`: the order, its line, and the job."""
    at = order.entered_at
    job_row, ops, material, costs = _job_state(job, rules, at, False, {}, at)
    return [
        Change(
            at,
            "SalesOrders",
            {
                "Company": COMPANY,
                "OrderNum": order.order_num,
                "CustomerCode": order.customer_code,
                "OrderDate": order.order_date.isoformat(),
                "LastModified": _timestamp(at),
            },
        ),
        Change(
            at,
            "SalesOrderLines",
            {
                "Company": COMPANY,
                "OrderNum": line.order_num,
                "OrderLine": line.line_num,
                "PartNum": line.part_id,
                "RevisionNum": line.rev,
                "OrderQty": line.qty,
                "UnitPrice": rules.parts[(line.part_id, line.rev)].price,
                "Deleted": False,
                "LastModified": _timestamp(at),
            },
        ),
        Change(at, "Jobs", job_row),
        *(Change(at, "JobOperations", op) for op in ops),
        Change(at, "JobMaterials", material),
        Change(at, "JobCosts", costs),
    ]


def _duplicate_orders(
    generated: GeneratedJobs, rules: _Rules, rate: float, seed: int
) -> list[Change]:
    """Clerks enter some orders twice; the copy is cancelled the next morning.

    The line is soft-deleted, the way an ERP keeps a cancelled line. Its job
    was never released, so the job, its operations, its material, and its
    costs are deleted outright. The plant never sees any of it.
    """
    rng = make_rng(seed, "soft_delete")
    lines = {line.order_num: line for line in generated.sales_order_lines}
    jobs = {job.order_num: job for job in generated.jobs}
    changes: list[Change] = []
    duplicates = 0
    for order in generated.sales_orders:
        # The same draws for every order, so a higher rate only adds duplicates.
        hit, minutes_later, cancel_minutes = (
            rng.random(),
            rng.uniform(5, 180),
            rng.uniform(0, 120),
        )
        if hit >= rate:
            continue
        seq = DUPLICATE_FIRST_SEQ + duplicates
        duplicates += 1
        entered = order.entered_at + timedelta(seconds=int(minutes_later * 60))
        copy = order.model_copy(
            update={
                "order_num": f"S{seq:06d}",
                "order_date": entered.astimezone(PLANT_TZ).date(),
                "entered_at": entered,
            }
        )
        line = lines[order.order_num].model_copy(update={"order_num": copy.order_num})
        job = jobs[order.order_num].model_copy(
            update={"job_num": f"J{seq:06d}", "order_num": copy.order_num}
        )
        cancelled = _next_working_morning(entered) + timedelta(
            seconds=int(cancel_minutes * 60)
        )
        for change in _entry(copy, line, job, rules):
            changes.append(change)
            if change.entity == "SalesOrderLines":
                cancelled_line = change.row | {
                    "Deleted": True,
                    "LastModified": _timestamp(cancelled),
                }
                changes.append(Change(cancelled, change.entity, cancelled_line))
            elif change.entity != "SalesOrders":
                changes.append(
                    Change(cancelled, change.entity, change.row, deleted=True)
                )
    return changes


def _next_working_morning(moment: datetime) -> datetime:
    """08:00 plant time on the next weekday after `moment`, in UTC."""
    day = moment.astimezone(PLANT_TZ).date() + timedelta(days=1)
    while day.weekday() >= 5:
        day += timedelta(days=1)
    return datetime.combine(day, time(8, 0), tzinfo=PLANT_TZ).astimezone(UTC)


def _releases(
    generated: GeneratedJobs,
    entered_at: Mapping[str, datetime],
    released_at: Mapping[str, datetime],
    rules: _Rules,
) -> list[Change]:
    """The planner releases the job and issues its sheet stock."""
    changes: list[Change] = []
    for job in generated.jobs:
        at = released_at[job.job_num]
        job_row, _, material, costs = _job_state(
            job, rules, entered_at[job.job_num], True, {}, at
        )
        changes.append(Change(at, "Jobs", job_row))
        changes.append(Change(at, "JobMaterials", material))
        changes.append(Change(at, "JobCosts", costs))
    return changes


def _split_plans(
    generated: GeneratedJobs,
    released_at: Mapping[str, datetime],
    rate: float,
    seed: int,
) -> dict[str, _Split]:
    """Which jobs the planner splits, when, and how the quantity is shared.

    A split comes one to four hours after release and moves a quarter to a
    half of the quantity to the new job.
    """
    rng = make_rng(seed, "job_split")
    plans: dict[str, _Split] = {}
    for job in generated.jobs:
        # The same draws for every job, so a higher rate only adds splits.
        hit, hours, share = rng.random(), rng.uniform(1, 4), rng.uniform(0.25, 0.5)
        if hit >= rate:
            continue
        child_qty = min(max(1, round(job.qty_released * share)), job.qty_released - 1)
        plans[job.job_num] = _Split(
            at=released_at[job.job_num] + timedelta(seconds=int(hours * 3600)),
            parent=job.model_copy(
                update={"qty_released": job.qty_released - child_qty}
            ),
            child=job.model_copy(
                update={
                    "job_num": job.job_num + SPLIT_SUFFIX,
                    "qty_released": child_qty,
                }
            ),
        )
    return plans


def _split_changes(
    splits: Mapping[str, _Split],
    entered_at: Mapping[str, datetime],
    posted: list[PostedLabor],
    rules: _Rules,
) -> list[Change]:
    """The parent is recosted for the quantity it kept; the child is created, released."""
    changes: list[Change] = []
    for job_num, split in splits.items():
        at = split.at
        # Labor already posted when the split is made (only if it posts at scan-off).
        so_far: dict[tuple[str, int], list[PostedLabor]] = defaultdict(list)
        for labor in posted:
            if labor.job_num == job_num and labor.posted_at <= at:
                so_far[(labor.job_num, labor.op_seq)].append(labor)
        job_row, _, material, costs = _job_state(
            split.parent, rules, entered_at[job_num], True, so_far, at
        )
        changes += [
            Change(at, "Jobs", job_row),
            Change(at, "JobMaterials", material),
            Change(at, "JobCosts", costs),
        ]
        job_row, ops, material, costs = _job_state(split.child, rules, at, True, {}, at)
        changes.append(Change(at, "Jobs", job_row))
        changes += [Change(at, "JobOperations", op) for op in ops]
        changes += [Change(at, "JobMaterials", material), Change(at, "JobCosts", costs)]
    return changes


def _labor_batches(
    posted: list[PostedLabor],
    generated: GeneratedJobs,
    entered_at: Mapping[str, datetime],
    splits: Mapping[str, _Split],
    rules: _Rules,
) -> list[Change]:
    """Each posting: the labor rows, then the operations, jobs, and costs they touched.

    With the nightly batch there are a few dozen postings in a 90-day window;
    with labor posting at scan-off there are thousands of one-scan postings,
    which still takes well under a second.
    """
    jobs = {job.job_num: job for job in generated.jobs}
    labor_by_op: dict[tuple[str, int], list[PostedLabor]] = defaultdict(list)
    changes: list[Change] = []
    numbered = enumerate(posted, start=FIRST_LABOR_DTL_SEQ)
    for at, group in itertools.groupby(numbered, key=lambda item: item[1].posted_at):
        batch = list(group)
        # The whole batch lands before any job is recosted, so each job's new
        # version is costed from everything posted so far.
        for seq, labor in batch:
            labor_by_op[(labor.job_num, labor.op_seq)].append(labor)
            changes.append(Change(at, "LaborDetails", _labor_row(seq, labor, rules)))
        touched_ops = {(labor.job_num, labor.op_seq) for _, labor in batch}
        for job_num in dict.fromkeys(labor.job_num for _, labor in batch):
            split = splits.get(job_num)
            job = split.parent if split and split.at <= at else jobs[job_num]
            job_row, ops, _, costs = _job_state(
                job, rules, entered_at[job_num], True, labor_by_op, at
            )
            # Only the operations this batch posted to changed; the others
            # would be the same row again.
            changes += [
                Change(at, "JobOperations", op)
                for op in ops
                if (job_num, op["OprSeq"]) in touched_ops
            ]
            changes.append(Change(at, "Jobs", job_row))
            changes.append(Change(at, "JobCosts", costs))
    return changes


def _job_state(
    job: Job,
    rules: _Rules,
    entered: datetime,
    issued: bool,
    labor_by_op: Mapping[tuple[str, int], list[PostedLabor]],
    now: datetime,
) -> tuple[Row, list[Row], Row, Row]:
    """A job's rows (job, operations, material, costs) as of `now`.

    `issued` is whether the job has been released and its material issued.
    The rows are always costed afresh from the whole labor list posted so
    far, never updated in place, so a job's last version is exactly what one
    costing pass over all of its labor gives. Everything is stamped `now`,
    except an operation whose stamp the ERP leaves alone when its quantities
    change (`lastmodified_skips`): that keeps the time it was created.
    """
    part = rules.parts[(job.part_id, job.rev)]
    std_labor = std_burden = act_labor = act_burden = 0.0
    ops: list[Row] = []

    for op in part.routing:
        rate = rules.rates[op.work_center]
        labor = labor_by_op.get((job.job_num, op.op_seq), [])
        qty_completed = sum(entry.qty_good for entry in labor)
        scrap_qty = sum(entry.qty_scrap for entry in labor)

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

        ops.append(
            {
                "Company": COMPANY,
                "JobNum": job.job_num,
                "OprSeq": op.op_seq,
                "WorkCenter": op.work_center,
                "StdSetupHours": round(op.setup_min / 60, DECIMALS),
                "StdRunHoursPerPiece": round(op.run_min_per_piece / 60, DECIMALS),
                "QtyCompleted": qty_completed,
                "ScrapQty": scrap_qty,
                "OpComplete": any(entry.op_complete for entry in labor),
                "LastModified": _timestamp(
                    now if labor and rules.stamp_ops_on_qty else entered
                ),
            }
        )

    final_op = ops[-1]
    job_complete = bool(final_op["OpComplete"])
    material_lb = job.qty_released * part.weight_lb
    std_material = material_lb * rules.cost_per_lb[part.material_id]
    material: Row = {
        "Company": COMPANY,
        "JobNum": job.job_num,
        "MtlSeq": MATERIAL_SEQ,
        "MtlPartNum": part.material_id,
        "RequiredQty": round(material_lb, DECIMALS),
        "IssuedQty": round(material_lb, DECIMALS) if issued else 0.0,
        "UnitCost": rules.cost_per_lb[part.material_id],
        "IssuedComplete": issued,
        "LastModified": _timestamp(now),
    }
    job_row: Row = {
        "Company": COMPANY,
        "JobNum": job.job_num,
        "OrderNum": job.order_num,
        "OrderLine": job.line_num,
        "PartNum": job.part_id,
        "RevisionNum": job.rev,
        "ProdQty": job.qty_released,
        "ReleaseDate": job.release_date.isoformat(),
        "ReqDueDate": job.due_date.isoformat(),
        "JobReleased": issued,
        "JobComplete": job_complete,
        "CompletedQty": final_op["QtyCompleted"] if job_complete else 0,
        "LastModified": _timestamp(now),
    }
    costs: Row = {
        "Company": COMPANY,
        "JobNum": job.job_num,
        "StdLaborCost": round(std_labor, 2),
        "StdBurdenCost": round(std_burden, 2),
        "StdMaterialCost": round(std_material, 2),
        "ActLaborCost": round(act_labor, 2),
        "ActBurdenCost": round(act_burden, 2),
        "ActMaterialCost": round(std_material, 2) if issued else 0.0,
        "LastModified": _timestamp(now),
    }
    return job_row, ops, material, costs


def _labor_row(seq: int, labor: PostedLabor, rules: _Rules) -> Row:
    # The stamp is when the row posted, unless this ERP leaves it at the time
    # the scan closed (`lastmodified_skips`), a day or so earlier.
    stamped = labor.posted_at if rules.stamp_labor_on_post else labor.closed_at
    return {
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
        "LastModified": _timestamp(stamped),
    }


def _parts(reference: ReferenceData) -> list[Row]:
    """Manufactured parts, then the purchased sheet stock they are cut from."""
    made: list[Row] = [
        {
            "Company": COMPANY,
            "PartNum": p.part_id,
            "Description": FAMILY_DESCRIPTIONS[p.family],
            "PartType": "M",
            "Family": p.family,
            "UOM": "EA",
            "UnitPrice": p.price,
            "UnitCost": None,
        }
        for p in reference.parts
    ]
    bought: list[Row] = [
        {
            "Company": COMPANY,
            "PartNum": m.material_id,
            "Description": m.description,
            "PartType": "P",
            "Family": None,
            "UOM": "LB",
            "UnitPrice": None,
            "UnitCost": m.cost_per_lb,
        }
        for m in reference.materials
    ]
    return made + bought


def _posted_labor(
    mes_scans: pl.DataFrame,
    mess: ErpMess,
    calendar: ShiftCalendarConfig,
) -> list[PostedLabor]:
    """Every MES scan as it posts to the ERP, in posting order."""
    posted: list[PostedLabor] = []
    for scan in mes_scans.sort("scan_off", "record_id").iter_rows(named=True):
        payroll_date = scan["scan_on"].astimezone(PLANT_TZ).date()
        # The labor exception report: nobody works past the end of their shift.
        worked_until = min(scan["scan_off"], _shift_end(scan["scan_on"], calendar))
        hours = (worked_until - scan["scan_on"]).total_seconds() / 3600
        posted.append(
            PostedLabor(
                employee_id=scan["employee_id"],
                job_num=scan["job_num"],
                op_seq=scan["op_seq"],
                work_center=scan["work_center"],
                payroll_date=payroll_date.isoformat(),
                hours=_round_hours(hours, mess.labor_rounding_hours),
                qty_good=scan["qty_good"],
                qty_scrap=scan["qty_scrap"],
                op_complete=scan["op_complete"],
                posted_at=_posting_time(
                    payroll_date, scan["scan_off"], mess.labor_post_time, calendar
                ),
                closed_at=scan["scan_off"],
            )
        )
    # A stable sort, so each batch keeps its scans in scan-off order.
    return sorted(posted, key=lambda labor: labor.posted_at)


def _posting_time(
    payroll_date: date,
    scan_off: datetime,
    post_time: time | None,
    calendar: ShiftCalendarConfig,
) -> datetime:
    """When a scan's labor posts to the ERP.

    The nightly batch posts a day's labor on the next working day, after the
    supervisor has worked that day's exception report. A scan must be closed
    to post: one forgotten on a Friday is closed at 06:00 Monday and posts
    Monday night with the rest of Friday's labor. With no batch time, labor
    posts the moment the scan closes.
    """
    if post_time is None:
        return scan_off
    day = payroll_date + timedelta(days=1)
    while True:
        batch = datetime.combine(day, post_time, tzinfo=PLANT_TZ)
        if day.weekday() in calendar.working_weekdays and batch >= scan_off:
            return batch.astimezone(UTC)
        day += timedelta(days=1)


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


def _values(change: Change) -> tuple:
    """A version as a store row: the entity's columns, then when it began."""
    return (*(change.row[c] for c in ENTITIES[change.entity]), _stamp(change.at))


def _stamp(moment: datetime | None) -> str | None:
    return None if moment is None else _timestamp(moment)


def _timestamp(moment: datetime) -> str:
    return moment.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
