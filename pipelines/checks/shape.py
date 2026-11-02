"""The data-shape checks: what each looks at, the SQL it runs, and when it breaches."""

from collections.abc import Callable
from dataclasses import dataclass

from erp_extract.curated import CURATED_DATABASE, column_name
from erp_extract.curated import TABLES as ERP_TABLES
from erp_extract.entities import ENTITIES
from machine_curate.tables import KEY as MACHINE_KEY
from machine_curate.tables import TABLES as MACHINE_TABLES

BASELINE_RUNS = 7  # how many past null rates a column is compared with
NULL_RATE_SLACK = 0.01  # one percentage point above the trailing maximum
BRAKE_TOLERANCE = (2, 0.01)  # strokes or share of the day's pieces, whichever is larger
CENT = 0.005

# Run a statement, return its rows as lists of text (Athena's result cells).
RunSql = Callable[[str], list[list[str | None]]]


@dataclass(frozen=True)
class Breach:
    table: str
    check: str
    detail: str

    def __str__(self) -> str:
        return f"{self.table}: {self.check}: {self.detail}"


@dataclass(frozen=True)
class TableShape:
    table: str  # in crestline_curated
    keys: tuple[str, ...]
    key_columns: tuple[str, ...]  # the columns the queries depend on


def _erp(entity: str, *fields: str) -> TableShape:
    return TableShape(
        ERP_TABLES[entity],
        tuple(column_name(k) for k in ENTITIES[entity][0]),
        tuple(column_name(f) for f in fields),
    )


ERP_SHAPES = [
    _erp("SalesOrderLines", "PartNum", "OrderQty", "UnitPrice"),
    _erp("Jobs", "OrderNum", "PartNum", "ProdQty", "JobComplete"),
    _erp("JobOperations", "WorkCenter", "StdSetupHours", "QtyCompleted"),
    _erp(
        "JobCosts", "StdLaborCost", "ActLaborCost", "ActBurdenCost", "ActMaterialCost"
    ),
    _erp("LaborDetails", "JobNum", "WorkCenter", "LaborHrs", "PayrollDate"),
]
MACHINE_SHAPES = [
    TableShape(MACHINE_TABLES[d], (MACHINE_KEY,), columns)
    for d, columns in {
        "pb01": ("device_ts_utc", "cyc_cnt", "mode", "published_at_utc"),
        "pb02": ("gateway_ts_utc", "cnt", "published_at_utc"),
        "pem01": ("gateway_ts_utc", "cnt", "published_at_utc"),
        "pcline": (
            "gateway_ts_utc",
            "hook_cnt",
            "line_speed_ft_min",
            "published_at_utc",
        ),
        "lsr01": ("decoded", "published_at_utc"),
    }.items()
]


def null_rates_sql(shape: TableShape, database: str = CURATED_DATABASE) -> str:
    counts = ", ".join(f"count_if({c} is null)" for c in shape.key_columns)
    return f"select count(*), {counts} from {database}.{shape.table}"


def duplicate_keys_sql(shape: TableShape, database: str = CURATED_DATABASE) -> str:
    keys = ", ".join(shape.keys)
    return (
        f"select count(*) from (select {keys} from {database}.{shape.table}"
        f" group by {keys} having count(*) > 1)"
    )


def check_null_rates(
    shape: TableShape, row: list[str | None], baseline: dict[str, list[float]]
) -> tuple[list[Breach], dict[str, list[float]]]:
    """Compare this run's null rates with the trailing ones; return breaches and the new baseline."""
    rows = int(row[0] or 0)
    breaches, updated = [], {}
    for column, nulls in zip(shape.key_columns, row[1:], strict=True):
        rate = int(nulls or 0) / rows if rows else 0.0
        past = baseline.get(column, [])
        if past and rate > max(past) + NULL_RATE_SLACK:
            breaches.append(
                Breach(
                    shape.table,
                    "null rate",
                    f"{column} is null in {rate:.1%} of {rows:,} rows "
                    f"(at most {max(past):.1%} in the last {len(past)} runs)",
                )
            )
        updated[column] = (past + [rate])[-BASELINE_RUNS:]
    return breaches, updated


def check_table(
    run_sql: RunSql,
    shape: TableShape,
    baseline: dict[str, list[float]],
    database: str = CURATED_DATABASE,
) -> tuple[list[Breach], dict[str, list[float]]]:
    """Null rates and key uniqueness for one table."""
    breaches, updated = check_null_rates(
        shape, run_sql(null_rates_sql(shape, database))[0], baseline
    )
    duplicates = int(run_sql(duplicate_keys_sql(shape, database))[0][0] or 0)
    if duplicates:
        breaches.append(
            Breach(
                shape.table, "primary key", f"{duplicates:,} keys held more than once"
            )
        )
    return breaches, updated


# -- reconciliation totals ------------------------------------------------------


def erp_labor_sql(database: str = CURATED_DATABASE) -> str:
    """Each job's posted labor at its work centers' rates is its actual labor cost."""
    return f"""
with posted as (
    select d.jobnum, sum(d.laborhrs * w.laborrate) as cost
    from {database}.erp_labor_details as d
    join {database}.erp_work_centers as w on w.workcenter = d.workcenter
    group by d.jobnum
)
select count(*),
       count_if(abs(coalesce(p.cost, 0) - c.actlaborcost) > {CENT}),
       max(abs(coalesce(p.cost, 0) - c.actlaborcost))
from {database}.erp_job_costs as c
left join posted as p on p.jobnum = c.jobnum
"""


def brake_strokes_sql(database: str) -> str:
    """Per plant day: rebuilt brake strokes and the pieces scanned at BRAKE.

    By the day a scan was opened: a forgotten scan is closed at 06:00 the
    next morning, so its pieces would land on the wrong day by scan-off.
    Only days with machine data count: the replay may cover fewer days
    than the MES files. The plant day is machines.sql's idiom (see
    machine_cycles_by_day): through a timestamp, Athena's days are UTC days.
    """
    minimum, share = BRAKE_TOLERANCE
    return f"""
with strokes as (
    select cast(with_timezone(counted_at_utc, 'UTC') at time zone 'America/Denver' as date) as day,
           sum(cycles) as strokes
    from {database}.machine_counts
    where machine_id in ('PB-01', 'PB-02')
    group by 1
),
pieces as (
    select cast(scan_on_local as date) as day, sum(qty_good + qty_scrap) as pieces
    from {database}.mes_labor
    where work_center = 'BRAKE'
    group by 1
)
select count(*),
       count_if(abs(s.strokes - p.pieces) > greatest({minimum}, {share} * p.pieces)),
       max(abs(s.strokes - p.pieces))
from strokes as s
join pieces as p on p.day = s.day
"""


def check_erp_labor(run_sql: RunSql, database: str = CURATED_DATABASE) -> list[Breach]:
    jobs, off, worst = run_sql(erp_labor_sql(database))[0]
    if int(off or 0):
        return [
            Breach(
                "erp_job_costs",
                "reconciliation",
                f"{off} of {jobs} jobs' actual labor cost isn't their posted hours "
                f"at their work centers' rates (worst by ${float(worst):,.2f})",
            )
        ]
    return []


def check_brake_strokes(run_sql: RunSql, database: str) -> list[Breach]:
    days, off, worst = run_sql(brake_strokes_sql(database))[0]
    if int(off or 0):
        return [
            Breach(
                "machine_pb01, machine_pb02",
                "reconciliation",
                f"on {off} of {days} days the brakes' strokes and the pieces scanned "
                f"at BRAKE differ by more than {BRAKE_TOLERANCE[0]} or "
                f"{BRAKE_TOLERANCE[1]:.0%} (worst by {worst})",
            )
        ]
    return []


def run_all(
    run_sql: RunSql,
    shapes: list[TableShape],
    baselines: dict[str, dict[str, list[float]]],
    reconciliations: list[Callable[[RunSql], list[Breach]]],
    database: str = CURATED_DATABASE,
) -> tuple[list[Breach], dict[str, dict[str, list[float]]]]:
    """Every table's checks, then the totals; returns breaches and the new baselines."""
    breaches, updated = [], {}
    for shape in shapes:
        found, updated[shape.table] = check_table(
            run_sql, shape, baselines.get(shape.table, {}), database
        )
        breaches += found
    for reconcile in reconciliations:
        breaches += reconcile(run_sql)
    return breaches, updated
