"""The truth writer: the answer key, on disk (spec §3.6).

The plant simulation records what really happened, in memory. This module
turns that into Parquet tables under `out/truth/` and adds the one thing the
simulation does not record directly: what each operation and job *really*
cost, from actual hours × the rates in spec §2.4.

Only `eval/` and the tests may read these files. The pipelines have to
reconstruct the same numbers from ERP, MES, and machine data, and the eval
scores how close they got.

Column names follow the in-memory records with one exception: interval
bounds are `started_at` / `ended_at` and a cycle's time is `completed_at`,
because `end` and `at` are SQL keywords that DuckDB will not accept unquoted.

Two tables from spec §3.6 are not here yet. `truth_material_consumption`
needs material lots, which arrive with the bad-lot scenario (M6).
`truth_scrap` gets its own shape once scrap can be found later than it was
made (the oven scenario, M3); until then every scrapped piece is already a
row in `truth_cycles` with `good = false` and its cause.
"""

from collections.abc import Iterable, Mapping
from pathlib import Path

import polars as pl

from crestline.reference import ReferenceData

from .records import MachineState, SimulationResult

UTC_TIMESTAMP = pl.Datetime("us", time_zone="UTC")

JOBS_SCHEMA = {
    "job_num": pl.String,
    "order_num": pl.String,
    "part_id": pl.String,
    "rev": pl.String,
    "qty_ordered": pl.Int64,
    "qty_released": pl.Int64,
    "release_date": pl.Date,
    "due_date": pl.Date,
    "released_at": UTC_TIMESTAMP,
    "status": pl.String,
    "started_at": UTC_TIMESTAMP,
    "completed_at": UTC_TIMESTAMP,
    "qty_completed": pl.Int64,
    "qty_scrapped": pl.Int64,
}

OPERATIONS_SCHEMA = {
    "job_num": pl.String,
    "op_seq": pl.Int64,
    "work_center": pl.String,
    "machine_id": pl.String,
    "std_setup_min": pl.Float64,
    "std_run_min_per_piece": pl.Float64,
    "qty_in": pl.Int64,
    "qty_good": pl.Int64,
    "qty_scrap": pl.Int64,
    "queued_at": UTC_TIMESTAMP,
    "setup_started_at": UTC_TIMESTAMP,
    "run_started_at": UTC_TIMESTAMP,
    "completed_at": UTC_TIMESTAMP,
}

MACHINE_STATES_SCHEMA = {
    "machine_id": pl.String,
    "state": pl.String,
    "reason": pl.String,  # downtime reason for DOWN, stop reason for PLANNED_STOP
    "started_at": UTC_TIMESTAMP,
    "ended_at": UTC_TIMESTAMP,
    "shift": pl.String,
    "job_num": pl.String,
    "op_seq": pl.Int64,
    "employee_id": pl.String,
}

CYCLES_SCHEMA = {
    "machine_id": pl.String,
    "job_num": pl.String,
    "op_seq": pl.Int64,
    "completed_at": UTC_TIMESTAMP,
    "good": pl.Boolean,
    "scrap_cause": pl.String,
}

# Table column -> record attribute, where the two differ (see module docstring).
RENAMED_ATTRIBUTES = {"started_at": "start", "ended_at": "end", "completed_at": "at"}

SECONDS = (pl.col("ended_at") - pl.col("started_at")).dt.total_seconds()

# Durations are summed as whole seconds and money is kept to the cent. That
# keeps every total exact: polars sums groups in parallel, and a sum of raw
# floats can come out different in the last bit from one run to the next,
# which would break "same seed, same answer key".
CENTS = 2


def build_truth_tables(
    result: SimulationResult, reference: ReferenceData
) -> dict[str, pl.DataFrame]:
    """The answer key as DataFrames, keyed by table name."""
    jobs = _frame(result.jobs, JOBS_SCHEMA).sort("job_num")
    machine_states = _frame(
        result.machine_states, MACHINE_STATES_SCHEMA, RENAMED_ATTRIBUTES
    ).sort("machine_id", "started_at")
    cycles = _frame(result.cycles, CYCLES_SCHEMA, RENAMED_ATTRIBUTES).sort(
        "machine_id", "completed_at"
    )
    labor = _labor(machine_states, reference)
    operations = _operations(result, machine_states, labor, reference)
    costs = _costs(jobs, operations, cycles, reference)
    return {
        "truth_jobs": jobs,
        "truth_operations": operations,
        "truth_machine_states": machine_states,
        "truth_cycles": cycles,
        "truth_labor": labor,
        "truth_costs": costs,
    }


def write_truth(tables: Mapping[str, pl.DataFrame], out_dir: Path) -> dict[Path, int]:
    """Write each table to `out_dir/<name>.parquet`; return rows written per file.

    Re-running overwrites the same files, so a backfill is safe to repeat.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    written: dict[Path, int] = {}
    for name, table in tables.items():
        path = out_dir / f"{name}.parquet"
        table.write_parquet(path)
        written[path] = table.height
    return written


def _frame(
    records: Iterable[object],
    schema: dict[str, pl.DataType],
    renamed: Mapping[str, str] | None = None,
) -> pl.DataFrame:
    """One row per record, each column read from the attribute of that name."""
    attributes = [(renamed or {}).get(column, column) for column in schema]
    rows = [tuple(getattr(record, a) for a in attributes) for record in records]
    return pl.DataFrame(rows, schema=schema, orient="row")


def _labor(machine_states: pl.DataFrame, reference: ReferenceData) -> pl.DataFrame:
    """Operator time on jobs: every busy stretch with someone at the machine.

    Operators stay on the job through setup, run, and downtime alike (they
    are the ones clearing the jam). NO_OPERATOR downtime is recorded with
    nobody at the machine, so it drops out here and costs no labor.
    """
    work_centers = pl.DataFrame(
        [(wc.machine_id, wc.work_center) for wc in reference.work_centers],
        schema={"machine_id": pl.String, "work_center": pl.String},
        orient="row",
    )
    return (
        machine_states.filter(
            pl.col("job_num").is_not_null() & pl.col("employee_id").is_not_null()
        )
        .join(work_centers, on="machine_id", how="left", validate="m:1")
        .select(
            "employee_id",
            "shift",
            "work_center",
            "machine_id",
            "job_num",
            "op_seq",
            pl.col("state").alias("activity"),
            "started_at",
            "ended_at",
        )
        .sort("employee_id", "started_at")
    )


def _operations(
    result: SimulationResult,
    machine_states: pl.DataFrame,
    labor: pl.DataFrame,
    reference: ReferenceData,
) -> pl.DataFrame:
    """Operations as recorded, plus actual hours and what they really cost.

    Burden is charged for every hour the job held the machine: setup, run,
    and downtime, because a machine that is down on your job is not
    available to anyone else. Labor is charged for the operator's hours
    (`truth_labor`). Both use the work center rates from spec §2.4.
    """
    state_seconds = (
        machine_states.filter(pl.col("job_num").is_not_null())
        .group_by("job_num", "op_seq")
        .agg(
            setup_s=SECONDS.filter(pl.col("state") == MachineState.SETUP).sum(),
            run_s=SECONDS.filter(pl.col("state") == MachineState.RUNNING).sum(),
            down_s=SECONDS.filter(pl.col("state") == MachineState.DOWN).sum(),
        )
    )
    labor_seconds = labor.group_by("job_num", "op_seq").agg(labor_s=SECONDS.sum())
    rates = pl.DataFrame(
        [
            (r.work_center, r.labor_per_hr, r.machine_burden_per_hr)
            for r in reference.rates
        ],
        schema={
            "work_center": pl.String,
            "labor_per_hr": pl.Float64,
            "burden_per_hr": pl.Float64,
        },
        orient="row",
    )
    seconds = ["setup_s", "run_s", "down_s", "labor_s"]
    return (
        _frame(result.operations, OPERATIONS_SCHEMA)
        .join(state_seconds, on=["job_num", "op_seq"], how="left", validate="1:1")
        .join(labor_seconds, on=["job_num", "op_seq"], how="left", validate="1:1")
        .join(rates, on="work_center", how="left", validate="m:1")
        .with_columns(pl.col(seconds).fill_null(0))
        .with_columns(
            setup_hours=pl.col("setup_s") / 3600,
            run_hours=pl.col("run_s") / 3600,
            down_hours=pl.col("down_s") / 3600,
            labor_hours=pl.col("labor_s") / 3600,
            labor_cost=(pl.col("labor_s") / 3600 * pl.col("labor_per_hr")).round(CENTS),
            burden_cost=(
                (pl.col("setup_s") + pl.col("run_s") + pl.col("down_s"))
                / 3600
                * pl.col("burden_per_hr")
            ).round(CENTS),
        )
        .drop(*seconds, "labor_per_hr", "burden_per_hr")
        .sort("job_num", "op_seq")
    )


def _costs(
    jobs: pl.DataFrame,
    operations: pl.DataFrame,
    cycles: pl.DataFrame,
    reference: ReferenceData,
) -> pl.DataFrame:
    """True cost per job: its operations' labor and burden, plus material.

    Hours stay on `truth_operations`; a job's hours are a plain sum there.

    Material is consumed when a blank is cut at the job's first operation,
    whether that piece is later scrapped or not, so the true pounds are the
    pieces actually cut there × part weight. For a finished job that is the
    released quantity. In M1 every pound costs the §2.2 price; lot-specific
    costs arrive with material lots in M6.
    """
    cost_per_lb = {m.material_id: m.cost_per_lb for m in reference.materials}
    parts = pl.DataFrame(
        [
            (p.part_id, p.rev, p.weight_lb, cost_per_lb[p.material_id])
            for p in reference.parts
        ],
        schema={
            "part_id": pl.String,
            "rev": pl.String,
            "weight_lb": pl.Float64,
            "cost_per_lb": pl.Float64,
        },
        orient="row",
    )
    first_ops = operations.group_by("job_num").agg(first_op_seq=pl.col("op_seq").min())
    pieces_cut = (
        cycles.join(first_ops, on="job_num")
        .filter(pl.col("op_seq") == pl.col("first_op_seq"))
        .group_by("job_num")
        .agg(pieces_cut=pl.len())
    )
    operation_totals = operations.group_by("job_num").agg(
        pl.col("labor_cost", "burden_cost").sum().round(CENTS)
    )
    return (
        jobs.select(
            "job_num",
            "part_id",
            "rev",
            "status",
            "qty_ordered",
            "qty_released",
            "qty_completed",
        )
        .join(operation_totals, on="job_num", how="left", validate="1:1")
        .join(pieces_cut, on="job_num", how="left", validate="1:1")
        .join(parts, on=["part_id", "rev"], how="left", validate="m:1")
        # A job released too late to start has no operations and no cycles.
        .with_columns(
            pl.col(pl.Float64).fill_null(0.0), pl.col("pieces_cut").fill_null(0)
        )
        .with_columns(material_lb=(pl.col("pieces_cut") * pl.col("weight_lb")).round(2))
        .with_columns(
            material_cost=(pl.col("material_lb") * pl.col("cost_per_lb")).round(CENTS)
        )
        .with_columns(
            total_cost=(
                pl.col("labor_cost") + pl.col("burden_cost") + pl.col("material_cost")
            ).round(CENTS)
        )
        .drop("pieces_cut", "weight_lb", "cost_per_lb")
        .sort("job_num")
    )
