from datetime import date
from pathlib import Path

import duckdb
import polars as pl
import pytest

from crestline.reference import ReferenceData
from crestline.sim import (
    PlantConfig,
    SimulationResult,
    build_truth_tables,
    generate_jobs,
    simulate,
    write_truth,
)
from crestline.sim.config import DowntimeConfig, DowntimeKind, VariationConfig
from crestline.sim.records import BUSY_STATES, DowntimeReason

FIRST_DAY = date(2026, 7, 6)  # a Monday
LAST_RELEASE = date(2026, 7, 10)
LAST_DAY = date(2026, 7, 17)
SEED = 42
CENT = 0.01  # money is rounded to the cent (halves to even)

TABLES = {
    "truth_jobs",
    "truth_operations",
    "truth_machine_states",
    "truth_cycles",
    "truth_labor",
    "truth_costs",
}


def run(reference_data: ReferenceData, config: PlantConfig | None = None):
    jobs = generate_jobs(reference_data, FIRST_DAY, LAST_RELEASE, SEED)
    return simulate(reference_data, jobs, FIRST_DAY, LAST_DAY, SEED, config=config)


def only_downtime(reason: DowntimeReason) -> DowntimeConfig:
    return DowntimeConfig(
        kinds=[DowntimeKind(reason=reason, weight=1, min_minutes=5, max_minutes=20)]
    )


def rates_by_work_center(
    reference_data: ReferenceData,
) -> dict[str, tuple[float, float]]:
    return {
        r.work_center: (r.labor_per_hr, r.machine_burden_per_hr)
        for r in reference_data.rates
    }


@pytest.fixture(scope="module")
def result(reference_data) -> SimulationResult:
    return run(reference_data)


@pytest.fixture(scope="module")
def tables(reference_data, result) -> dict[str, pl.DataFrame]:
    return build_truth_tables(result, reference_data)


@pytest.fixture(scope="module")
def written(tables, tmp_path_factory) -> dict[Path, int]:
    return write_truth(tables, tmp_path_factory.mktemp("out") / "truth")


def test_writes_one_parquet_file_per_table(written):
    assert {path.name for path in written} == {f"{t}.parquet" for t in TABLES}


def test_every_table_opens_in_duckdb_by_bare_column_name(tables, written):
    for path, rows in written.items():
        columns = ", ".join(tables[path.stem].columns)
        (count,) = duckdb.sql(
            f"select count(*) from (select {columns} from '{path}')"
        ).fetchone()
        assert count == rows > 0


def test_timestamps_are_utc(tables, written):
    for path in written:
        for name, column_type, *_ in duckdb.sql(
            f"describe select * from '{path}'"
        ).fetchall():
            if name.endswith("_at"):
                assert column_type == "TIMESTAMP WITH TIME ZONE", (path.stem, name)
                assert tables[path.stem][name].dtype == pl.Datetime("us", "UTC")


def test_row_counts_match_the_simulation(result, tables):
    assert tables["truth_jobs"].height == len(result.jobs)
    assert tables["truth_operations"].height == len(result.operations)
    assert tables["truth_machine_states"].height == len(result.machine_states)
    assert tables["truth_cycles"].height == len(result.cycles)
    assert tables["truth_labor"].height == sum(
        s.state in BUSY_STATES and s.employee_id is not None
        for s in result.machine_states
    )
    assert tables["truth_costs"].height == len(result.jobs)


def test_same_seed_writes_the_same_tables(reference_data, tables):
    again = build_truth_tables(run(reference_data), reference_data)
    for name, table in tables.items():
        assert again[name].equals(table), name


def test_without_variation_or_downtime_costs_land_on_standard(reference_data):
    config = PlantConfig(
        downtime=DowntimeConfig(mean_run_minutes_between_events=None),
        variation=VariationConfig(
            setup_sigma=0, machine_cycle_sigma=0, manual_cycle_sigma=0
        ),
    )
    ops = build_truth_tables(run(reference_data, config), reference_data)[
        "truth_operations"
    ].filter(pl.col("completed_at").is_not_null())
    rates = rates_by_work_center(reference_data)

    assert ops.height > 0
    for op in ops.iter_rows(named=True):
        std_hours = (
            op["std_setup_min"] + op["std_run_min_per_piece"] * op["qty_in"]
        ) / 60
        labor_rate, burden_rate = rates[op["work_center"]]
        assert op["down_hours"] == 0
        assert op["labor_cost"] == pytest.approx(std_hours * labor_rate, abs=CENT)
        assert op["burden_cost"] == pytest.approx(std_hours * burden_rate, abs=CENT)


@pytest.mark.parametrize(
    ("reason", "operator_on_the_job"),
    [(DowntimeReason.JAM, True), (DowntimeReason.NO_OPERATOR, False)],
)
def test_downtime_costs_labor_only_when_someone_is_there(
    reference_data, reason, operator_on_the_job
):
    config = PlantConfig(downtime=only_downtime(reason))
    ops = build_truth_tables(run(reference_data, config), reference_data)[
        "truth_operations"
    ]
    rates = rates_by_work_center(reference_data)

    assert ops["down_hours"].sum() > 0
    for op in ops.iter_rows(named=True):
        machine_hours = op["setup_hours"] + op["run_hours"] + op["down_hours"]
        labor_hours = machine_hours - (0 if operator_on_the_job else op["down_hours"])
        labor_rate, burden_rate = rates[op["work_center"]]
        assert op["labor_hours"] == pytest.approx(labor_hours)
        assert op["labor_cost"] == pytest.approx(labor_hours * labor_rate, abs=CENT)
        # The machine is tied up either way, so burden always includes downtime.
        assert op["burden_cost"] == pytest.approx(machine_hours * burden_rate, abs=CENT)


def test_job_costs_roll_up_from_operations_plus_material(reference_data, tables):
    ops = tables["truth_operations"]
    costs = tables["truth_costs"]
    per_job = ops.group_by("job_num").agg(
        pl.col("labor_cost").sum(), pl.col("burden_cost").sum()
    )
    joined = costs.join(per_job, on="job_num", how="left", suffix="_ops").fill_null(0)

    for job in joined.iter_rows(named=True):
        assert job["labor_cost"] == pytest.approx(job["labor_cost_ops"])
        assert job["burden_cost"] == pytest.approx(job["burden_cost_ops"])
        assert job["total_cost"] == pytest.approx(
            job["labor_cost"] + job["burden_cost"] + job["material_cost"]
        )


def test_material_is_every_piece_cut_scrapped_or_not(reference_data, tables):
    parts = {(p.part_id, p.rev): p for p in reference_data.parts}
    cost_per_lb = {m.material_id: m.cost_per_lb for m in reference_data.materials}
    complete = tables["truth_costs"].filter(pl.col("status") == "COMPLETE")

    assert complete.height > 0
    assert (complete["qty_completed"] < complete["qty_released"]).any()  # some scrap
    for job in complete.iter_rows(named=True):
        part = parts[(job["part_id"], job["rev"])]
        assert job["material_lb"] == pytest.approx(job["qty_released"] * part.weight_lb)
        assert job["material_cost"] == pytest.approx(
            job["material_lb"] * cost_per_lb[part.material_id], abs=CENT
        )
