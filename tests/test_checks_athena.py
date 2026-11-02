"""The data-shape checks on Athena: each passes on the real data and trips on a planted breach.

Planted in copies, never in the real tables: each test copies what its
check reads into the fixture database (`crestline_fixtures`) with one
fault planted, runs the check against the copy, and drops the copies.
The real curated tables are only read.

Opt-in, because it runs in AWS: `pytest -m athena`. Needs the stacks, the
90-day replay curated, and `crestline athena setup` run.
"""

import pytest

from checks import shape
from checks.aws import athena_sql
from crestline.athena import Athena
from crestline.stack import foundation_outputs

pytestmark = pytest.mark.athena

CURATED = "crestline_curated"


@pytest.fixture(scope="module")
def athena() -> Athena:
    return Athena.from_stack()


@pytest.fixture(scope="module")
def run(athena):
    return athena_sql(athena.client, athena.workgroup)


@pytest.fixture(scope="module")
def fixtures() -> str:
    return foundation_outputs()["FixtureDatabaseName"]


@pytest.fixture
def copies(athena, fixtures):
    """Copy tables into the fixture database; dropped after the test.

    A copy's timestamps are cast to milliseconds: Athena writes no finer
    (a microsecond column, as Iceberg's are, is refused).
    """
    made: list[str] = []

    def copy(name: str, select: str) -> None:
        athena.execute(f"drop table if exists {fixtures}.{name}")
        athena.execute(
            f"create table {fixtures}.{name} with (format = 'PARQUET') as {select}"
        )
        made.append(name)

    yield copy
    for name in made:
        athena.execute(f"drop table if exists {fixtures}.{name}")


def baseline_of(run, s: shape.TableShape) -> dict[str, list[float]]:
    """The real table's own null rates, as a baseline."""
    _, rates = shape.check_null_rates(s, run(shape.null_rates_sql(s))[0], {})
    return rates


def test_every_table_passes_on_the_real_data(run):
    for s in shape.ERP_SHAPES + shape.MACHINE_SHAPES:
        breaches, _ = shape.check_table(run, s, baseline_of(run, s))
        assert breaches == [], s.table
    assert shape.check_erp_labor(run) == []
    assert (
        shape.check_brake_strokes(run, foundation_outputs()["GlueDatabaseName"]) == []
    )


def test_a_column_going_null_trips_the_null_rate(run, copies, fixtures):
    pb01 = next(s for s in shape.MACHINE_SHAPES if s.table == "machine_pb01")
    baseline = baseline_of(run, pb01)
    # Planted: a tenth of the rows lose their stroke counter.
    copies(
        "machine_pb01",
        f"""select message_id, cast(device_ts_utc as timestamp(3)) as device_ts_utc, mode,
                   cast(published_at_utc as timestamp(3)) as published_at_utc,
                   case when rand() < 0.1 then null else cyc_cnt end as cyc_cnt
            from {CURATED}.machine_pb01""",
    )
    [breach], _ = shape.check_table(run, pb01, baseline, fixtures)
    assert breach.check == "null rate" and "cyc_cnt" in breach.detail


def test_a_key_held_twice_trips_the_primary_key(run, copies, fixtures):
    jobs = next(s for s in shape.ERP_SHAPES if s.table == "erp_jobs")
    columns = ", ".join(jobs.keys + jobs.key_columns)
    # Planted: one job twice.
    copies(
        "erp_jobs",
        f"""select {columns} from {CURATED}.erp_jobs
            union all
            (select {columns} from {CURATED}.erp_jobs order by jobnum limit 1)""",
    )
    breaches, _ = shape.check_table(run, jobs, baseline_of(run, jobs), fixtures)
    assert [b.check for b in breaches] == ["primary key"]


def test_a_job_whose_labor_does_not_cost_out_trips_the_erp_total(run, copies, fixtures):
    copies(
        "erp_work_centers",
        f"select workcenter, laborrate from {CURATED}.erp_work_centers",
    )
    copies(
        "erp_labor_details",
        f"select jobnum, workcenter, laborhrs from {CURATED}.erp_labor_details",
    )
    # Planted: one job's actual labor cost $25 off what its hours say.
    copies(
        "erp_job_costs",
        f"""select jobnum,
                   actlaborcost + case when jobnum = (select min(jobnum) from {CURATED}.erp_job_costs)
                                       then 25 else 0 end as actlaborcost
            from {CURATED}.erp_job_costs""",
    )
    [breach] = shape.check_erp_labor(run, fixtures)
    assert "1 of" in breach.detail and "$25.00" in breach.detail


def test_strokes_missing_from_a_day_trip_the_brake_total(run, copies, fixtures):
    raw = foundation_outputs()["GlueDatabaseName"]
    copies(
        "mes_labor",
        f"select scan_on_local, work_center, qty_good, qty_scrap from {raw}.mes_labor",
    )
    # Planted: a day's brake strokes lost, as if a counter had stopped reporting.
    copies(
        "machine_counts",
        f"""select machine_id, cast(counted_at_utc as timestamp(3)) as counted_at_utc,
                   case when date(counted_at_utc) = date '2026-08-05' then 0 else cycles end as cycles
            from {raw}.machine_counts""",
    )
    [breach] = shape.check_brake_strokes(run, fixtures)
    assert "on 1 of" in breach.detail or "on 2 of" in breach.detail
