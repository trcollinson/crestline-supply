from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import polars as pl
import pytest

from crestline.mess import ErpMess, MesMess
from crestline.reference import ReferenceData, load_reference_data
from crestline.sim import (
    GeneratedJobs,
    Scenarios,
    build_truth_tables,
    generate_jobs,
    simulate,
)
from crestline.sim.backfill import backfill_window, extraction_time
from crestline.sim.systems import erp, mes

REFERENCE_DIR = Path(__file__).resolve().parent.parent / "reference"


@pytest.fixture
def fake_aws_credentials(monkeypatch, tmp_path):
    """Fake keys, and no access to the real AWS config or profile."""
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    monkeypatch.setenv("AWS_CONFIG_FILE", str(tmp_path / "no-config"))
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(tmp_path / "no-credentials"))
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-west-2")


@pytest.fixture(scope="session")
def reference_data() -> ReferenceData:
    return load_reference_data(REFERENCE_DIR)


@dataclass(frozen=True)
class PlantRun:
    """A short backfill's worth of plant history, for the system tests."""

    first_day: date
    last_day: date
    extracted_at: datetime
    jobs: GeneratedJobs
    truth: dict[str, pl.DataFrame]


@pytest.fixture(scope="session")
def plant_run(reference_data) -> PlantRun:
    end = date(2026, 7, 18)  # a Saturday: Mon Jul 6 .. Fri Jul 17
    first_day, last_day = backfill_window(12, end)
    jobs = generate_jobs(reference_data, first_day, last_day, seed=42)
    result = simulate(
        reference_data, jobs, first_day, last_day, seed=42, scenarios=Scenarios()
    )
    return PlantRun(
        first_day=first_day,
        last_day=last_day,
        extracted_at=extraction_time(end),
        jobs=jobs,
        truth=build_truth_tables(result, reference_data),
    )


@pytest.fixture(scope="session")
def scans(plant_run, reference_data) -> pl.DataFrame:
    """The plant run's MES scans, with the default messes."""
    return mes.scan_records(plant_run.truth, reference_data, MesMess(), seed=42)


# The M1 ERP: no duplicate orders, no splits, honest `LastModified`. The tests
# of costing and of the change log run on it; each ERP mess has its own tests
# that turn it on.
M1_ERP = ErpMess(soft_delete_rate=0, job_split_rate=0, lastmodified_skips=[])


@pytest.fixture(scope="session")
def erp_mess() -> Callable[..., ErpMess]:
    """The M1 ERP's settings, with any changes: `erp_mess(labor_post_time=None)`."""
    return lambda **changes: M1_ERP.model_copy(update=changes)


@pytest.fixture(scope="session")
def build_erp_changes(
    plant_run, reference_data, scans
) -> Callable[..., erp.ErpChanges]:
    """The plant run's ERP change log, with other settings or an earlier extract."""
    released_at = dict(
        plant_run.truth["truth_jobs"].select("job_num", "released_at").iter_rows()
    )

    def build(
        mess: ErpMess | None = None, extracted_at: datetime | None = None
    ) -> erp.ErpChanges:
        return erp.build_erp(
            plant_run.jobs,
            released_at,
            scans,
            reference_data,
            mess or M1_ERP,
            extracted_at or plant_run.extracted_at,
            seed=42,
        )

    return build


@pytest.fixture(scope="session")
def erp_changes(build_erp_changes) -> erp.ErpChanges:
    """The plant run's ERP change log, with the default messes."""
    return build_erp_changes()
