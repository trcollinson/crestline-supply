from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import polars as pl
import pytest

from crestline.reference import ReferenceData, load_reference_data
from crestline.sim import (
    GeneratedJobs,
    Scenarios,
    build_truth_tables,
    generate_jobs,
    simulate,
)
from crestline.sim.backfill import backfill_window, extraction_time

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
