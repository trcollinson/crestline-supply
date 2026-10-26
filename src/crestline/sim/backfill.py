"""`crestline sim backfill`: simulate a stretch of history and write it out (spec §6).

A backfill is how the data platform gets its history: run the plant over the
last N days, keep the answer key, and write what the MES, the ERP, and the
brake controllers recorded over the same days.

The extract is taken at 06:00 plant time on `--end`, before 1st shift, the
way someone would pull it first thing in the morning. By then the MES's
nightly file for the last day (dropped at 02:15) has landed, but that day's
labor has not posted to the ERP yet: the ERP runs a day behind the MES.
"""

from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path

from crestline.mess import load_mess
from crestline.reference import load_reference_data

from .clock import PLANT_TZ
from .jobs import generate_jobs
from .plant import simulate
from .records import JobStatus
from .scenarios import load_scenarios
from .systems import erp, machines, mes
from .truth import build_truth_tables, write_truth

EXTRACT_LOCAL_TIME = time(6, 0)


def backfill_window(days: int, end: date) -> tuple[date, date]:
    """First and last simulated day for `days` days ending at `end`, exclusive.

    `--days 90 --end 2026-10-01` covers 2026-07-03 through 2026-09-30, so a
    live run that starts on `end` picks up where the backfill stopped and no
    day is simulated twice.
    """
    if days < 1:
        raise ValueError(f"days must be at least 1, got {days}")
    return end - timedelta(days=days), end - timedelta(days=1)


def extraction_time(end: date) -> datetime:
    """06:00 plant time on `end`, in UTC."""
    return datetime.combine(end, EXTRACT_LOCAL_TIME, tzinfo=PLANT_TZ).astimezone(UTC)


@dataclass(frozen=True)
class BackfillSummary:
    first_day: date
    last_day: date
    seed: int
    jobs: int
    jobs_complete: int
    written: dict[Path, int]  # file -> rows


def run_backfill(
    days: int,
    end: date,
    seed: int | None,
    out_dir: Path,
    reference_dir: Path = Path("reference"),
    scenarios_path: Path = Path("scenarios.yaml"),
    mess_path: Path = Path("mess.yaml"),
) -> BackfillSummary:
    reference = load_reference_data(reference_dir)
    scenarios = load_scenarios(scenarios_path)
    mess = load_mess(mess_path)
    seed = mess.seed if seed is None else seed
    first_day, last_day = backfill_window(days, end)
    extracted_at = extraction_time(end)

    jobs = generate_jobs(reference, first_day, last_day, seed)
    result = simulate(reference, jobs, first_day, last_day, seed, scenarios=scenarios)
    truth = build_truth_tables(result, reference)

    written = write_truth(truth, out_dir / "truth")
    _remove_previous_system_outputs(out_dir)

    scans = mes.scan_records(truth, reference, mess.mes, seed)
    written |= mes.write_labor_files(
        scans, first_day, last_day, extracted_at, out_dir / "mes" / "outbound"
    )

    # The ERP gets its own order book and the MES labor, never the simulation.
    released_at = dict(truth["truth_jobs"].select("job_num", "released_at").iter_rows())
    erp_changes = erp.build_erp(
        jobs, released_at, scans, reference, mess.erp, extracted_at, seed=seed
    )
    written |= erp.write_erp(erp_changes, out_dir / "erp")

    written |= machines.write_brake_streams(
        truth, reference, mess.machines, seed, out_dir / "machines"
    )

    return BackfillSummary(
        first_day=first_day,
        last_day=last_day,
        seed=seed,
        jobs=len(result.jobs),
        jobs_complete=sum(j.status == JobStatus.COMPLETE for j in result.jobs),
        written=written,
    )


def _remove_previous_system_outputs(out_dir: Path) -> None:
    """Delete files an earlier backfill wrote, so a shorter re-run leaves none behind.

    Only this command's own file patterns are touched.
    """
    for pattern in (
        "mes/outbound/labor_*.csv",
        "erp/export/*.jsonl",
        "erp/erp.db",
        "machines/pb0*.jsonl",
    ):
        for path in out_dir.glob(pattern):
            path.unlink()
