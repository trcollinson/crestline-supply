import csv
import re
from datetime import datetime, time, timedelta

import polars as pl
import pytest

from crestline.mess import MesMess
from crestline.sim.clock import PLANT_TZ
from crestline.sim.systems import mes

LOCAL_TIMESTAMP = re.compile(r"^\d{4}-\d\d-\d\d \d\d:\d\d:\d\d$")


SEED = 42
ON_TIME = MesMess(forgot_scan_off_rate=0)


@pytest.fixture(scope="module")
def scans(plant_run, reference_data) -> pl.DataFrame:
    """Every operator scans off on time, so the checks below can be exact."""
    return mes.scan_records(plant_run.truth, reference_data, ON_TIME, SEED)


@pytest.fixture(scope="module")
def forgetful_scans(plant_run, reference_data) -> pl.DataFrame:
    return mes.scan_records(plant_run.truth, reference_data, MesMess(), SEED)


def local(moment: datetime) -> datetime:
    return moment.astimezone(PLANT_TZ)


def test_one_scan_per_operator_job_operation_and_shift(scans):
    days = scans.with_columns(
        day=pl.col("scan_on").dt.convert_time_zone(mes.LOCAL_TZ).dt.date()
    )
    assert (
        not days.select("employee_id", "job_num", "op_seq", "day").is_duplicated().any()
    )
    assert scans["record_id"].is_unique().all()


def test_scans_cover_every_stretch_of_real_work(plant_run, scans):
    covering = (
        plant_run.truth["truth_labor"]
        .join(scans, on=["employee_id", "job_num", "op_seq"])
        .filter(
            (pl.col("started_at") >= pl.col("scan_on"))
            & (pl.col("ended_at") <= pl.col("scan_off"))
        )
    )
    assert covering.height == plant_run.truth["truth_labor"].height


def test_operators_stay_scanned_on_through_breaks(plant_run, scans):
    scanned = (scans["scan_off"] - scans["scan_on"]).dt.total_seconds().sum()
    labor = plant_run.truth["truth_labor"]
    worked = (labor["ended_at"] - labor["started_at"]).dt.total_seconds().sum()
    assert scanned > worked * 1.05


def test_reported_pieces_match_what_was_made(plant_run, scans):
    reported = scans.group_by("job_num", "op_seq").agg(
        pl.col("qty_good").sum().alias("reported_good"),
        pl.col("qty_scrap").sum().alias("reported_scrap"),
    )
    finished = plant_run.truth["truth_operations"].filter(
        pl.col("completed_at").is_not_null()
    )
    joined = finished.join(reported, on=["job_num", "op_seq"], how="left")

    assert (joined["qty_good"] == joined["reported_good"]).all()
    assert (joined["qty_scrap"] == joined["reported_scrap"]).all()


def test_scan_off_is_completion_or_the_end_of_the_shift(plant_run, scans):
    completed_at = dict(
        plant_run.truth["truth_operations"]
        .select(pl.concat_str("job_num", "op_seq", separator="/"), "completed_at")
        .iter_rows()
    )
    for scan in scans.iter_rows(named=True):
        if scan["op_complete"]:
            key = f"{scan['job_num']}/{scan['op_seq']}"
            assert scan["scan_off"] == completed_at[key]
        else:
            assert local(scan["scan_off"]).time() in {time(14, 30), time(23, 0)}
    assert scans["op_complete"].sum() == (
        plant_run.truth["truth_operations"]["completed_at"].is_not_null().sum()
    )


def test_nightly_files_are_local_time_with_no_offset(plant_run, scans, tmp_path):
    written = mes.write_labor_files(
        scans, plant_run.first_day, plant_run.last_day, plant_run.extracted_at, tmp_path
    )

    days = (plant_run.last_day - plant_run.first_day).days + 1
    assert len(written) == days  # the last day's file landed at 02:15
    assert sum(written.values()) == scans.height
    for path, rows in written.items():
        with path.open(newline="", encoding="utf-8") as f:
            records = list(csv.reader(f))
        assert records[0] == mes.LABOR_FILE_COLUMNS
        assert len(records) == rows + 1
        for record in records[1:]:
            fields = dict(zip(mes.LABOR_FILE_COLUMNS, record, strict=True))
            assert LOCAL_TIMESTAMP.match(fields["scan_on"])
            assert LOCAL_TIMESTAMP.match(fields["scan_off"])
            assert fields["scan_off"].startswith(
                f"{path.stem[6:10]}-{path.stem[10:12]}-{path.stem[12:14]}"
            )

    weekend = tmp_path / "labor_20260711.csv"  # a Saturday
    assert weekend.read_text(encoding="utf-8").splitlines() == [
        ",".join(mes.LABOR_FILE_COLUMNS)
    ]


def test_the_last_file_is_missing_before_the_nightly_export(plant_run, scans, tmp_path):
    before_export = datetime.combine(
        plant_run.last_day + timedelta(days=1), time(1, 0), tzinfo=PLANT_TZ
    )
    written = mes.write_labor_files(
        scans, plant_run.first_day, plant_run.last_day, before_export, tmp_path
    )
    assert f"labor_{plant_run.last_day:%Y%m%d}.csv" not in {p.name for p in written}


def test_forgotten_scans_close_at_six_the_next_working_morning(scans, forgetful_scans):
    joined = scans.join(forgetful_scans, on="record_id", suffix="_forgetful")
    forgotten = joined.filter(pl.col("scan_off") != pl.col("scan_off_forgetful"))

    assert joined.height == scans.height
    assert 0.01 < forgotten.height / scans.height < 0.06  # about 3%
    for scan in forgotten.iter_rows(named=True):
        closed = local(scan["scan_off_forgetful"])
        next_working_day = local(scan["scan_on"]).date() + timedelta(days=1)
        while next_working_day.weekday() >= 5:
            next_working_day += timedelta(days=1)
        assert (closed.date(), closed.time()) == (next_working_day, time(6, 0))
    # The pieces were on the traveler: only the time is wrong.
    for column in ("qty_good", "qty_scrap", "op_complete", "scan_on"):
        assert (joined[column] == joined[f"{column}_forgetful"]).all()


def test_forgotten_scans_land_in_the_file_for_the_day_they_closed(
    plant_run, forgetful_scans, tmp_path
):
    written = mes.write_labor_files(
        forgetful_scans,
        plant_run.first_day,
        plant_run.last_day,
        plant_run.extracted_at,
        tmp_path,
    )
    closed_in_window = forgetful_scans.filter(
        pl.col("scan_off").dt.convert_time_zone(mes.LOCAL_TZ).dt.date()
        <= plant_run.last_day
    )
    # A scan still open when the window ends is in no file yet.
    assert sum(written.values()) == closed_in_window.height
