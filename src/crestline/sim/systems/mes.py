"""The MES: operator scans and the nightly labor files (spec §4.2).

Operators scan onto a job operation when they start work on it and scan off
when it is finished or their shift ends; the next shift's operator scans on
to carry on. At scan-off they key in how many good and scrap pieces they
made. That is all the MES knows. It cannot tell setup from run, it does not
know which of the two brakes the job ran on, and its clock keeps running
through breaks, because nobody scans off to go to lunch.

Operators also forget to scan off (`mes.forgot_scan_off_rate` in
`mess.yaml`). The scan stays open until a supervisor closes it at 06:00 the
next working morning, and the MES records a 10-16 hour "run".

Every night at about 02:15 the MES drops `labor_YYYYMMDD.csv` with the scans
that closed that day, in plant local time with no UTC offset.
"""

from collections.abc import Mapping
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path

import polars as pl

from crestline.mess import MesMess
from crestline.reference import ReferenceData
from crestline.rng import make_rng

from ..clock import PLANT_TZ
from ..config import ShiftCalendarConfig

LOCAL_TZ = PLANT_TZ.key
NIGHTLY_EXPORT_TIME = time(2, 15)  # local
LOCAL_TIMESTAMP = "%Y-%m-%d %H:%M:%S"
# The MES has been running for years; its record IDs did not start at 1.
FIRST_RECORD_ID = 4_810_001

# Spec §4.2, in order.
LABOR_FILE_COLUMNS = [
    "record_id",
    "record_type",
    "employee_id",
    "employee_name",
    "job_num",
    "op_seq",
    "work_center",
    "scan_on",
    "scan_off",
    "qty_good",
    "qty_scrap",
    "notes",
]


def scan_records(
    truth: Mapping[str, pl.DataFrame],
    reference: ReferenceData,
    mess: MesMess,
    seed: int,
    calendar: ShiftCalendarConfig | None = None,
) -> pl.DataFrame:
    """One scan per operator per job operation per shift, times in UTC.

    Besides the labor-file columns this carries `op_complete`: the operator
    marks the operation complete at the final scan-off. The MES passes that
    to the ERP with the labor, but it is not part of the nightly file.
    """
    calendar = calendar or ShiftCalendarConfig()
    shift_ends = pl.DataFrame(
        [(s.name, s.end.hour, s.end.minute) for s in calendar.shifts],
        schema={"shift": pl.String, "end_hour": pl.Int64, "end_minute": pl.Int64},
        orient="row",
    )
    employees = pl.DataFrame(
        [(e.employee_id, e.name) for e in reference.employees],
        schema={"employee_id": pl.String, "employee_name": pl.String},
        orient="row",
    )
    operations = truth["truth_operations"].select(
        "job_num", "op_seq", pl.col("completed_at").alias("op_completed_at")
    )

    # Everything one operator did on one job operation in one shift is one
    # scan: on at their first touch, off when the operation finished or,
    # if it didn't, at the end of their shift.
    scans = (
        truth["truth_labor"]
        .with_columns(
            local_date=pl.col("started_at").dt.convert_time_zone(LOCAL_TZ).dt.date()
        )
        .group_by(
            "employee_id", "job_num", "op_seq", "work_center", "shift", "local_date"
        )
        .agg(scan_on=pl.col("started_at").min(), last_worked=pl.col("ended_at").max())
        .join(operations, on=["job_num", "op_seq"], how="left", validate="m:1")
        .join(shift_ends, on="shift", how="left", validate="m:1")
        .with_columns(
            op_complete=pl.col("op_completed_at")
            .is_between(pl.col("scan_on"), pl.col("last_worked"))
            .fill_null(False),
            shift_end=(
                pl.col("local_date").cast(pl.Datetime("us"))
                + pl.duration(hours=pl.col("end_hour"), minutes=pl.col("end_minute"))
            )
            .dt.replace_time_zone(LOCAL_TZ)
            .dt.convert_time_zone("UTC"),
        )
        .with_columns(
            scan_off=pl.when(pl.col("op_complete"))
            .then(pl.col("op_completed_at"))
            .otherwise(pl.col("shift_end"))
        )
    )

    # Pieces are counted, not timed: what the operator keys in at scan-off.
    session = ["employee_id", "job_num", "op_seq", "scan_on"]
    counts = (
        truth["truth_cycles"]
        .join(scans.select(*session, "scan_off"), on=["job_num", "op_seq"])
        .filter(
            (pl.col("completed_at") > pl.col("scan_on"))
            & (pl.col("completed_at") <= pl.col("scan_off"))
        )
        .group_by(session)
        .agg(
            qty_good=pl.col("good").sum().cast(pl.Int64),
            qty_scrap=(~pl.col("good")).sum().cast(pl.Int64),
        )
    )

    scans = (
        scans.join(counts, on=session, how="left", validate="1:1")
        .join(employees, on="employee_id", how="left", validate="m:1")
        .with_columns(pl.col("qty_good", "qty_scrap").fill_null(0))
        # The MES creates the record when the operator scans on.
        .sort("scan_on", "employee_id", "job_num", "op_seq")
        .with_row_index("record_id", offset=FIRST_RECORD_ID)
        .with_columns(
            pl.col("record_id").cast(pl.Int64),
            record_type=pl.lit("N"),  # "C" corrections arrive with later messes
            notes=pl.lit(None, dtype=pl.String),  # written as an empty field
        )
        .select(*LABOR_FILE_COLUMNS, "op_complete")
    )
    return _forget_some_scan_offs(scans, mess, seed, calendar)


def _forget_some_scan_offs(
    scans: pl.DataFrame, mess: MesMess, seed: int, calendar: ShiftCalendarConfig
) -> pl.DataFrame:
    """Leave a seeded share of scans open until a supervisor closes them.

    The pieces were still written on the job traveler, so the counts are
    right (they were taken at the real scan-off, above). Only the time is
    wrong: the supervisor's cleanup at the start of 1st shift on the next
    working morning closes it, which makes a 2nd-shift scan a 10-16 hour
    run, a 1st-shift one up to a day, and a Friday one a whole weekend.
    """
    rng = make_rng(seed, stream="mes")
    supervisor_closes_at = calendar.shifts[0].start
    scan_offs = []
    for scan_on, scan_off in scans.select("scan_on", "scan_off").iter_rows():
        if rng.random() < mess.forgot_scan_off_rate:
            day = scan_on.astimezone(PLANT_TZ).date() + timedelta(days=1)
            while day.weekday() not in calendar.working_weekdays:
                day += timedelta(days=1)
            scan_off = datetime.combine(
                day, supervisor_closes_at, tzinfo=PLANT_TZ
            ).astimezone(UTC)
        scan_offs.append(scan_off)
    return scans.with_columns(
        pl.Series("scan_off", scan_offs, dtype=pl.Datetime("us", "UTC"))
    )


def write_labor_files(
    scans: pl.DataFrame,
    first_day: date,
    last_day: date,
    extracted_at: datetime,
    out_dir: Path,
) -> dict[Path, int]:
    """Write the nightly files that have landed by `extracted_at`.

    One file per calendar day, weekends included (the export runs every
    night and writes just a header when nobody worked), holding every scan
    that closed that day.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    local = scans.with_columns(
        closed_on=pl.col("scan_off").dt.convert_time_zone(LOCAL_TZ).dt.date(),
        scan_on=pl.col("scan_on")
        .dt.convert_time_zone(LOCAL_TZ)
        .dt.strftime(LOCAL_TIMESTAMP),
        scan_off=pl.col("scan_off")
        .dt.convert_time_zone(LOCAL_TZ)
        .dt.strftime(LOCAL_TIMESTAMP),
    )
    written: dict[Path, int] = {}
    day = first_day
    while day <= last_day:
        dropped_at = datetime.combine(
            day + timedelta(days=1), NIGHTLY_EXPORT_TIME, tzinfo=PLANT_TZ
        )
        if dropped_at <= extracted_at:
            rows = local.filter(pl.col("closed_on") == day).select(LABOR_FILE_COLUMNS)
            path = out_dir / f"labor_{day:%Y%m%d}.csv"
            rows.write_csv(path)
            written[path] = rows.height
        day += timedelta(days=1)
    return written
