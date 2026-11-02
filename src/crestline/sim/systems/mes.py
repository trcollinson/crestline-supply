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
that closed that day, in plant local time with no UTC offset, and
`downtime_YYYYMMDD.csv` with the stops someone wrote down that ended that
day. People write down the stops they notice and have time for: a stop
under `mes.min_recorded_downtime_minutes` never (so microstops never are),
a stop of `always_recorded_downtime_minutes` or more always, and the ones in
between most of the time (`downtime_unrecorded_rate` is the share they
skip). Times are typed to the minute, the reason is free text
(`free_text_noise`: `jam`, `JAMMED`, `jam - see Ray`, blank), and
`entered_by` is the operator at the machine.
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
from ..records import DowntimeReason, MachineState

LOCAL_TZ = PLANT_TZ.key
NIGHTLY_EXPORT_TIME = time(2, 15)  # local
LOCAL_TIMESTAMP = "%Y-%m-%d %H:%M:%S"
# The MES has been running for years; its record IDs did not start at 1.
FIRST_RECORD_ID = 4_810_001
FIRST_DOWNTIME_RECORD_ID = 918_001  # the downtime log is its own table

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
DOWNTIME_FILE_COLUMNS = [
    "record_id",
    "record_type",
    "work_center",
    "machine_id",
    "start_time",
    "end_time",
    "reason_text",
    "entered_by",
]

# How people write a reason down, by what really happened. The canonical
# phrase comes first; with `free_text_noise: high` any of them can appear,
# `{name}` is a first name from the roster, and some are left blank.
CANONICAL_REASON = {
    DowntimeReason.JAM: "jam",
    DowntimeReason.TOOLING: "tooling",
    DowntimeReason.MATERIAL_WAIT: "material wait",
    DowntimeReason.MAINTENANCE: "maintenance",
    DowntimeReason.QUALITY_HOLD: "quality hold",
    DowntimeReason.NO_OPERATOR: "no operator",
    DowntimeReason.PROGRAM_ERROR: "program error",
    DowntimeReason.MICROSTOP: "microstop",
}
REASON_PHRASES = {
    DowntimeReason.JAM: ["jam", "JAMMED", "jammed", "Jam", "jam - see {name}", "jammed up", "part jam"],
    DowntimeReason.TOOLING: ["tooling", "die change??", "tool change", "Tooling", "tooling - {name}", "tool chg"],
    DowntimeReason.MATERIAL_WAIT: ["waiting on matl", "no matl", "no material", "waiting for sheets", "out of matl", "matl"],
    DowntimeReason.MAINTENANCE: ["maint", "PM", "maintenance", "maint - hydraulics", "Maint", "scheduled maint"],
    DowntimeReason.QUALITY_HOLD: ["QC hold", "quality hold", "waiting on QC", "hold", "qc", "on hold - {name}"],
    DowntimeReason.NO_OPERATOR: ["no operator", "nobody on machine", "no op", "operator away", "no one here"],
    DowntimeReason.PROGRAM_ERROR: ["program error", "prog err", "bad program", "program wouldn't load", "prog error", "wrong program"],
    DowntimeReason.MICROSTOP: ["stop", "short stop", "quick stop", "?"],
}  # fmt: skip
BLANK_REASON_SHARE = 0.05


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
    """Write the nightly labor files that have landed by `extracted_at`.

    One file per calendar day, weekends included (the export runs every
    night and writes just a header when nobody worked), holding every scan
    that closed that day.
    """
    local = scans.with_columns(
        closed_on=pl.col("scan_off").dt.convert_time_zone(LOCAL_TZ).dt.date(),
        scan_on=_local_text(pl.col("scan_on")),
        scan_off=_local_text(pl.col("scan_off")),
    )
    return _write_nightly_files(
        local, "labor", LABOR_FILE_COLUMNS, first_day, last_day, extracted_at, out_dir
    )


# --- The downtime file --------------------------------------------------------


def merge_stops(machine_states: pl.DataFrame) -> pl.DataFrame:
    """Each unplanned stop as the operator saw it: one row per stop.

    The answer key records a stop in chunks, cut at every break and shift
    end; the person who writes it down sees one stop from when the machine
    quit to when it ran again, break included. Consecutive DOWN chunks of
    one machine, reason, and job, with nothing but planned stops between
    them, are one stop. `employee_id` is the operator on the job; a stop
    with nobody at the machine (NO_OPERATOR) takes the operator nearest in
    time on that machine and shift. The eval scores against these.
    """
    rows = machine_states.sort("machine_id", "started_at").iter_rows(named=True)
    stops: list[dict[str, object]] = []
    current: dict[str, object] | None = None
    for row in rows:
        if row["state"] == MachineState.PLANNED_STOP:
            continue  # a break does not end a stop
        same = (
            current is not None
            and row["state"] == MachineState.DOWN
            and (row["machine_id"], row["reason"], row["job_num"], row["op_seq"])
            == (
                current["machine_id"],
                current["reason"],
                current["job_num"],
                current["op_seq"],
            )
        )
        if same:
            assert current is not None
            current["ended_at"] = row["ended_at"]
            current["employee_id"] = current["employee_id"] or row["employee_id"]
            continue
        if current is not None:
            stops.append(current)
            current = None
        if row["state"] == MachineState.DOWN:
            current = {
                "machine_id": row["machine_id"],
                "reason": row["reason"],
                "job_num": row["job_num"],
                "op_seq": row["op_seq"],
                "shift": row["shift"],
                "started_at": row["started_at"],
                "ended_at": row["ended_at"],
                "employee_id": row["employee_id"],
            }
    if current is not None:
        stops.append(current)

    schema = {
        "machine_id": pl.String,
        "reason": pl.String,
        "job_num": pl.String,
        "op_seq": pl.Int64,
        "shift": pl.String,
        "started_at": pl.Datetime("us", "UTC"),
        "ended_at": pl.Datetime("us", "UTC"),
        "employee_id": pl.String,
    }
    merged = pl.DataFrame(stops, schema=schema, orient="row")
    return _fill_nobody(merged, machine_states).sort("machine_id", "started_at")


def _fill_nobody(stops: pl.DataFrame, machine_states: pl.DataFrame) -> pl.DataFrame:
    """A stop with no operator is written down by the one who was there around it."""
    staffed = (
        machine_states.filter(pl.col("employee_id").is_not_null())
        .with_columns(
            local_date=pl.col("started_at").dt.convert_time_zone(LOCAL_TZ).dt.date()
        )
        .select("machine_id", "shift", "local_date", "started_at", "employee_id")
        .sort("started_at")
    )
    nobody = stops.filter(pl.col("employee_id").is_null()).with_columns(
        local_date=pl.col("started_at").dt.convert_time_zone(LOCAL_TZ).dt.date()
    )
    if nobody.is_empty():
        return stops
    nearest = (
        nobody.drop("employee_id")
        .sort("started_at")
        .join_asof(
            staffed,
            on="started_at",
            by=["machine_id", "shift", "local_date"],
            strategy="nearest",
            check_sortedness=False,  # sorted above; polars can't check within groups
        )
        .drop("local_date")
    )
    return pl.concat(
        [
            stops.filter(pl.col("employee_id").is_not_null()),
            nearest.select(stops.columns),
        ]
    )


def downtime_records(
    truth: Mapping[str, pl.DataFrame],
    reference: ReferenceData,
    mess: MesMess,
    seed: int,
) -> pl.DataFrame:
    """The stops someone wrote down, with the file's columns and the truth's.

    One draw per stop decides whether it is written down and one how, for
    every stop whether or not the thresholds admit it, so changing a
    threshold never changes another stop's fate. `record_id` is assigned in
    the order the stops ended: people log a stop once it is over.
    """
    stops = merge_stops(truth["truth_machine_states"])
    work_center = {wc.machine_id: wc.work_center for wc in reference.work_centers}
    first_names = {}
    for employee in reference.employees:
        first_names.setdefault(employee.work_center, []).append(
            employee.name.split()[0]
        )
    logged = make_rng(seed, stream="mes_downtime")
    wording = make_rng(seed, stream="mes_downtime_text")
    keep: list[bool] = []
    texts: list[str | None] = []
    for row in stops.sort("started_at", "machine_id").iter_rows(named=True):
        minutes = (row["ended_at"] - row["started_at"]).total_seconds() / 60
        skipped = logged.random() < mess.downtime_unrecorded_rate
        keep.append(
            minutes >= mess.always_recorded_downtime_minutes
            or (minutes >= mess.min_recorded_downtime_minutes and not skipped)
        )
        texts.append(
            _reason_text(
                row["reason"],
                first_names[work_center[row["machine_id"]]],
                mess,
                wording,
            )
        )
    recorded = (
        stops.sort("started_at", "machine_id")
        .with_columns(
            recorded=pl.Series(keep, dtype=pl.Boolean),
            reason_text=pl.Series(texts, dtype=pl.String),
        )
        .filter(pl.col("recorded"))
        .drop("recorded")
        .with_columns(
            work_center=pl.col("machine_id").replace_strict(work_center),
            start_time=_minute_text(pl.col("started_at")),
            end_time=_minute_text(pl.col("ended_at")),
            entered_by=pl.col("employee_id"),
            record_type=pl.lit("N"),
        )
        .sort("ended_at", "started_at", "machine_id")
        .with_row_index("record_id", offset=FIRST_DOWNTIME_RECORD_ID)
        .with_columns(pl.col("record_id").cast(pl.Int64))
    )
    return recorded.select(
        *DOWNTIME_FILE_COLUMNS, "started_at", "ended_at", "reason", "job_num", "op_seq"
    )


def _reason_text(reason: str, names: list[str], mess: MesMess, rng) -> str | None:
    """Always drawn, so a stop's wording does not depend on the noise setting."""
    phrase = rng.choice(REASON_PHRASES[reason])
    blank = rng.random() < BLANK_REASON_SHARE
    name = rng.choice(sorted(names))
    if mess.free_text_noise == "none":
        return CANONICAL_REASON[reason]
    if blank:
        return None
    return phrase.replace("{name}", name)


def write_downtime_files(
    records: pl.DataFrame,
    first_day: date,
    last_day: date,
    extracted_at: datetime,
    out_dir: Path,
) -> dict[Path, int]:
    """Write the nightly downtime files that have landed by `extracted_at`.

    A stop goes in the file for the day it ended, on the labor file's
    schedule; weekends get a header.
    """
    local = records.with_columns(
        closed_on=pl.col("ended_at").dt.convert_time_zone(LOCAL_TZ).dt.date()
    )
    return _write_nightly_files(
        local,
        "downtime",
        DOWNTIME_FILE_COLUMNS,
        first_day,
        last_day,
        extracted_at,
        out_dir,
    )


# --- Shared ---------------------------------------------------------------------


def _local_text(column: pl.Expr) -> pl.Expr:
    return column.dt.convert_time_zone(LOCAL_TZ).dt.strftime(LOCAL_TIMESTAMP)


def _minute_text(column: pl.Expr) -> pl.Expr:
    """Local time typed to the nearest minute, as an operator enters it."""
    return (
        (column + pl.duration(seconds=30))
        .dt.truncate("1m")
        .dt.convert_time_zone(LOCAL_TZ)
        .dt.strftime(LOCAL_TIMESTAMP)
    )


def _write_nightly_files(
    local: pl.DataFrame,
    prefix: str,
    columns: list[str],
    first_day: date,
    last_day: date,
    extracted_at: datetime,
    out_dir: Path,
) -> dict[Path, int]:
    """One `<prefix>_YYYYMMDD.csv` per calendar day, dropped at 02:15 the next morning.

    `local` carries `closed_on`, the local date that puts a row in a file.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    written: dict[Path, int] = {}
    day = first_day
    while day <= last_day:
        dropped_at = datetime.combine(
            day + timedelta(days=1), NIGHTLY_EXPORT_TIME, tzinfo=PLANT_TZ
        )
        if dropped_at <= extracted_at:
            rows = local.filter(pl.col("closed_on") == day).select(columns)
            path = out_dir / f"{prefix}_{day:%Y%m%d}.csv"
            rows.write_csv(path)
            written[path] = rows.height
        day += timedelta(days=1)
    return written
