"""Scoring the brief's backtest against the answer key (M5 step 5).

Read by `brief_backtest.py score`; it needs only what `run` and `narrate`
left on disk and `<out>/truth/`.

**Issues, not mornings.** PB-02's second shift looks more profitable in the
ERP than it is on every morning of the window, so counting alert-mornings
would score one problem sixty times. An issue is an unbroken run of plant
days on which the same rule fires on the same subject, which is what the
brief's ledger calls one alert ("new", then "still open since"). Each issue
is judged once, on the plant day it opened, when the brief called it new.

**Verdicts, with no model judging:**
- *true, scenario*: the answer key's planted story. Laser scrap on a plant
  day with `NOZZLE_WEAR` pieces; PB-02 2nd shift's margin, with the answer
  key's setups on those jobs over 1.5 times standard.
- *true, real event*: the answer key crosses the same threshold, measured
  against the platform's own baseline, and the cause is something that
  happened (a stop, a work center starved of work); the reason names it.
- *false*: the answer key doesn't cross it (the platform's number was
  wrong: the reason says which way, and why where the data can tell), or it
  does but only by the plant's ordinary chance (setup and random scrap on a
  day's pieces: real, but nothing to act on), or a Saturday's one shift.

The rules are re-applied in Python to the rows the SQL's rules read, so a
threshold can be swept without a query; `rule_mismatches` checks they give
the SQL's own alerts at the shipped thresholds, morning for morning.
"""

import csv
import json
import re
from dataclasses import asdict, dataclass, field, replace
from datetime import UTC, date, datetime, time
from decimal import Decimal
from math import sqrt
from pathlib import Path
from zoneinfo import ZoneInfo

import duckdb
import polars as pl
import yaml

from brief_backtest import facts_dir, mornings, rows_dir
from crestline.athena import split_statements
from crestline.brief.check import _rounded
from crestline.brief.morning import BASELINE_WORKING_DAYS, UNIT_DECIMALS, MorningFacts

PLANT_TZ = ZoneInfo("America/Denver")
SHIFT_START = time(6, 0)  # first shift: when a brief's reader can act
SCENARIO_SETUP_RATIO = 1.5  # PB-02 2nd shift's true setups, as a multiple of standard
WEAR = "NOZZLE_WEAR"
FAMILIES = ("scrap", "stops", "output", "margin")
RULE_METRIC = {
    "scrap": "scrap_above_baseline",
    "stops": "stop_minutes_above_usual",
    "output": "output_below_usual",
    "margin": "erp_margin_above_rebuilt",
}
FAMILY_OF = {metric: family for family, metric in RULE_METRIC.items()}

# The sweeps: each rule's threshold over a range around the shipped value.
SWEEPS = {
    "scrap": [("scrap_standard_errors", k) for k in (2, 2.5, 3, 3.5, 4, 5, 6)],
    "stops": [
        (("stop_minutes_factor", f), ("stop_minutes_floor", m))
        for f in (1.5, 2, 3, 4)
        for m in (30, 60, 90, 120)
    ],
    "output": [("output_low_share", s) for s in (0.2, 0.3, 0.4, 0.5, 0.6)],
    "margin": [("margin_gap_points", p) for p in (1, 2, 3, 4, 5)],
}


@dataclass(frozen=True)
class Thresholds:
    scrap_standard_errors: float
    stop_minutes_factor: float
    stop_minutes_floor: float
    output_low_share: float
    margin_gap_points: float
    margin_min_jobs: float


def shipped(queries_dir: Path = Path("queries")) -> Thresholds:
    """brief.sql's thresholds, read from its own view."""
    sql = (queries_dir / "brief.sql").read_text(encoding="utf-8")
    [statement] = [
        s
        for s in split_statements(sql)
        if s.startswith("create or replace view brief_thresholds ")
    ]
    con = duckdb.connect()
    con.execute(statement)
    return Thresholds(
        **con.sql("select * from brief_thresholds").pl().row(0, named=True)
    )


# --- What the backtest left -------------------------------------------------------


@dataclass
class Backtest:
    out_dir: Path
    mornings: list[date]  # the mornings with a plant day, in order
    plant_day: dict[date, date]
    sql_alerts: dict[date, set[str]]  # the SQL's own anomaly fact IDs
    rows: dict[str, pl.DataFrame]  # each rule view, every morning, with `morning`


def load(out_dir: Path) -> Backtest:
    plant_day: dict[date, date] = {}
    sql_alerts: dict[date, set[str]] = {}
    frames: dict[str, list[pl.DataFrame]] = {}
    for morning in mornings(out_dir):
        path = facts_dir(out_dir) / f"{morning.isoformat()}.json"
        if not path.exists():
            raise SystemExit(f"no facts for {morning}: run `brief_backtest.py run`")
        found = MorningFacts.from_json(path.read_text(encoding="utf-8"))
        if found.plant_day is None:
            continue
        plant_day[morning] = found.plant_day
        sql_alerts[morning] = set(found.anomaly_ids)
        for view in (rows_dir(out_dir) / morning.isoformat()).glob("*.parquet"):
            frames.setdefault(view.stem, []).append(
                pl.read_parquet(view).with_columns(
                    morning=pl.lit(morning), plant_day=pl.lit(found.plant_day)
                )
            )
    rows = {view: pl.concat(f, how="diagonal_relaxed") for view, f in frames.items()}
    return Backtest(out_dir, sorted(plant_day), plant_day, sql_alerts, rows)


# --- The rules, in Python ---------------------------------------------------------


ALERT_SCHEMA = {
    "morning": pl.Date,
    "plant_day": pl.Date,
    "fact_id": pl.String,
    "family": pl.String,
    "subject": pl.String,
    "value": pl.Float64,
}


def _alert_frame(
    frame: pl.DataFrame, family: str, subject: str, value: str
) -> pl.DataFrame:
    return frame.select(
        "morning",
        "plant_day",
        fact_id=pl.lit("anomaly:")
        + pl.col(subject)
        + pl.lit(f":{RULE_METRIC[family]}"),
        family=pl.lit(family),
        subject=pl.col(subject),
        value=pl.col(value).cast(pl.Float64),
    )


def alerts(bt: Backtest, t: Thresholds, frozen_scrap: bool = False) -> pl.DataFrame:
    """Every alert the rules raise at these thresholds, one row per morning."""
    scrap = (
        _frozen_scrap(bt, t.scrap_standard_errors)
        if frozen_scrap
        else _alert_frame(
            bt.rows["brief_scrap"].filter(
                pl.col("standard_errors") > t.scrap_standard_errors
            ),
            "scrap",
            "machine_id",
            "standard_errors",
        )
    )
    stops = bt.rows["brief_stops"].filter(
        (pl.col("stop_minutes") > t.stop_minutes_factor * pl.col("usual_stop_minutes"))
        & (pl.col("stop_minutes") >= t.stop_minutes_floor)
    )
    output = bt.rows["brief_output"].filter(
        pl.col("pieces") < t.output_low_share * pl.col("usual_pieces")
    )
    margin = bt.rows["brief_margin"].filter(
        (pl.col("subject") != "all jobs")
        & (pl.col("gap_points") > t.margin_gap_points)
        & (pl.col("jobs") >= t.margin_min_jobs)
    )
    return pl.concat(
        [
            scrap.cast(ALERT_SCHEMA),
            _alert_frame(stops, "stops", "machine_id", "stop_minutes").cast(
                ALERT_SCHEMA
            ),
            _alert_frame(output, "output", "work_center", "pieces").cast(ALERT_SCHEMA),
            _alert_frame(margin, "margin", "subject", "gap_points").cast(ALERT_SCHEMA),
        ]
    ).sort("morning", "fact_id")


def standard_errors(rate: float, baseline: float | None, pieces: int) -> float | None:
    """brief_scrap's measure: how far a rate on `pieces` is from a baseline rate."""
    if baseline is None or not pieces or baseline <= 0 or baseline >= 1:
        return None
    return (rate - baseline) / sqrt(baseline * (1 - baseline) / pieces)


def _frozen_scrap(bt: Backtest, k: float) -> pl.DataFrame:
    """The scrap rule with its baseline frozen while an alert is open.

    The trailing baseline takes in the failure it is measuring: by Aug 20 the
    laser's had risen from about 1.6% to 2.6%. Frozen, an open alert keeps
    being measured against the baseline it opened against (the 20 working
    days before the first day it fired), until a morning it doesn't fire.
    """
    index = {m: i for i, m in enumerate(bt.mornings)}
    fired = []
    for (machine,), group in bt.rows["brief_scrap"].group_by("machine_id"):
        frozen, last = None, None
        for row in group.sort("morning").iter_rows(named=True):
            if last is None or index[row["morning"]] != index[last] + 1:
                frozen = None  # a morning with no row ends an alert
            last = row["morning"]
            baseline = frozen if frozen is not None else row["baseline_rate"]
            se = standard_errors(row["rate"], baseline, row["pieces"])
            if se is not None and se > k:
                frozen = baseline
                fired.append({**row, "standard_errors": se, "machine_id": machine})
            else:
                frozen = None
    frame = (
        pl.DataFrame(fired, schema=bt.rows["brief_scrap"].schema)
        if fired
        else (bt.rows["brief_scrap"].clear())
    )
    return _alert_frame(frame, "scrap", "machine_id", "standard_errors")


def first_scored(bt: Backtest) -> date:
    """The first morning whose plant day has a full baseline behind it: 20
    working days. Alerts before it are the warm-up, counted apart."""
    return bt.mornings[BASELINE_WORKING_DAYS]


def scored_alerts(found: pl.DataFrame, bt: Backtest) -> pl.DataFrame:
    """Alerts from the first scored morning on. An issue open since the warm-up
    is judged on that morning, the first a full baseline could raise it."""
    return found.filter(pl.col("morning") >= first_scored(bt))


def rule_mismatches(bt: Backtest, t: Thresholds) -> list[str]:
    """Mornings where the Python rules and the SQL's alerts disagree."""
    found = alerts(bt, t)
    python = {
        m: set(found.filter(pl.col("morning") == m)["fact_id"]) for m in bt.mornings
    }
    return [
        f"{m}: SQL {sorted(bt.sql_alerts[m] - python[m])}, Python {sorted(python[m] - bt.sql_alerts[m])}"
        for m in bt.mornings
        if python[m] != bt.sql_alerts[m]
    ]


def issues(found: pl.DataFrame, all_mornings: list[date]) -> pl.DataFrame:
    """Alerts grouped into issues: a run of consecutive mornings, per fact ID."""
    index = pl.DataFrame(
        {"morning": all_mornings, "i": range(len(all_mornings))},
        schema={"morning": pl.Date, "i": pl.Int64},
    )
    return (
        found.join(index, on="morning")
        .sort("fact_id", "i")
        .with_columns(
            run=(pl.col("i").diff().over("fact_id").fill_null(0) != 1)
            .cum_sum()
            .over("fact_id")
        )
        .group_by("fact_id", "run")
        .agg(
            pl.first("family"),
            pl.first("subject"),
            opened_morning=pl.min("morning"),
            opened_day=pl.min("plant_day"),
            last_day=pl.max("plant_day"),
            mornings=pl.len(),
            value=pl.first("value"),
        )
        .drop("run")
        .sort("opened_day", "fact_id")
    )


# --- The answer key ---------------------------------------------------------------


def _local_day(column: str) -> pl.Expr:
    return pl.col(column).dt.convert_time_zone("America/Denver").dt.date()


@dataclass
class Truth:
    scrap: pl.DataFrame  # machine-day: pieces, scrapped, and by cause
    pieces: pl.DataFrame  # work center-day: pieces made
    down: pl.DataFrame  # machine-day: minutes down in stops over 5 minutes, top reason
    states: pl.DataFrame  # every interval, with minutes
    operations: pl.DataFrame
    costs: pl.DataFrame
    cycles: pl.DataFrame
    events: pl.DataFrame
    labor_files: pl.DataFrame = field(
        default_factory=pl.DataFrame
    )  # every MES scan, with its file's date


def truth(out_dir: Path) -> Truth:
    folder = out_dir / "truth"
    cycles = pl.read_parquet(folder / "truth_cycles.parquet").with_columns(
        day=_local_day("completed_at")
    )
    operations = pl.read_parquet(folder / "truth_operations.parquet")
    work_centers = operations.select("work_center", "machine_id").unique()
    scrap = cycles.group_by("machine_id", "day").agg(
        pieces=pl.len(),
        scrapped=(~pl.col("good")).sum(),
        wear=(pl.col("scrap_cause") == WEAR).sum(),
        setup=(pl.col("scrap_cause") == "SETUP_SCRAP").sum(),
        random=(pl.col("scrap_cause") == "RANDOM").sum(),
    )
    pieces = (
        cycles.join(work_centers, on="machine_id")
        .group_by("work_center", "day")
        .agg(pieces=pl.len())
    )
    states = pl.read_parquet(folder / "truth_machine_states.parquet").with_columns(
        minutes=(pl.col("ended_at") - pl.col("started_at")).dt.total_seconds() / 60,
        day=_local_day("started_at"),
    )
    events_path = folder / "truth_events.parquet"
    events = pl.read_parquet(events_path) if events_path.exists() else pl.DataFrame()
    return Truth(
        scrap=scrap,
        pieces=pieces,
        down=_true_down(states),
        states=states,
        operations=operations,
        costs=pl.read_parquet(folder / "truth_costs.parquet"),
        cycles=cycles,
        events=events,
        labor_files=_labor_files(out_dir),
    )


def _true_down(states: pl.DataFrame) -> pl.DataFrame:
    """Minutes down per machine-day, counting only stops over 5 minutes.

    The answer key cuts a stop at every break; the operator (and the
    platform's 5-minute cut) sees one stop. Consecutive DOWN chunks of one
    machine, reason, and job, with only planned stops between, are one stop,
    as `merge_stops` has it.
    """
    worked = states.filter(pl.col("state") != "PLANNED_STOP").sort(
        "machine_id", "started_at"
    )
    key = ["machine_id", "state", "reason", "job_num", "op_seq"]
    changed = pl.any_horizontal(
        [pl.col(c).ne_missing(pl.col(c).shift(1).over("machine_id")) for c in key]
    )
    stops = (
        worked.with_columns(stop=changed.cum_sum())
        .filter(pl.col("state") == "DOWN")
        .with_columns(stop_minutes=pl.sum("minutes").over("stop"))
        .filter(pl.col("stop_minutes") > 5)
    )
    by_reason = stops.group_by("machine_id", "day", "reason").agg(pl.sum("minutes"))
    return by_reason.group_by("machine_id", "day").agg(
        down_minutes=pl.sum("minutes"),
        reasons=pl.struct("reason", "minutes").sort_by("minutes", descending=True),
    )


def _labor_files(out_dir: Path) -> pl.DataFrame:
    """Every MES scan with the date of the file it came in (one per day closed)."""
    rows = []
    for path in sorted((out_dir / "mes" / "outbound").glob("labor_*.csv")):
        stem = path.stem[6:]
        file_day = date(int(stem[:4]), int(stem[4:6]), int(stem[6:]))
        with path.open(newline="", encoding="utf-8") as f:
            for record in csv.DictReader(f):
                rows.append(
                    {
                        "file_day": file_day,
                        "work_center": record["work_center"],
                        "scan_day": date.fromisoformat(record["scan_on"][:10]),
                        "pieces": int(record["qty_good"]) + int(record["qty_scrap"]),
                    }
                )
    return pl.DataFrame(rows)


# --- Verdicts ---------------------------------------------------------------------


@dataclass(frozen=True)
class Verdict:
    verdict: str  # "scenario", "real", or "false"
    reason: str


def verdict(issue: dict, t: Thresholds, bt: Backtest, tr: Truth) -> Verdict:
    """One issue, judged on the plant day it opened."""
    day, morning, subject = (
        issue["opened_day"],
        issue["opened_morning"],
        issue["subject"],
    )
    if day.weekday() >= 5:
        return Verdict("false", "calendar: a Saturday, one shift")
    judge = {"scrap": _scrap_verdict, "stops": _stop_verdict, "output": _output_verdict,
             "margin": _margin_verdict}[issue["family"]]  # fmt: skip
    return judge(subject, day, morning, t, bt, tr)


def _row(frame: pl.DataFrame, morning: date, **match: str) -> dict:
    found = frame.filter(pl.col("morning") == morning)
    for column, value in match.items():
        found = found.filter(pl.col(column) == value)
    return found.row(0, named=True)


def _scrap_verdict(machine, day, morning, t, bt, tr) -> Verdict:
    platform = _row(bt.rows["brief_scrap"], morning, machine_id=machine)
    true = tr.scrap.filter((pl.col("machine_id") == machine) & (pl.col("day") == day))
    if true.is_empty():
        return Verdict(
            "false", f"data: the answer key has no {machine} pieces that day"
        )
    true = true.row(0, named=True)
    if true["wear"] > 0:
        return Verdict(
            "scenario",
            f"laser_nozzle_degradation: {true['wear']} of {true['pieces']} pieces scrapped for nozzle wear",
        )
    true_rate = true["scrapped"] / true["pieces"]
    said = f"platform {100 * platform['rate']:.1f}% of {platform['pieces']}"
    key = f"answer key {100 * true_rate:.1f}% of {true['pieces']}"
    causes = f"{true['setup']} setup, {true['random']} random"
    se = standard_errors(true_rate, platform["baseline_rate"], true["pieces"])
    if se is not None and se > t.scrap_standard_errors:
        return Verdict(
            "false",
            f"chance: the answer key agrees ({key}), all ordinary scrap ({causes})",
        )
    where = " (brake attribution)" if machine in ("PB-01", "PB-02") else ""
    kind = _data(se or 0.0, t.scrap_standard_errors)
    return Verdict(
        "false",
        f"{kind}{where}: {said}, {key} ({se or 0:.1f} standard errors), "
        f"all ordinary scrap ({causes})",
    )


def _data(true_value: float, threshold: float) -> str:
    """'data', or 'borderline' when the answer key's value is within 5% of
    the threshold it didn't cross: the platform was nearly right."""
    return "borderline" if abs(true_value - threshold) <= 0.05 * threshold else "data"


def _overlap(states: pl.DataFrame, start: datetime, end: datetime) -> pl.DataFrame:
    # DuckDB hands back the platform's UTC timestamps without a zone.
    start, end = (t if t.tzinfo else t.replace(tzinfo=UTC) for t in (start, end))
    return states.filter(
        (pl.col("started_at") < end) & (pl.col("ended_at") > start)
    ).with_columns(
        overlap=(
            pl.min_horizontal(pl.col("ended_at"), pl.lit(end))
            - pl.max_horizontal(pl.col("started_at"), pl.lit(start))
        ).dt.total_seconds()
        / 60
    )


def _mix(frame: pl.DataFrame, minutes: str = "overlap") -> str:
    """Minutes by state (and reason), largest first: 'IDLE 52, DOWN JAM 31'."""
    if frame.is_empty():
        return "nothing"
    grouped = (
        frame.with_columns(
            label=pl.when(pl.col("reason").is_null())
            .then(pl.col("state"))
            .otherwise(pl.col("state") + pl.lit(" ") + pl.col("reason"))
        )
        .group_by("label")
        .agg(pl.sum(minutes))
        .sort(minutes, descending=True)
    )
    return ", ".join(f"{label} {m:.0f}" for label, m in grouped.iter_rows())


def _stop_verdict(machine, day, morning, t, bt, tr) -> Verdict:
    platform = _row(bt.rows["brief_stops"], morning, machine_id=machine)
    true = tr.down.filter((pl.col("machine_id") == machine) & (pl.col("day") == day))
    true_minutes = true["down_minutes"][0] if not true.is_empty() else 0.0
    usual = platform["usual_stop_minutes"]
    if (
        true_minutes > t.stop_minutes_factor * usual
        and true_minutes >= t.stop_minutes_floor
    ):
        reasons = ", ".join(
            f"{r['reason']} {r['minutes']:.0f}" for r in true["reasons"][0][:2]
        )
        return Verdict(
            "real",
            f"{true_minutes:.0f} minutes down in the answer key (usual {usual:.0f}): {reasons}",
        )
    stops = bt.rows["brief_machine_stops"].filter(
        (pl.col("morning") == morning) & (pl.col("machine_id") == machine)
    )
    machine_states = tr.states.filter(pl.col("machine_id") == machine)
    seen = pl.concat(
        [
            _overlap(machine_states, s["started_at_utc"], s["ended_at_utc"])
            for s in stops.iter_rows(named=True)
        ]
        or [machine_states.clear().with_columns(overlap=pl.lit(0.0))]
    )
    bar = max(t.stop_minutes_factor * usual, t.stop_minutes_floor)
    return Verdict(
        "false",
        f"{_data(true_minutes, bar)}: the platform's {platform['stop_minutes']:.0f} stop "
        f"minutes were, in the answer key, {_mix(seen)} (true down {true_minutes:.0f}, "
        f"threshold {bar:.0f})",
    )


def _output_verdict(work_center, day, morning, t, bt, tr) -> Verdict:
    platform = _row(bt.rows["brief_output"], morning, work_center=work_center)
    true = tr.pieces.filter(
        (pl.col("work_center") == work_center) & (pl.col("day") == day)
    )
    true_pieces = true["pieces"][0] if not true.is_empty() else 0
    usual = platform["usual_pieces"]
    if true_pieces < t.output_low_share * usual:
        machines = tr.operations.filter(pl.col("work_center") == work_center)[
            "machine_id"
        ].unique()
        shift_time = tr.states.filter(
            pl.col("machine_id").is_in(machines.to_list())
            & (pl.col("day") == day)
            & (pl.col("state") != "PLANNED_STOP")
        )
        return Verdict(
            "real",
            f"{true_pieces} pieces in the answer key (usual {usual:.0f}); its shift time: "
            f"{_mix(shift_time, 'minutes')}",
        )
    late = tr.labor_files.filter(
        (pl.col("work_center") == work_center)
        & (pl.col("scan_day") == day)
        & (pl.col("file_day") >= morning)
    )["pieces"].sum()
    why = f"; {late} pieces in scans still open at 05:30" if late else ""
    return Verdict(
        "false",
        f"{_data(true_pieces, t.output_low_share * usual)}: the platform counted "
        f"{platform['pieces']}, the answer key {true_pieces} (usual {usual:.0f}){why}",
    )


def _margin_verdict(subject, day, morning, t, bt, tr) -> Verdict:
    jobs = bt.rows["brief_margin_jobs"].filter(
        (pl.col("morning") == morning)
        & ((pl.col("brake") + pl.lit(" ") + pl.col("setup_shift")) == subject)
    )
    costs = jobs.join(tr.costs.select("job_num", "total_cost"), on="job_num")
    true_gap = (
        100
        * (costs["total_cost"].sum() - costs["erp_cost"].sum())
        / costs["revenue"].sum()
    )
    brake_ops = tr.operations.filter(
        pl.col("job_num").is_in(jobs["job_num"].to_list())
        & (pl.col("work_center") == "BRAKE")
    )
    setups = brake_ops["setup_hours"].sum() / (brake_ops["std_setup_min"].sum() / 60)
    told = f"answer key's gap {true_gap:.1f} points on {jobs.height} jobs, setups {setups:.2f}x standard"
    if subject == "PB-02 2nd" and setups > SCENARIO_SETUP_RATIO:
        return Verdict("scenario", f"pb02_second_shift_setups: {told}")
    if true_gap > t.margin_gap_points:
        return Verdict("real", told)
    return Verdict("false", f"data: {told}")


def judged(found: pl.DataFrame, t: Thresholds, bt: Backtest, tr: Truth) -> pl.DataFrame:
    """Issues, each with its verdict and reason."""
    grouped = issues(found, bt.mornings)
    verdicts = [verdict(i, t, bt, tr) for i in grouped.iter_rows(named=True)]
    return grouped.with_columns(
        verdict=pl.Series([v.verdict for v in verdicts], dtype=pl.String),
        reason=pl.Series([v.reason for v in verdicts], dtype=pl.String),
    )


def by_family(
    found: pl.DataFrame, scored: pl.DataFrame, warm_up: pl.DataFrame | None = None
) -> pl.DataFrame:
    """Alert-mornings and issues by family, and how many were true."""
    counts = []
    for family in FAMILIES:
        mine = scored.filter(pl.col("family") == family)
        counts.append(
            {
                "family": family,
                "alert_mornings": found.filter(pl.col("family") == family).height,
                "issues": mine.height,
                "scenario": mine.filter(pl.col("verdict") == "scenario").height,
                "real": mine.filter(pl.col("verdict") == "real").height,
                "false": mine.filter(pl.col("verdict") == "false").height,
                **(
                    {
                        "warm_up_mornings": warm_up.filter(
                            pl.col("family") == family
                        ).height
                    }
                    if warm_up is not None
                    else {}
                ),
            }
        )
    return pl.DataFrame(counts)


# --- The nozzle: lead time, and what the wait cost --------------------------------


@dataclass(frozen=True)
class Nozzle:
    variant: str
    first_morning: date | None  # the first morning the brief raised it
    first_day: date | None  # the plant day that brief was about
    ramp_start: date
    over_5_pct: date | None  # the first day the laser's true scrap passed 5%
    noticed: date | None  # the Monday report's morning (nozzle changed)
    after_ramp_working_days: int | None
    early_working_days: int | None  # mornings before the Monday report
    wear_pieces: int  # every piece scrapped for nozzle wear
    wear_pieces_after_alert: int  # made from 06:00 on the brief's morning to the change
    wear_cost_after_alert: float  # their true cost
    wear_cost: float


def piece_costs(tr: Truth) -> pl.DataFrame:
    """Each job's true cost of a piece cut on the laser: its material, and the
    laser operation's true labor and burden, per piece the laser cut."""
    laser = tr.operations.filter(pl.col("machine_id") == "LSR-01").select(
        "job_num",
        cut=pl.col("qty_good") + pl.col("qty_scrap"),
        laser_cost=pl.col("labor_cost") + pl.col("burden_cost"),
    )
    return laser.join(tr.costs.select("job_num", "material_cost"), on="job_num").select(
        "job_num",
        piece_cost=(pl.col("laser_cost") + pl.col("material_cost")) / pl.col("cut"),
    )


def nozzle(
    variant: str, scored: pl.DataFrame, bt: Backtest, tr: Truth
) -> Nozzle | None:
    if tr.events.is_empty():
        return None
    event = {r["event"]: r for r in tr.events.iter_rows(named=True)}
    ramp_start = event["ramp_start"]["local_date"]
    changed = event.get("nozzle_changed")
    first = scored.filter(
        (pl.col("family") == "scrap") & (pl.col("subject") == "LSR-01")
        & (pl.col("verdict") == "scenario")
    ).sort("opened_morning")  # fmt: skip
    first_morning = first["opened_morning"][0] if not first.is_empty() else None
    first_day = first["opened_day"][0] if not first.is_empty() else None
    laser = tr.scrap.filter(pl.col("machine_id") == "LSR-01").sort("day")
    over = laser.filter(pl.col("scrapped") / pl.col("pieces") > 0.05)
    noticed = changed["local_date"] if changed else None

    plant_days = sorted(set(bt.plant_day.values()))
    wear = tr.cycles.filter(pl.col("scrap_cause") == WEAR).join(
        piece_costs(tr), on="job_num", how="left"
    )
    after = wear.head(0)
    if first_morning is not None:
        acted = datetime.combine(
            first_morning, SHIFT_START, tzinfo=PLANT_TZ
        ).astimezone(UTC)
        until = changed["at_utc"] if changed else datetime.max.replace(tzinfo=UTC)
        after = wear.filter(
            (pl.col("completed_at") >= acted) & (pl.col("completed_at") < until)
        )

    def working_days(a: date | None, b: date | None) -> int | None:
        if a is None or b is None:
            return None
        return sum(1 for d in plant_days if a <= d < b)

    return Nozzle(
        variant=variant,
        first_morning=first_morning,
        first_day=first_day,
        ramp_start=ramp_start,
        over_5_pct=over["day"][0] if not over.is_empty() else None,
        noticed=noticed,
        after_ramp_working_days=working_days(ramp_start, first_morning),
        early_working_days=working_days(first_morning, noticed),
        wear_pieces=wear.height,
        wear_pieces_after_alert=after.height,
        wear_cost_after_alert=round(after["piece_cost"].sum(), 2),
        wear_cost=round(wear["piece_cost"].sum(), 2),
    )


# --- The sweeps -------------------------------------------------------------------


def sweep(family: str, base: Thresholds, bt: Backtest, tr: Truth) -> pl.DataFrame:
    """The rule's issues and verdicts at each threshold in its range."""
    out = []
    for setting in SWEEPS[family]:
        pairs = setting if isinstance(setting[0], tuple) else (setting,)
        t = replace(base, **dict(pairs))
        variants = (
            [("as built", False), ("frozen", True)]
            if family == "scrap"
            else [("", False)]
        )
        for name, frozen in variants:
            found = scored_alerts(alerts(bt, t, frozen_scrap=frozen), bt).filter(
                pl.col("family") == family
            )
            scored = judged(found, t, bt, tr)
            row = {
                **{k: v for k, v in pairs},
                **({"baseline": name} if name else {}),
                "alert_mornings": found.height,
                "issues": scored.height,
                "scenario": (scored["verdict"] == "scenario").sum(),
                "real": (scored["verdict"] == "real").sum(),
                "false": (scored["verdict"] == "false").sum(),
                # Which planted stories it found: one split in two is still one.
                "scenarios_found": scored.filter(pl.col("verdict") == "scenario")[
                    "reason"
                ]
                .str.split(":")
                .list.first()
                .n_unique(),
            }
            if family == "scrap":
                n = nozzle(name, scored, bt, tr)
                row["first_nozzle_morning"] = n.first_morning if n else None
                row["laser_issues"] = scored.filter(
                    pl.col("subject") == "LSR-01"
                ).height
            out.append(row)
    return pl.DataFrame(out)


def _grid(family: str) -> dict[str, list[float]]:
    """Each of the rule's parameters and the values its sweep tries, in order."""
    grid: dict[str, list[float]] = {}
    for setting in SWEEPS[family]:
        for name, value in setting if isinstance(setting[0], tuple) else (setting,):
            grid.setdefault(name, [])
            if value not in grid[name]:
                grid[name].append(value)
    return {name: sorted(values) for name, values in grid.items()}


def choose(family: str, table: pl.DataFrame, base: Thresholds) -> dict[str, float]:
    """The rule's threshold, picked from its sweep by one stated criterion.

    Keep every planted scenario the sweep can find (and, for scrap, the
    earliest morning it raises the nozzle); among those, the fewest false
    issues; among those, the setting nearest the shipped one, in steps along
    the sweep's grid. Scrap is chosen on its baseline as built: the frozen
    baseline is a variant to report, not the rule.
    """
    if family == "scrap":
        table = table.filter(pl.col("baseline") == "as built")
        table = table.filter(
            pl.col("first_nozzle_morning") == pl.col("first_nozzle_morning").min()
        )
    table = table.filter(pl.col("scenarios_found") == pl.col("scenarios_found").max())
    table = table.filter(pl.col("false") == pl.col("false").min())
    grid = _grid(family)
    shipped_at = {
        name: values.index(getattr(base, name)) for name, values in grid.items()
    }

    def steps(row: dict) -> int:
        return sum(abs(grid[n].index(row[n]) - shipped_at[n]) for n in grid)

    best = min(table.iter_rows(named=True), key=steps)
    return {name: float(best[name]) for name in grid}


def chosen(sweeps: dict[str, pl.DataFrame], base: Thresholds) -> Thresholds:
    picked: dict[str, float] = {}
    for family, table in sweeps.items():
        picked |= choose(family, table, base)
    return replace(base, **picked)


# --- Narration, from the brief logs -----------------------------------------------

_FIRST_NUMBER = re.compile(r"\d[\d,]*(?:\.\d+)?")
# A unit right after the number, or one word on ("391 total jobs").
_WRITTEN_UNIT = re.compile(
    r"\d\s*(?:[a-z]+\s+)?(%|percent|×|x\b|points?|pieces?|jobs?|stops?|scans?|min\b|minutes?)",
    re.IGNORECASE,
)
_UNIT_OF = {"%": "percent", "percent": "percent", "×": "ratio", "x": "ratio",
            "point": "points", "piece": "pieces", "job": "jobs", "stop": "stops",
            "scan": "scans", "min": "minutes", "minute": "minutes"}  # fmt: skip


def _unit_written(text: str) -> str | None:
    found = _WRITTEN_UNIT.search(text)
    if found is None:
        return None
    word = found[1].lower()
    return _UNIT_OF.get(word) or _UNIT_OF.get(word.removesuffix("s"))


def orphan_kind(orphan: dict, facts: MorningFacts) -> str:
    """What an orphan the check found really is, for the narration table.

    - `citation`: a fact about something the block names holds this number
      (or, in a block that names nothing, a fact in the unit written): the
      model stated a right number and cited it wrongly or not at all;
    - `wrong number`: no such fact holds it: worked out, or another
      subject's number; these are listed for reading by hand;
    - the check's own `derived`, `date`, `time`, `unknown citation`.
    The check is right to fail all of them; this says which failures a
    reader would have been misled by.
    """
    if orphan["reason"] in ("derived", "date", "time", "unknown citation"):
        return orphan["reason"]
    found = _FIRST_NUMBER.search(orphan["text"])
    if found is None:
        return "wrong number"
    plain = found[0].replace(",", "")
    value = Decimal(plain)
    decimals = len(plain.split(".")[1]) if "." in plain else 0
    block = orphan["block"]
    # "plant" names the plant's own facts, not every "plant day".
    named = {
        f.subject for f in facts.facts if f.subject in block.replace("plant day", "")
    }
    unit = _unit_written(orphan["text"])
    candidates = [
        f
        for f in facts.facts
        if (f.subject in named or f.family == "context")
        or (not named and unit is not None and f.unit == unit)
    ]
    for fact in candidates:
        if decimals > UNIT_DECIMALS.get(fact.unit, 2):
            continue
        for candidate in (fact.value, fact.baseline):
            if candidate is not None and _rounded(candidate, decimals) == value:
                return "citation"
    return "wrong number"


def _facts(out_dir: Path, morning: str) -> MorningFacts:
    path = facts_dir(out_dir) / f"{morning}.json"
    return MorningFacts.from_json(path.read_text(encoding="utf-8"))


def narration(out_dir: Path) -> dict[str, pl.DataFrame]:
    """Per model, each morning's newest brief: what went out and why."""
    found = {}
    runs = out_dir / "backtest" / "briefs"
    for model in sorted(p.name for p in runs.iterdir()) if runs.exists() else []:
        logs = sorted((runs / model / "logs").glob("*.jsonl"))
        latest: dict[str, dict] = {}
        for path in logs:  # sorted by start time, so a re-run replaces
            records = [json.loads(line) for line in path.read_text().splitlines()]
            brief = records[0]
            reply = next((r for r in records if r["type"] == "model_reply"), {})
            check = next(
                (
                    r
                    for r in records
                    if r["type"] == "check" and r["version"] == "model"
                ),
                {},
            )
            delivered = next((r for r in records if r["type"] == "delivered"), None)
            if delivered is None:
                continue
            latest[brief["morning"]] = {
                "morning": brief["morning"],
                "version": delivered["version"],
                "reason": delivered["reason"],
                "stop_reason": reply.get("stop_reason"),
                "input_tokens": reply.get("usage", {}).get("inputTokens"),
                "output_tokens": reply.get("usage", {}).get("outputTokens"),
                "ms": reply.get("ms"),
                "numbers": check.get("numbers"),
                # The morning's alerts the model's brief never cited: an alert
                # left out passes the check, which only reads what is written.
                "alerts_left_out": json.dumps(
                    sorted(
                        set(_facts(out_dir, brief["morning"]).anomaly_ids)
                        - set(check.get("cited", []))
                    )
                ),
                "tables": sum(
                    line.lstrip().startswith("|")
                    for line in reply.get("text", "").splitlines()
                ),
                "kinds": json.dumps(
                    sorted(
                        orphan_kind(o, _facts(out_dir, brief["morning"]))
                        for o in check.get("orphans", [])
                    )
                ),
                "to_read": json.dumps(
                    [
                        o
                        for o in check.get("orphans", [])
                        if o["reason"] in TO_READ
                        or orphan_kind(o, _facts(out_dir, brief["morning"]))
                        == "wrong number"
                    ]
                ),
                "orphans": json.dumps(
                    [f"{o['text']} ({o['reason']})" for o in check.get("orphans", [])]
                ),
            }
        if latest:
            found[model] = pl.DataFrame(list(latest.values())).sort("morning")
    return found


# --- The whole score --------------------------------------------------------------


@dataclass
class Score:
    out_dir: Path
    thresholds: Thresholds
    mornings: list[date]
    first_scored: date
    mismatches: list[str]
    alerts: pl.DataFrame
    scored: pl.DataFrame
    families: pl.DataFrame
    nozzles: list[Nozzle]
    sweeps: dict[str, pl.DataFrame]
    chosen: Thresholds | None
    narration: dict[str, pl.DataFrame]


def score(
    out_dir: Path, thresholds: Thresholds | None = None, sweeps: bool = True
) -> Score:
    t = thresholds or shipped()
    bt, tr = load(out_dir), truth(out_dir)
    every = alerts(bt, t)
    found = scored_alerts(every, bt)
    scored = judged(found, t, bt, tr)
    frozen = judged(scored_alerts(alerts(bt, t, frozen_scrap=True), bt), t, bt, tr)
    warm_up = every.filter(pl.col("morning") < first_scored(bt))
    tables = {f: sweep(f, t, bt, tr) for f in FAMILIES} if sweeps else {}
    return Score(
        out_dir=out_dir,
        thresholds=t,
        mornings=bt.mornings,
        first_scored=first_scored(bt),
        mismatches=rule_mismatches(bt, shipped()),
        alerts=found,
        scored=scored,
        families=by_family(found, scored, warm_up),
        nozzles=[
            n
            for n in (
                nozzle("as built", scored, bt, tr),
                nozzle("frozen", frozen, bt, tr),
            )
            if n
        ],
        sweeps=tables,
        chosen=chosen(tables, t) if tables else None,
        narration=narration(out_dir),
    )


def _table(frame: pl.DataFrame) -> str:
    head = "| " + " | ".join(frame.columns) + " |"
    rule = "|" + "---|" * len(frame.columns)
    body = [
        "| " + " | ".join("" if v is None else str(v) for v in row) + " |"
        for row in frame.iter_rows()
    ]
    return "\n".join([head, rule, *body])


REVIEW = Path(__file__).with_name("brief_backtest_review.yaml")
MISLEADING = ("wrong", "computed", "invented")
# The check's reasons that put a number beside a citation that doesn't hold
# it, or a number nothing holds: each must be read by hand.
TO_READ = ("misattributed", "wrong unit", "orphan", "unknown citation", "derived",
           "date", "time")  # fmt: skip


def review(out_dir: Path) -> list[dict]:
    """The hand readings for this backfill's narration (none for another)."""
    if not REVIEW.exists():
        return []
    found = yaml.safe_load(REVIEW.read_text(encoding="utf-8"))
    if found["out"] != out_dir.name:
        return []
    return [{**r, "morning": str(r["morning"])} for r in found["orphans"]]


def narration_summary(model: str, table: pl.DataFrame, read: list[dict]) -> list[str]:
    """One model's briefs: how many went out, and what stopped the others.

    A failed brief is *misleading* if a number in it, read by hand, was wrong,
    worked out, or invented (brief_backtest_review.yaml), and *citation only*
    if every number in it was a fact's, cited in the wrong place or not at
    all. Any orphan that needs reading and has none is listed as unread.
    """
    failed = table.filter(pl.col("version") != "model")
    readings = {(r["morning"], r["number"]): r for r in read if r["model"] == model}
    to_read = [
        (row["morning"], o)
        for row in failed.iter_rows(named=True)
        for o in json.loads(row["to_read"])
    ]
    logged = {(m, o["text"]) for m, o in to_read}
    stray = [key for key in readings if key not in logged]
    if stray:
        raise SystemExit(f"{REVIEW.name}: no such {model} orphan in the logs: {stray}")
    unread = [(m, o) for m, o in to_read if (m, o["text"]) not in readings]
    misleading = {m for (m, _), r in readings.items() if r["verdict"] in MISLEADING}
    verdicts = pl.DataFrame(
        [r for r in readings.values() if r["verdict"] in MISLEADING],
        schema={
            "morning": pl.String,
            "number": pl.String,
            "verdict": pl.String,
            "note": pl.String,
        },
    )
    passed = table.height - failed.height
    went_out = table.filter(pl.col("version") == "model").with_columns(
        left=pl.col("alerts_left_out").str.json_decode(pl.List(pl.String))
    )
    silent = went_out.filter(pl.col("left").list.len() > 0)
    early = failed.filter(pl.col("stop_reason") != "end_turn").height
    # A brief with an orphan nobody has read can't be called either way.
    unread_briefs = {m for m, _ in unread} - misleading
    return [
        (
            f"{passed} of {table.height} model briefs passed the number check and went "
            f"out; the template went out on the other {failed.height}: "
            f"{len(misleading)} would have stated a wrong, worked-out, or invented "
            f"number; {failed.height - len(misleading) - len(unread_briefs) - early} "
            "only cited right numbers in the wrong place; "
            f"{len(unread_briefs)} not yet read by hand; {early} stopped early or "
            "got no reply. "
            f"{(table['tables'] > 0).sum()} briefs used a table (each number "
            "cited under it, so in another block). Tokens: "
            f"{table['input_tokens'].sum():,} in, {table['output_tokens'].sum():,} "
            f"out; median {table['ms'].median() / 1000:.1f} s a brief."
        ),
        "",
        "The misleading numbers, read by hand (every one refused by the check):",
        "",
        _table(verdicts.sort("morning")),
        "",
        (
            f"Of the {went_out.height} that went out, {silent.height} left out an "
            "alert the facts held (the check reads what is written, not what is "
            "missing):"
        ),
        "",
        *[
            f"- {row['morning']}: {', '.join(row['left'])}"
            for row in silent.iter_rows(named=True)
        ],
        "",
        f"Orphans still to read by hand: {len(unread) or 'none'}",
        *[
            f"- {m}: **{o['text']}** ({o['reason']}) in: {o['block']}"
            for m, o in unread
        ],
        "",
        "Every failed brief:",
        "",
        _table(failed.select("morning", "reason", "orphans")),
    ]


def report(s: Score) -> str:
    lines = [
        f"# Brief backtest: {s.out_dir}",
        "",
        (
            f"{len(s.mornings)} mornings with a plant day, {s.mornings[0]} to "
            f"{s.mornings[-1]}; scored from {s.first_scored}, the first with 20 working "
            f"days of baseline (earlier alert-mornings: warm-up). "
            f"Thresholds: {asdict(s.thresholds)}."
        ),
        "",
        "Python rules against the SQL's alerts at brief.sql's thresholds: "
        + (
            "equal on every morning."
            if not s.mismatches
            else f"{len(s.mismatches)} mornings differ:"
        ),
        *s.mismatches,
        "",
        "## Alerts by family (issues, judged once each)",
        "",
        _table(s.families),
        "",
        "## Every issue",
        "",
        _table(
            s.scored.select(
                "fact_id", "opened_day", "last_day", "mornings", "verdict", "reason"
            )
        ),
        "",
        "## The nozzle",
        "",
    ]
    for n in s.nozzles:
        lines += [
            (
                f"- **{n.variant}**: first raised the morning of {n.first_morning} "
                f"(plant day {n.first_day}); the ramp started {n.ramp_start}, "
                f"{n.after_ramp_working_days} working days before; true daily scrap "
                f"first over 5% on {n.over_5_pct}; the Monday report caught it "
                f"{n.noticed}, {n.early_working_days} working days later. Wear scrap: "
                f"{n.wear_pieces} pieces (${n.wear_cost:,.2f}), "
                f"{n.wear_pieces_after_alert} of them (${n.wear_cost_after_alert:,.2f}) "
                "made from 06:00 on the brief's morning to the change."
            ),
        ]
    for family, table in s.sweeps.items():
        lines += ["", f"## Sweep: {family}", "", _table(table)]
    if s.chosen:
        lines += [
            "",
            "## Thresholds the sweeps pick",
            "",
            (
                "Every planted scenario (and the earliest nozzle morning) kept, then "
                "the fewest false issues, then the setting nearest the shipped one. "
                "Picked on this backfill's own mornings; no held-out window."
            ),
            "",
            _table(
                pl.DataFrame(
                    {
                        "threshold": list(asdict(s.chosen)),
                        "shipped": list(asdict(s.thresholds).values()),
                        "picked": list(asdict(s.chosen).values()),
                    }
                )
            ),
        ]
    for model, table in s.narration.items():
        lines += [
            "",
            f"## Narration: {model}",
            "",
            *narration_summary(model, table, review(s.out_dir)),
        ]
    return "\n".join(lines) + "\n"
