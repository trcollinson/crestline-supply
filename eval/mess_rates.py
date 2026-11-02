"""How often each mess really happened, against what mess.yaml says.

A mess with a rate is a promise: `forgot_scan_off_rate: 0.03` says about 3%
of scans are left open. This counts how often each one actually shows up in
what the plant's systems wrote, month by month, and says whether the count
is within three binomial standard deviations of the configured rate (so a
mess drawn correctly lands inside nearly every time, and one switched off,
or running at twice its rate, lands outside).

Everything is counted from the interfaces, not from the simulator's draws:

- **forgotten scan-offs**: MES scans closed at the supervisor's 06:00 on a
  later morning, out of all scans (by the month of the scan-on)
- **duplicate orders**: orders whose line was cancelled as a duplicate,
  against the orders that weren't (each of those was one chance for a
  clerk to enter it twice; by order date)
- **job splits**: `J104221-1` children, against the other jobs (each one
  chance for the planner to split it; by release date)
- **stale stamps**: the ERP's `LastModified` left behind its real change
  time (`SysValidFrom`): on every update of a job operation, and on every
  labor row. These aren't rates but switches, so the share is all or nothing.
- **duplicate messages** (M3): a machine message identical to one published
  within the last three seconds, against the messages that weren't
- **malformed messages**: GW01 messages that arrived as text that won't parse
- **outages**: counted per gateway-hour (the knob is per week, so the rate
  per hour is a 168th of it) from what each gateway leaves behind: a gap
  in GW02's 30-second polls, a rebirth of the Sparkplug node (an NBIRTH
  after the first), a late burst from GW03. GW01's outages leave no trace
  at night, when PB-01 has nothing to say, so they aren't counted.
- **counter resets**: a legacy counter dropping from below 65,500 (a drop
  from the top is the rollover), against the working days of the window
- **unrecorded downtime**: stops between the two thresholds that the MES
  downtime file does not hold, against all such stops. This one reads the
  answer key (`eval/` may): the file can't know what it didn't record.

The ERP's store keeps every version of every row, so the stamps can be
checked against when each change really happened.

    uv run python eval/mess_rates.py [--out out] [--mess mess.yaml]
"""

import argparse
import csv
import json
import math
import sqlite3
from collections import deque
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import datetime, time, timedelta
from pathlib import Path

import polars as pl

from crestline.mess import MessConfig, load_mess
from crestline.sim.clock import PLANT_TZ
from crestline.sim.systems.mes import merge_stops

SUPERVISOR_CLOSES_AT = time(6, 0)  # spec §4.2: the next working morning
SIGMAS = 3.0
HOURS_PER_WEEK = 168
DUPLICATE_WITHIN = timedelta(seconds=3)
POLL_GAP = timedelta(seconds=60)  # an outage is two minutes or more
LATE_BY = timedelta(seconds=60)  # a replayed sample is published well after its time
ROLLOVER_FROM = 65_500  # a drop from here up is the 16-bit wrap, not a reset

# A delivered machine message as the counters see it: when it was published
# (UTC), its topic, and its payload (a JSON object, or the text that arrived).
Delivered = tuple[datetime, str, object]

# (month, hits, trials) for each row counted: a forgotten scan is
# ("2026-07", 1, 1), a scan closed properly ("2026-07", 0, 1). A duplicate
# order is a hit but not a chance of its own, ("2026-07", 1, 0): the chance
# was its original's.
Trials = Iterable[tuple[str, int, int]]


@dataclass(frozen=True)
class Rate:
    """How often a mess happened in one month (or overall), against its knob."""

    knob: str
    configured: float
    hits: int
    trials: int

    @property
    def realized(self) -> float:
        return self.hits / self.trials if self.trials else 0.0

    @property
    def band(self) -> tuple[float, float]:
        """The hit counts within SIGMAS binomial standard deviations."""
        mean = self.trials * self.configured
        spread = SIGMAS * math.sqrt(mean * (1 - self.configured))
        return mean - spread, mean + spread

    @property
    def ok(self) -> bool:
        low, high = self.band
        return low <= self.hits <= high


def forgotten_scan_offs(scans: Iterable[tuple[datetime, datetime]]) -> Trials:
    """Scans as (scan_on, scan_off) in plant local time."""
    for scan_on, scan_off in scans:
        closed_by_supervisor = (
            scan_off.time() == SUPERVISOR_CLOSES_AT and scan_off.date() > scan_on.date()
        )
        yield f"{scan_on:%Y-%m}", int(closed_by_supervisor), 1


def duplicate_orders(orders: Iterable[dict], lines: Iterable[dict]) -> Trials:
    """SalesOrders and SalesOrderLines rows; a cancelled line is a duplicate."""
    cancelled = {line["OrderNum"] for line in lines if line["Deleted"]}
    for order in orders:
        duplicate = order["OrderNum"] in cancelled
        yield str(order["OrderDate"])[:7], int(duplicate), int(not duplicate)


def job_splits(jobs: Iterable[dict]) -> Trials:
    """Jobs rows; each `-1` child is one split of the job before it."""
    for job in jobs:
        child = job["JobNum"].endswith("-1")
        yield str(job["ReleaseDate"])[:7], int(child), int(not child)


def stale_stamps(
    versions: Iterable[tuple[tuple, str, str]], updates_only: bool
) -> Trials:
    """Row versions as (key, SysValidFrom, LastModified), both ISO 8601 UTC.

    With `updates_only`, a key's first version (its creation, stamped by
    every ERP) isn't counted.
    """
    seen = set()
    for key, valid_from, last_modified in sorted(versions, key=lambda v: (v[0], v[1])):
        first = key not in seen
        seen.add(key)
        if updates_only and first:
            continue
        yield valid_from[:7], int(last_modified != valid_from), 1


def rates(knob: str, configured: float, trials: Trials) -> dict[str, Rate]:
    """One Rate per month, plus "all" for the whole run."""
    counts: dict[str, list[int]] = {}
    for month, hits, chances in trials:
        for bucket in (month, "all"):
            hits_trials = counts.setdefault(bucket, [0, 0])
            hits_trials[0] += hits
            hits_trials[1] += chances
    return {
        month: Rate(knob, configured, hits, n)
        for month, (hits, n) in sorted(counts.items())
    }


def configured(mess: MessConfig) -> dict[str, float]:
    """Each measured mess's configured rate per trial; a switch is 1 on, 0 off."""
    skips = mess.erp.lastmodified_skips
    machines = mess.machines
    return {
        "mes.forgot_scan_off_rate": mess.mes.forgot_scan_off_rate,
        "erp.soft_delete_rate": mess.erp.soft_delete_rate,
        "erp.job_split_rate": mess.erp.job_split_rate,
        "erp.lastmodified_skips: JobOperations.qty": float(
            "JobOperations.qty" in skips
        ),
        "erp.lastmodified_skips: LaborDetails.post": float(
            "LaborDetails.post" in skips
        ),
        "machines.duplicate_rate": machines.duplicate_rate,
        "machines.malformed_json_rate": machines.malformed_json_rate,
        "machines.outages_per_week": machines.outages_per_week / HOURS_PER_WEEK,
        "machines.counter_reset_rate": machines.counter_reset_rate,
        "mes.downtime_unrecorded_rate": mess.mes.downtime_unrecorded_rate,
    }


# --- Machine messages ------------------------------------------------------


def month_of(at: datetime) -> str:
    return f"{at:%Y-%m}"


def duplicate_messages(messages: Iterable[Delivered]) -> Trials:
    """A message identical to one published within three seconds is a copy."""
    recent: deque[Delivered] = deque()
    for at, topic, payload in messages:
        while recent and at - recent[0][0] > DUPLICATE_WITHIN:
            recent.popleft()
        if any(t == topic and p == payload for _, t, p in recent):
            yield month_of(at), 1, 0  # a hit, not a chance of its own
        else:
            yield month_of(at), 0, 1
            recent.append((at, topic, payload))


def malformed_messages(messages: Iterable[Delivered]) -> Trials:
    """A JSON gateway's message that arrived as text that won't parse."""
    for at, _, payload in messages:
        yield month_of(at), int(isinstance(payload, str)), 1


def gap_outages(polls: Iterable[Delivered]) -> list[datetime]:
    """Where a polled gateway's stream stops for longer than a poll."""
    outages = []
    previous: datetime | None = None
    for at, _, _ in polls:
        if previous is not None and at - previous > POLL_GAP:
            outages.append(previous)
        previous = at
    return outages


def rebirth_outages(messages: Iterable[Delivered]) -> list[datetime]:
    """Every NBIRTH after the first is a Sparkplug node reconnecting."""
    births = [at for at, topic, _ in messages if topic.split("/")[2] == "NBIRTH"]
    return births[1:]


def late_burst_outages(samples: Iterable[Delivered]) -> list[datetime]:
    """Runs of samples published long after their own time: a replayed buffer.

    A duplicate of a replayed sample arrives a moment after the burst, so a
    late sample within a minute of the last one belongs to the same outage.
    """
    outages = []
    last_late: datetime | None = None
    for at, _, payload in samples:
        taken = datetime.fromtimestamp(payload["t"] / 1000, tz=at.tzinfo)
        if at - taken > LATE_BY:
            if last_late is None or at - last_late > LATE_BY:
                outages.append(taken)
            last_late = at
    return outages


def outage_trials(
    outages: Iterable[datetime], window: tuple[datetime, datetime]
) -> Trials:
    """Outages as hits; every hour of the window as a chance."""
    for at in outages:
        yield month_of(at), 1, 0
    start, end = window
    hour = start
    while hour < end:
        yield month_of(hour), 0, 1
        hour += timedelta(hours=1)


def counter_resets(
    polls: Iterable[Delivered], window: tuple[datetime, datetime]
) -> Trials:
    """A counter dropping from below the top is a power cycle; a working day is a chance."""
    previous: int | None = None
    for at, _, payload in polls:
        if isinstance(payload, dict):
            cnt = payload["cnt"]
            if previous is not None and cnt < previous < ROLLOVER_FROM:
                yield month_of(at), 1, 0
            previous = cnt
    day = window[0].astimezone(PLANT_TZ).date()
    last = window[1].astimezone(PLANT_TZ).date()
    while day < last:
        if day.weekday() < 5:
            yield f"{day:%Y-%m}", 0, 1
        day += timedelta(days=1)


def unrecorded_downtime(
    candidates: Iterable[tuple[datetime, bool]],
) -> Trials:
    """Stops between the thresholds, as (when it ended, whether it was written down)."""
    for ended_at, recorded in candidates:
        yield month_of(ended_at), int(not recorded), 1


def downtime_candidates(
    machine_states: pl.DataFrame, mess: MessConfig, recorded: set[tuple[str, str, str]]
) -> list[tuple[datetime, bool]]:
    """The stops the rate applies to, keyed the way the file writes them."""
    stops = merge_stops(machine_states)
    minutes = (pl.col("ended_at") - pl.col("started_at")).dt.total_seconds() / 60
    between = stops.filter(
        (minutes >= mess.mes.min_recorded_downtime_minutes)
        & (minutes < mess.mes.always_recorded_downtime_minutes)
    )
    return [
        (ended, (machine, minute_text(started), minute_text(ended)) in recorded)
        for machine, started, ended in between.select(
            "machine_id", "started_at", "ended_at"
        ).rows()
    ]


def minute_text(at: datetime) -> str:
    """Plant local time to the nearest minute, as the downtime file writes it."""
    rounded = (
        (at + timedelta(seconds=30))
        .astimezone(PLANT_TZ)
        .replace(second=0, microsecond=0)
    )
    return rounded.strftime("%Y-%m-%d %H:%M:%S")


def machine_window(messages: list[Delivered]) -> tuple[datetime, datetime]:
    return messages[0][0], messages[-1][0]


def from_files(path: Path) -> Iterator[Delivered]:
    """One machine file's messages, in the order the file holds them (publish order)."""
    with path.open(encoding="utf-8") as file:
        for line in file:
            row = json.loads(line)
            yield (
                datetime.fromisoformat(row["published_at"]),
                row["topic"],
                row["payload"],
            )


# --- Reading one backfill's output -------------------------------------------


def from_backfill(out: Path, mess: MessConfig) -> dict[str, dict[str, Rate]]:
    """Every measured mess, by month, from the files and store a backfill wrote."""
    expected = configured(mess)
    db = sqlite3.connect(out / "erp" / "erp.db")
    db.row_factory = sqlite3.Row

    def rows(sql: str) -> list[dict]:
        return [dict(row) for row in db.execute(sql)]

    def versions(table: str, key: str) -> list[tuple[tuple, str, str]]:
        sql = f"select {key}, SysValidFrom, LastModified from {table}"
        history = (
            f" union all select {key}, SysValidFrom, LastModified from {table}_History"
        )
        has_history = db.execute(
            "select 1 from sqlite_master where name = ?", [f"{table}_History"]
        ).fetchone()
        result = db.execute(sql + (history if has_history else "")).fetchall()
        width = key.count(",") + 1
        return [(tuple(r[:width]), r[width], r[width + 1]) for r in result]

    scans = []
    for path in sorted((out / "mes" / "outbound").glob("labor_*.csv")):
        with path.open(newline="", encoding="utf-8") as file:
            for row in csv.DictReader(file):
                scans.append(
                    (
                        datetime.fromisoformat(row["scan_on"]),
                        datetime.fromisoformat(row["scan_off"]),
                    )
                )
    machine_files = out / "machines"
    pb01 = list(from_files(machine_files / "pb01.jsonl"))
    pb02 = list(from_files(machine_files / "pb02.jsonl"))
    pem01 = list(from_files(machine_files / "pem01.jsonl"))
    lsr01 = list(from_files(machine_files / "lsr01.jsonl"))
    pcline = list(from_files(machine_files / "pcline.jsonl"))
    window = machine_window(pb02)
    recorded = set()
    for path in sorted((out / "mes" / "outbound").glob("downtime_*.csv")):
        with path.open(newline="", encoding="utf-8") as file:
            for row in csv.DictReader(file):
                recorded.add((row["machine_id"], row["start_time"], row["end_time"]))
    machine_states = pl.read_parquet(out / "truth" / "truth_machine_states.parquet")

    trials = {
        "mes.forgot_scan_off_rate": forgotten_scan_offs(scans),
        "erp.soft_delete_rate": duplicate_orders(
            rows("select * from SalesOrders"), rows("select * from SalesOrderLines")
        ),
        "erp.job_split_rate": job_splits(rows("select * from Jobs")),
        "erp.lastmodified_skips: JobOperations.qty": stale_stamps(
            versions("JobOperations", "JobNum, OprSeq"), updates_only=True
        ),
        "erp.lastmodified_skips: LaborDetails.post": stale_stamps(
            versions("LaborDetails", "LaborDtlSeq"), updates_only=False
        ),
        "machines.duplicate_rate": [
            t
            for device in (pb01, pb02, pem01, lsr01, pcline)
            for t in duplicate_messages(device)
        ],
        "machines.malformed_json_rate": malformed_messages(pb01),
        "machines.outages_per_week": [
            *outage_trials(gap_outages(pb02), window),
            *outage_trials(rebirth_outages(lsr01), window),
            *outage_trials(late_burst_outages(pcline), window),
        ],
        "machines.counter_reset_rate": [
            *counter_resets(pb02, window),
            *counter_resets(pem01, window),
        ],
        "mes.downtime_unrecorded_rate": unrecorded_downtime(
            downtime_candidates(machine_states, mess, recorded)
        ),
    }
    return {knob: rates(knob, expected[knob], t) for knob, t in trials.items()}


def report(measured: dict[str, dict[str, Rate]]) -> str:
    header = (
        f"  {'mess':<44}{'month':>8}{'hits':>10}{'of':>12}{'realized':>10}"
        f"{'configured':>12}{'3-sd band':>22}  ok"
    )
    lines = [header]
    for knob, by_month in measured.items():
        for month, rate in by_month.items():
            low, high = rate.band
            lines.append(
                f"  {knob:<44}{month:>8}{rate.hits:>10,}{rate.trials:>12,}"
                f"{rate.realized:>10.2%}{rate.configured:>12.2%}"
                f"{f'{max(low, 0):,.1f}–{high:,.1f}':>22}  {'yes' if rate.ok else 'NO'}"
            )
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--out", type=Path, default=Path("out"), help="backfill output")
    parser.add_argument("--mess", type=Path, default=Path("mess.yaml"))
    args = parser.parse_args(argv)
    print(report(from_backfill(args.out, load_mess(args.mess))))


if __name__ == "__main__":
    main()
