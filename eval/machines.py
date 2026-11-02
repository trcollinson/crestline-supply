"""Score the machine data rebuild against the answer key (M3 step 6).

queries/machines.sql rebuilds what each machine counted from what its
gateway published, and queries/downtime.sql finds its stops; neither sees
the simulation. This script is the other half. It reads the answer key in
out/truth/, which stays on this machine, and reports:

- **cycles per machine per day** against `truth_cycles`: the share of
  machine-days within 2% (M3's done-when), with every miss given a cause.
  Next to it, the "before": the margin query's arithmetic (counter differences in the
  order the device stamped them, mod 65,536 for a 16-bit counter) applied
  to every machine's raw messages;
- **downtime recall and precision** against `truth_machine_states`, by
  machine, generation, and duration band (under 2 minutes, 2 to 5, over 5),
  for the downtime rebuilt from machine data and for the MES downtime file
  alike, so the plant's own record and the rebuild are scored the same way.
  Every true stop either found it or has a named reason it didn't;
- **hours of downtime** per source: the MES file, the rebuild, and truth.

How a stop is scored. The answer key records a stop in chunks, cut at
every break; a true stop is those chunks merged, as the operator saw it
(`merge_stops`, the same stops the MES file is written from). A true stop is
**found** if a stop from the source, on the same machine, overlaps it at
all; a source stop is **right** if it overlaps any true DOWN chunk on its
machine. Hours compare time: how much of the true DOWN time a source covers
(recall), and how much of a source's time was truly DOWN (precision). The
MES file's local, minute-rounded times are read by the plant's rule (an
ambiguous local time is its earlier occurrence, spec §4.2). Only the five
machines that send data are scored, the MES file included.

    uv run python eval/machines.py [--out out]

eval/ is the only place outside the simulator and the tests allowed to read
out/truth/.
"""

import argparse
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import polars as pl

from crestline import query
from crestline.sim.clock import PLANT_TZ
from crestline.sim.systems.mes import merge_stops

MACHINES = ["LSR-01", "PB-01", "PB-02", "PEM-01", "PC-LINE"]
GENERATION = {
    "LSR-01": "modern",
    "PB-01": "mid-age",
    "PB-02": "legacy",
    "PEM-01": "legacy",
    "PC-LINE": "process",
}
WITHIN = 0.02  # a machine-day's count within 2% of the truth
BANDS = ["under 2 min", "2 to 5 min", "over 5 min"]
SOURCES = {
    "rebuilt": "rebuilt from machine data",
    "mes": "MES downtime file",
    "both": "rebuilt + MES (overlaps merged)",
}

# Why a machine-day's count missed. "unexplained" should never appear.
DAY_MISSES = {
    "reset_as_rollover": "power cycle read as a 16-bit rollover (a 65,000-stroke day)",
    "unexplained": "unexplained",
}

# Why a true stop wasn't found, most specific first.
STOP_MISSES = {
    "laser_tooling_as_setup": "laser: a nozzle or lens change shows as setup",
    "laser_maintenance_as_idle": "laser: maintenance shows as idle",
    "outside_operation_run": "before an operation's first stroke or after its last (looks like setup, or done)",
    "across_lost_polls": "GW02 lost the polls (an outage), so the gap can't be read",
    "under_gap_rule": "too short for the 3-minute stroke-gap rule (or cut under it by a break)",
    "never_written_down": "under 5 minutes: nobody writes it down",
    "not_written_down": "5 to 30 minutes: not written down (busy fixing it)",
    "unexplained": "unexplained",
}

# The margin query's arithmetic on every machine's raw messages: counter differences in
# the order the device (or gateway) stamped them, wrapped mod 65,536 where
# the counter is 16 bits. No deduplication, no clock correction, and no idea
# that a 16-bit counter can be reset.
NAIVE_SQL = """
with laser_aliases as (
    select distinct metric_alias, metric_name from lsr01_metrics where message_type = 'DBIRTH'
),

raw_counts as (
    select 'PB-01' as machine_id, device_ts_utc as ts, cyc_cnt as counter, null as modulus from pb01_data
    union all
    select 'PB-02', gateway_ts_utc, cnt, 65536 from pb02_counts
    union all
    select 'PEM-01', gateway_ts_utc, cnt, 65536 from pem01_counts
    union all
    select 'PC-LINE', gateway_ts_utc, hook_cnt, null from pcline_process
    union all
    select 'LSR-01', m.metric_ts_utc, m.int_value, null
    from lsr01_metrics as m
    left join laser_aliases as a on a.metric_alias = m.metric_alias
    where coalesce(m.metric_name, a.metric_name) = 'PartCount'
),

deltas as (
    select machine_id,
           ts,
           counter - lag(counter) over (partition by machine_id order by ts) as delta,
           modulus
    from raw_counts
)

select machine_id,
       cast(timezone('America/Denver', ts at time zone 'UTC') as date) as work_date,
       sum(case when modulus is null then delta else (delta + modulus) % modulus end) as cycles
from deltas
where delta is not null
group by 1, 2
"""


@dataclass(frozen=True)
class Scores:
    days: pl.DataFrame  # machine, work_date, truth, rebuilt, naive, and why each missed
    stops: pl.DataFrame  # one row per true stop and source: found, band, miss reason
    source_stops: pl.DataFrame  # one row per source stop: right or not, band
    hours: pl.DataFrame  # machine, source, true hours, source hours, overlap

    def days_within(self, column: str) -> float:
        return _within(self.days, column).mean()


def score(out_dir: Path, queries_dir: Path = query.QUERIES_DIR) -> Scores:
    con = query.connect(out_dir, queries_dir)
    query.run(con, "downtime", queries_dir)
    truth_dir = out_dir / "truth"
    states = pl.read_parquet(truth_dir / "truth_machine_states.parquet").filter(
        pl.col("machine_id").is_in(MACHINES)
    )
    cycles = pl.read_parquet(truth_dir / "truth_cycles.parquet").filter(
        pl.col("machine_id").is_in(MACHINES)
    )
    days = _days(con, cycles)

    chunks = _naive_utc(
        states.filter(pl.col("state") == "DOWN").select(
            "machine_id", "reason", "started_at", "ended_at"
        )
    )
    true_stops = _naive_utc(
        merge_stops(states).select("machine_id", "reason", "started_at", "ended_at")
    ).with_row_index("stop_id")
    sources = {
        "rebuilt": con.sql(
            "select machine_id, started_at_utc as started_at, ended_at_utc as ended_at from machine_downtime"
        ).pl(),
        "mes": _mes_stops(out_dir),
    }
    # What the plant could know with both: the machines catch the short
    # stops, the operators the ones a machine can't name.
    sources["both"] = _merge(pl.concat([sources["rebuilt"], sources["mes"]]))
    con.register("chunks", chunks)
    con.register("true_stops", true_stops)
    stops, source_stops, hours = [], [], []
    for source, frame in sources.items():
        con.register("source_stops", frame)
        stops.append(_found(con, source))
        source_stops.append(_right(con, source))
        hours.append(_hours(con, source))
    return Scores(
        days,
        pl.concat(stops),
        pl.concat(source_stops),
        pl.concat(hours),
    )


def _merge(stops: pl.DataFrame) -> pl.DataFrame:
    """Overlapping stops on one machine as one stop."""
    merged: list[dict] = []
    for row in stops.sort("machine_id", "started_at").iter_rows(named=True):
        last = merged[-1] if merged else None
        if (
            last
            and last["machine_id"] == row["machine_id"]
            and row["started_at"] <= last["ended_at"]
        ):
            last["ended_at"] = max(last["ended_at"], row["ended_at"])
        else:
            merged.append(dict(row))
    return pl.DataFrame(merged, schema=stops.schema)


def _naive_utc(frame: pl.DataFrame) -> pl.DataFrame:
    """Truth timestamps as the plain UTC timestamps the views use."""
    return frame.with_columns(
        pl.col("started_at", "ended_at")
        .dt.convert_time_zone("UTC")
        .dt.replace_time_zone(None)
    )


def _within(days: pl.DataFrame, column: str) -> pl.Series:
    return (days[column] - days["truth"]).abs() <= WITHIN * days["truth"]


def _days(con: duckdb.DuckDBPyConnection, cycles: pl.DataFrame) -> pl.DataFrame:
    truth = cycles.group_by(
        "machine_id",
        work_date=pl.col("completed_at").dt.convert_time_zone(str(PLANT_TZ)).dt.date(),
    ).agg(truth=pl.len())
    rebuilt = con.sql(
        "select machine_id, work_date, cast(cycles as bigint) as rebuilt from machine_cycles_by_day"
    ).pl()
    naive = con.sql(NAIVE_SQL).pl().with_columns(naive=pl.col("cycles").cast(pl.Int64))
    resets = con.sql(
        """
        select distinct machine_id,
               cast(timezone('America/Denver', counted_at_utc at time zone 'UTC') as date) as work_date,
               true as had_reset
        from machine_counts
        where counted_as = 'reset'
        """
    ).pl()
    days = (
        truth.join(rebuilt, on=["machine_id", "work_date"], how="left")
        .join(naive.drop("cycles"), on=["machine_id", "work_date"], how="left")
        .join(resets, on=["machine_id", "work_date"], how="left")
        .with_columns(
            pl.col("truth").cast(pl.Int64),
            pl.col("rebuilt", "naive").fill_null(0),
            pl.col("had_reset").fill_null(False),
        )
        .sort("machine_id", "work_date")
    )
    days = days.with_columns(
        rebuilt_ok=_within(days, "rebuilt"), naive_ok=_within(days, "naive")
    )

    def why(ok: str) -> pl.Expr:
        return (
            pl.when(pl.col(ok))
            .then(None)
            .when(pl.col("had_reset"))
            .then(pl.lit("reset_as_rollover"))
            .otherwise(pl.lit("unexplained"))
        )

    return days.with_columns(rebuilt_miss=why("rebuilt_ok"), naive_miss=why("naive_ok"))


def _mes_stops(out_dir: Path) -> pl.DataFrame:
    """The MES downtime file's stops on the five machines, in UTC."""
    files = sorted((out_dir / "mes" / "outbound").glob("downtime_*.csv"))
    frame = pl.concat(
        [
            pl.read_csv(f, infer_schema=False) for f in files
        ]  # a quiet day's file is a header
    ).filter(pl.col("machine_id").is_in(MACHINES))

    def utc(text: str) -> datetime:
        # zoneinfo reads an ambiguous local time as its earlier occurrence
        # (fold=0), the plant's rule.
        local = datetime.fromisoformat(text).replace(tzinfo=PLANT_TZ)
        return local.astimezone(UTC).replace(tzinfo=None)

    return pl.DataFrame(
        {
            "machine_id": frame["machine_id"],
            "started_at": [utc(t) for t in frame["start_time"]],
            "ended_at": [utc(t) for t in frame["end_time"]],
        },
        schema={
            "machine_id": pl.String,
            "started_at": pl.Datetime("us"),
            "ended_at": pl.Datetime("us"),
        },
    )


BAND_SQL = """
case when date_diff('second', {start}, {end}) < 120 then 'under 2 min'
     when date_diff('second', {start}, {end}) <= 300 then '2 to 5 min'
     else 'over 5 min' end
"""


def _band(start: str, end: str) -> str:
    return BAND_SQL.format(start=start, end=end)


def _found(con: duckdb.DuckDBPyConnection, source: str) -> pl.DataFrame:
    """Every true stop, whether the source found it, and if not, why not."""
    return con.sql(
        f"""
        with down as (
            -- A stop's DOWN time, without the breaks or nights inside it,
            -- and its longest piece between them.
            select t.stop_id,
                   sum(date_diff('second', c.started_at, c.ended_at)) as down_seconds,
                   max(date_diff('second', c.started_at, c.ended_at)) as longest_piece_seconds
            from true_stops as t
            join chunks as c
              on c.machine_id = t.machine_id
             and c.started_at >= t.started_at
             and c.ended_at   <= t.ended_at
            group by t.stop_id
        ),

        matched as (
            select t.stop_id,
                   t.machine_id,
                   t.reason,
                   t.started_at,
                   t.ended_at,
                   d.down_seconds,
                   d.longest_piece_seconds,
                   case when d.down_seconds < 120 then 'under 2 min'
                        when d.down_seconds <= 300 then '2 to 5 min'
                        else 'over 5 min' end as band,
                   exists (
                       select 1 from source_stops as s
                       where s.machine_id = t.machine_id
                         and s.started_at < t.ended_at
                         and s.ended_at   > t.started_at
                   ) as found
            from true_stops as t
            join down as d on d.stop_id = t.stop_id
        )

        select '{source}' as source,
               m.*,
               case
                   when m.found then null
                   -- The MES file decides on the stop as the operator saw it,
                   -- from the moment it quit to the moment it ran again.
                   when '{source}' in ('mes', 'both')
                        and date_diff('second', m.started_at, m.ended_at) >= 1800 then 'unexplained'
                   when '{source}' = 'mes' then
                       case when date_diff('second', m.started_at, m.ended_at) < 300 then 'never_written_down'
                            when date_diff('second', m.started_at, m.ended_at) < 1800 then 'not_written_down'
                            else 'unexplained' end
                   when m.machine_id = 'LSR-01' and m.reason = 'TOOLING' then 'laser_tooling_as_setup'
                   when m.machine_id = 'LSR-01' and m.reason = 'MAINTENANCE' then 'laser_maintenance_as_idle'
                   when m.machine_id in ('PB-01', 'PB-02', 'PEM-01') and not exists (
                       select 1 from operation_runs as o
                       where o.machine_id = m.machine_id
                         and o.first_stroke_at_utc < m.ended_at
                         and o.last_stroke_at_utc  > m.started_at
                   ) then 'outside_operation_run'
                   when m.machine_id in ('PB-02', 'PEM-01') and exists (
                       select 1 from machine_counts as c
                       where c.machine_id = m.machine_id
                         and date_diff('second', c.previous_at_utc, c.counted_at_utc) > 45
                         and c.previous_at_utc < m.ended_at
                         and c.counted_at_utc  > m.started_at
                   ) then 'across_lost_polls'
                   when m.machine_id in ('PB-01', 'PB-02', 'PEM-01')
                        and m.longest_piece_seconds < 180 then 'under_gap_rule'
                   else 'unexplained'
               end as miss
        from matched as m
        """
    ).pl()


def _right(con: duckdb.DuckDBPyConnection, source: str) -> pl.DataFrame:
    """Every stop the source reports, and whether the machine was truly down."""
    return con.sql(
        f"""
        select '{source}' as source,
               s.machine_id,
               {_band("s.started_at", "s.ended_at")} as band,
               exists (
                   select 1 from chunks as c
                   where c.machine_id = s.machine_id
                     and c.started_at < s.ended_at
                     and c.ended_at   > s.started_at
               ) as right
        from source_stops as s
        """
    ).pl()


def _hours(con: duckdb.DuckDBPyConnection, source: str) -> pl.DataFrame:
    """True DOWN hours, the source's hours, and how much of each the other covers."""
    return con.sql(
        f"""
        with working as (
            -- The source's stops in working time only. An operator writes a
            -- stop from when the machine quit to when it ran again, so one
            -- that starts on a Friday night and ends on Monday morning spans
            -- a weekend nobody was working; truth counts only working time.
            select s.machine_id,
                   greatest(s.started_at, w.starts_at_utc) as started_at,
                   least(s.ended_at, w.ends_at_utc)        as ended_at
            from source_stops as s
            join work_windows as w
              on s.started_at < w.ends_at_utc
             and s.ended_at   > w.starts_at_utc
        ),

        overlap as (
            select s.machine_id,
                   sum(date_diff('millisecond', greatest(s.started_at, c.started_at),
                                 least(s.ended_at, c.ended_at))) / 3.6e6 as hours
            from working as s
            join chunks as c
              on c.machine_id = s.machine_id
             and c.started_at < s.ended_at
             and c.ended_at   > s.started_at
            group by s.machine_id
        ),

        truth as (
            select machine_id, sum(date_diff('millisecond', started_at, ended_at)) / 3.6e6 as hours
            from chunks group by machine_id
        ),

        written as (
            select machine_id, sum(date_diff('millisecond', started_at, ended_at)) / 3.6e6 as hours
            from source_stops group by machine_id
        ),

        reported as (
            select machine_id, sum(date_diff('millisecond', started_at, ended_at)) / 3.6e6 as hours
            from working group by machine_id
        )

        select '{source}' as source,
               t.machine_id,
               t.hours                  as true_hours,
               coalesce(w.hours, 0)     as written_hours,
               coalesce(r.hours, 0)     as source_hours,
               coalesce(o.hours, 0)     as overlap_hours
        from truth as t
        left join written as w on w.machine_id = t.machine_id
        left join reported as r on r.machine_id = t.machine_id
        left join overlap as o on o.machine_id = t.machine_id
        """
    ).pl()


# --- The report ---------------------------------------------------------------


def _pct(part: float, whole: float) -> str:
    return f"{100 * part / whole:5.1f}%" if whole else "    -"


def report(scores: Scores) -> str:
    days = scores.days
    lines = [
        (
            f"Machine data eval: {days.height:,} machine-days with production, "
            f"{scores.stops.filter(pl.col('source') == 'rebuilt').height:,} true stops"
        ),
        "",
        f"Cycles per machine per day within {WITHIN:.0%} of the truth:",
        "  machine   generation   days   rebuilt   the margin query's arithmetic",
    ]
    for machine in MACHINES:
        mine = days.filter(pl.col("machine_id") == machine)
        lines.append(
            f"  {machine:<9} {GENERATION[machine]:<10} {mine.height:>6}   "
            f"{_pct(mine['rebuilt_ok'].sum(), mine.height)}    "
            f"{_pct(mine['naive_ok'].sum(), mine.height)}"
        )
    lines.append(
        f"  {'all':<9} {'':<10} {days.height:>6}   "
        f"{_pct(days['rebuilt_ok'].sum(), days.height)}    "
        f"{_pct(days['naive_ok'].sum(), days.height)}"
    )
    for column, label in (
        ("rebuilt_miss", "rebuilt"),
        ("naive_miss", "the margin query's arithmetic"),
    ):
        misses = days[column].drop_nulls().value_counts().sort("count", descending=True)
        lines.append(
            f"  {label} misses: {sum(misses['count']) if misses.height else 0}"
        )
        for reason, count in misses.iter_rows():
            lines.append(f"    {count:>5}  {DAY_MISSES[reason]}")

    lines += ["", "Downtime: true stops found (recall), by duration band"]
    lines.append(
        "  source    machine   generation   "
        + "   ".join(f"{b:>11}" for b in BANDS)
        + "         all"
    )
    for source in SOURCES:
        found = scores.stops.filter(pl.col("source") == source)
        for machine in [*MACHINES, "all"]:
            mine = (
                found
                if machine == "all"
                else found.filter(pl.col("machine_id") == machine)
            )
            cells = []
            for band in [*BANDS, None]:
                cell = mine if band is None else mine.filter(pl.col("band") == band)
                cells.append(
                    f"{_pct(cell['found'].sum(), cell.height)} of {cell.height:>3}"
                )
            lines.append(
                f"  {source:<9} {machine:<9} {GENERATION.get(machine, ''):<10}   "
                + "   ".join(cells)
            )

    lines += ["", "Downtime: stops reported that were real (precision)"]
    for source, label in SOURCES.items():
        right = scores.source_stops.filter(pl.col("source") == source)
        per_machine = ", ".join(
            f"{m} {_pct(r['right'].sum(), r.height).strip()} of {r.height}"
            for m in MACHINES
            for r in [right.filter(pl.col("machine_id") == m)]
        )
        lines.append(
            f"  {label}: {_pct(right['right'].sum(), right.height).strip()} "
            f"of {right.height} ({per_machine})"
        )

    lines += ["", "Why a true stop wasn't found:"]
    for source, label in SOURCES.items():
        missed = scores.stops.filter((pl.col("source") == source) & ~pl.col("found"))
        lines.append(f"  {label}: {missed.height} missed")
        counts = (
            missed.group_by("miss")
            .agg(
                stops=pl.len(),
                hours=(pl.col("down_seconds").cast(pl.Float64) / 3600).sum(),
            )
            .sort("stops", descending=True)
        )
        for reason, stops, hours in counts.iter_rows():
            lines.append(f"    {stops:>5}  ({hours:5.1f} h)  {STOP_MISSES[reason]}")

    lines += [
        "",
        "Hours of downtime, in working time: truth, each source, and how much of",
        "each the other covers (MES as written counts the nights and weekends a",
        "stop ran across)",
        (
            "  machine   generation    truth   MES as written      MES  rebuilt"
            "   MES covers  rebuilt covers  both cover   MES right  rebuilt right"
        ),
    ]
    hours = scores.hours.pivot(
        on="source",
        index=["machine_id", "true_hours"],
        values=["written_hours", "source_hours", "overlap_hours"],
    ).sort(pl.col("machine_id").replace_strict({m: i for i, m in enumerate(MACHINES)}))
    totals = hours.drop("machine_id").sum().with_columns(machine_id=pl.lit("all"))
    for row in pl.concat([hours, totals.select(hours.columns)]).iter_rows(named=True):
        machine = row["machine_id"]
        lines.append(
            f"  {machine:<9} {GENERATION.get(machine, ''):<10} "
            f"{row['true_hours']:>8.1f} {row['written_hours_mes']:>16.1f}"
            f" {row['source_hours_mes']:>8.1f} {row['source_hours_rebuilt']:>8.1f}"
            f"      {_pct(row['overlap_hours_mes'], row['true_hours'])}"
            f"          {_pct(row['overlap_hours_rebuilt'], row['true_hours'])}"
            f"      {_pct(row['overlap_hours_both'], row['true_hours'])}"
            f"      {_pct(row['overlap_hours_mes'], row['source_hours_mes'])}"
            f"         {_pct(row['overlap_hours_rebuilt'], row['source_hours_rebuilt'])}"
        )
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", type=Path, default=Path("out"))
    args = parser.parse_args(argv)
    print(report(score(args.out)))


if __name__ == "__main__":
    main()
