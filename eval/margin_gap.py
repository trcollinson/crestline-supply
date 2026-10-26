"""Score the margin gap query against the answer key (M1 step 11).

The query (queries/margin_gap.sql) rebuilds what every finished job really
cost from what the shop floor recorded, and never sees the simulation. This
script is the other half. It reads the answer key in out/truth/, which stays
on this machine, and reports:

- how close each job's rebuilt cost came to its true cost (and, for
  comparison, how close the ERP's own "actual" cost came);
- why the rebuild missed where it did. Operators are scanned on for all the
  time they work, so nearly every miss charges too much: scanned-on time
  that nobody worked. The exception is the edge of the extract: a scan
  forgotten on the last working day is only closed at 06:00 the next working
  morning, too late for any file in the extract, so that work is missing;
- whether each brake operation landed on the right brake and shift;
- whether the query's headline holds: margin by brake and setup shift, as
  the ERP reports it, as the query rebuilt it, and as it truly was.

    uv run python eval/margin_gap.py [--out out]

eval/ is the only place outside the simulator and the tests allowed to read
out/truth/.
"""

import argparse
from dataclasses import dataclass
from pathlib import Path

import duckdb
import polars as pl

from crestline import query

TRUTH_TABLES = ["truth_costs", "truth_operations", "truth_machine_states"]
CLOSE = 0.01  # within 1% of the true cost
NEAR = 0.05  # within 5%

# Why a rebuilt job cost missed, most likely first. A miss that fits none
# of these is "unexplained", which the eval should never report.
TOO_MUCH = {
    "forgotten_scan_off": "forgotten scan-off, closed after the real finish",
    "operator_away": "operator away from the machine, still scanned on",
    "machine_idle": "machine idle while scanned on (e.g. waiting out a break)",
}
TOO_LITTLE = {
    "open_at_extract": "scan still open at the extract, so in no MES file yet",
    "split_off_material": "material issued to a job split off from this one",
}

JOBS_SQL = """
with forgotten as (
    select job_num, bool_or(was_left_open) as forgotten_scan_off
    from scans
    group by job_num
),

-- The machine was down because nobody was at it.
away as (
    select distinct job_num
    from truth_machine_states
    where reason = 'NO_OPERATOR'
),

-- An operation that was worked but has no scans in the MES files.
unscanned as (
    select distinct o.job_num
    from truth_operations as o
    where o.labor_hours > 0
      and not exists (
          select 1 from scans as s
          where s.job_num = o.job_num and s.op_seq = o.op_seq
      )
),

-- The planner split the job in the ERP: part of its material went to a
-- child job (J104221-1), though the floor ran the whole lot under this one.
split as (
    select regexp_replace(JobNum, '-1$', '') as job_num
    from erp_jobs
    where JobNum like '%-1'
),

-- The machine sat idle in the middle of one of the job's operations.
idle as (
    select distinct o.job_num
    from truth_operations as o
    join truth_machine_states as s
      on s.machine_id = o.machine_id
     and s.state = 'IDLE'
     and s.started_at >= o.setup_started_at
     and s.ended_at   <= o.completed_at
)

select m.job_num,
       m.brake,
       m.setup_shift,
       m.revenue,
       m.erp_cost,
       m.rebuilt_cost,
       t.total_cost                             as true_cost,
       coalesce(f.forgotten_scan_off, false)    as forgotten_scan_off,
       a.job_num is not null                    as operator_away,
       i.job_num is not null                    as machine_idle,
       u.job_num is not null                    as unscanned_operation,
       sp.job_num is not null                   as split_off
from job_margins as m
join truth_costs as t on t.job_num = m.job_num
left join forgotten as f on f.job_num = m.job_num
left join away      as a on a.job_num = m.job_num
left join idle      as i on i.job_num = m.job_num
left join unscanned as u on u.job_num = m.job_num
left join split     as sp on sp.job_num = m.job_num
"""

BRAKES_SQL = """
with true_setup_shift as (
    select job_num, op_seq, min_by(shift, started_at) as shift
    from truth_machine_states
    where state = 'SETUP'
    group by job_num, op_seq
)

select count(*)                                          as brake_operations,
       count(b.brake)                                    as placed,
       count(*) filter (where b.brake = o.machine_id)    as right_brake,
       count(*) filter (where b.setup_shift = s.shift)   as right_shift
from truth_operations as o
join job_margins as m on m.job_num = o.job_num          -- finished jobs only
left join brake_operations as b
  on b.job_num = o.job_num and b.op_seq = o.op_seq
left join true_setup_shift as s
  on s.job_num = o.job_num and s.op_seq = o.op_seq
where o.work_center = 'BRAKE'
"""

GROUPS_SQL = """
select m.brake,
       coalesce(m.setup_shift, '')                                   as shift,
       count(*)                                                      as jobs,
       round(100 * (1 - sum(m.erp_cost)     / sum(m.revenue)), 1)    as erp_margin_pct,
       round(100 * (1 - sum(m.rebuilt_cost) / sum(m.revenue)), 1)    as rebuilt_margin_pct,
       round(100 * (1 - sum(t.total_cost)   / sum(m.revenue)), 1)    as true_margin_pct
from job_margins as m
join truth_costs as t on t.job_num = m.job_num
group by 1, 2
order by 1, 2
"""


@dataclass(frozen=True)
class Scores:
    jobs: pl.DataFrame  # one row per finished job, with its error and reason
    brakes: dict[str, int]
    groups: pl.DataFrame  # margin by the query's brake and setup shift

    def within(self, cost: str, tolerance: float) -> float:
        """Share of jobs whose `cost` ("rebuilt" or "erp") is within tolerance."""
        return (self.jobs[f"{cost}_error"].abs() < tolerance).mean()

    def misses(self) -> pl.DataFrame:
        return self.jobs.filter(pl.col("rebuilt_error").abs() >= CLOSE)


def score(out_dir: Path, queries_dir: Path = query.QUERIES_DIR) -> Scores:
    con = query.connect(out_dir, queries_dir)
    query.run(con, "margin-gap", queries_dir).fetchall()  # leaves its views on con
    _add_truth(con, out_dir / "truth")

    jobs = (
        con.sql(JOBS_SQL)
        .pl()
        .with_columns(
            rebuilt_error=(pl.col("rebuilt_cost") - pl.col("true_cost"))
            / pl.col("true_cost"),
            erp_error=(pl.col("erp_cost") - pl.col("true_cost")) / pl.col("true_cost"),
        )
        .with_columns(reason=_reason())
        .sort("job_num")
    )
    brakes = con.sql(BRAKES_SQL).pl().row(0, named=True)
    return Scores(jobs, brakes, con.sql(GROUPS_SQL).pl())


def report(scores: Scores) -> str:
    jobs, misses = scores.jobs, scores.misses()
    lines = [
        f"Margin gap eval: {jobs.height:,} finished jobs against the answer key",
        "",
        (
            f"Rebuilt job cost is within 1% of the true cost for "
            f"{scores.within('rebuilt', CLOSE):.1%} of jobs "
            f"(within 5% for {scores.within('rebuilt', NEAR):.1%})."
        ),
        "",
        "Job cost vs. true cost    within 1%   within 5%   median error",
    ]
    for cost, label in [("rebuilt", "rebuilt (the query)"), ("erp", "ERP actual")]:
        median = jobs[f"{cost}_error"].abs().median()
        lines.append(
            f"  {label:<22}{scores.within(cost, CLOSE):>10.1%}"
            f"{scores.within(cost, NEAR):>12.1%}{median:>15.2%}"
        )

    counts = dict(misses["reason"].value_counts().iter_rows())
    lines += ["", f"Its {misses.height} misses of 1% or more:"]
    for heading, reasons in [
        ("charged too much, for scanned-on time nobody worked", TOO_MUCH),
        ("charged too little", TOO_LITTLE),
    ]:
        lines.append(f"  {heading}:")
        lines += [f"  {counts.get(r, 0):>5}  {label}" for r, label in reasons.items()]
    if counts.get("unexplained"):
        lines.append(f"  {counts['unexplained']:>5}  unexplained")

    b = scores.brakes
    lines += [
        "",
        (
            f"Brake operations: {b['placed']:,} of {b['brake_operations']:,} placed; "
            f"right brake for {b['right_brake']:,}, right setup shift for "
            f"{b['right_shift']:,}."
        ),
        "",
        "Margin by brake and setup shift (the query's groups)",
        f"  {'brake':<12}{'shift':<7}{'jobs':>5}{'ERP':>9}{'rebuilt':>10}{'true':>8}",
    ]
    for g in scores.groups.iter_rows(named=True):
        lines.append(
            f"  {g['brake']:<12}{g['shift']:<7}{g['jobs']:>5}"
            f"{g['erp_margin_pct']:>8.1f}%{g['rebuilt_margin_pct']:>9.1f}%"
            f"{g['true_margin_pct']:>7.1f}%"
        )
    return "\n".join(lines)


def _reason() -> pl.Expr:
    """The most likely reason for a job's error, in the direction it went."""
    too_much = pl.col("rebuilt_error") > 0
    return (
        pl.when(too_much & pl.col("forgotten_scan_off"))
        .then(pl.lit("forgotten_scan_off"))
        .when(too_much & pl.col("operator_away"))
        .then(pl.lit("operator_away"))
        .when(too_much & pl.col("machine_idle"))
        .then(pl.lit("machine_idle"))
        .when(~too_much & pl.col("split_off"))
        .then(pl.lit("split_off_material"))
        .when(~too_much & pl.col("unscanned_operation"))
        .then(pl.lit("open_at_extract"))
        .otherwise(pl.lit("unexplained"))
    )


def _add_truth(con: duckdb.DuckDBPyConnection, truth_dir: Path) -> None:
    for table in TRUTH_TABLES:
        path = (truth_dir / f"{table}.parquet").resolve()
        con.execute(f"create view {table} as select * from read_parquet('{path}')")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Score the margin gap query against the answer key in OUT/truth/."
    )
    parser.add_argument(
        "--out", type=Path, default=Path("out"), help="backfill output (default: out)"
    )
    args = parser.parse_args(argv)
    print(report(score(args.out)))


if __name__ == "__main__":
    main()
