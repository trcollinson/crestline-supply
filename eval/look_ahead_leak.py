"""What a backtest that reads one day ahead would have told the plant (M5 step 9).

The brief for a morning may use only what had arrived by 05:30 that morning
(queries/as_of.sql). The easiest way to break that is quiet: a scan someone
forgot to close is closed by a supervisor at 06:00 the next working morning
and lands in that day's file, so on a Monday the scans Friday's shifts left
open have not arrived yet, but the full data has them, already closed.

This script computes one morning's facts twice on DuckDB: as the brief does,
and with one leak let in, the scans opened before the morning that closed
after it. Then it lists the scans that leaked and every fact that moved.
Nothing in the leaked run looks wrong: no fact is missing, no alert fires
that shouldn't, the numbers are just a little better than the plant knew.

    uv run python eval/look_ahead_leak.py --morning 2026-08-17 [--out out]

Reads the backfill's own files, not the answer key, and no AWS.
"""

import argparse
from datetime import date
from pathlib import Path

import duckdb
import polars as pl

from crestline.brief import facts

# The leak: scans opened before the morning, wherever they closed. The brief's
# own cut (as_of.sql) keeps only scans that closed before it.
LEAKY_MES_LABOR = """
create or replace view mes_labor as
select * from crestline_raw.mes_labor
where scan_on_local < (select cast(morning as timestamp) from brief_cut)
"""

LEAKED_SCANS = """
select employee_id, job_num, op_seq, work_center,
       scan_on_local, scan_off_local, qty_good, qty_scrap
from crestline_raw.mes_labor
where scan_on_local < (select cast(morning as timestamp) from brief_cut)
  and scan_off_local >= (select cast(morning as timestamp) from brief_cut)
order by scan_on_local
"""


def leaked_facts(
    out: Path, morning: date
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """The morning's facts as the brief sees them, with the leak, and the scans that leaked."""
    con: duckdb.DuckDBPyConnection = facts.connect(out, morning)
    honest = facts.facts(con)
    scans = con.sql(LEAKED_SCANS).pl()
    con.execute(LEAKY_MES_LABOR)
    leaky = facts.facts(con)
    return honest, leaky, scans


def moved(honest: pl.DataFrame, leaky: pl.DataFrame) -> pl.DataFrame:
    """Every fact whose value or baseline differs between the two runs."""
    columns = ["fact_id", "plant_day", "value", "baseline", "unit"]
    both = honest.select(columns).join(
        leaky.select(columns), on="fact_id", how="full", suffix="_leaked", coalesce=True
    )
    return both.filter(
        pl.col("value").ne_missing(pl.col("value_leaked"))
        | pl.col("baseline").ne_missing(pl.col("baseline_leaked"))
        | pl.col("plant_day").ne_missing(pl.col("plant_day_leaked"))
    ).select("fact_id", "value", "value_leaked", "baseline", "baseline_leaked", "unit")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--morning", type=date.fromisoformat, required=True)
    parser.add_argument("--out", type=Path, default=Path("out"), help="the backfill")
    args = parser.parse_args()

    honest, leaky, scans = leaked_facts(args.out, args.morning)
    changes = moved(honest, leaky)
    alerts = changes.filter(pl.col("fact_id").str.starts_with("anomaly:"))
    plant_days = sorted(set(honest["plant_day"]) | set(leaky["plant_day"]))

    with pl.Config(tbl_rows=-1, tbl_cols=-1, tbl_width_chars=160, fmt_str_lengths=60):
        print(f"Morning {args.morning}, plant day {', '.join(map(str, plant_days))}.")
        print(
            f"\nScans opened before the morning that hadn't arrived ({scans.height}):"
        )
        print(scans)
        print(f"\nFacts that moved: {changes.height} of {honest.height}")
        print(changes)
        print(
            f"\nAlerts that changed: {alerts.height}"
            + ("" if alerts.height else " (the leak is quiet)")
        )


if __name__ == "__main__":
    main()
