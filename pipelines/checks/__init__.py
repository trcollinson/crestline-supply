"""Checks on the shape of the curated data (M3 step 8), run after every merge.

A merge can succeed and still leave the data wrong: a source starts sending
nulls, a key turns up twice, or two tables that should agree stop agreeing.
Three kinds of check, kept few on purpose (noisy checks get ignored):

- **Null rate** per key column, against the table's own trailing baseline:
  a breach is more than one percentage point above the highest rate of the
  last `BASELINE_RUNS` runs. A column that is legitimately null (a job not
  released yet has no release date) sets its own baseline; a sudden jump
  is what trips it. The first run only records a baseline.
- **Primary key uniqueness**: any key held twice is a breach.
- **One reconciliation total per fact table**, two totals that must agree:
  ERP labor (each job's posted hours times its work centers' labor rates is
  its actual labor cost, to the cent) and machines (each plant day's
  rebuilt brake strokes are the pieces the MES reported at BRAKE on scans
  opened that day, within `BRAKE_TOLERANCE`).

A breach fails the run, so the alarm fires: a check that only logs is a
check nobody reads.
"""
