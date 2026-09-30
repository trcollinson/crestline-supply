"""The `crestline` command line.

This lives in its own module rather than the package `__init__` so that
importing any `crestline` subpackage does not also import the simulator:
pipeline code must never import from `sim` (the truth-isolation rule).
"""

import argparse
from collections import defaultdict
from datetime import date
from pathlib import Path

import polars as pl

from crestline import query, upload
from crestline.athena import Athena
from crestline.sim.backfill import run_backfill

MAX_FILES_LISTED = 10


def main(argv: list[str] | None = None) -> None:
    args = _parser().parse_args(argv)
    args.run(args)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="crestline",
        description="Crestline Supply plant simulator and data platform CLI.",
    )
    commands = parser.add_subparsers(title="commands", metavar="COMMAND", required=True)

    sim = commands.add_parser("sim", help="run the plant simulation")
    sim_commands = sim.add_subparsers(
        title="commands", metavar="COMMAND", required=True
    )

    backfill = sim_commands.add_parser(
        "backfill",
        help="simulate a stretch of history and write its outputs",
        description=(
            "Simulate the DAYS days before END. Writes the answer key to "
            "OUT/truth/, and what the MES, ERP, and brake controllers recorded "
            "to OUT/mes/, OUT/erp/, and OUT/machines/, as extracted at 06:00 "
            "plant time on END."
        ),
    )
    backfill.add_argument(
        "--days", type=int, default=90, help="days to simulate (default: 90)"
    )
    backfill.add_argument(
        "--end",
        type=date.fromisoformat,
        required=True,
        help="first day after the window, YYYY-MM-DD (exclusive)",
    )
    backfill.add_argument(
        "--seed", type=int, default=42, help="random seed (default: 42)"
    )
    backfill.add_argument(
        "--out", type=Path, default=Path("out"), help="output folder (default: out)"
    )
    backfill.add_argument(
        "--upload",
        action="store_true",
        help=(
            "then push the MES, ERP, and machine files to the raw bucket "
            "(AWS account and region from AWS_PROFILE and AWS_REGION)"
        ),
    )
    backfill.set_defaults(run=_backfill)

    query_command = commands.add_parser(
        "query",
        help="run a query from queries/ against a backfill's output",
        description=(
            "Run queries/NAME.sql over the MES, ERP, and machine data (never "
            "the answer key): with DuckDB over the files in OUT, or with "
            "Athena over the raw bucket."
        ),
    )
    query_command.add_argument("name", help="query to run, e.g. margin-gap")
    query_command.add_argument(
        "--out", type=Path, default=Path("out"), help="backfill output (default: out)"
    )
    query_command.add_argument(
        "--engine",
        choices=["duckdb", "athena"],
        default="duckdb",
        help=(
            "duckdb reads OUT on this machine; athena reads the raw bucket "
            "(run `crestline athena setup` first) (default: duckdb)"
        ),
    )
    query_command.set_defaults(run=_query)

    athena = commands.add_parser("athena", help="set up Athena over the raw bucket")
    athena_commands = athena.add_subparsers(
        title="commands", metavar="COMMAND", required=True
    )
    setup = athena_commands.add_parser(
        "setup",
        help="create the raw tables and source views",
        description=(
            "Create the raw tables (queries/athena/tables.sql) and source views "
            "(queries/athena/sources.sql) in the stack's Glue database. Run "
            "again after uploading a new extract (AWS account and region from "
            "AWS_PROFILE and AWS_REGION)."
        ),
    )
    setup.set_defaults(run=_athena_setup)

    return parser


def _backfill(args: argparse.Namespace) -> None:
    summary = run_backfill(args.days, args.end, args.seed, args.out)
    print(
        f"Simulated {summary.first_day} .. {summary.last_day} "
        f"({args.days} days, seed {args.seed}): "
        f"{summary.jobs:,} jobs, {summary.jobs_complete:,} complete"
    )
    by_folder: dict[Path, list[tuple[Path, int]]] = defaultdict(list)
    for path, rows in summary.written.items():
        by_folder[path.parent].append((path, rows))
    for folder, files in by_folder.items():
        if len(files) > MAX_FILES_LISTED:  # the nightly MES files
            rows = sum(rows for _, rows in files)
            print(f"  {folder}/  {len(files)} files, {rows:,} rows")
        else:
            for path, rows in files:
                print(f"  {path}  {rows:,} rows")

    if args.upload:
        uploaded = upload.upload_backfill(
            args.out, ingest_date=args.end, bucket=upload.raw_bucket_name()
        )
        print(
            f"Uploaded to s3://{uploaded.bucket}/ (ingest_date={args.end}): "
            f"{len(uploaded.uploaded)} new or changed, "
            f"{len(uploaded.unchanged)} unchanged, "
            f"{len(uploaded.deleted)} deleted"
        )


def _query(args: argparse.Namespace) -> None:
    if args.engine == "duckdb":
        con = query.connect(args.out)
        query.run(con, args.name).show(max_width=200)
        return

    athena = Athena.from_stack()
    result = athena.run_query(args.name)
    with pl.Config(tbl_rows=-1, tbl_cols=-1, tbl_width_chars=200):
        print(result.frame)
    print(
        f"Athena workgroup {athena.workgroup}: {_megabytes(result.bytes_scanned)} scanned"
    )


def _athena_setup(args: argparse.Namespace) -> None:
    athena = Athena.from_stack()
    scanned = athena.setup()
    print(
        f"Created the raw tables and source views in {athena.database} "
        f"(workgroup {athena.workgroup}): {_megabytes(scanned)} scanned"
    )


def _megabytes(size: int) -> str:
    return f"{size / 1e6:.1f} MB"
