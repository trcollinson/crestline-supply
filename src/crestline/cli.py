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
        "--seed",
        type=int,
        help="random seed (default: the seed in mess.yaml, 42)",
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

    curate = commands.add_parser(
        "curate",
        help="merge new raw ERP pages into the curated tables",
        description=(
            "Do what the poller's curate step does after every run: merge each "
            "entity's raw pages not merged yet into its Iceberg table, with one "
            "Athena MERGE per entity (AWS account and region from AWS_PROFILE and "
            "AWS_REGION)."
        ),
    )
    curate.add_argument(
        "entities",
        nargs="*",
        metavar="ENTITY",
        help="entities to curate (default: all)",
    )
    curate.add_argument(
        "--rebuild",
        action="store_true",
        help="merge from the first raw read, not from the last one merged",
    )
    curate.add_argument(
        "--allow-mass-delete",
        action="store_true",
        help="let a MERGE delete more than 5%% of a table (once you have checked why)",
    )
    curate.add_argument(
        "--dry-run",
        action="store_true",
        help="print each MERGE instead of running it",
    )
    curate.set_defaults(run=_curate)

    erp_api = commands.add_parser("erp-api", help="the deployed ERP API (ErpApiStack)")
    erp_api_commands = erp_api.add_subparsers(
        title="commands", metavar="COMMAND", required=True
    )
    secrets_command = erp_api_commands.add_parser(
        "secrets",
        help="create the API's secrets in Parameter Store",
        description=(
            "Create the deployed ERP API's client secret and token signing key as "
            "SecureString parameters, if missing (AWS account and region from "
            "AWS_PROFILE and AWS_REGION). Run before the first deploy."
        ),
    )
    action = secrets_command.add_mutually_exclusive_group()
    action.add_argument("--rotate", action="store_true", help="replace both values")
    action.add_argument("--delete", action="store_true", help="remove both parameters")
    secrets_command.set_defaults(run=_erp_api_secrets)
    url = erp_api_commands.add_parser("url", help="print the deployed API's URL")
    url.set_defaults(run=_erp_api_url)

    return parser


def _backfill(args: argparse.Namespace) -> None:
    summary = run_backfill(args.days, args.end, args.seed, args.out)
    print(
        f"Simulated {summary.first_day} .. {summary.last_day} "
        f"({args.days} days, seed {summary.seed}): "
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


def _curate(args: argparse.Namespace) -> None:
    from crestline.stack import curate_setup
    from erp_extract import curate
    from erp_extract.curated import merge_sql
    from erp_extract.entities import ENTITIES

    setup = curate_setup()
    for entity in args.entities or ENTITIES:
        if not args.dry_run:
            report = curate.curate(
                entity,
                setup,
                rebuild=args.rebuild,
                max_deleted_share=1.0
                if args.allow_mass_delete
                else curate.MAX_DELETED_SHARE,
            )
            if report["reads"]:
                done = (
                    f"merged {report['reads']} read(s) from {report['from']} "
                    f"through {report['merged_through']}"
                )
            else:
                done = f"nothing new since {report['merged_through']}"
            if "upkept_on" in report:
                done += ", compacted and vacuumed"
            print(f"{entity}: {done}, {_megabytes(report['bytes_scanned'])} scanned")
            continue
        item = setup["table"].get_item(Key={"entity": entity}).get("Item", {})
        merged_through = None if args.rebuild else item.get("merged_through")
        reads = curate.new_reads(setup["s3"], setup["bucket"], entity, merged_through)
        window = curate.pick_window(reads)
        if window is None:
            print(f"-- {entity}: nothing new to merge\n")
            continue
        print(
            merge_sql(
                entity,
                curated_database=setup["curated_database"],
                raw_database=setup["raw_database"],
                first_read=window.first,
                last_read=window.last,
                starts_complete=window.starts_complete,
            )
        )


def _erp_api_secrets(args: argparse.Namespace) -> None:
    from crestline import erp_secrets

    if args.delete:
        removed = erp_secrets.delete()
        print(f"Deleted: {', '.join(removed) or 'nothing (there were none)'}")
        return
    for name, outcome in erp_secrets.ensure(rotate=args.rotate).items():
        print(f"{name}: {outcome}")


def _erp_api_url(args: argparse.Namespace) -> None:
    from crestline.stack import ERP_API_STACK_NAME, stack_outputs

    print(stack_outputs(ERP_API_STACK_NAME)["ErpApiUrl"].rstrip("/"))


def _megabytes(size: int) -> str:
    return f"{size / 1e6:.1f} MB"
