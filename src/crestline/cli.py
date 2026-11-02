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
        "--mess",
        type=Path,
        default=Path("mess.yaml"),
        help="the interface imperfections to simulate (default: mess.yaml)",
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

    machines = commands.add_parser(
        "machines", help="the gateways in AWS IoT Core (MachineIngestStack)"
    )
    machines_commands = machines.add_subparsers(
        title="commands", metavar="COMMAND", required=True
    )
    setup = machines_commands.add_parser(
        "setup",
        help="create the gateways' certificates and upload the Sparkplug schema",
        description=(
            "Create a certificate and private key for each gateway, attached to "
            "its thing and policy, if missing (keys go to .secrets/machines/, "
            "gitignored), and put the Sparkplug B descriptor set in the stack's "
            "schema bucket. Run after deploying CrestlineMachineIngest. --delete "
            "deletes the certificates and the descriptor set; run it before cdk "
            "destroy."
        ),
    )
    setup.add_argument("--delete", action="store_true", help="undo it all")
    setup.set_defaults(run=_machines_setup)
    publish = machines_commands.add_parser(
        "publish",
        help="replay the backfill's machine files into IoT Core",
        description=(
            "Send every machine message published in the plant-local days FROM to "
            "TO (inclusive) to AWS IoT Core, in publish order, SPEED times faster, "
            "each gateway with its own certificate; then write a manifest to "
            "OUT/replays/."
        ),
    )
    publish.add_argument("--from", dest="first", type=date.fromisoformat, required=True)
    publish.add_argument("--to", dest="last", type=date.fromisoformat, required=True)
    publish.add_argument("--speed", type=float, default=60.0, help="default: 60")
    publish.add_argument(
        "--out", type=Path, default=Path("out"), help="backfill output (default: out)"
    )
    publish.add_argument(
        "--devices", nargs="+", help="only these devices (lsr01 pb01 pb02 pem01 pcline)"
    )
    publish.add_argument(
        "--limit", type=int, help="send only the first N messages (a smoke test)"
    )
    publish.set_defaults(run=_machines_publish)
    account = machines_commands.add_parser(
        "account",
        help="check a replay landed: sent = raw - redelivered + errors, per device",
    )
    account.add_argument("--replay", help="the replay's name (default: the latest)")
    account.add_argument("--out", type=Path, default=Path("out"))
    account.set_defaults(run=_machines_account)
    load = machines_commands.add_parser(
        "loadtest",
        help="send 1,000 synthetic tags through IoT Core to measure the cost",
        description=(
            "Send a synthetic gateway's 1,000 tags at 1 Hz through the deployed "
            "CrestlineMachineLoadtest stack for MINUTES: mode A one message per tag "
            "change, mode B one batch a second, together unless --modes says "
            "otherwise. --report NAME prints what CloudWatch counted for a run; "
            "--cleanup deletes the certificate and empties the bucket before cdk "
            "destroy."
        ),
    )
    load.add_argument("--minutes", type=int, default=60, help="default: 60")
    load.add_argument("--modes", nargs="+", choices=["a", "b"], default=["a", "b"])
    load.add_argument("--out", type=Path, default=Path("out"))
    load_action = load.add_mutually_exclusive_group()
    load_action.add_argument(
        "--report", metavar="NAME", help="report on a finished run"
    )
    load_action.add_argument(
        "--cleanup", action="store_true", help="tidy up for destroy"
    )
    load.set_defaults(run=_machines_loadtest)

    return parser


def _backfill(args: argparse.Namespace) -> None:
    summary = run_backfill(
        args.days, args.end, args.seed, args.out, mess_path=args.mess
    )
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
        f"Created the raw tables, source views, and machine views in {athena.database} "
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
            try:
                report = curate.curate(
                    entity,
                    setup,
                    rebuild=args.rebuild,
                    max_deleted_share=1.0
                    if args.allow_mass_delete
                    else curate.MAX_DELETED_SHARE,
                )
            except curate.CurateFailed as failure:
                # A refusal is the guard working, not a crash: say why, and stop.
                raise SystemExit(f"{entity}: {failure}") from None
            if report["reads"]:
                done = (
                    f"merged {report['reads']} read(s) from {report['from']} "
                    f"through {report['merged_through']}"
                )
            else:
                done = f"nothing new since {report['merged_through']}"
            if report.get("audit"):
                done += f", {report['rows_deleted_actual']:,} deleted (audit: {report['audit']})"
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


def _machines_setup(args: argparse.Namespace) -> None:
    from crestline import machine_setup

    bucket = machine_setup.schema_bucket()
    if args.delete:
        for gateway, count in machine_setup.delete().items():
            print(f"{gateway}: {count} certificate(s) deleted")
        machine_setup.delete_schema(bucket)
        print(f"Schema deleted from {bucket}")
        return
    for gateway, outcome in machine_setup.ensure().items():
        print(f"{gateway}: {outcome}")
    machine_setup.put_schema(bucket)
    print(f"Schema uploaded to {bucket}")


def _machines_publish(args: argparse.Namespace) -> None:
    from datetime import UTC, datetime
    from itertools import islice

    from crestline import machine_setup
    from crestline.replay_account import REPLAYS_DIR
    from crestline_machines import publisher

    devices = publisher.devices_named(args.devices)
    start, end = publisher.plant_days(args.first, args.last)
    messages = publisher.outgoing(args.out / "machines", start, end, devices)
    if args.limit:
        messages = islice(messages, args.limit)
    connections = publisher.connections_needed(
        args.out / "machines", start, end, devices, args.speed
    )
    print(f"Connections: {connections}")
    clients = publisher.connect(
        machine_setup.endpoint(), machine_setup.CERTS_DIR, connections
    )
    name = publisher.replay_name(datetime.now(UTC))
    print(
        f"Replay {name}: {start:%Y-%m-%d %H:%M}Z to {end:%Y-%m-%d %H:%M}Z at {args.speed:g}x"
    )
    manifest = publisher.replay(
        messages, clients, name=name, start=start, end=end, speed=args.speed
    )
    path = args.out / REPLAYS_DIR / f"{name}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(manifest.to_json(), encoding="utf-8")
    for device, tally in manifest.devices.items():
        print(f"  {device}: {tally.sent:,} sent")
    print(f"  acknowledged {manifest.acknowledged}, refused {manifest.refused}")
    for gateway, drops in manifest.disconnects.items():
        for drop in drops:
            print(f"  {gateway} dropped and reconnected: {drop}")
    print(f"Manifest: {path}")


def _machines_account(args: argparse.Namespace) -> None:
    from crestline import replay_account

    path = (
        args.out / replay_account.REPLAYS_DIR / f"{args.replay}.json"
        if args.replay
        else replay_account.latest_manifest(args.out)
    )
    result = replay_account.account(path, upload.raw_bucket_name())
    print(result.report())
    if not result.ok:
        raise SystemExit(1)


def _machines_loadtest(args: argparse.Namespace) -> None:
    from crestline import machine_loadtest

    if args.cleanup:
        certificates, objects = machine_loadtest.cleanup()
        print(f"Deleted {certificates} certificate(s) and {objects:,} object(s)")
        return
    if args.report:
        path = args.out / machine_loadtest.LOADTESTS_DIR / f"{args.report}.json"
        for mode, counts in machine_loadtest.report(path).items():
            print(f"Mode {mode.upper()}")
            for label, value in counts.items():
                print(f"  {label:<24} {value:>16,.0f}")
        return
    path = machine_loadtest.run(args.out, args.minutes, args.modes)
    print(Path(path).read_text(encoding="utf-8"))


def _megabytes(size: int) -> str:
    return f"{size / 1e6:.1f} MB"
