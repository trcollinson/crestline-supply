"""One run of the robust extractor against a live ERP API.

    uv run python -m erp_extract --base-url http://127.0.0.1:8100 --out out/raw

Reads the state the last run left in `<out>/state.json`, writes each read's
raw pages under `<out>/source=erp-api/...`, and saves the state again. Run it
on a schedule; `--merge` also prints how many rows the raw pages add up to.
Credentials come from `--client-id`/`--client-secret` or
`CRESTLINE_ERP_CLIENT_ID`/`CRESTLINE_ERP_CLIENT_SECRET`.
"""

import argparse
import os
import random
from pathlib import Path

from .client import ErpClient, http_transport
from .merge import rebuild
from .raw import RawWriter
from .robust import RobustExtractor, State


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="python -m erp_extract")
    parser.add_argument("--base-url", required=True, help="e.g. http://127.0.0.1:8100")
    parser.add_argument("--out", type=Path, default=Path("out/raw"))
    parser.add_argument(
        "--client-id",
        default=os.environ.get("CRESTLINE_ERP_CLIENT_ID", "crestline-local"),
    )
    parser.add_argument(
        "--client-secret",
        default=os.environ.get("CRESTLINE_ERP_CLIENT_SECRET", "local-secret"),
    )
    parser.add_argument("--seed", type=int, default=42, help="for backoff jitter")
    parser.add_argument(
        "--merge", action="store_true", help="also report the merged copy"
    )
    args = parser.parse_args(argv)

    args.out.mkdir(parents=True, exist_ok=True)
    state_path = args.out / "state.json"
    client = ErpClient(
        http_transport(args.base_url), args.client_id, args.client_secret
    )
    extractor = RobustExtractor(
        client,
        RawWriter(args.out),
        State.load(state_path),
        rng=random.Random(args.seed),
    )
    extractor.run()
    extractor.state.save(state_path)

    tally = extractor.tally
    statuses = ", ".join(f"{s}: {n}" for s, n in sorted(tally.requests.items()))
    reads = ", ".join(f"{k} {n}" for k, n in sorted(tally.reads.items()))
    run = max(extractor.state.runs.values())
    print(f"Run {run}: reads {reads}; requests {statuses}")
    if tally.abandoned:
        print(f"Reads dropped (next run reads them again): {dict(tally.abandoned)}")
    if args.merge:
        replica, _ = rebuild(args.out)
        held = ", ".join(f"{e} {len(rows):,}" for e, rows in replica.rows.items())
        print(f"Merged copy: {held}")


if __name__ == "__main__":
    main()
