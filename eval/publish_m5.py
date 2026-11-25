"""Pack M5's model-run logs for publishing, scrubbed (M5 step 9).

    uv run python eval/publish_m5.py            # AWS (read-only): asks STS for the account
    uv run python eval/publish_m5.py --account 123456789012

Every number M5 reports from a model run is scored from these files: the
backtest's narration (`eval/brief_backtest.py score`) from each brief's log,
and the twenty plant-manager questions (`eval/plant_questions.py score --run
step7`) from each session's log. Everything else those scores read
reproduces from the seed (the backfill, its answer key, and `brief_backtest.py
run`'s facts and rule rows), so only what a model wrote is published.
Scrubbed as M4's are (`publish_sessions.scrub`): the account ID becomes
`123456789012` and the gateway's ID `xxxxxxxxxx`; nothing else is edited.

Out: `out-m5/release/crestline-m5-logs.tar.gz`, its paths relative to
`out-m5/`, so `tar -xzf crestline-m5-logs.tar.gz -C out-m5` puts each file
where the scores read it. The plant questions' six sessions set aside when
the AWS login expired (`step7/excluded-auth/`) are in it too; the score
doesn't read them.
"""

import argparse
from pathlib import Path

import boto3

import publish_sessions

OUT = Path("out-m5")
TARGET = OUT / "release" / "crestline-m5-logs.tar.gz"
BRIEFS = (
    OUT / "backtest" / "briefs"
)  # one folder per model and prompt: sonnet, sonnet-v2, ...
SESSIONS = OUT / "agent" / "sessions" / "plant" / "step7"


def files(out: Path = OUT) -> list[Path]:
    briefs = out / BRIEFS.relative_to(OUT)
    sessions = out / SESSIONS.relative_to(OUT)
    return sorted(
        [p for p in briefs.rglob("*") if p.suffix in {".md", ".jsonl"}]
        + list(sessions.rglob("*.jsonl"))
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--account", help="the account ID to replace (default: STS)")
    args = parser.parse_args()
    account = args.account or boto3.client("sts").get_caller_identity()["Account"]
    count, size = publish_sessions.pack(account, files(), root=OUT, target=TARGET)
    print(f"{count} files, {size / 1e6:.1f} MB: {TARGET}")


if __name__ == "__main__":
    main()
