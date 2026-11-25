"""Pack the agent's session logs for publishing, scrubbed (M4 step 8).

    uv run python eval/publish_sessions.py            # AWS (read-only): asks STS for the account
    uv run python eval/publish_sessions.py --account 123456789012

Every number reported from a model run is scored from these logs
(`eval/injection.py score --sessions <dir>/sessions/injection`), so they are
published as they were written, with two things replaced: the AWS account
ID (in each role's ARN) becomes `123456789012`, the placeholder AWS's own
documentation uses, and the gateway's ID (in its URL) becomes `xxxxxxxxxx`.
Nothing else is edited, and the archive is checked afterwards: if the
account ID survives anywhere in it, nothing is written.

Out: `out/release/crestline-m4-sessions.tar.gz`, holding `sessions/`
(injection, benign, and the demo) and the files beside it in `out/agent/`
(the guardrail surveys, the reason texts' scores, latency, and the per-session
scores).
"""

import argparse
import io
import re
import tarfile
from pathlib import Path

import boto3

AGENT = Path("out/agent")
TARGET = Path("out/release/crestline-m4-sessions.tar.gz")
PLACEHOLDER_ACCOUNT = "123456789012"
GATEWAY_ID = re.compile(r"(crestline-agent-)[a-z0-9]{10}")


def files() -> list[Path]:
    logs = sorted((AGENT / "sessions").rglob("*.jsonl"))
    beside = sorted(p for p in AGENT.iterdir() if p.suffix in {".jsonl", ".csv"})
    return logs + beside


def scrub(text: str, account: str) -> str:
    text = text.replace(account, PLACEHOLDER_ACCOUNT)
    return GATEWAY_ID.sub(r"\1xxxxxxxxxx", text)


def pack(
    account: str,
    paths: list[Path] | None = None,
    root: Path = AGENT,
    target: Path = TARGET,
) -> tuple[int, int]:
    """Write `paths` (M4's logs by default) to `target`, named from `root`, scrubbed."""
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as tar:
        for path in files() if paths is None else paths:
            data = scrub(path.read_text(), account).encode()
            if account.encode() in data:
                raise SystemExit(f"the account ID survived scrubbing in {path}")
            info = tarfile.TarInfo(str(path.relative_to(root)))
            info.size, info.mtime = len(data), int(path.stat().st_mtime)
            tar.addfile(info, io.BytesIO(data))
    with tarfile.open(fileobj=io.BytesIO(buffer.getvalue())) as tar:
        for member in tar:
            if account.encode() in tar.extractfile(member).read():
                raise SystemExit(f"the account ID is in {member.name}")
        count = len(tar.getmembers())
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(buffer.getvalue())
    return count, len(buffer.getvalue())


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--account", help="the account ID to replace (default: STS)")
    args = parser.parse_args()
    account = args.account or boto3.client("sts").get_caller_identity()["Account"]
    count, size = pack(account)
    print(f"{count} files, {size / 1e6:.1f} MB: {TARGET}")


if __name__ == "__main__":
    main()
