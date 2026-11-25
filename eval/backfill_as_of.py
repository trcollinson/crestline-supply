"""A backfill's output as the plant's systems held it on one morning (M5 step 3).

The morning brief's look-ahead test needs a second copy of the data, made a
different way from the brief's own cut (queries/as_of.sql), and compared
with it. This one copies the files, keeping what had physically arrived by
05:30 plant time on the morning:

- the MES nightly files dated before the morning (each lands at 02:15 the
  next day);
- each machine's messages published before 05:30;
- the ERP **as it stood** at 05:30, every row in the version it had then,
  rebuilt from the store's history (`erp.db`) by the ERP API's own as-of
  reader. The brief's cut can't do that: the curated tables keep each
  row's newest version only.

    uv run python eval/backfill_as_of.py --morning 2026-08-17 --out out-m5 --to /tmp/as-of

Only the files the brief's sources read are copied (the ERP export, the MES
files, the machine files); the answer key is not.
"""

import argparse
import json
import re
import shutil
from datetime import UTC, date, datetime, time
from pathlib import Path
from zoneinfo import ZoneInfo

from crestline_erp_api.store import Store

PLANT_TZ = ZoneInfo("America/Denver")
BRIEF_TIME = time(5, 30)
NIGHTLY_FILE = re.compile(r"^(labor|downtime)_(\d{8})\.csv$")
PUBLISHED_AT = re.compile(rb'"published_at":"([^"]+)"')


def cut_at(morning: date) -> datetime:
    """05:30 plant time on the morning, in UTC: when the brief runs."""
    return datetime.combine(morning, BRIEF_TIME, tzinfo=PLANT_TZ).astimezone(UTC)


def cut_backfill(source: Path, target: Path, morning: date) -> None:
    """Copy `source`'s output to `target` as it stood at 05:30 on `morning`."""
    cut = cut_at(morning)
    _copy_mes(source / "mes" / "outbound", target / "mes" / "outbound", morning)
    _copy_machines(source / "machines", target / "machines", cut)
    _write_erp_as_of(source / "erp" / "erp.db", target / "erp" / "export", cut)


def _copy_mes(source: Path, target: Path, morning: date) -> None:
    target.mkdir(parents=True, exist_ok=True)
    for path in sorted(source.iterdir()):
        match = NIGHTLY_FILE.match(path.name)
        if match and date.fromisoformat(match[2]) < morning:  # YYYYMMDD
            shutil.copy2(path, target / path.name)


def _copy_machines(source: Path, target: Path, cut: datetime) -> None:
    """Each device's lines published before the cut (the files are in
    `published_at` order, but the check doesn't rely on it)."""
    target.mkdir(parents=True, exist_ok=True)
    for path in sorted(source.glob("*.jsonl")):
        with path.open("rb") as lines, (target / path.name).open("wb") as kept:
            for line in lines:
                published_at = datetime.fromisoformat(
                    PUBLISHED_AT.search(line)[1].decode()
                )
                if published_at < cut:
                    kept.write(line)


def _write_erp_as_of(store_path: Path, target: Path, cut: datetime) -> None:
    """The ERP export as of the cut: every entity's rows in their version then."""
    target.mkdir(parents=True, exist_ok=True)
    store = Store(store_path)
    as_of = cut.strftime("%Y-%m-%dT%H:%M:%SZ")  # the store's own format
    for entity in store.entities.values():
        source, parameters = entity.source(as_of)
        columns = ", ".join(f'"{column}"' for column in entity.columns)
        rows = store.fetch(f"SELECT {columns} FROM {source}", parameters)
        with (target / f"{entity.name}.jsonl").open("w", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(entity.record(row), separators=(",", ":")) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--morning", type=date.fromisoformat, required=True)
    parser.add_argument("--out", type=Path, default=Path("out"), help="the backfill")
    parser.add_argument(
        "--to", type=Path, required=True, help="where to write the copy"
    )
    args = parser.parse_args()
    cut_backfill(args.out, args.to, args.morning)
    print(f"{args.out} as of {cut_at(args.morning):%Y-%m-%d %H:%M}Z -> {args.to}")


if __name__ == "__main__":
    main()
