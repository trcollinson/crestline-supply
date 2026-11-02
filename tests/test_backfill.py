import contextlib
import io
import re
import sqlite3
from datetime import date
from pathlib import Path

import duckdb
import pytest

from crestline.cli import main
from crestline.sim.backfill import backfill_window
from crestline.sim.records import DowntimeReason, MachineState, ScrapCause, StopReason

REPO_ROOT = Path(__file__).resolve().parent.parent

# Words only the answer key uses: true machine states, downtime reasons, stop
# reasons, and scrap causes. None of the plant's systems can know these.
TRUTH_ONLY_WORDS = re.compile(
    r"\b(?:"
    + "|".join(
        member.value
        for enum in (MachineState, DowntimeReason, StopReason, ScrapCause)
        for member in enum
    )
    + r")\b"
)
MACHINE_IDS = re.compile(r"\b(?:LSR-01|DB-01|PB-01|PB-02|PEM-01|PC-LINE|AS-01)\b")
# What the brakes cannot know: jobs, orders, operators, part numbers.
BUSINESS_KEYS = re.compile(
    r"\bJ\d{6}\b|\bS\d{6}\b|\bEMP-\d{3}\b|\b(?:ENC|BRK|PNL)-\d{4}\b"
)


def backfill(out: Path, days: int, end: str) -> str:
    stdout = io.StringIO()
    with pytest.MonkeyPatch.context() as mp, contextlib.redirect_stdout(stdout):
        mp.chdir(REPO_ROOT)  # reference/, scenarios.yaml, mess.yaml
        main(["sim", "backfill", "--days", str(days), "--end", end, "--out", str(out)])
    return stdout.getvalue()


@pytest.fixture(scope="module")
def out(tmp_path_factory) -> Path:
    out = tmp_path_factory.mktemp("out")
    printed = backfill(out, days=14, end="2026-07-20")
    assert "2026-07-06 .. 2026-07-19" in printed
    return out


def test_end_is_exclusive():
    first_day, last_day = backfill_window(90, date(2026, 10, 1))

    assert (first_day, last_day) == (date(2026, 7, 3), date(2026, 9, 30))
    assert (last_day - first_day).days + 1 == 90


def test_rejects_an_empty_window():
    with pytest.raises(ValueError):
        backfill_window(0, date(2026, 10, 1))


def test_writes_the_answer_key_and_every_system(out):
    assert sorted(p.name for p in (out / "truth").iterdir()) == [
        "truth_costs.parquet",
        "truth_cycles.parquet",
        "truth_jobs.parquet",
        "truth_labor.parquet",
        "truth_machine_states.parquet",
        "truth_operations.parquet",
    ]
    assert len(list((out / "mes" / "outbound").glob("labor_*.csv"))) == 14
    assert len(list((out / "mes" / "outbound").glob("downtime_*.csv"))) == 14
    assert (out / "erp" / "erp.db").exists()
    assert len(list((out / "erp" / "export").glob("*.jsonl"))) == 11
    assert sorted(p.name for p in (out / "machines").iterdir()) == [
        "lsr01.jsonl",
        "pb01.jsonl",
        "pb02.jsonl",
        "pcline.jsonl",
        "pem01.jsonl",
    ]

    (first_release, last_release) = duckdb.sql(
        f"select min(release_date), max(release_date) "
        f"from '{out / 'truth' / 'truth_jobs.parquet'}'"
    ).fetchone()
    assert first_release >= date(2026, 7, 6)
    assert last_release <= date(2026, 7, 19)


def test_system_outputs_hold_nothing_only_the_answer_key_knows(out):
    """M1 step 6's done-when: every fact in these files is one a system could see."""
    erp_db_rows = []
    with sqlite3.connect(out / "erp" / "erp.db") as db:
        tables = [
            name
            for (name,) in db.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        ]
        assert "Jobs_History" in tables  # every superseded version is checked too
        for table in tables:
            erp_db_rows += [str(row) for row in db.execute(f"SELECT * FROM {table}")]
    system_text = {
        "erp.db": "\n".join(erp_db_rows),
        **{
            str(path.relative_to(out)): path.read_text(encoding="utf-8")
            for folder in ("mes", "erp", "machines")
            for path in (out / folder).rglob("*")
            if path.suffix in {".csv", ".jsonl"}
        },
    }
    assert len(system_text) == 1 + 14 + 14 + 11 + 5

    for name, text in system_text.items():
        assert not TRUTH_ONLY_WORDS.search(text), (name, TRUTH_ONLY_WORDS.search(text))
        if name.startswith("mes/outbound/downtime_"):
            pass  # the downtime log names the machine that stopped: that is its job
        elif name == "machines/lsr01.jsonl":
            # A Sparkplug topic names its device; it must not name another.
            assert set(MACHINE_IDS.findall(text)) <= {"LSR-01"}, name
        else:
            assert not MACHINE_IDS.search(text), (name, MACHINE_IDS.search(text))
        if name.startswith("machines"):
            assert not BUSINESS_KEYS.search(text), (name, BUSINESS_KEYS.search(text))


def test_a_shorter_rerun_leaves_no_stale_files(tmp_path):
    backfill(tmp_path, days=14, end="2026-07-20")
    (tmp_path / "machines" / "pb03.jsonl").write_text("")  # as if an old run wrote it
    backfill(tmp_path, days=7, end="2026-07-20")

    assert len(list((tmp_path / "mes" / "outbound").glob("labor_*.csv"))) == 7
    assert len(list((tmp_path / "mes" / "outbound").glob("downtime_*.csv"))) == 7
    assert len(list((tmp_path / "machines").glob("*.jsonl"))) == 5


def test_two_runs_are_byte_identical(out, tmp_path):
    """Same seed, same files: every imperfection flows from the seeded streams."""
    backfill(tmp_path, days=14, end="2026-07-20")
    files = sorted(p.relative_to(out) for p in out.rglob("*") if p.is_file())

    assert files == sorted(
        p.relative_to(tmp_path) for p in tmp_path.rglob("*") if p.is_file()
    )
    assert len(files) == 6 + 1 + 14 + 14 + 11 + 5
    for file in files:
        assert (out / file).read_bytes() == (tmp_path / file).read_bytes(), file
