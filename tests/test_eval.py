"""The margin gap eval, scored on four weeks of plant history."""

import contextlib
import io
from pathlib import Path

import pytest

from crestline.cli import main as crestline
from margin_gap import main, report, score

REPO_ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture(scope="module")
def out(tmp_path_factory) -> Path:
    out = tmp_path_factory.mktemp("out")
    with pytest.MonkeyPatch.context() as mp, contextlib.redirect_stdout(io.StringIO()):
        mp.chdir(REPO_ROOT)
        crestline(
            [
                "sim",
                "backfill",
                "--days",
                "28",
                "--end",
                "2026-08-03",
                "--out",
                str(out),
            ]
        )
    return out


@pytest.fixture(scope="module")
def scores(out):
    with pytest.MonkeyPatch.context() as mp:
        mp.chdir(REPO_ROOT)
        return score(out)


def test_rebuilt_cost_is_close_to_the_truth_and_beats_the_erp(scores):
    assert scores.within("rebuilt", 0.05) >= 0.85
    assert scores.within("rebuilt", 0.05) >= scores.within("erp", 0.05) + 0.2


def test_every_miss_has_a_reason(scores):
    misses = scores.misses()
    assert misses.height > 0
    assert "unexplained" not in misses["reason"].to_list()
    assert "forgotten_scan_off" in misses["reason"].to_list()


def test_undercharges_come_only_from_scans_open_at_the_extract(scores):
    # These four weeks end on a Friday: scans forgotten that day are closed
    # at 06:00 Monday, the moment of the extract, so no file has them yet.
    under = scores.misses().filter(scores.misses()["rebuilt_error"] < 0)
    assert under.height > 0
    assert set(under["reason"]) == {"open_at_extract"}


def test_brake_operations_land_on_the_right_brake_and_shift(scores):
    brakes = scores.brakes
    assert brakes["placed"] >= 0.99 * brakes["brake_operations"]
    assert brakes["right_brake"] >= 0.99 * brakes["placed"]
    assert brakes["right_shift"] >= 0.99 * brakes["placed"]


def test_the_truth_confirms_pb02_second_shift(scores):
    groups = {(g["brake"], g["shift"]): g for g in scores.groups.iter_rows(named=True)}
    pb02_2nd = groups.pop(("PB-02", "2nd"))
    brakes = [g for (brake, _), g in groups.items() if brake.startswith("PB-")]
    # Truly the least profitable brake and shift...
    assert pb02_2nd["true_margin_pct"] <= min(g["true_margin_pct"] for g in brakes) - 3
    # ...and the ERP says otherwise.
    assert pb02_2nd["erp_margin_pct"] >= pb02_2nd["true_margin_pct"] + 3


def test_command_line_prints_the_headline(out, capsys):
    with pytest.MonkeyPatch.context() as mp:
        mp.chdir(REPO_ROOT)
        main(["--out", str(out)])
    printed = capsys.readouterr().out
    assert "Rebuilt job cost is within 1% of the true cost for" in printed
    assert "PB-02" in printed


def test_report_lists_every_reason(scores):
    text = report(scores)
    assert "forgotten scan-off" in text
    assert "unexplained" not in text
