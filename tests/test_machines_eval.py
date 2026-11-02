"""The machine data eval (eval/machines.py), on four weeks of plant history."""

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from crestline.sim.backfill import run_backfill
from machines import main, report, score

REPO_ROOT = Path(__file__).resolve().parent.parent


def backfill(tmp_path_factory, name: str, mess: str | None = None) -> Path:
    tmp = tmp_path_factory.mktemp(name)
    mess_path = REPO_ROOT / "mess.yaml"
    if mess is not None:
        mess_path = tmp / "mess.yaml"
        mess_path.write_text(mess)
    with pytest.MonkeyPatch.context() as mp:
        mp.chdir(REPO_ROOT)
        run_backfill(28, date(2026, 8, 3), 42, tmp / "out", mess_path=mess_path)
        return tmp / "out"


@pytest.fixture(scope="module")
def out(tmp_path_factory) -> Path:
    return backfill(tmp_path_factory, "messy")


@pytest.fixture(scope="module")
def scores(out):
    with pytest.MonkeyPatch.context() as mp:
        mp.chdir(REPO_ROOT)
        return score(out)


@pytest.fixture(scope="module")
def control(tmp_path_factory):
    """The same four weeks with every mess off."""
    out = backfill(tmp_path_factory, "clean", "mess_level: 0\n")
    with pytest.MonkeyPatch.context() as mp:
        mp.chdir(REPO_ROOT)
        return score(out)


def recall(scores, source: str) -> float:
    return scores.stops.filter(pl.col("source") == source)["found"].mean()


def test_every_machine_day_is_within_two_percent(scores):
    assert scores.days.height >= 5 * 15
    assert scores.days_within("rebuilt") == 1.0
    assert scores.days["rebuilt_miss"].null_count() == scores.days.height


def test_post_1_arithmetic_misses_exactly_the_days_a_counter_was_reset(scores):
    naive_misses = scores.days.filter(pl.col("naive_miss").is_not_null())
    assert naive_misses.height > 0
    assert set(naive_misses["naive_miss"]) == {"reset_as_rollover"}
    assert set(naive_misses["machine_id"]) <= {"PB-02", "PEM-01"}


def test_every_missed_stop_has_a_reason(scores):
    missed = scores.stops.filter(~pl.col("found"))
    assert missed.height > 0
    assert "unexplained" not in missed["miss"].to_list()
    by_source = {
        source: set(group["miss"]) for (source,), group in missed.group_by("source")
    }
    assert by_source["mes"] <= {"never_written_down", "not_written_down"}
    assert "laser_maintenance_as_idle" in by_source["rebuilt"]
    assert "under_gap_rule" in by_source["rebuilt"]


def test_the_rebuild_reports_real_stops_and_together_they_beat_either(scores):
    right = scores.source_stops.group_by("source").agg(pl.col("right").mean())
    assert dict(right.iter_rows())["rebuilt"] >= 0.95
    assert recall(scores, "both") > max(
        recall(scores, "rebuilt"), recall(scores, "mes")
    )
    # Short stops are the machines' to find; nobody writes them down.
    short = scores.stops.filter(pl.col("band") == "under 2 min")
    found = dict(short.group_by("source").agg(pl.col("found").mean()).iter_rows())
    assert found["rebuilt"] > found["mes"] + 0.3


def test_the_control_finds_every_stop_in_the_mes_file_and_every_cycle(control):
    assert control.days_within("rebuilt") == 1.0
    assert control.days_within("naive") == 1.0
    assert recall(control, "mes") == 1.0
    # What the machines can't see doesn't depend on the messes.
    misses = set(
        control.stops.filter(~pl.col("found") & (pl.col("source") == "rebuilt"))["miss"]
    )
    assert "across_lost_polls" not in misses
    assert "unexplained" not in misses


def test_the_report_names_every_section(scores):
    text = report(scores)
    for heading in (
        "within 2%",
        "recall",
        "precision",
        "wasn't found",
        "Hours of downtime",
    ):
        assert heading in text


def test_main_prints_the_report(out, capsys):
    with pytest.MonkeyPatch.context() as mp:
        mp.chdir(REPO_ROOT)
        main(["--out", str(out)])
    assert "the margin query's arithmetic" in capsys.readouterr().out
