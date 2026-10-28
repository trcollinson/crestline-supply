"""The extraction eval: the naive extractor, nightly, against the messy changing API.

A short backfill (12 days) stands in for the 90. The control is the point: on
an ERP with nothing wrong, the naive extractor is exactly right, so every
miss in the other runs is owed to a mess.
"""

import copy
import re

import pytest

from crestline.mess import MessConfig
from crestline.sim.systems import erp
from erp_extract.formats import LEGACY
from erp_extract.raw import SOURCE
from extraction import (
    Scenario,
    final_state,
    main,
    normalize,
    report,
    run_naive,
    run_robust,
    score,
    window,
)

CLEAN_API = MessConfig(mess_level=0)
# Everything the ERP can get wrong, at rates high enough for 12 days to show it.
MESSY_ERP = {
    "soft_delete_rate": 0.1,
    "job_split_rate": 0.1,
    "lastmodified_skips": ["JobOperations.qty", "LaborDetails.post"],
}


@pytest.fixture(scope="module")
def clean_db(erp_changes, tmp_path_factory):
    out = tmp_path_factory.mktemp("clean")
    erp.write_erp(erp_changes, out)
    return out / "erp.db"


@pytest.fixture(scope="module")
def messy_db(build_erp_changes, erp_mess, tmp_path_factory):
    out = tmp_path_factory.mktemp("messy")
    erp.write_erp(build_erp_changes(erp_mess(**MESSY_ERP)), out)
    return out / "erp.db"


@pytest.fixture(scope="module")
def messy(messy_db):
    replica, tally, nights = run_naive(messy_db, MessConfig())
    return score(messy_db, replica, tally, nights)


def test_the_window_is_the_days_the_store_covers(clean_db, plant_run):
    first, last = window(clean_db)

    assert first == plant_run.first_day
    assert last == plant_run.last_day


def test_on_an_erp_with_nothing_wrong_the_naive_extractor_is_exactly_right(clean_db):
    replica, tally, nights = run_naive(clean_db, CLEAN_API)
    scores = score(clean_db, replica, tally, nights)

    assert nights == 12
    assert scores.wrong == 0
    for entity, s in scores.entities.items():
        assert (s.correct, s.kept_deleted, s.pulled_twice) == (s.erp_rows, 0, 0), entity
    assert set(tally.requests) == {200}  # nothing was refused


def test_with_every_mess_on_it_is_wrong_in_the_ways_the_messes_predict(messy):
    # An operation's quantities change without its stamp, so a load by stamp
    # never sees them: operations stay as they were created.
    operations = messy.entities["JobOperations"]
    assert operations.stale > 0.5 * operations.erp_rows
    assert set(operations.stale_fields) == {"QtyCompleted", "ScrapQty", "OpComplete"}

    # Orders cancelled as duplicates were loaded the evening they were entered
    # and the copy never hears they are gone.
    for entity in ("Jobs", "JobOperations", "JobMaterials", "JobCosts"):
        assert messy.entities[entity].kept_deleted > 0, entity
    # One job, one material, one cost record per cancelled copy.
    ghosts = {
        messy.entities[e].kept_deleted for e in ("Jobs", "JobMaterials", "JobCosts")
    }
    assert len(ghosts) == 1

    # Labor keeps the time its scan closed, and a high-water mark still finds
    # it on a nightly cadence: the mess is there and costs nothing here.
    assert messy.entities["LaborDetails"].wrong == 0
    # Master data is reloaded in full every night and is right.
    for entity in ("WorkCenters", "Customers", "Parts", "PartRevisions"):
        assert messy.entities[entity].wrong == 0


def test_a_client_hammering_the_api_is_throttled_and_loses_reads(messy_db):
    scenario = Scenario(seconds_per_request=0.1)  # 10 requests a second, 5 allowed
    replica, tally, nights = run_naive(messy_db, MessConfig(), scenario)
    scores = score(messy_db, replica, tally, nights)

    assert tally.requests[429] > 0
    assert sum(tally.abandoned.values()) > 0
    assert sum(s.missed for s in scores.entities.values()) > 0


def test_an_erp_slower_than_a_token_lasts_locks_the_naive_extractor_out(messy_db):
    # 15 ERP minutes a request: the token (one ERP hour) is dead by the fifth,
    # and a 401 is just another failure to try again.
    scenario = Scenario(erp_seconds_per_request=900.0)
    replica, tally, nights = run_naive(messy_db, MessConfig(), scenario)
    scores = score(messy_db, replica, tally, nights)

    assert tally.requests[401] > 0
    assert scores.wrong > 0.5 * scores.erp_rows


def test_the_same_run_gives_the_same_numbers(messy_db, messy):
    replica, tally, nights = run_naive(messy_db, MessConfig())
    again = score(messy_db, replica, tally, nights)

    assert again.entities == messy.entities
    assert again.tally.requests == messy.tally.requests


def test_each_kind_of_error_is_counted_where_it_belongs(clean_db):
    replica, tally, nights = run_naive(clean_db, CLEAN_API)
    jobs = replica.rows["Jobs"]
    keys = list(jobs)

    del jobs[keys[0]]  # never got it
    replica.removed["Jobs"][keys[1]] = jobs.pop(keys[1])  # got it, threw it away
    jobs[keys[2]] = jobs[keys[2]] | {"CompletedQty": -1}  # not what the ERP has now
    jobs[("J999999",)] = copy.deepcopy(jobs[keys[3]]) | {
        "JobNum": "J999999"
    }  # the ERP deleted it
    tally.pulled_twice["Jobs"] = 5

    s = score(clean_db, replica, tally, nights).entities["Jobs"]
    assert (s.missed, s.wrongly_deleted, s.stale, s.kept_deleted, s.pulled_twice) == (
        1,
        1,
        1,
        1,
        5,
    )
    assert s.stale_fields == {"CompletedQty": 1}
    assert s.correct == s.erp_rows - 3


def test_the_api_s_formats_are_put_back_the_way_the_store_holds_them(clean_db):
    _, types = final_state(clean_db)

    served = {
        "LaborDtlSeq": "1207001.00000000",
        "LaborHrs": "0.60000000",
        "PayrollDate": "/Date(1783296000000)/",
        "LastModified": "/Date(1783346400000)/",
        "EmployeeNum": "EMP-003",
        "OprSeq": "20.00000000",
    }
    normal = normalize(served, types["LaborDetails"] | {"OprSeq": "INTEGER"})
    assert normal == {
        "LaborDtlSeq": 1207001,
        "LaborHrs": 0.6,
        "PayrollDate": "2026-07-06",
        "LastModified": "2026-07-06T14:00:00Z",
        "EmployeeNum": "EMP-003",
        "OprSeq": 20,
    }
    assert LEGACY.match(served["PayrollDate"])


def test_the_report_names_every_kind_of_error_and_what_the_api_said(messy):
    text = report(messy)

    assert "Naive extractor, 12 nights" in text
    assert re.search(r"Of the ERP's [\d,]+ rows, [\d,]+ \(\d+\.\d%\)", text)
    for heading in ("missed", "lost", "stale", "kept deleted", "pulled twice"):
        assert heading in text
    assert "Stale rows differ in:" in text
    assert "Requests by status: 200:" in text


def test_command_line_runs_against_a_backfill(clean_db, tmp_path, capsys):
    store = tmp_path / "out" / "erp"
    store.mkdir(parents=True)
    (store / "erp.db").write_bytes(clean_db.read_bytes())
    mess = tmp_path / "mess.yaml"
    mess.write_text("mess_level: 0\n")

    main(["--out", str(tmp_path / "out"), "--mess", str(mess)])

    printed = capsys.readouterr().out
    assert "Naive extractor, 12 nights" in printed
    assert "(0.0%)" in printed


# --- The robust extractor: the same nights, scored on the copy its raw pages make


def robust_scores(db, raw_root, scenario=None, mess=None):
    replica, tally, nights, merged = run_robust(
        db, mess or MessConfig(), raw_root, scenario
    )
    return score(db, replica, tally, nights, "robust", merged)


def assert_exact(scores) -> None:
    for entity, s in scores.entities.items():
        assert (s.missed, s.wrongly_deleted, s.stale) == (0, 0, 0), entity
        assert (s.kept_deleted, s.pulled_twice) == (0, 0), entity
        assert s.correct == s.erp_rows, entity
    if scores.extractor == "robust":
        assert scores.tally.short == {}  # every complete read proved it


def test_the_robust_extractor_is_exact_with_every_mess_on(messy_db, messy, tmp_path):
    scores = robust_scores(messy_db, tmp_path)

    assert_exact(scores)
    # The naive extractor, on the same ERP, is not:
    assert messy.wrong > 0 and messy.kept_deleted > 0
    # What made the difference: sweeps put right the operations whose stamps
    # never moved, and removed the cancelled duplicates' jobs.
    assert scores.merge.sweep_corrected["JobOperations"] > 0
    for entity in ("Jobs", "JobOperations", "JobMaterials", "JobCosts"):
        assert scores.merge.sweep_removed[entity] > 0, entity
    assert scores.tally.requests[429] + scores.tally.requests[500] > 0
    assert (tmp_path / SOURCE / "entity=Jobs").is_dir()


@pytest.mark.parametrize(
    "scenario",
    [Scenario(seconds_per_request=0.1), Scenario(erp_seconds_per_request=900.0)],
    ids=["hammering the API", "an ERP slower than its tokens last"],
)
def test_the_robust_extractor_is_exact_where_the_naive_one_falls_over(
    messy_db, tmp_path, scenario
):
    scores = robust_scores(messy_db, tmp_path, scenario)

    assert_exact(scores)
    assert scores.tally.abandoned == {}
    assert scores.tally.requests[429] > 0 or scores.tally.requests[401] > 0


def test_the_robust_extractor_is_exact_on_the_control_too(clean_db, tmp_path):
    scores = robust_scores(clean_db, tmp_path, mess=CLEAN_API)

    assert_exact(scores)
    assert set(scores.tally.requests) == {200}


def test_the_robust_extractor_writes_the_same_raw_pages_every_time(messy_db, tmp_path):
    robust_scores(messy_db, tmp_path / "a")
    robust_scores(messy_db, tmp_path / "b")

    first = sorted(
        p.relative_to(tmp_path / "a") for p in (tmp_path / "a").rglob("*.jsonl")
    )
    second = sorted(
        p.relative_to(tmp_path / "b") for p in (tmp_path / "b").rglob("*.jsonl")
    )
    assert first == second and first
    assert all(
        (tmp_path / "a" / f).read_bytes() == (tmp_path / "b" / f).read_bytes()
        for f in first
    )


def test_the_report_names_the_extractor_and_what_its_sweeps_did(messy_db, tmp_path):
    text = report(robust_scores(messy_db, tmp_path))

    assert text.startswith("Robust extractor, 12 nights")
    assert "(0.0%)" in text and "holds 0 rows the ERP has deleted" in text
    assert "What the nightly sweeps changed" in text
    assert "deleted rows removed" in text
    assert "Complete reads that came up short: none" in text
