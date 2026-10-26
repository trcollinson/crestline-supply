"""The robust extractor's behavior, against a scripted API, one rule at a time."""

import json
import random
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime

from erp_extract.__main__ import main as extract_main
from erp_extract.client import ErpClient, Response
from erp_extract.merge import rebuild
from erp_extract.raw import SOURCE, Page, RawWriter, pages_in, read_files
from erp_extract.robust import (
    MAX_ATTEMPTS,
    OVERLAP,
    RobustExtractor,
    State,
    keyset,
    server_time,
)

T0 = datetime(2026, 7, 6, 5, 0, tzinfo=UTC)  # 23:00 plant time the night before


def ok(rows: list[dict], more: bool = False) -> Response:
    body = {"value": rows} | ({"@odata.nextLink": "/ignored"} if more else {})
    return Response(200, {}, body)


def refused(status: int, **headers: str) -> Response:
    return Response(status, headers, {"error": {"code": "No", "message": "no"}})


class Script:
    """A fake API: `handler(path)` answers each GET; every response carries a
    Date that moves on 30 seconds a request, as the ERP's clock would."""

    def __init__(self, handler) -> None:
        self.handler = handler
        self.now = T0
        self.gets: list[str] = []
        self.logins = 0

    def __call__(self, method, path, headers, body) -> Response:
        date = {"date": format_datetime(self.now, usegmt=True)}
        self.now += timedelta(seconds=30)
        if path.endswith("/auth/token"):
            self.logins += 1
            return Response(200, date, {"access_token": f"t{self.logins}"})
        self.gets.append(path)
        response = self.handler(path)
        return Response(response.status, dict(response.headers) | date, response.body)

    def paths(self, entity: str) -> list[str]:
        return [p for p in self.gets if f"/{entity}?" in p]


def queued(entity: str, *answers: Response):
    """`entity` gets these answers in turn, then empty pages; the rest are empty."""
    remaining = list(answers)

    def handler(path: str) -> Response:
        if f"/{entity}?" in path and remaining:
            return remaining.pop(0)
        return ok([])

    return handler


def robust(script: Script, tmp_path, **options) -> RobustExtractor:
    slept: list[float] = []
    extractor = RobustExtractor(
        ErpClient(script, "id", "secret"),
        RawWriter(tmp_path),
        rng=random.Random(1),
        sleep=slept.append,
        clock=lambda: 1_800_000_000.0,
        **options,
    )
    extractor.slept = slept
    return extractor


# --- Failures --------------------------------------------------------------


def test_an_expired_token_means_sign_in_again_and_carry_on(tmp_path):
    row = {"JobNum": "J1", "LastModified": "2026-07-06T04:00:00Z"}
    script = Script(queued("Jobs", refused(401), ok([row])))
    extractor = robust(script, tmp_path)
    extractor.run()

    assert script.logins == 2  # the run's, and one when the token died
    assert extractor.tally.requests[401] == 1
    assert extractor.tally.abandoned == {}


def test_too_many_requests_means_wait_as_long_as_retry_after_says(tmp_path):
    script = Script(queued("Customers", refused(429, **{"retry-after": "7"})))
    extractor = robust(script, tmp_path)
    extractor.run()

    assert extractor.slept == [7.0]
    assert extractor.tally.abandoned == {}


def test_a_server_error_means_back_off_exponentially_with_jitter(tmp_path):
    def failing_three_times():
        return Script(queued("Customers", refused(500), refused(502), refused(503)))

    extractor = robust(failing_three_times(), tmp_path)
    extractor.run()

    waits = extractor.slept
    assert len(waits) == 3
    for attempt, wait in enumerate(waits):
        assert 0.5 * 2**attempt <= wait <= 2**attempt  # half to all of 1, 2, 4 s
    again = robust(failing_three_times(), tmp_path / "again")
    again.run()
    assert again.slept == waits  # same seed, same waits


def test_a_read_that_keeps_failing_is_dropped_whole(tmp_path):
    row = {"JobNum": "J1", "LastModified": "2026-07-06T04:00:00Z"}
    first_page_then_failures = [ok([row], more=True)] + [refused(500)] * (
        2 * MAX_ATTEMPTS
    )
    extractor = robust(Script(queued("Jobs", *first_page_then_failures)), tmp_path)
    extractor.run()

    assert extractor.tally.abandoned["Jobs"] == 2  # the incremental, and the sweep
    assert extractor.tally.requests[500] == 2 * MAX_ATTEMPTS
    assert len(extractor.slept) == 2 * (MAX_ATTEMPTS - 1)  # no wait before giving up
    assert read_files(tmp_path, "Jobs") == []  # nothing from either is written...
    assert "Jobs" not in extractor.state.watermarks  # ...and the watermark stays put


def test_a_bad_request_is_this_codes_mistake_and_is_not_retried(tmp_path):
    script = Script(queued("Parts", refused(400)))
    extractor = robust(script, tmp_path)
    extractor.run()

    assert len(script.paths("Parts")) == 1
    assert extractor.tally.abandoned["Parts"] == 1


# --- Reading ------------------------------------------------------------------


def test_pages_ask_for_the_rows_after_the_last_key_never_skip(tmp_path):
    first = [
        {"JobNum": "J1", "OprSeq": "10.00000000", "LastModified": "a"},
        {"JobNum": "O'Neil", "OprSeq": "20.00000000", "LastModified": "a"},
    ]
    script = Script(queued("JobOperations", ok(first, more=True)))
    robust(script, tmp_path).run()

    second = script.paths("JobOperations")[1]
    assert "skip" not in second
    assert "%24orderby=JobNum,OprSeq&%24top=500" in second
    # The last key read, with its string number written as a number:
    assert keyset(("JobNum", "OprSeq"), first[-1]) == (
        "JobNum gt 'O''Neil' or (JobNum eq 'O''Neil' and OprSeq gt 20)"
    )
    assert "JobNum%20gt%20%27O%27%27Neil%27" in second


def test_the_watermark_is_the_erps_clock_when_the_read_began_less_an_hour(tmp_path):
    script = Script(lambda path: ok([]))
    extractor = robust(script, tmp_path)
    extractor.run()
    began = datetime.fromisoformat(extractor.state.watermarks["SalesOrders"])
    extractor.run()

    first, second = script.paths("SalesOrders")
    assert "LastModified" not in first  # everything, the first time
    since = (began - OVERLAP).strftime("%Y-%m-%dT%H%%3A%M%%3A%SZ")
    assert f"LastModified%20gt%20{since}" in second
    assert T0 < began < script.now


def test_the_erps_time_comes_from_x_server_time_when_a_front_door_replaced_date():
    # Behind a Lambda function URL, Date is the URL's wall clock and the ERP's
    # own Date is renamed; X-Server-Time comes through as the ERP sent it.
    behind_a_front_door = Response(
        200,
        {
            "date": "Thu, 22 Oct 2026 22:43:27 GMT",
            "x-server-time": "2026-10-01T12:00:00Z",
        },
        {},
    )
    direct = Response(200, {"date": "Thu, 01 Oct 2026 12:00:00 GMT"}, {})

    assert server_time(behind_a_front_door) == datetime(2026, 10, 1, 12, tzinfo=UTC)
    assert server_time(direct) == datetime(2026, 10, 1, 12, tzinfo=UTC)


def test_labor_is_read_by_its_sequence_number_not_its_stamp(tmp_path):
    rows = [{"LaborDtlSeq": "1207001.00000000", "LastModified": "/Date(0)/"}]
    script = Script(queued("LaborDetails", ok(rows)))
    extractor = robust(script, tmp_path)
    extractor.run()
    extractor.run()

    first, second = script.paths("LaborDetails")
    assert "filter" not in first
    assert "LaborDtlSeq%20gt%201207001" in second and "LastModified" not in second
    assert extractor.state.sequences == {"LaborDetails": 1207001}


def test_each_entity_is_read_the_way_it_changes(tmp_path):
    script = Script(lambda p: ok([]))
    extractor = robust(script, tmp_path, sweep_every=2)
    extractor.run()
    extractor.run()
    extractor.run()

    assert extractor.tally.reads == {
        "full": 3 * 4,  # master data, every run
        "sequence": 3,  # LaborDetails
        "incremental": 3 * 6,  # orders, lines, and the four job entities
        "sweep": 2 * 4,  # the four job entities, on runs 1 and 3
    }


# --- What it writes -------------------------------------------------------------


def test_each_read_is_one_file_of_raw_pages_in_the_spec_layout(tmp_path):
    rows = [
        {
            "JobNum": "J1",
            "LastModified": "2026-07-06T04:00:00Z",
            "ProdQty": "40.00000000",
        }
    ]
    script = Script(queued("Jobs", ok(rows, more=True), ok([]), ok(rows)))
    robust(script, tmp_path).run()

    files = read_files(tmp_path, "Jobs")
    # Master data, orders, and lines come first, so Jobs' reads are the 7th and 8th.
    assert [f.name for f in files] == [
        "20260706T050000Z-006-incremental.jsonl",
        "20260706T050000Z-007-sweep.jsonl",
    ]
    incremental = files[0]
    assert incremental.parent.parent.parent.name == SOURCE
    assert incremental.parent.name == "ingest_date=2026-07-06"
    lines = pages_in(incremental)
    assert [line["page"] for line in lines] == [1, 2]
    assert lines[0]["body"]["value"] == rows  # untouched, string numbers and all
    assert set(lines[0]) == set(Page.__dataclass_fields__)
    assert lines[0]["server_time"].endswith("Z") and lines[0]["read"] == "incremental"
    assert not list(tmp_path.rglob("*.partial"))


def test_a_run_is_skipped_while_the_erps_clock_stands_still(tmp_path):
    script = Script(lambda path: ok([]))
    extractor = robust(script, tmp_path)
    extractor.run(["Jobs"])
    files, gets = sorted(tmp_path.rglob("*.jsonl")), len(script.gets)
    # A replay at the end of its history: the ERP's clock reads what it did.
    # Reading again would reuse the last run's name, and so its files.
    script.now = T0
    extractor.run(["Jobs"])
    assert len(script.gets) == gets and sorted(tmp_path.rglob("*.jsonl")) == files
    assert extractor.tally.skipped["Jobs"] == 1 and extractor.state.runs["Jobs"] == 1
    # Once the clock moves on, the entity is read again.
    script.now = T0 + timedelta(hours=1)
    extractor.run(["Jobs"])
    assert len(script.gets) > gets and extractor.state.runs["Jobs"] == 2
    assert extractor.state.last_runs["Jobs"] == "20260706T060000Z"


def test_state_survives_between_runs(tmp_path):
    state = State(
        runs={"Jobs": 3},
        watermarks={"Jobs": "2026-07-06T05:00:00Z"},
        sequences={"LaborDetails": 7},
        last_runs={"Jobs": "20260706T050000Z"},
    )
    state.save(tmp_path / "state.json")
    assert State.load(tmp_path / "state.json") == state
    assert State.load(tmp_path / "missing.json") == State()


# --- Merging raw reads into a copy ----------------------------------------------


def write(writer: RawWriter, entity, kind, rows, run, ordinal, day="2026-07-06"):
    page = Page(entity, run, kind, 1, "/u", "t", "t", {"value": rows})
    writer.write([page], ordinal, day)


def test_merge_replays_increments_then_lets_a_sweep_say_what_exists(tmp_path):
    writer = RawWriter(tmp_path)
    write(
        writer,
        "Jobs",
        "incremental",
        [{"JobNum": "J1", "Q": 0}, {"JobNum": "J9", "Q": 0}],
        "20260706T050000Z",
        0,
    )
    write(
        writer,
        "Jobs",
        "incremental",
        [{"JobNum": "J2", "Q": 0}],
        "20260707T050000Z",
        0,
        "2026-07-07",
    )
    write(
        writer,
        "Jobs",
        "sweep",
        [{"JobNum": "J1", "Q": 5}, {"JobNum": "J2", "Q": 0}, {"JobNum": "J3", "Q": 0}],
        "20260707T050000Z",
        1,
        "2026-07-07",
    )

    replica, tally = rebuild(tmp_path)
    assert replica.rows["Jobs"] == {
        ("J1",): {"JobNum": "J1", "Q": 5},
        ("J2",): {"JobNum": "J2", "Q": 0},
        ("J3",): {"JobNum": "J3", "Q": 0},
    }
    assert set(replica.removed["Jobs"]) == {("J9",)}  # gone from the ERP
    assert (
        tally.sweep_corrected["Jobs"],
        tally.sweep_added["Jobs"],
        tally.sweep_removed["Jobs"],
    ) == (1, 1, 1)


def test_merge_counts_a_row_read_twice_in_one_read(tmp_path):
    writer = RawWriter(tmp_path)
    write(
        writer,
        "Customers",
        "full",
        [{"CustomerCode": "ALP"}, {"CustomerCode": "ALP"}],
        "20260706T050000Z",
        0,
    )

    replica, tally = rebuild(tmp_path)
    assert tally.pulled_twice["Customers"] == 1
    assert list(replica.rows["Customers"]) == [("ALP",)]


# --- From the command line ---------------------------------------------------------


def test_one_run_from_the_command_line(tmp_path, monkeypatch, capsys):
    alpine = ok([{"CustomerCode": "ALP", "Name": "Alpine"}])
    script = Script(queued("Customers", alpine, alpine))
    monkeypatch.setattr("erp_extract.__main__.http_transport", lambda url: script)
    out = tmp_path / "raw"

    extract_main(["--base-url", "http://erp", "--out", str(out), "--merge"])
    extract_main(["--base-url", "http://erp", "--out", str(out), "--merge"])

    printed = capsys.readouterr().out
    assert "Run 1:" in printed and "Run 2:" in printed
    assert "Customers 1" in printed
    assert json.loads((out / "state.json").read_text())["runs"]["Customers"] == 2
