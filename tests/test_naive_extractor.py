"""The naive extractor's behavior, against a scripted API.

Each test sets out what the API says and checks what the extractor does with
it, so every shortcut in `naive.py` is shown here as well as measured by the
eval.
"""

import io
import urllib.error
from collections.abc import Callable
from datetime import UTC, datetime

import pytest

from crestline.sim.systems import erp
from erp_extract.client import BASE, ErpClient, LoginFailed, Response, http_transport
from erp_extract.entities import ENTITIES, key_of
from erp_extract.formats import iso, parse_timestamp
from erp_extract.naive import NaiveExtractor, ReadFailed

Handler = Callable[[str, str], Response]


def ok(rows: list[dict], next_link: str | None = None) -> Response:
    body = {"value": rows} | ({"@odata.nextLink": next_link} if next_link else {})
    return Response(200, {}, body)


def refused(status: int, **headers: str) -> Response:
    return Response(status, headers, {"error": {"code": "No", "message": "no"}})


class Script:
    """A fake API: `handler(method, path)` answers, and every request is kept."""

    def __init__(self, handler: Handler) -> None:
        self.handler = handler
        self.requests: list[tuple[str, str]] = []

    def __call__(self, method, path, headers, body) -> Response:
        self.requests.append((method, path))
        if path.endswith("/auth/token"):
            return Response(200, {}, {"access_token": "t", "token_type": "Bearer"})
        return self.handler(method, path)

    def entity_requests(self, entity: str) -> list[str]:
        return [p for m, p in self.requests if m == "GET" and f"/{entity}?" in p]


def extractor(script: Script) -> NaiveExtractor:
    return NaiveExtractor(ErpClient(script, "id", "secret"))


def job(num: str, stamp: str, **fields) -> dict:
    return {"JobNum": num, "LastModified": stamp, **fields}


def empty_except(entity: str, answer: Response) -> Handler:
    """Every entity is empty except `entity`, which gets `answer`."""

    def handler(method: str, path: str) -> Response:
        return answer if f"/{entity}?" in path else ok([])

    return handler


def test_it_signs_in_once_a_run_and_never_looks_at_the_token_again():
    def handler(method, path):
        return refused(401) if "/Jobs?" in path else ok([])

    script = Script(handler)
    naive = extractor(script)
    naive.run()
    naive.run()

    logins = [p for m, p in script.requests if m == "POST"]
    assert logins == [f"{BASE}/auth/token"] * 2  # one per run, not one per 401
    assert naive.tally.requests[401] == 3 * 2  # Jobs, tried three times, each night


def test_a_failed_request_is_tried_three_times_at_once_ignoring_retry_after():
    script = Script(empty_except("Jobs", refused(429, **{"retry-after": "30"})))
    naive = extractor(script)
    naive.run()

    assert len(script.entity_requests("Jobs")) == 3  # no waiting, no fourth try
    assert naive.tally.requests[429] == 3
    assert naive.tally.abandoned["Jobs"] == 1
    assert naive.tally.abandoned["Customers"] == 0  # the next entity is still read


def test_a_request_that_works_on_the_second_try_costs_nothing():
    answers = iter([refused(500), ok([job("J1", "2026-07-06T14:00:00Z")])])
    script = Script(lambda m, p: next(answers) if "/Jobs?" in p else ok([]))
    naive = extractor(script)
    naive.run()

    assert list(naive.replica.rows["Jobs"]) == [("J1",)]
    assert naive.tally.abandoned["Jobs"] == 0


def test_the_first_run_reads_everything_and_later_runs_start_at_the_newest_stamp():
    pages = {
        0: ok([job("J1", "2026-07-06T14:00:00Z"), job("J2", "2026-07-07T09:30:00Z")])
    }
    script = Script(lambda m, p: pages.get(0) if "/Jobs?" in p else ok([]))
    naive = extractor(script)
    naive.run()
    naive.run()

    first, second = script.entity_requests("Jobs")
    assert first == f"{BASE}/Jobs?%24top=500"  # `$` written %24: see entity_path
    assert (
        second
        == f"{BASE}/Jobs?%24top=500&%24filter=LastModified%20gt%202026-07-07T09%3A30%3A00Z"
    )
    assert naive.replica.newest["Jobs"] == datetime(2026, 7, 7, 9, 30, tzinfo=UTC)


def test_it_follows_next_link_and_replaces_a_row_it_already_has():
    link = f"{BASE}/Jobs?$skip=1&$top=1"
    script = Script(
        lambda m, p: (
            ok([job("J1", "2026-07-06T14:00:00Z", JobComplete=True)])
            if p.endswith("$skip=1&$top=1")
            else ok([job("J1", "2026-07-05T08:00:00Z", JobComplete=False)], link)
            if "/Jobs?" in p
            else ok([])
        )
    )
    naive = extractor(script)
    naive.run()

    assert naive.replica.rows["Jobs"] == {
        ("J1",): job("J1", "2026-07-06T14:00:00Z", JobComplete=True)
    }
    assert (
        naive.tally.pulled_twice["Jobs"] == 1
    )  # the same job on two pages of one read
    assert naive.tally.received["Jobs"] == 2


def test_a_read_that_dies_midway_keeps_its_rows_and_moves_the_newest_stamp_past_the_rest():
    link = f"{BASE}/Jobs?$skip=1&$top=1"
    pages = iter([ok([job("J9", "2026-07-09T22:00:00Z")], link)] + [refused(500)] * 3)
    script = Script(lambda m, p: next(pages) if "/Jobs?" in p else ok([]))
    naive = extractor(script)
    naive.run()

    assert list(naive.replica.rows["Jobs"]) == [("J9",)]
    # Jobs the dead page held, stamped before J9's, will never be asked for.
    assert naive.replica.newest["Jobs"] == datetime(2026, 7, 9, 22, 0, tzinfo=UTC)
    assert naive.tally.abandoned["Jobs"] == 1


def test_master_data_is_deleted_and_loaded_again_every_run():
    state = {"rows": [{"WorkCenter": "LASER"}, {"WorkCenter": "BRAKE"}]}
    script = Script(lambda m, p: ok(state["rows"]) if "/WorkCenters?" in p else ok([]))
    naive = extractor(script)
    naive.run()
    assert set(naive.replica.rows["WorkCenters"]) == {("LASER",), ("BRAKE",)}
    assert "filter" not in script.entity_requests("WorkCenters")[0]

    state["rows"] = [{"WorkCenter": "LASER"}]
    naive.run()
    assert set(naive.replica.rows["WorkCenters"]) == {("LASER",)}

    # A reload that fails leaves the copy empty, and remembers what it threw away.
    script.handler = lambda m, p: refused(500) if "/WorkCenters?" in p else ok([])
    naive.run()
    assert naive.replica.rows["WorkCenters"] == {}
    assert set(naive.replica.removed["WorkCenters"]) == {("LASER",), ("BRAKE",)}


def test_it_reads_the_legacy_dates_the_api_sometimes_serves():
    stamp = "/Date(1783346400000)/"  # 2026-07-06T14:00:00Z
    script = Script(
        lambda m, p: (
            ok([{"LaborDtlSeq": 1, "LastModified": stamp}])
            if "/LaborDetails?" in p
            else ok([])
        )
    )
    naive = extractor(script)
    naive.run()
    naive.run()

    assert naive.replica.newest["LaborDetails"] == datetime(
        2026, 7, 6, 14, 0, tzinfo=UTC
    )
    assert script.entity_requests("LaborDetails")[1].endswith(
        "gt%202026-07-06T14%3A00%3A00Z"
    )


def test_the_entity_table_matches_the_store_the_simulator_writes():
    assert {e: list(key) for e, (key, _) in ENTITIES.items()} == erp.PRIMARY_KEYS
    for entity, (_, has_stamp) in ENTITIES.items():
        assert has_stamp == ("LastModified" in erp.ENTITIES[entity]), entity
    assert key_of("JobOperations", {"JobNum": "J1", "OprSeq": 20}) == ("J1", 20)


def test_timestamps_in_both_forms_and_back():
    expected = datetime(2026, 7, 6, 14, 0, tzinfo=UTC)
    assert parse_timestamp("2026-07-06T14:00:00Z") == expected
    assert parse_timestamp("/Date(1783346400000)/") == expected
    assert parse_timestamp("2026-07-06T08:00:00-06:00") == expected
    assert iso(expected) == "2026-07-06T14:00:00Z"


def test_the_client_signs_in_with_the_credentials_and_keeps_the_token():
    sent = []

    def transport(method, path, headers, body):
        sent.append((method, path, body, dict(headers)))
        return Response(200, {}, {"access_token": "abc", "token_type": "Bearer"})

    client = ErpClient(transport, "me", "pw")
    client.login()
    client.get(f"{BASE}/Jobs")

    assert sent[0][:2] == ("POST", f"{BASE}/auth/token")
    assert sent[0][2] == b"grant_type=client_credentials&client_id=me&client_secret=pw"
    assert sent[1][3] == {"Authorization": "Bearer abc"}


def test_a_refused_login_is_an_error():
    client = ErpClient(
        lambda *a: Response(401, {}, {"error": "invalid_client"}), "me", "x"
    )
    with pytest.raises(LoginFailed):
        client.login()


def test_a_read_that_fails_every_try_raises_read_failed_to_its_caller():
    script = Script(lambda m, p: refused(500))
    naive = extractor(script)
    naive.client.login()
    with pytest.raises(ReadFailed):
        naive._get(f"{BASE}/Jobs")


def test_the_http_transport_reads_status_headers_and_body(monkeypatch):
    def fake_urlopen(request):
        raise urllib.error.HTTPError(
            request.full_url,
            429,
            "Too Many Requests",
            {"Retry-After": "1"},
            io.BytesIO(b'{"error": 1}'),
        )

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)
    reply = http_transport("http://erp.example")("GET", "/x", {}, None)

    assert reply == Response(429, {"retry-after": "1"}, {"error": 1})
