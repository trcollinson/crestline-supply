"""The ERP API in clean mode, served from a short backfill's ERP store.

The wall clock is a fake the tests move; the ERP's own clock is set through
`Settings`. With no clock start, the store is served as it stands (step 1);
with one, as of the ERP's clock (step 2).
"""

import json
import sqlite3
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

import crestline_erp_api.__main__ as erp_api_main
from crestline.mess import MessConfig
from crestline.sim.systems import erp
from crestline_erp_api.app import create_app
from crestline_erp_api.settings import Settings
from crestline_erp_api.store import Store

BASE = "/erp/api/v2"
NOW = datetime(2026, 7, 18, 12, tzinfo=UTC).timestamp()  # Sat, 18 Jul 2026 12:00 UTC
# A clean API, except that its tokens last an hour (clean tokens never expire).
HOUR_TOKENS = MessConfig(mess_level=0, erp={"token_ttl_seconds": 3600})


@pytest.fixture(scope="module")
def store(erp_changes, tmp_path_factory):
    """`erp.db` and its JSONL export, as the backfill writes them."""
    out = tmp_path_factory.mktemp("erp")
    erp.write_erp(erp_changes, out)
    return out


@pytest.fixture(scope="module")
def settings(store) -> Settings:
    return Settings(db_path=store / "erp.db")


@pytest.fixture(scope="module")
def clock():
    """The API's clock, which a test can move forward."""
    return {"now": NOW}


@pytest.fixture(scope="module")
def client(settings, clock):
    app = create_app(settings, clock=lambda: clock["now"])
    client = TestClient(app)
    client.headers["Authorization"] = f"Bearer {token(client, settings)}"
    return client


def token(client: TestClient, settings: Settings, **overrides: str) -> str:
    form = {
        "grant_type": "client_credentials",
        "client_id": settings.client_id,
        "client_secret": settings.client_secret,
    } | overrides
    response = client.post(f"{BASE}/auth/token", data=form)
    assert response.status_code == 200, response.text
    return response.json()["access_token"]


def read_all(client: TestClient, url: str) -> tuple[list[dict], int]:
    """Follow @odata.nextLink to the end: every row, and how many pages."""
    rows: list[dict] = []
    pages = 0
    while url:
        response = client.get(url)
        assert response.status_code == 200, response.text
        body = response.json()
        rows += body["value"]
        pages += 1
        url = body.get("@odata.nextLink")
    return rows, pages


def export(store, entity: str) -> list[dict]:
    """The entity's JSONL export, in primary key order."""
    lines = (store / "export" / f"{entity}.jsonl").read_text().splitlines()
    rows = [json.loads(line) for line in lines]
    return sorted(rows, key=lambda row: [row[k] for k in erp.PRIMARY_KEYS[entity]])


@pytest.mark.parametrize("top", [500, 37])
def test_paging_through_every_entity_returns_the_store_exactly(client, store, top):
    db = sqlite3.connect(store / "erp.db")
    for entity, columns in erp.ENTITIES.items():
        served, pages = read_all(client, f"{BASE}/{entity}?$top={top}")
        expected = export(store, entity)

        # Same rows, same order, same values, and the same JSON types.
        assert json.dumps(served) == json.dumps(expected), entity
        (count,) = db.execute(f"SELECT count(*) FROM {entity}").fetchone()
        assert len(served) == count
        assert pages == max(1, -(-count // top))  # ceiling: the last page is partial
        assert all(list(row) == list(columns) for row in served)
    db.close()


def test_a_page_links_to_the_next_until_the_last(client):
    first = client.get(f"{BASE}/Jobs?$top=50").json()
    assert len(first["value"]) == 50
    # `$` written %24, so a front door that refuses a bare `$` lets it through.
    assert first["@odata.nextLink"] == f"{BASE}/Jobs?%24skip=50&%24top=50"

    jobs, _ = read_all(client, f"{BASE}/Jobs?$top=50")
    last = client.get(f"{BASE}/Jobs?$skip={len(jobs) - 3}&$top=50").json()
    assert len(last["value"]) == 3
    assert "@odata.nextLink" not in last


def test_top_defaults_to_100_and_is_cut_to_500(client):
    assert len(client.get(f"{BASE}/LaborDetails").json()["value"]) == 100

    page = client.get(f"{BASE}/LaborDetails?$top=1000").json()
    assert len(page["value"]) == 500
    assert page["@odata.nextLink"].endswith("%24skip=500&%24top=500")


def test_filter_on_last_modified_returns_only_later_changes(client, store):
    since = "2026-07-14T08:00:00-06:00"  # 14:00 UTC, written in plant time
    changed, _ = read_all(
        client, f"{BASE}/JobOperations?$filter=LastModified gt {since}"
    )
    expected = [
        row
        for row in export(store, "JobOperations")
        if row["LastModified"] > "2026-07-14T14:00:00Z"
    ]
    assert changed == expected
    assert 0 < len(changed) < len(export(store, "JobOperations"))


def test_filter_clauses_join_with_and_and_keep_the_link(client, store):
    url = f"{BASE}/Jobs?$filter=JobComplete eq true and ProdQty ge 40&$top=10"
    jobs, _ = read_all(client, url)
    assert jobs == [
        row
        for row in export(store, "Jobs")
        if row["JobComplete"] and row["ProdQty"] >= 40
    ]
    link = client.get(url).json()["@odata.nextLink"]
    assert link.startswith(f"{BASE}/Jobs?%24filter=JobComplete%20eq%20true%20and")


def test_filter_with_or_and_parentheses(client, store):
    url = f"{BASE}/JobOperations?$filter=JobNum gt 'J100190' or (JobNum eq 'J100190' and OprSeq ge 30)"
    rows, _ = read_all(client, url)
    assert rows == [
        row
        for row in export(store, "JobOperations")
        if row["JobNum"] > "J100190"
        or (row["JobNum"] == "J100190" and row["OprSeq"] >= 30)
    ]
    assert rows


def test_filter_on_text_and_null(client):
    part = client.get(f"{BASE}/Parts?$filter=PartNum eq 'BRK-1042'").json()["value"]
    assert [(p["PartNum"], p["PartType"]) for p in part] == [("BRK-1042", "M")]

    bought = client.get(f"{BASE}/Parts?$filter=UnitPrice eq null").json()["value"]
    assert {p["PartType"] for p in bought} == {"P"}
    assert len(bought) == 4


def test_orderby_pages_stay_in_order(client, store):
    jobs, pages = read_all(client, f"{BASE}/Jobs?$orderby=ProdQty desc&$top=37")
    assert pages > 1
    expected = sorted(export(store, "Jobs"), key=lambda row: row["JobNum"])
    expected.sort(key=lambda row: row["ProdQty"], reverse=True)  # stable: ties by key
    assert jobs == expected


def test_master_data_has_no_change_stamp_to_filter_on(client):
    response = client.get(f"{BASE}/Parts?$filter=LastModified gt 2026-07-01T00:00:00Z")
    assert response.status_code == 400
    assert response.json() == {
        "error": {"code": "BadRequest", "message": "Parts has no field LastModified"}
    }


@pytest.mark.parametrize(
    "query",
    [
        "$select=JobNum",
        "$top=0",
        "$skip=-1",
        "$filter=ProdQty eq 'ten'",
        "$filter=JobNum eq",
        "$filter=(JobNum eq 'J100001'",
        "$orderby=Nope",
    ],
)
def test_a_query_the_api_cannot_serve_is_a_400(client, query):
    response = client.get(f"{BASE}/Jobs?{query}")
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "BadRequest"


def test_an_unknown_entity_is_a_404(client):
    response = client.get(f"{BASE}/Widgets")
    assert response.status_code == 404
    assert response.json()["error"]["code"] == "NotFound"


def test_the_token_endpoint_checks_the_client(client, settings):
    url = f"{BASE}/auth/token"
    wrong_secret = {
        "grant_type": "client_credentials",
        "client_id": settings.client_id,
        "client_secret": "guess",
    }
    response = client.post(url, data=wrong_secret)
    assert (response.status_code, response.json()) == (401, {"error": "invalid_client"})

    wrong_grant = wrong_secret | {"grant_type": "password"}
    response = client.post(url, data=wrong_grant)
    assert (response.status_code, response.json()) == (
        400,
        {"error": "unsupported_grant_type"},
    )

    good = wrong_secret | {"client_secret": settings.client_secret}
    issued = client.post(url, data=good)
    assert issued.json()["token_type"] == "Bearer"
    assert "expires_in" not in issued.json()  # a clean API's tokens never expire

    hourly = TestClient(create_app(settings, mess=HOUR_TOKENS))
    assert hourly.post(url, data=good).json()["expires_in"] == 3600


def test_every_entity_needs_a_valid_unexpired_token(settings, clock):
    app = create_app(settings, clock=lambda: clock["now"], mess=HOUR_TOKENS)
    client = TestClient(app)
    url = f"{BASE}/Jobs?$top=1"

    response = client.get(url)
    assert response.status_code == 401
    assert response.headers["WWW-Authenticate"] == 'Bearer error="invalid_token"'

    bearer = token(client, settings)
    assert (
        client.get(url, headers={"Authorization": f"Bearer {bearer}"}).status_code
        == 200
    )
    forged = bearer[:-2] + ("AA" if not bearer.endswith("AA") else "BB")
    assert (
        client.get(url, headers={"Authorization": f"Bearer {forged}"}).status_code
        == 401
    )

    # A second copy of the API with the same signing key accepts the token...
    other = TestClient(
        create_app(settings, clock=lambda: clock["now"], mess=HOUR_TOKENS)
    )
    assert (
        other.get(url, headers={"Authorization": f"Bearer {bearer}"}).status_code == 200
    )
    # ...one with a different key does not.
    stranger = TestClient(
        create_app(
            settings.model_copy(update={"signing_key": "other"}),
            lambda: NOW,
            mess=HOUR_TOKENS,
        )
    )
    assert (
        stranger.get(url, headers={"Authorization": f"Bearer {bearer}"}).status_code
        == 401
    )

    # An hour later the token has expired.
    later = TestClient(create_app(settings, clock=lambda: NOW + 3600, mess=HOUR_TOKENS))
    assert (
        later.get(url, headers={"Authorization": f"Bearer {bearer}"}).status_code == 401
    )


def test_settings_come_from_the_environment(tmp_path):
    settings = Settings.from_env(
        {
            "CRESTLINE_ERP_DB": str(tmp_path / "erp.db"),
            "CRESTLINE_ERP_CLIENT_ID": "poller",
            "CRESTLINE_ERP_MESS": str(tmp_path / "mess.yaml"),
        }
    )
    assert settings.db_path == tmp_path / "erp.db"
    assert settings.client_id == "poller"
    assert settings.mess_path == tmp_path / "mess.yaml"
    assert Settings().mess_path is None  # no mess file: a clean API
    assert settings.client_secret == Settings().client_secret


def test_the_api_refuses_to_start_without_a_store(tmp_path):
    with pytest.raises(FileNotFoundError, match="run a backfill first"):
        create_app(Settings(db_path=tmp_path / "missing.db"))


# --- The ERP's clock: serving the store as of a moment ---------------------


@pytest.fixture(scope="module")
def batches(erp_changes) -> list[datetime]:
    """When the nightly labor batches posted, in order."""
    return sorted(
        {c.at for c in erp_changes.changes if c.entity == "LaborDetails" and c.at}
    )


def app_at(settings: Settings, clock: dict, **clock_settings: object) -> TestClient:
    """An authorized client on the API with its ERP clock set."""
    app = create_app(
        settings.model_copy(update=clock_settings), clock=lambda: clock["now"]
    )
    client = TestClient(app)
    client.headers["Authorization"] = f"Bearer {token(client, settings)}"
    return client


def as_of(erp_changes, at: datetime, entity: str) -> list[dict]:
    """The simulator's own answer: the log replayed to `at`, in key order."""
    rows = erp_changes.as_of(at)[entity]
    return sorted(rows, key=lambda row: [row[k] for k in erp.PRIMARY_KEYS[entity]])


def server_time(response) -> datetime:
    return parsedate_to_datetime(response.headers["Date"])


def test_two_reads_at_two_clocks_differ_and_each_is_the_erp_as_of_then(
    plant_run, settings, clock, erp_changes, batches
):
    moments = [batches[1] - timedelta(seconds=1), batches[1], plant_run.extracted_at]
    labor_rows = []
    for at in moments:
        frozen = app_at(settings, clock, clock_start=at, clock_speed=0.0)
        for entity in erp.ENTITIES:
            served, _ = read_all(frozen, f"{BASE}/{entity}?$top=500")
            expected = as_of(erp_changes, at, entity)
            assert json.dumps(served) == json.dumps(expected), (entity, at)
        labor_rows.append(len(read_all(frozen, f"{BASE}/LaborDetails?$top=500")[0]))
        jobs, _ = read_all(frozen, f"{BASE}/Jobs?$top=500")
        if at != plant_run.extracted_at:
            assert any(
                not job["JobReleased"] for job in jobs
            )  # entered, not yet released
    assert (
        labor_rows[0] < labor_rows[1] < labor_rows[2]
    )  # the batch posted at batches[1]


def test_no_served_row_is_newer_than_the_erps_clock(settings, clock, batches):
    at = batches[2] + timedelta(hours=10)
    frozen = app_at(settings, clock, clock_start=at, clock_speed=0.0)
    for entity in ("SalesOrders", "Jobs", "JobOperations", "JobCosts", "LaborDetails"):
        rows, _ = read_all(frozen, f"{BASE}/{entity}?$top=500")
        assert rows
        assert all(datetime.fromisoformat(row["LastModified"]) <= at for row in rows)
    assert server_time(frozen.get(f"{BASE}/Jobs?$top=1")) == at


def test_every_response_says_what_time_the_erp_thinks_it_is(settings, clock, client):
    at = datetime(2026, 7, 10, 12, tzinfo=UTC)
    frozen = app_at(settings, clock, clock_start=at, clock_speed=0.0)
    anonymous = TestClient(frozen.app)
    form = {
        "grant_type": "client_credentials",
        "client_id": settings.client_id,
        "client_secret": settings.client_secret,
    }
    responses = [
        frozen.post(f"{BASE}/auth/token", data=form),
        frozen.get(f"{BASE}/Jobs?$top=1"),
        frozen.get(f"{BASE}/Jobs?$top=0"),
        frozen.get(f"{BASE}/Widgets"),
        anonymous.get(f"{BASE}/Jobs"),
    ]
    assert [r.status_code for r in responses] == [200, 200, 400, 404, 401]
    for response in responses:
        assert response.headers["Date"] == "Fri, 10 Jul 2026 12:00:00 GMT"
        # The same instant where a front door won't replace it with its own.
        assert response.headers["X-Server-Time"] == "2026-07-10T12:00:00Z"
        dates = [k for k, _ in response.headers.multi_items() if k.lower() == "date"]
        assert len(dates) == 1

    # With no clock start the ERP is on wall time, and every row is current.
    assert (
        client.get(f"{BASE}/Jobs?$top=1").headers["Date"]
        == "Sat, 18 Jul 2026 12:00:00 GMT"
    )


def test_the_clock_runs_at_speed_from_the_anchor(settings, clock):
    start = datetime(2026, 7, 6, 12, tzinfo=UTC)
    url = f"{BASE}/Jobs?$top=1"
    running = app_at(settings, clock, clock_start=start, clock_speed=3600.0)
    try:
        assert server_time(running.get(url)) == start  # anchored where the wall was
        clock["now"] = NOW + 2
        assert server_time(running.get(url)) == start + timedelta(hours=2)
        clock["now"] = NOW + 0.5
        assert server_time(running.get(url)) == start + timedelta(minutes=30)
        clock["now"] = NOW + 0.0003  # 1.08 ERP seconds: the clock has whole seconds
        assert server_time(running.get(url)) == start + timedelta(seconds=1)
        # An explicit anchor makes two copies agree, whenever each started.
        clock["now"] = NOW
        anchored = app_at(
            settings,
            clock,
            clock_start=start,
            clock_speed=3600.0,
            clock_anchor=datetime.fromtimestamp(NOW - 1, UTC),
        )
        assert server_time(anchored.get(url)) == start + timedelta(hours=1)
    finally:
        clock["now"] = NOW


def test_consecutive_pages_of_one_read_see_the_erp_move(
    settings, clock, erp_changes, batches
):
    # One wall second is an ERP hour: a read that starts an hour before a
    # batch posts crosses it between its first and second page.
    start = batches[3] - timedelta(hours=1)
    running = app_at(settings, clock, clock_start=start, clock_speed=3600.0)
    try:
        first = running.get(f"{BASE}/LaborDetails?$top=100")
        clock["now"] = NOW + 1.5  # ninety ERP minutes later
        second = running.get(first.json()["@odata.nextLink"])
    finally:
        clock["now"] = NOW

    assert server_time(first) == start < batches[3]
    assert server_time(second) == start + timedelta(minutes=90) > batches[3]
    before = as_of(erp_changes, start, "LaborDetails")
    after = as_of(erp_changes, start + timedelta(minutes=90), "LaborDetails")
    assert len(after) > len(before) > 100
    assert first.json()["value"] == before[:100]
    assert second.json()["value"] == after[100:200]


def test_the_stores_own_columns_and_history_are_not_part_of_the_api(
    client, settings, clock, batches
):
    frozen = app_at(settings, clock, clock_start=batches[1], clock_speed=0.0)
    for api in (client, frozen):
        assert "SysValidFrom" not in api.get(f"{BASE}/Jobs?$top=1").json()["value"][0]
        hidden = api.get(f"{BASE}/Jobs?$filter=SysValidFrom gt 2026-07-01T00:00:00Z")
        assert hidden.json()["error"]["message"] == "Jobs has no field SysValidFrom"
        assert api.get(f"{BASE}/Jobs?$orderby=SysValidTo").status_code == 400
        assert api.get(f"{BASE}/Jobs_History").status_code == 404
        assert api.get(f"{BASE}/WorkCenters_History").status_code == 404


def test_the_clock_comes_from_the_environment_with_an_offset():
    settings = Settings.from_env(
        {
            "CRESTLINE_ERP_CLOCK_START": "2026-07-03T06:00:00Z",
            "CRESTLINE_ERP_CLOCK_SPEED": "3600",
            "CRESTLINE_ERP_CLOCK_ANCHOR": "2026-10-22T14:00:00-06:00",
        }
    )
    assert settings.clock_start == datetime(2026, 7, 3, 6, tzinfo=UTC)
    assert settings.clock_speed == 3600
    assert settings.clock_anchor == datetime(2026, 10, 22, 20, tzinfo=UTC)
    assert (Settings().clock_start, Settings().clock_speed) == (None, 1.0)
    # A flag wins over the environment.
    assert (
        Settings.from_env(
            {"CRESTLINE_ERP_CLOCK_SPEED": "2"}, clock_speed="5"
        ).clock_speed
        == 5
    )
    for bad in (
        {"CRESTLINE_ERP_CLOCK_START": "2026-07-03T06:00:00"},  # no offset
        {"CRESTLINE_ERP_CLOCK_SPEED": "-1"},
    ):
        with pytest.raises(ValidationError):
            Settings.from_env(bad)


def test_main_passes_the_clock_flags_and_leaves_the_date_header_to_the_app(
    monkeypatch, tmp_path
):
    calls: dict[str, object] = {}
    monkeypatch.setattr(
        erp_api_main,
        "create_app",
        lambda settings: calls.setdefault("settings", settings),
    )
    monkeypatch.setattr(
        erp_api_main.uvicorn,
        "run",
        lambda app, **kwargs: calls.setdefault("run", (app, kwargs)),
    )
    erp_api_main.main(
        [
            "--db",
            str(tmp_path / "erp.db"),
            "--mess",
            str(tmp_path / "mess.yaml"),
            "--clock-start",
            "2026-07-03T06:00:00Z",
            "--clock-speed",
            "60",
            "--port",
            "9",
        ]
    )
    settings = calls["settings"]
    assert settings.db_path == tmp_path / "erp.db"
    assert settings.mess_path == tmp_path / "mess.yaml"
    assert settings.clock_start == datetime(2026, 7, 3, 6, tzinfo=UTC)
    assert (settings.clock_speed, settings.clock_anchor) == (60, None)
    assert calls["run"] == (
        settings,
        {"host": "127.0.0.1", "port": 9, "date_header": False},
    )


def test_replaying_needs_a_store_that_keeps_history(tmp_path):
    db = sqlite3.connect(tmp_path / "erp.db")
    db.execute("CREATE TABLE Jobs (JobNum TEXT PRIMARY KEY)")
    db.commit()
    db.close()
    create_app(Settings(db_path=tmp_path / "erp.db"))  # as it stands: fine
    with pytest.raises(ValueError, match="keeps no history"):
        create_app(
            Settings(
                db_path=tmp_path / "erp.db",
                clock_start=datetime(2026, 7, 3, tzinfo=UTC),
            )
        )


def test_a_replay_stops_at_the_stores_last_change_unless_told_to_run_on(
    settings, clock, store
):
    end = Store(store / "erp.db").last_change()
    start = end - timedelta(days=1)
    url = f"{BASE}/Jobs?$top=1"
    try:
        clock["now"] = NOW
        stopping = app_at(settings, clock, clock_start=start, clock_speed=86_400.0)
        running_on = app_at(
            settings, clock, clock_start=start, clock_speed=86_400.0, clock_stops=False
        )
        clock["now"] = NOW + 30  # a month of ERP time later
        assert server_time(stopping.get(url)) == end
        assert server_time(running_on.get(url)) == start + timedelta(days=30)
    finally:
        clock["now"] = NOW
