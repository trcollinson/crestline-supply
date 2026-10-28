"""What the ERP API does to a client, each mess on and off.

The store is a short backfill's clean ERP, so a difference between a messy
read and a clean one is the mess and nothing else.
"""

import re
from datetime import UTC, date, datetime, timedelta

import pytest
from fastapi.testclient import TestClient

from crestline.mess import MessConfig
from crestline.sim.systems import erp
from crestline_erp_api.app import create_app
from crestline_erp_api.odata import encode_skiptoken
from crestline_erp_api.settings import Settings

BASE = "/erp/api/v2"
NOW = datetime(2026, 7, 18, 12, tzinfo=UTC).timestamp()
JULY = datetime(2026, 7, 10, 12, tzinfo=UTC)
DECIMAL_STRING = re.compile(r"^-?\d+\.\d{8}$")
LEGACY_DATE = re.compile(r"^/Date\((-?\d+)\)/$")


def mess(seed: int = 42, **erp_mess: object) -> MessConfig:
    """A clean API with these ERP imperfections turned on."""
    return MessConfig(seed=seed, mess_level=0, erp=erp_mess)


@pytest.fixture(scope="module")
def settings(erp_changes, tmp_path_factory) -> Settings:
    out = tmp_path_factory.mktemp("erp")
    erp.write_erp(erp_changes, out)
    return Settings(db_path=out / "erp.db")


@pytest.fixture(scope="module")
def batches(erp_changes) -> list[datetime]:
    return sorted({c.at for c in erp_changes.changes if c.entity == "LaborDetails"})


@pytest.fixture
def wall():
    """The API's wall clock, which a test can move."""
    return {"now": NOW}


def api(settings, wall, config: MessConfig | None = None, **clock) -> TestClient:
    """An authorized client on the API."""
    app = create_app(
        settings.model_copy(update=clock), clock=lambda: wall["now"], mess=config
    )
    client = TestClient(app)
    response = client.post(
        f"{BASE}/auth/token",
        data={
            "grant_type": "client_credentials",
            "client_id": settings.client_id,
            "client_secret": settings.client_secret,
        },
    )
    client.headers["Authorization"] = f"Bearer {response.json()['access_token']}"
    return client


def read_all(client: TestClient, url: str) -> list[dict]:
    rows = []
    while url:
        response = client.get(url)
        assert response.status_code == 200, response.text
        rows += response.json()["value"]
        url = response.json().get("@odata.nextLink")
    return rows


# --- Token expiry ------------------------------------------------------------


def test_a_token_lasts_its_lifetime_and_a_clean_one_never_ends(settings, wall):
    short = api(settings, wall, mess(token_ttl_seconds=1800))
    forever = api(settings, wall, mess(token_ttl_seconds=None))
    url = f"{BASE}/Jobs?$top=1"

    wall["now"] = NOW + 1799
    assert short.get(url).status_code == 200
    wall["now"] = NOW + 1800
    assert short.get(url).status_code == 401
    wall["now"] = NOW + 10**8
    assert forever.get(url).status_code == 200


def test_the_token_response_says_how_long_it_lasts(settings, wall):
    for config, expected in ((mess(token_ttl_seconds=1800), 1800), (mess(), None)):
        client = TestClient(create_app(settings, mess=config))
        issued = client.post(
            f"{BASE}/auth/token",
            data={
                "grant_type": "client_credentials",
                "client_id": settings.client_id,
                "client_secret": settings.client_secret,
            },
        ).json()
        assert issued.get("expires_in") == expected


def test_a_token_counts_the_erps_clock_so_a_replay_outlives_it(settings, wall):
    # One wall second is an ERP hour: a read that takes a few seconds spans
    # several tokens' lifetimes, as a long extraction would.
    hourly = mess(token_ttl_seconds=3600)
    running = api(settings, wall, hourly, clock_start=JULY, clock_speed=3600.0)
    longer = api(
        settings,
        wall,
        mess(token_ttl_seconds=3 * 3600),
        clock_start=JULY,
        clock_speed=3600.0,
    )
    frozen = api(settings, wall, hourly, clock_start=JULY, clock_speed=0.0)
    url = f"{BASE}/Jobs?$top=1"

    wall["now"] = NOW + 0.5  # half an ERP hour
    assert running.get(url).status_code == 200
    wall["now"] = NOW + 1  # an ERP hour
    assert running.get(url).status_code == 401
    assert longer.get(url).status_code == 200
    wall["now"] = NOW + 10_000  # a frozen ERP does not age
    assert frozen.get(url).status_code == 200


# --- Throttling --------------------------------------------------------------


def test_requests_past_the_limit_get_a_429_and_a_retry_after(settings, wall):
    limited = api(settings, wall, mess(throttle_rps=2))
    url = f"{BASE}/WorkCenters"

    assert [limited.get(url).status_code for _ in range(2)] == [200, 200]
    refused = limited.get(url)
    assert refused.status_code == 429
    assert refused.headers["Retry-After"] == "1"
    assert refused.json()["error"]["code"] == "TooManyRequests"
    assert "Date" in refused.headers

    wall["now"] = NOW + 1  # the first two are a second old
    assert limited.get(url).status_code == 200


def test_throttling_off_lets_everything_through(settings, wall):
    open_api = api(settings, wall, mess(throttle_rps=None))
    assert {open_api.get(f"{BASE}/WorkCenters").status_code for _ in range(40)} == {200}


def test_a_request_that_is_not_authorized_does_not_use_up_the_limit(settings, wall):
    limited = api(settings, wall, mess(throttle_rps=1))
    anonymous = TestClient(limited.app)

    assert [anonymous.get(f"{BASE}/WorkCenters").status_code for _ in range(3)] == [
        401
    ] * 3
    assert limited.get(f"{BASE}/WorkCenters").status_code == 200


# --- Random 500s ---------------------------------------------------------------


def failures(settings, wall, seed: int, rate: float, requests: int = 200) -> list[bool]:
    client = api(settings, wall, mess(seed=seed, random_500_rate=rate))
    return [
        client.get(f"{BASE}/WorkCenters").status_code == 500 for _ in range(requests)
    ]


def test_random_500s_on_and_off(settings, wall):
    assert not any(failures(settings, wall, 42, 0))
    assert all(failures(settings, wall, 42, 1))
    some = failures(settings, wall, 42, 0.2)
    assert 10 < sum(some) < 90  # about one in five of 200

    body = {
        "error": {
            "code": "InternalError",
            "message": "the server hit an unexpected condition",
        }
    }
    client = api(settings, wall, mess(random_500_rate=1))
    response = client.get(f"{BASE}/WorkCenters")
    assert (response.status_code, response.json()) == (500, body)


def test_the_same_seed_fails_the_same_requests(settings, wall):
    first = failures(settings, wall, 42, 0.2)
    assert failures(settings, wall, 42, 0.2) == first
    assert failures(settings, wall, 7, 0.2) != first


# --- nextLink drift ------------------------------------------------------------


def read_jobs_across_a_batch(settings, wall, batches, drift: bool):
    """Page `Jobs` newest-first at 3600x, so a batch lands between pages 1 and 2."""
    start = batches[3] - timedelta(hours=1)
    client = api(
        settings,
        wall,
        mess(nextlink_drift=drift),
        clock_start=start,
        clock_speed=3600.0,
    )
    url = f"{BASE}/Jobs?$orderby=LastModified desc&$top=50"
    keys, links = [], []
    while url:
        body = client.get(url).json()
        keys += [row["JobNum"] for row in body["value"]]
        url = body.get("@odata.nextLink")
        links.append(url)
        wall["now"] += 1.5  # an ERP hour and a half
    return start, keys, links[:-1]


def test_skip_paging_repeats_rows_when_the_erp_changes_mid_read(
    settings, wall, batches
):
    _, keys, links = read_jobs_across_a_batch(settings, wall, batches, drift=True)

    assert len(links) >= 2
    assert all("%24skip=" in link and "skiptoken" not in link for link in links)
    assert len(keys) > len(set(keys))  # rows pushed down a page come back


def test_a_skiptoken_pins_the_first_pages_instant_so_nothing_drifts(
    settings, wall, batches, erp_changes
):
    start, keys, links = read_jobs_across_a_batch(settings, wall, batches, drift=False)

    assert len(links) >= 2
    assert all("%24skiptoken=" in link and "%24skip=" not in link for link in links)
    assert len(keys) == len(set(keys))
    # And it is exactly the ERP as it stood when the read began.
    then = sorted(erp_changes.as_of(start)["Jobs"], key=lambda r: r["JobNum"])
    then.sort(key=lambda r: r["LastModified"], reverse=True)
    assert keys == [row["JobNum"] for row in then]


def test_an_erp_that_is_not_being_replayed_pages_with_skip_either_way(settings, wall):
    for drift in (True, False):
        client = api(settings, wall, mess(nextlink_drift=drift))
        link = client.get(f"{BASE}/LaborDetails?$top=50").json()["@odata.nextLink"]
        assert link == f"{BASE}/LaborDetails?%24skip=50&%24top=50"


def test_a_skiptoken_has_to_be_one_the_api_issued(settings, wall):
    client = api(settings, wall, mess(), clock_start=JULY, clock_speed=0.0)
    token = encode_skiptoken(3, "2026-07-10T12:00:00Z")

    page = client.get(f"{BASE}/WorkCenters?$top=2&$skiptoken={token}").json()["value"]
    assert [row["WorkCenter"] for row in page] == ["LASER", "PEM"]  # skipped three

    for query in ("$skiptoken=garbage", f"$skiptoken={token}&$skip=3"):
        response = client.get(f"{BASE}/WorkCenters?{query}")
        assert response.status_code == 400
        assert response.json()["error"]["code"] == "BadRequest"


# --- Formats ------------------------------------------------------------------


def row_numbers(rows: list[dict], columns: dict[str, str]) -> list[tuple]:
    return [(row[c]) for row in rows for c in columns]


def test_numbers_as_strings_with_eight_decimals(settings, wall):
    plain = api(settings, wall, mess(numbers_as_strings=False))
    strings = api(settings, wall, mess(numbers_as_strings=True))
    url = f"{BASE}/JobCosts?$top=50"

    clean, messy = plain.get(url).json()["value"], strings.get(url).json()["value"]
    assert all(
        isinstance(v, float)
        for row in clean
        for k, v in row.items()
        if k.endswith("Cost")
    )
    for before, after in zip(clean, messy, strict=True):
        for column, value in before.items():
            if isinstance(value, float):
                assert DECIMAL_STRING.match(after[column]), column
                assert float(after[column]) == value
            else:
                assert after[column] == value  # text stays text

    jobs = strings.get(f"{BASE}/Jobs?$top=1").json()["value"][0]
    assert DECIMAL_STRING.match(jobs["ProdQty"])
    assert DECIMAL_STRING.match(jobs["OrderLine"])
    assert isinstance(jobs["JobReleased"], bool) and isinstance(jobs["JobNum"], str)
    # Filters still take numbers.
    big = strings.get(f"{BASE}/Jobs?$filter=ProdQty ge 40&$top=500").json()["value"]
    assert big and all(float(row["ProdQty"]) >= 40 for row in big)
    # The count is the API's, not a column, so it stays a number.
    assert isinstance(
        strings.get(f"{BASE}/Jobs?$count=true").json()["@odata.count"], int
    )


def from_legacy(value: str) -> datetime:
    match = LEGACY_DATE.match(value)
    assert match, value
    return datetime.fromtimestamp(int(match.group(1)) / 1000, UTC)


def test_legacy_dates_on_the_entities_that_have_them(settings, wall):
    iso = api(settings, wall, mess(legacy_date_format_entities=[]))
    legacy = api(settings, wall, mess(legacy_date_format_entities=["LaborDetails"]))
    url = f"{BASE}/LaborDetails?$top=50"

    clean, messy = iso.get(url).json()["value"], legacy.get(url).json()["value"]
    for before, after in zip(clean, messy, strict=True):
        assert from_legacy(after["LastModified"]) == datetime.fromisoformat(
            before["LastModified"]
        )
        # A date (no time of day) is midnight UTC.
        assert from_legacy(after["PayrollDate"]).date() == date.fromisoformat(
            before["PayrollDate"]
        )
        assert from_legacy(after["PayrollDate"]).time().isoformat() == "00:00:00"
        assert after["LaborHrs"] == before["LaborHrs"]  # only dates change

    # Other entities keep ISO 8601, and filters still take it.
    job = legacy.get(f"{BASE}/Jobs?$top=1").json()["value"][0]
    assert job["LastModified"].endswith("Z") and "/Date" not in job["ReleaseDate"]
    since = clean[10]["LastModified"]
    later = read_all(
        legacy, f"{BASE}/LaborDetails?$filter=LastModified gt {since}&$top=500"
    )
    assert later and all(
        from_legacy(r["LastModified"]) > datetime.fromisoformat(since) for r in later
    )

    only_jobs = api(settings, wall, mess(legacy_date_format_entities=["Jobs"]))
    assert LEGACY_DATE.match(
        only_jobs.get(f"{BASE}/Jobs?$top=1").json()["value"][0]["LastModified"]
    )
    assert only_jobs.get(url).json()["value"] == clean


def test_level_two_serves_the_same_data_as_level_zero_in_the_messy_formats(
    settings, wall
):
    clean = api(settings, wall, MessConfig(mess_level=0))
    messy = api(
        settings, wall, MessConfig(erp={"throttle_rps": None, "random_500_rate": 0})
    )

    for entity in erp.ENTITIES:
        url = f"{BASE}/{entity}?$top=500"
        before, after = read_all(clean, url), read_all(messy, url)
        assert len(before) == len(after) > 0, entity
        for a, b in zip(before, after, strict=True):
            for column, value in a.items():
                served = b[column]
                if isinstance(value, bool) or value is None or served == value:
                    assert served == value
                elif isinstance(value, int | float):
                    assert float(served) == value
                else:
                    assert from_legacy(served) in (
                        datetime.fromisoformat(value),
                        datetime.fromisoformat(value).replace(tzinfo=UTC),
                    )


def test_the_api_serves_the_mess_in_the_file_it_is_given(settings, wall, tmp_path):
    path = tmp_path / "mess.yaml"
    path.write_text(
        "mess_level: 0\nerp:\n  token_ttl_seconds: 600\n  numbers_as_strings: true\n"
    )
    client = TestClient(create_app(settings.model_copy(update={"mess_path": path})))

    issued = client.post(
        f"{BASE}/auth/token",
        data={
            "grant_type": "client_credentials",
            "client_id": settings.client_id,
            "client_secret": settings.client_secret,
        },
    ).json()
    assert issued["expires_in"] == 600
    page = client.get(
        f"{BASE}/Jobs?$top=1",
        headers={"Authorization": f"Bearer {issued['access_token']}"},
    )
    assert DECIMAL_STRING.match(page.json()["value"][0]["ProdQty"])
