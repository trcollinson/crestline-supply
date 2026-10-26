"""Step 1's test, against the deployed API (M2 step 6's done-when).

Skipped unless `CRESTLINE_ERP_URL` names a deployed ERP API. The store it
serves must be the backfill in `out/` (`CRESTLINE_OUT` to point elsewhere),
which is what the stack packages. The client secret comes from
`CRESTLINE_ERP_CLIENT_SECRET`, or from Parameter Store with your AWS
credentials:

    export AWS_PROFILE=personal AWS_REGION=us-west-2
    CRESTLINE_ERP_URL=$(uv run crestline erp-api url) uv run pytest tests/test_erp_api_remote.py

With `CRESTLINE_ERP_EXPECT_DATE` set (the ERP clock the stack was deployed
with, frozen), it also checks that the ERP's time comes through. A Lambda
function URL replaces the API's `Date` with its own wall clock (renaming ours
`x-amzn-Remapped-date`) and renames `WWW-Authenticate` the same way, so the
ERP's time is read from `X-Server-Time`, which it leaves alone.
"""

import json
import os
import urllib.error
import urllib.request
from datetime import datetime
from email.utils import parsedate_to_datetime
from pathlib import Path

import pytest

from crestline import erp_secrets
from crestline.sim.systems import erp
from erp_extract.client import BASE, ErpClient, entity_path, http_transport

URL = os.environ.get("CRESTLINE_ERP_URL", "").rstrip("/")
OUT = Path(os.environ.get("CRESTLINE_OUT", "out"))

pytestmark = pytest.mark.skipif(
    not URL, reason="set CRESTLINE_ERP_URL to a deployed API"
)


@pytest.fixture(scope="module")
def client() -> ErpClient:
    secret = (
        os.environ.get("CRESTLINE_ERP_CLIENT_SECRET")
        or erp_secrets.read()["client_secret"]
    )
    client = ErpClient(http_transport(URL), "crestline-poller", secret)
    client.login()
    return client


def export(entity: str) -> list[dict]:
    lines = (OUT / "erp" / "export" / f"{entity}.jsonl").read_text().splitlines()
    rows = [json.loads(line) for line in lines]
    return sorted(rows, key=lambda row: [row[k] for k in erp.PRIMARY_KEYS[entity]])


def read_all(client: ErpClient, path: str) -> tuple[list[dict], int]:
    rows, pages = [], 0
    while path:
        response = client.get(path)
        assert response.status == 200, (response.status, response.body)
        rows += response.body["value"]
        pages += 1
        path = response.body.get("@odata.nextLink")
    return rows, pages


@pytest.mark.parametrize("top", [500, 37])
def test_paging_through_every_entity_returns_the_store_exactly(client, top):
    for entity, columns in erp.ENTITIES.items():
        served, pages = read_all(client, entity_path(entity, top=str(top)))
        expected = export(entity)

        assert json.dumps(served) == json.dumps(expected), entity
        assert pages == max(1, -(-len(expected) // top))
        assert all(list(row) == list(columns) for row in served)


def test_no_token_is_a_401_that_says_why():
    try:
        urllib.request.urlopen(f"{URL}{BASE}/Jobs")
    except urllib.error.HTTPError as refused:
        assert refused.code == 401
        # The function URL renames the header; the challenge is intact.
        challenge = (
            refused.headers["WWW-Authenticate"]
            or refused.headers["x-amzn-Remapped-www-authenticate"]
        )
        assert challenge == 'Bearer error="invalid_token"'
    else:
        pytest.fail("the API served a page without a token")


def test_the_erps_time_comes_through(client):
    with urllib.request.urlopen(
        urllib.request.Request(
            f"{URL}{BASE}/WorkCenters?%24top=1",
            headers={"Authorization": f"Bearer {client.token}"},
        )
    ) as reply:
        dates = reply.headers.get_all("Date")
        server_time = reply.headers["X-Server-Time"]
        remapped = reply.headers["x-amzn-Remapped-date"]
    assert len(dates) == 1
    expected = os.environ.get("CRESTLINE_ERP_EXPECT_DATE")
    if expected:
        moment = datetime.fromisoformat(expected)
        assert datetime.fromisoformat(server_time) == moment
        if remapped:  # behind a function URL: our Date, renamed, is the ERP's too
            assert parsedate_to_datetime(remapped) == moment
