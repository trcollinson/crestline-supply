"""The API in Lambda: a function URL's events through Mangum, and its secrets."""

import base64
import json
from urllib.parse import urlencode

import pytest

from crestline.sim.systems import erp
from crestline_erp_api import lambda_handler

HOST = "abc123.lambda-url.us-west-2.on.aws"
SECRETS = {"client_secret": "s3cret", "signing_key": "k3y"}


def event(method: str, path: str, query: str = "", body: str = "", **headers) -> dict:
    """A function URL request, as Lambda hands it over (payload format 2.0)."""
    return {
        "version": "2.0",
        "routeKey": "$default",
        "rawPath": path,
        "rawQueryString": query,
        "headers": {"host": HOST, **headers},
        "requestContext": {
            "domainName": HOST,
            "http": {
                "method": method,
                "path": path,
                "protocol": "HTTP/1.1",
                "sourceIp": "203.0.113.7",
                "userAgent": "test",
            },
            "stage": "$default",
        },
        "body": body,
        "isBase64Encoded": False,
    }


@pytest.fixture
def deployed(erp_changes, tmp_path, monkeypatch):
    """The handler as Lambda would run it: env from the stack, secrets from SSM."""
    erp.write_erp(erp_changes, tmp_path)
    monkeypatch.setenv("CRESTLINE_ERP_DB", str(tmp_path / "erp.db"))
    monkeypatch.setenv("CRESTLINE_ERP_CLIENT_ID", "crestline-poller")
    monkeypatch.setenv("CRESTLINE_ERP_CLIENT_SECRET_PARAMETER", "/x/client-secret")
    monkeypatch.setenv("CRESTLINE_ERP_SIGNING_KEY_PARAMETER", "/x/signing-key")
    asked = []

    def fake_read(names):
        asked.append(names)
        return {setting: SECRETS[setting] for setting in names}

    monkeypatch.setattr(lambda_handler, "read_parameters", fake_read)
    lambda_handler.asgi_handler.cache_clear()
    yield asked
    lambda_handler.asgi_handler.cache_clear()


def call(e: dict) -> tuple[int, dict, dict]:
    reply = lambda_handler.handler(e, None)
    body = reply["body"]
    if reply.get("isBase64Encoded"):
        body = base64.b64decode(body).decode()
    headers = {k.lower(): v for k, v in reply["headers"].items()}
    return reply["statusCode"], headers, json.loads(body)


def test_a_token_then_a_page_through_a_function_url(deployed, erp_changes):
    form = urlencode(
        {
            "grant_type": "client_credentials",
            "client_id": "crestline-poller",
            "client_secret": "s3cret",
        }
    )
    status, headers, issued = call(
        event(
            "POST",
            "/erp/api/v2/auth/token",
            body=form,
            **{"content-type": "application/x-www-form-urlencoded"},
        )
    )
    assert status == 200 and "date" in headers
    token = issued["access_token"]

    status, headers, page = call(
        event(
            "GET",
            "/erp/api/v2/WorkCenters",
            "%24top=2",
            authorization=f"Bearer {token}",
        )
    )
    assert status == 200
    assert [row["WorkCenter"] for row in page["value"]] == ["ASSY", "BRAKE"]
    assert page["@odata.nextLink"] == "/erp/api/v2/WorkCenters?%24skip=2&%24top=2"
    # The secrets were read once, for the cold start, by parameter name.
    assert deployed == [
        {"client_secret": "/x/client-secret", "signing_key": "/x/signing-key"}
    ]


def test_the_wrong_secret_and_no_token_are_refused(deployed):
    form = urlencode(
        {
            "grant_type": "client_credentials",
            "client_id": "crestline-poller",
            "client_secret": "local-secret",
        }
    )
    status, _, body = call(
        event(
            "POST",
            "/erp/api/v2/auth/token",
            body=form,
            **{"content-type": "application/x-www-form-urlencoded"},
        )
    )
    assert (status, body) == (
        401,
        {"error": "invalid_client"},
    )  # not the laptop default

    status, headers, _ = call(event("GET", "/erp/api/v2/Jobs"))
    assert status == 401
    assert headers["www-authenticate"] == 'Bearer error="invalid_token"'


def test_parameters_are_read_decrypted_by_name():
    class FakeSsm:
        def __init__(self):
            self.calls = []

        def get_parameter(self, Name, WithDecryption):
            self.calls.append((Name, WithDecryption))
            return {"Parameter": {"Value": f"value of {Name}"}}

    ssm = FakeSsm()
    values = lambda_handler.read_parameters(
        {"client_secret": "/a", "signing_key": "/b"}, ssm
    )

    assert values == {"client_secret": "value of /a", "signing_key": "value of /b"}
    assert ssm.calls == [("/a", True), ("/b", True)]
    assert lambda_handler.read_parameters({}) == {}  # nothing to read, no AWS client
