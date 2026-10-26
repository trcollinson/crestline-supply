"""A client for the ERP API: sign in, then ask for pages.

The client does as little as it can, so that what an extractor does with
tokens, throttling, and failures is visible in the extractor and not hidden
here. It takes a `Transport`, a function that sends one request, so the same
code runs over real HTTP (`http_transport`) or in-process in a test.
"""

import json
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from urllib.parse import quote, urlencode

BASE = "/erp/api/v2"


@dataclass(frozen=True)
class Response:
    status: int
    headers: Mapping[str, str]  # names in lower case
    body: dict

    @property
    def ok(self) -> bool:
        return self.status == 200


def entity_path(entity: str, **options: str) -> str:
    """`/erp/api/v2/Jobs?%24top=500&%24filter=...`: an entity with query options.

    OData's options begin with `$`. A query string may carry a bare `$`, but
    some front doors refuse it (an AWS Lambda function URL answers 400), so
    it is always written `%24`. The values are percent-encoded too, commas
    aside, so `$orderby=JobNum,OprSeq` stays readable.
    """
    query = "&".join(
        f"%24{name}={quote(value, safe=',')}" for name, value in options.items()
    )
    return f"{BASE}/{entity}?{query}"


# (method, path, headers, body) -> Response
Transport = Callable[[str, str, Mapping[str, str], bytes | None], Response]


class LoginFailed(Exception):
    """The token endpoint refused the client."""


class ErpClient:
    def __init__(
        self, transport: Transport, client_id: str, client_secret: str
    ) -> None:
        self._send = transport
        self._credentials = {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
        }
        self.token: str | None = None

    def login(self) -> Response:
        """Trade the client's credentials for a bearer token."""
        response = self._send(
            "POST",
            f"{BASE}/auth/token",
            {"Content-Type": "application/x-www-form-urlencoded"},
            urlencode(self._credentials).encode(),
        )
        if not response.ok:
            raise LoginFailed(response.body)
        self.token = response.body["access_token"]
        return response

    def get(self, path: str) -> Response:
        """One request with the current token; the caller reads the status."""
        headers = {"Authorization": f"Bearer {self.token}"}
        return self._send("GET", path, headers, None)


def http_transport(base_url: str) -> Transport:
    """Send requests over HTTP to the API at `base_url`."""

    def send(
        method: str, path: str, headers: Mapping[str, str], body: bytes | None
    ) -> Response:
        request = urllib.request.Request(
            base_url + path, data=body, headers=dict(headers), method=method
        )
        try:
            with urllib.request.urlopen(request) as reply:
                status, received = reply.status, reply.headers
                content = reply.read()
        except urllib.error.HTTPError as error:
            status, received, content = error.code, error.headers, error.read()
        return Response(
            status,
            {name.lower(): value for name, value in received.items()},
            json.loads(content) if content else {},
        )

    return send
