"""The ERP API's routes (spec §4.1).

- `POST /erp/api/v2/auth/token`: client ID and secret in, bearer token out.
- `GET /erp/api/v2/<Entity>`: one page of rows as `{"value": [...]}`, with
  `@odata.nextLink` while more rows remain. See `odata.py` for the query
  options.

Every response carries a `Date` header with the ERP's time (see `clock.py`),
the way any server says what time it thinks it is, and the same instant in
`X-Server-Time` (ISO 8601), because a front door may replace `Date` with its
own: an AWS Lambda function URL does, renaming ours `x-amzn-Remapped-date`. A page shows the ERP as
it stood at that instant; between one page and the next, the ERP moves on.
Tokens count the same clock, so a replay at speed outlives them as a long
extraction would.

How badly the API behaves is `mess.yaml`'s `erp` section (spec §5): short-
lived tokens, throttling, random 500s, paging that drifts, string numbers,
and `/Date(ms)/` dates. With no mess file the API is clean.

Errors follow the shapes clients of such APIs expect: OAuth's
`{"error": "invalid_client"}` from the token endpoint, and
`{"error": {"code": ..., "message": ...}}` everywhere else.
"""

import dataclasses
import threading
import time
from collections.abc import Callable
from urllib.parse import parse_qs, quote, urlencode

from fastapi import FastAPI, Header, Request
from fastapi.responses import JSONResponse

from crestline.mess import MessConfig, load_mess
from crestline.rng import make_rng

from .auth import credentials_match, issue_token, token_is_valid
from .clock import ErpClock, http_date, timestamp
from .limits import RateLimit
from .odata import QueryError, decode_skiptoken, encode_skiptoken, parse_query
from .settings import Settings
from .store import Store

BASE = "/erp/api/v2"


def create_app(
    settings: Settings,
    clock: Callable[[], float] = time.time,
    mess: MessConfig | None = None,
) -> FastAPI:
    """The API over the store at `settings.db_path`.

    `clock` is the wall clock (tests move it): the ERP's own clock runs from
    it, and so does the throttle. `mess` is how the API misbehaves; the
    default is the file at `settings.mess_path`, or none at all.
    """
    if mess is None:
        mess = (
            load_mess(settings.mess_path)
            if settings.mess_path
            else MessConfig(mess_level=0)
        )
    erp_mess = mess.erp
    limiter = RateLimit(erp_mess.throttle_rps) if erp_mess.throttle_rps else None
    failures = make_rng(mess.seed, "erp_api")
    failures_lock = threading.Lock()
    store = Store(settings.db_path)
    erp_clock = ErpClock(
        start=settings.clock_start,
        speed=settings.clock_speed,
        anchor=settings.clock_anchor.timestamp() if settings.clock_anchor else clock(),
        wall=clock,
        end=store.last_change() if settings.clock_stops else None,
    )
    if erp_clock.replaying and not store.versioned:
        raise ValueError(
            f"{settings.db_path} keeps no history, so it can't be replayed;"
            " run a newer backfill"
        )
    app = FastAPI(title="Crestline ERP API", version="2")

    @app.middleware("http")
    async def stamp_server_time(request: Request, call_next):
        # Read the clock once per request, so a page and its Date agree.
        now = erp_clock.now()
        request.state.erp_now = now
        response = await call_next(request)
        response.headers["Date"] = http_date(now)
        response.headers["X-Server-Time"] = timestamp(now)
        return response

    @app.post(f"{BASE}/auth/token")
    async def token(request: Request) -> JSONResponse:
        form = parse_qs((await request.body()).decode())

        def field(name: str) -> str:
            return form.get(name, [""])[0]

        if field("grant_type") != "client_credentials":
            return JSONResponse({"error": "unsupported_grant_type"}, status_code=400)
        if not (
            credentials_match(field("client_id"), settings.client_id)
            and credentials_match(field("client_secret"), settings.client_secret)
        ):
            return JSONResponse({"error": "invalid_client"}, status_code=401)
        ttl = erp_mess.token_ttl_seconds
        issued = {
            "access_token": issue_token(
                settings.client_id,
                settings.signing_key,
                request.state.erp_now.timestamp(),
                ttl,
            ),
            "token_type": "Bearer",
        }
        return JSONResponse(issued if ttl is None else issued | {"expires_in": ttl})

    @app.get(BASE + "/{entity_name}")
    def entity_page(
        entity_name: str, request: Request, authorization: str = Header("")
    ) -> JSONResponse:
        scheme, _, token = authorization.partition(" ")
        if scheme.lower() != "bearer" or not token_is_valid(
            token, settings.signing_key, request.state.erp_now.timestamp()
        ):
            return JSONResponse(
                _error("Unauthorized", "missing, invalid, or expired bearer token"),
                status_code=401,
                headers={"WWW-Authenticate": 'Bearer error="invalid_token"'},
            )
        wait = limiter.admit(clock()) if limiter else None
        if wait is not None:
            return JSONResponse(
                _error("TooManyRequests", f"slow down; retry in {wait} s"),
                status_code=429,
                headers={"Retry-After": str(wait)},
            )
        with failures_lock:
            fails = failures.random() < erp_mess.random_500_rate
        if fails:
            return JSONResponse(
                _error("InternalError", "the server hit an unexpected condition"),
                status_code=500,
            )
        entity = store.entities.get(entity_name)
        if entity is None:
            return JSONResponse(
                _error("NotFound", f"no entity {entity_name}"), status_code=404
            )
        options = dict(request.query_params)
        try:
            query = parse_query(entity, options)
        except QueryError as error:
            return JSONResponse(_error("BadRequest", str(error)), status_code=400)

        as_of = timestamp(request.state.erp_now) if erp_clock.replaying else None
        if "$skiptoken" in options:
            if "$skip" in options:
                return JSONResponse(
                    _error("BadRequest", "use $skip or $skiptoken, not both"),
                    status_code=400,
                )
            try:
                skip, pinned = decode_skiptoken(options["$skiptoken"])
            except QueryError as error:
                return JSONResponse(_error("BadRequest", str(error)), status_code=400)
            query = dataclasses.replace(query, skip=skip)
            as_of = pinned if erp_clock.replaying else None
        rows = store.fetch(*query.sql(as_of))
        body: dict[str, object] = {
            "value": [
                entity.record(
                    row,
                    legacy_dates=entity_name in erp_mess.legacy_date_format_entities,
                    numbers_as_strings=erp_mess.numbers_as_strings,
                )
                for row in rows[: query.top]
            ]
        }
        if len(rows) > query.top:
            next_skip = query.skip + query.top
            # Nothing can drift in a store that isn't being replayed, so the
            # cursor only matters, and only appears, when the clock is running.
            cursor = (
                None
                if erp_mess.nextlink_drift or as_of is None
                else encode_skiptoken(next_skip, as_of)
            )
            body["@odata.nextLink"] = _next_link(
                entity_name, options, next_skip, query.top, cursor
            )
        return JSONResponse(body)

    return app


def _next_link(
    entity: str, options: dict[str, str], skip: int, top: int, cursor: str | None
) -> str:
    """The same query, one page on: `/erp/api/v2/Jobs?%24skip=500&%24top=500`.

    With a `cursor` it carries `$skiptoken` in place of `$skip`. The `$` of
    each option is written `%24`: a query string may carry a bare `$`, but
    some front doors refuse one (a Lambda function URL answers 400), and a
    link the client can't follow is no link.
    """
    params = {
        name: options[name] for name in ("$filter", "$orderby") if name in options
    }
    params |= {"$skip": str(skip)} if cursor is None else {"$skiptoken": cursor}
    params["$top"] = str(top)
    return f"{BASE}/{entity}?{urlencode(params, quote_via=quote, safe="',")}"


def _error(code: str, message: str) -> dict[str, object]:
    return {"error": {"code": code, "message": message}}
