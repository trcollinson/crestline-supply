"""Bearer tokens for the ERP API (OAuth 2 client credentials).

A client trades its ID and secret for a token that expires after a lifetime
(or never, if the lifetime is None). Times are seconds on the ERP's clock.
Tokens are signed, not stored: the token carries the
client and its expiry, plus an HMAC over them that only a holder of the
signing key could make. Any copy of the API with the same key accepts it,
with no session table to share.
"""

import base64
import hashlib
import hmac
import json


def issue_token(
    client_id: str, signing_key: str, now: float, ttl_seconds: int | None
) -> str:
    claims = {
        "sub": client_id,
        "exp": None if ttl_seconds is None else int(now) + ttl_seconds,
    }
    payload = _encode(json.dumps(claims, separators=(",", ":")).encode())
    return f"{payload}.{_sign(payload, signing_key)}"


def token_is_valid(token: str, signing_key: str, now: float) -> bool:
    """True if this API signed the token and it hasn't expired."""
    payload, _, signature = token.partition(".")
    if not hmac.compare_digest(
        signature.encode(), _sign(payload, signing_key).encode()
    ):
        return False
    try:
        claims = json.loads(
            base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4))
        )
    except ValueError:
        return False
    return claims["exp"] is None or now < claims["exp"]


def credentials_match(given: str, expected: str) -> bool:
    """Compare secrets in constant time, so timing doesn't leak them."""
    return hmac.compare_digest(given.encode(), expected.encode())


def _sign(payload: str, signing_key: str) -> str:
    digest = hmac.new(signing_key.encode(), payload.encode(), hashlib.sha256).digest()
    return _encode(digest)


def _encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()
