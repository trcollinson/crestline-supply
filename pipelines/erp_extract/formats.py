"""Timestamps as the API serves them.

Most are ISO 8601 (`2026-07-06T14:30:00Z`); the "legacy" entities write
`/Date(1783348200000)/`, milliseconds since the epoch.
"""

import re
from datetime import UTC, datetime

LEGACY = re.compile(r"^/Date\((-?\d+)\)/$")


def parse_timestamp(value: str) -> datetime:
    """A served timestamp in either form, as an aware UTC datetime."""
    legacy = LEGACY.match(value)
    if legacy:
        return datetime.fromtimestamp(int(legacy.group(1)) / 1000, UTC)
    return datetime.fromisoformat(value).astimezone(UTC)


def iso(moment: datetime) -> str:
    """A moment in the form `$filter` takes: whole seconds, UTC, `Z`."""
    return moment.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
