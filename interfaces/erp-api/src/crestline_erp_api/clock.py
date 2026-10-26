"""The ERP's clock: what time the ERP thinks it is.

A backfill is dated history. Served as it is, every row is current and the
ERP's now is the wall clock. Given a start time, the API instead replays
that history: its clock runs from `start` at `speed` times the wall clock,
and every request sees the ERP as it stood at that instant (speed 0 freezes
it). A 90-day history can then go by in minutes against a client paging
through it, which is where extraction problems show. The replay stops at
the end of the history (`end`, the store's last change): past it there is
nothing to replay, and a clock that ran on would only carry clients into
dates the history never had.

Every value comes from settings, so any number of copies of the API read
the same time from the same wall clock, with nothing to share between them.
"""

import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime


@dataclass(frozen=True)
class ErpClock:
    start: datetime | None  # UTC; None = no replay, now is the wall clock
    speed: float = 1.0  # ERP seconds per wall second; 0 freezes it at `start`
    anchor: float = 0.0  # wall time, epoch seconds, at which the clock read `start`
    wall: Callable[[], float] = time.time
    end: datetime | None = None  # UTC; a replay stops here (None = it never stops)

    @property
    def replaying(self) -> bool:
        return self.start is not None

    def now(self) -> datetime:
        """The ERP's time, in whole seconds, the way its rows are stamped."""
        if self.start is None:
            moment = datetime.fromtimestamp(self.wall(), UTC)
        else:
            moment = self.start + timedelta(
                seconds=self.speed * (self.wall() - self.anchor)
            )
            if self.end is not None:
                moment = min(moment, self.end)
        return moment.replace(microsecond=0)


def timestamp(moment: datetime) -> str:
    """The store's form of a timestamp: `2026-07-06T20:30:00Z`."""
    return moment.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def http_date(moment: datetime) -> str:
    """The `Date` header's form: `Mon, 06 Jul 2026 20:30:00 GMT`."""
    return format_datetime(moment.astimezone(UTC), usegmt=True)
