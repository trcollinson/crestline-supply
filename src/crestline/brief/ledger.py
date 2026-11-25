"""Which of this morning's alerts are new, and how long the others have been open
(M5 step 4).

Some problems are chronic: PB-02's second shift has looked more profitable
in the ERP than it is every morning of the window. A brief that announces it
as news every day teaches its reader to skip the alerts, so the brief says
"new" the first morning and "still open since <day>" after that.

What counts is what the earlier briefs said, not what today's data would
say about those days (a scan that arrived late can change an old day's
facts). So the ledger walks back through the earlier briefs' own facts: the
brief read on the morning of the plant day, then the one before that, while
the same alert fired every time. A brief that was never written is
computed as of its own morning, which the as-of layer allows, and kept.

The walk stops after `max_days` plant days ("open at least since ..."),
which bounds what a cold start costs, or at the first morning with no data
("open since the first brief on record").
"""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import date

from crestline.brief.morning import MorningFacts

MAX_DAYS = 20


@dataclass(frozen=True)
class AlertStatus:
    fact_id: str
    since: date  # the first plant day of the unbroken run it has fired on
    new: bool  # fired today and not on the plant day before
    # Still open where the walk stopped: open since then, or earlier.
    at_least: bool = False
    first_on_record: bool = False  # open since the first plant day with data

    def describe(self) -> str:
        """`New`, or since when, in the brief's words."""
        if self.new:
            return "New"
        if self.at_least:
            return f"Open since at least {self.since.isoformat()}"
        if self.first_on_record:
            return f"Open since {self.since.isoformat()}, the first day on record"
        return f"Still open since {self.since.isoformat()}"


def statuses(
    today: MorningFacts,
    facts_for: Callable[[date], MorningFacts],
    max_days: int = MAX_DAYS,
) -> dict[str, AlertStatus]:
    """Each of today's alerts, new or open since when."""
    if today.plant_day is None:
        return {}
    since = {fact_id: today.plant_day for fact_id in today.anomaly_ids}
    open_ids = set(since)
    stopped_at_limit = stopped_at_start = set()
    current = today
    for step in range(max_days):
        if not open_ids:
            break
        # The brief read on the plant day's own morning was about the day before.
        earlier = facts_for(current.plant_day)
        if earlier.plant_day is None:
            stopped_at_start = open_ids
            break
        open_ids &= set(earlier.anomaly_ids)
        for fact_id in open_ids:
            since[fact_id] = earlier.plant_day
        current = earlier
        if step == max_days - 1:
            stopped_at_limit = open_ids

    return {
        fact_id: AlertStatus(
            fact_id,
            day,
            new=day == today.plant_day,
            at_least=fact_id in stopped_at_limit,
            first_on_record=fact_id in stopped_at_start,
        )
        for fact_id, day in since.items()
    }


def dates(found: dict[str, AlertStatus]) -> frozenset[date]:
    """The days the alerts opened: dates a brief may state."""
    return frozenset(status.since for status in found.values())
