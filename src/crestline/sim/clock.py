"""Simulation clock: SimPy counts whole seconds; the plant keeps local time.

Sim second 0 is local midnight (America/Denver) at the start of the first
day. Whole seconds keep every arithmetic step exact (no floating-point drift
at window boundaries) and match the resolution of real shop-floor systems.
Everything the simulation records is converted to UTC here, once, so the
truth tables are UTC per the spec and local wall-clock time only matters
where the shift calendar is defined.
"""

from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from zoneinfo import ZoneInfo

PLANT_TZ = ZoneInfo("America/Denver")


@dataclass(frozen=True)
class SimClock:
    start_utc: datetime

    @classmethod
    def starting_on(cls, first_day: date) -> "SimClock":
        local_midnight = datetime.combine(first_day, time(0), tzinfo=PLANT_TZ)
        return cls(start_utc=local_midnight.astimezone(UTC))

    def to_utc(self, seconds: int) -> datetime:
        return self.start_utc + timedelta(seconds=seconds)

    def to_local(self, seconds: int) -> datetime:
        return self.to_utc(seconds).astimezone(PLANT_TZ)

    def seconds_at(self, local_day: date, local_time: time) -> int:
        """Sim seconds at a wall-clock time on a given local day.

        Going through zoneinfo is what makes DST transitions come out right:
        06:00 local is 12:00 UTC in summer and 13:00 UTC in winter.
        """
        local = datetime.combine(local_day, local_time, tzinfo=PLANT_TZ)
        return int((local.astimezone(UTC) - self.start_utc).total_seconds())
