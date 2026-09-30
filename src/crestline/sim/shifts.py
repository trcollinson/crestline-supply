"""Shift calendar: when the plant is working and when it is not.

The calendar is precomputed for the whole simulation window as a list of
*windows* (working time) and *stops* (breaks, meals, nights, weekends).
Windows are cut at every stop and at the 1st/2nd shift boundary, so any
interval the simulation records inside a window belongs to exactly one shift
and never straddles a planned stop. That one rule is what lets the truth
tables, the labor records, and later the PB-02 scenario all reason about
"which shift was that" without special cases.
"""

from bisect import bisect_right
from dataclasses import dataclass
from datetime import date, time, timedelta

from .clock import SimClock
from .config import ShiftCalendarConfig
from .records import StopReason


@dataclass(frozen=True, slots=True)
class Window:
    """A contiguous stretch of working time on one shift, in sim seconds."""

    start: int
    end: int
    shift: str
    local_date: date


@dataclass(frozen=True, slots=True)
class Stop:
    """A planned stop, in sim seconds."""

    start: int
    end: int
    reason: StopReason


class ShiftCalendar:
    def __init__(
        self,
        clock: SimClock,
        config: ShiftCalendarConfig,
        first_day: date,
        last_day: date,
    ) -> None:
        self.clock = clock
        self.end = clock.seconds_at(last_day + timedelta(days=1), time(0))

        windows: list[Window] = []
        breaks: list[Stop] = []
        day = first_day
        while day <= last_day:
            if day.weekday() in config.working_weekdays:
                for shift in config.shifts:
                    cursor = clock.seconds_at(day, shift.start)
                    for brk in shift.breaks:
                        brk_start = clock.seconds_at(day, brk.start)
                        brk_end = brk_start + brk.minutes * 60
                        windows.append(Window(cursor, brk_start, shift.name, day))
                        breaks.append(Stop(brk_start, brk_end, brk.kind))
                        cursor = brk_end
                    windows.append(
                        Window(
                            cursor, clock.seconds_at(day, shift.end), shift.name, day
                        )
                    )
            day += timedelta(days=1)

        self.windows = windows
        self.stops = self._fill_gaps(windows, breaks)
        self._starts = [w.start for w in windows]

    def _fill_gaps(self, windows: list[Window], breaks: list[Stop]) -> list[Stop]:
        """Any time that is neither a window nor a break is NO_SHIFT."""
        covered = sorted(
            [(w.start, w.end) for w in windows] + [(s.start, s.end) for s in breaks]
        )
        stops: list[Stop] = []
        cursor = 0
        for start, end in covered:
            if start > cursor:
                stops.append(Stop(cursor, start, StopReason.NO_SHIFT))
            cursor = max(cursor, end)
        if cursor < self.end:
            stops.append(Stop(cursor, self.end, StopReason.NO_SHIFT))
        return sorted(stops + breaks, key=lambda s: s.start)

    @property
    def working_days(self) -> list[date]:
        return sorted({w.local_date for w in self.windows})

    def window_at(self, seconds: int) -> Window | None:
        """The window containing `seconds` (start inclusive, end exclusive)."""
        i = bisect_right(self._starts, seconds) - 1
        if i >= 0 and self.windows[i].end > seconds:
            return self.windows[i]
        return None

    def next_window(self, seconds: int) -> Window | None:
        """The window containing `seconds`, else the next one to start.

        Returns None once the calendar is exhausted.
        """
        current = self.window_at(seconds)
        if current is not None:
            return current
        i = bisect_right(self._starts, seconds)
        return self.windows[i] if i < len(self.windows) else None

    def shift_at(self, seconds: int) -> str | None:
        window = self.window_at(seconds)
        return window.shift if window is not None else None
