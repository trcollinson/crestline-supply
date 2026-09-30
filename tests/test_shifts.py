from datetime import date, time

from crestline.sim.clock import SimClock
from crestline.sim.config import ShiftCalendarConfig
from crestline.sim.records import StopReason
from crestline.sim.shifts import ShiftCalendar

MONDAY = date(2026, 7, 6)
SECOND_SUNDAY = date(2026, 7, 19)  # two full weeks


def make_calendar(first_day: date, last_day: date) -> tuple[SimClock, ShiftCalendar]:
    clock = SimClock.starting_on(first_day)
    return clock, ShiftCalendar(clock, ShiftCalendarConfig(), first_day, last_day)


def test_windows_fall_on_weekdays_inside_shift_hours():
    clock, calendar = make_calendar(MONDAY, SECOND_SUNDAY)

    assert {w.local_date.weekday() for w in calendar.windows} == {0, 1, 2, 3, 4}
    for window in calendar.windows:
        start, end = clock.to_local(window.start), clock.to_local(window.end)
        assert start.date() == window.local_date == end.date()
        assert time(6, 0) <= start.time() < end.time() <= time(23, 0)
        expected_shift = "1st" if start.time() < time(14, 30) else "2nd"
        assert window.shift == expected_shift
        # Cut at the shift boundary: no window straddles 14:30.
        assert end.time() <= time(14, 30) or start.time() >= time(14, 30)


def test_each_working_day_has_920_working_minutes():
    _, calendar = make_calendar(MONDAY, SECOND_SUNDAY)

    minutes_by_day: dict[date, float] = {}
    for window in calendar.windows:
        minutes_by_day[window.local_date] = (
            minutes_by_day.get(window.local_date, 0) + (window.end - window.start) / 60
        )

    assert len(minutes_by_day) == 10
    # Two 8.5 h shifts minus a 10 + 30 + 10 minute stop pattern on each.
    assert set(minutes_by_day.values()) == {920}


def test_windows_and_stops_tile_the_whole_timeline():
    _, calendar = make_calendar(MONDAY, SECOND_SUNDAY)

    segments = sorted(
        [(w.start, w.end) for w in calendar.windows]
        + [(s.start, s.end) for s in calendar.stops]
    )
    cursor = 0
    for start, end in segments:
        assert start == cursor
        assert end > start
        cursor = end
    assert cursor == calendar.end
    assert {s.reason for s in calendar.stops} == set(StopReason)


def test_window_lookup_at_boundaries():
    clock, calendar = make_calendar(MONDAY, MONDAY)

    shift_change = clock.seconds_at(MONDAY, time(14, 30))
    assert calendar.window_at(shift_change - 1).shift == "1st"
    assert calendar.window_at(shift_change).shift == "2nd"

    meal_start = clock.seconds_at(MONDAY, time(10, 0))
    assert calendar.window_at(meal_start) is None
    assert calendar.next_window(meal_start).start == meal_start + 30 * 60

    before_shift = clock.seconds_at(MONDAY, time(5, 0))
    assert calendar.window_at(before_shift) is None
    assert calendar.next_window(before_shift).start == clock.seconds_at(
        MONDAY, time(6, 0)
    )

    assert calendar.next_window(calendar.end) is None


def test_dst_fall_back_keeps_shifts_on_local_time():
    # 2026-11-01 is the fall-back date in America/Denver: Friday Oct 30 is MDT,
    # Monday Nov 2 is MST. 06:00 local must be 12:00 UTC and then 13:00 UTC.
    clock, calendar = make_calendar(date(2026, 10, 30), date(2026, 11, 2))

    def first_window_on(day: date):
        return min(
            (w for w in calendar.windows if w.local_date == day), key=lambda w: w.start
        )

    friday = first_window_on(date(2026, 10, 30))
    monday = first_window_on(date(2026, 11, 2))
    assert (
        clock.to_local(friday.start).time()
        == clock.to_local(monday.start).time()
        == time(6, 0)
    )
    assert clock.to_utc(friday.start).hour == 12
    assert clock.to_utc(monday.start).hour == 13
