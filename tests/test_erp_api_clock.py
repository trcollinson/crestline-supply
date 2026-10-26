"""The ERP's clock, on a fake wall clock."""

from datetime import UTC, datetime, timedelta

from crestline_erp_api.clock import ErpClock, http_date, timestamp

START = datetime(2026, 7, 3, 6, tzinfo=UTC)
WALL = 1_800_000_000.0  # some wall-clock instant, epoch seconds


def clock_after(seconds: float, speed: float) -> ErpClock:
    return ErpClock(START, speed=speed, anchor=WALL, wall=lambda: WALL + seconds)


def test_frozen_at_the_start():
    assert clock_after(999, speed=0).now() == START
    assert clock_after(999, speed=0).replaying


def test_runs_from_the_anchor_at_speed():
    assert clock_after(90, speed=1).now() == START + timedelta(seconds=90)
    assert clock_after(2, speed=3600).now() == START + timedelta(hours=2)
    assert clock_after(0.5, speed=3600).now() == START + timedelta(minutes=30)


def test_a_replay_stops_at_the_end_of_its_history():
    end = START + timedelta(hours=2)
    ten_hours_in = ErpClock(
        START, speed=3600, anchor=WALL, wall=lambda: WALL + 10, end=end
    )
    assert ten_hours_in.now() == end
    # Without an end it runs on into dates the history never had.
    assert clock_after(10, speed=3600).now() == START + timedelta(hours=10)


def test_whole_seconds_only_like_the_stores_stamps():
    assert clock_after(0.0003, speed=3600).now() == START + timedelta(seconds=1)
    assert clock_after(0.9, speed=1).now() == START


def test_without_a_start_the_erp_is_on_wall_time():
    clock = ErpClock(None, wall=lambda: WALL + 0.7)
    assert not clock.replaying
    assert clock.now() == datetime.fromtimestamp(WALL, UTC)


def test_the_two_forms_of_a_moment():
    assert timestamp(START) == "2026-07-03T06:00:00Z"
    assert http_date(START) == "Fri, 03 Jul 2026 06:00:00 GMT"
    local = datetime(2026, 7, 3, 0, tzinfo=UTC).astimezone()  # any zone: still UTC out
    assert timestamp(local) == "2026-07-03T00:00:00Z"
