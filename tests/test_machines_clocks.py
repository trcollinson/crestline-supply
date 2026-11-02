"""Device clocks that drift (`machines.clock_drift_seconds`)."""

import math
from datetime import UTC, datetime, timedelta

from crestline.mess import MessConfig
from crestline.rng import make_rng
from crestline.sim.systems import machines
from crestline.sim.systems.machines.clocks import DriftingClock

SEED = 42
START = datetime(2026, 7, 6, 6, tzinfo=UTC)
YEAR = (START, START + timedelta(days=365))


def test_a_zero_knob_is_exactly_true_time():
    clock = DriftingClock(0, *YEAR, make_rng(SEED, stream="clock/PB-01"))
    at = START + timedelta(days=100, seconds=12345, microseconds=0)

    assert clock.offset(at) == 0
    assert clock.device_time(at) == at
    assert clock.device_time(at).microsecond == 0


def test_the_offset_is_about_the_knob_and_scales_with_it():
    rng = lambda: make_rng(SEED, stream="clock/PB-01")
    big = DriftingClock(140, *YEAR, rng())
    small = DriftingClock(70, *YEAR, rng())
    hours = [START + timedelta(hours=h) for h in range(0, 365 * 24, 6)]

    rms = math.sqrt(sum(big.offset(at) ** 2 for at in hours) / len(hours))
    assert 0.5 * 140 <= rms <= 1.5 * 140
    assert max(abs(big.offset(at)) for at in hours) > 140  # it does wander
    for at in hours[:50]:  # the same path, half the size
        assert small.offset(at) == big.offset(at) / 2


def test_the_clock_never_jumps():
    clock = DriftingClock(140, *YEAR, make_rng(SEED, stream="clock/PB-01"))
    at = START
    while at < START + timedelta(days=3):
        later = at + timedelta(seconds=60)
        assert (
            abs(clock.offset(later) - clock.offset(at)) < 1.0
        )  # under a second a minute
        assert clock.device_time(later) > clock.device_time(at)  # monotonic
        at = later


def test_gw02_stamps_both_its_machines_with_the_one_clock(plant_run):
    mess = MessConfig().machines
    assert mess.clock_drift_seconds.seconds("GW02") == 20
    clocks = machines.clocks(mess, SEED, machines.machine_window(plant_run.truth))
    pb02 = next(
        machines.polled_counter_messages(
            "PB-02", plant_run.truth, mess, clocks["GW02"], SEED
        )
    )
    pem01 = next(
        machines.polled_counter_messages(
            "PEM-01", plant_run.truth, mess, clocks["GW02"], SEED
        )
    )

    assert pb02.payload["gw_ts"] == pem01.payload["gw_ts"]
    assert pb02.payload["gw_ts"] != pb02.at.strftime("%Y-%m-%dT%H:%M:%SZ")
    assert clocks["PB-01"].offset(pb02.at) != clocks["GW02"].offset(pb02.at)
