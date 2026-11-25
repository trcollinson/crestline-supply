"""The degrading laser nozzle (M5 step 1, spec §3.5).

Short windows with the ramp moved to the first days, so each story plays out
in two or three weeks instead of the backfill's ninety days.
"""

import math
from collections import Counter
from datetime import date, datetime
from pathlib import Path

import pytest

from crestline.sim import Plant, Scenarios, generate_jobs, load_scenarios, simulate
from crestline.sim.clock import PLANT_TZ
from crestline.sim.records import MachineState, ScrapCause, SimulationResult

REPO_ROOT = Path(__file__).resolve().parent.parent
FIRST_DAY = date(2026, 7, 6)  # a Monday
LAST_RELEASE = date(2026, 7, 17)
LAST_DAY = date(2026, 7, 24)
SEED = 42
LASER = "LSR-01"


def nozzle(**settings) -> Scenarios:
    return Scenarios(laser_nozzle_degradation={"enabled": True, **settings})


def run(reference_data, scenarios: Scenarios) -> SimulationResult:
    jobs = generate_jobs(reference_data, FIRST_DAY, LAST_RELEASE, SEED)
    return simulate(
        reference_data, jobs, FIRST_DAY, LAST_DAY, SEED, scenarios=scenarios
    )


def local_day(at: datetime) -> date:
    return at.astimezone(PLANT_TZ).date()


def what_happened(result: SimulationResult) -> tuple:
    """Everything the plant did, without the scenario's own notes."""
    return result.jobs, result.operations, result.machine_states, result.cycles


@pytest.fixture(scope="module")
def plain(reference_data) -> SimulationResult:
    return run(reference_data, Scenarios())


def test_off_by_default_and_in_the_repos_scenarios_file():
    assert Scenarios().laser_nozzle_degradation.enabled is False
    assert (
        load_scenarios(REPO_ROOT / "scenarios.yaml").laser_nozzle_degradation.enabled
        is False
    )
    m5 = load_scenarios(REPO_ROOT / "eval" / "scenarios_nozzle.yaml")
    assert m5.laser_nozzle_degradation.enabled is True
    assert m5.pb02_second_shift_setups == Scenarios().pb02_second_shift_setups


def test_a_ramp_that_scraps_nothing_changes_nothing(reference_data, plain):
    """The nozzle draws on its own stream: drawing alone moves no other draw.

    With both rates at 0 the scenario still draws for every good laser piece
    from day 1 to the end; only a piece it scraps may change what follows.
    """
    result = run(reference_data, nozzle(start_day=1, start_rate=0, end_rate=0))

    assert what_happened(result) == what_happened(plain)
    # 15 working days from Jul 6 is Jul 24, the window's last day: not capped.
    assert [e.event for e in result.events] == ["ramp_start", "peak"]


def test_the_check_above_fails_if_the_nozzle_drew_on_the_plant_stream(
    reference_data, plain
):
    """The same zero-rate ramp, its draws moved onto the plant's stream."""
    jobs = generate_jobs(reference_data, FIRST_DAY, LAST_RELEASE, SEED)
    config = nozzle(start_day=1, start_rate=0, end_rate=0).laser_nozzle_degradation
    wear = config.wear(SEED, FIRST_DAY)
    plant = Plant(
        reference_data,
        jobs.jobs,
        FIRST_DAY,
        LAST_DAY,
        SEED,
        setup_multiplier=Scenarios().setup_multiplier(),
        wear=wear,
    )
    wear.rng = plant.rng  # the mistake this test exists to catch

    assert what_happened(plant.run()) != what_happened(plain)


def test_daily_nozzle_scrap_follows_the_ramp(reference_data):
    """Each day's NOZZLE_WEAR count is within 3 binomial standard deviations.

    The nozzle is drawn for every laser piece the plant judged good, so those
    pieces (good, plus the ones the nozzle scrapped) are the trials.
    """
    scenarios = nozzle(
        start_day=2,
        ramp_days=5,
        start_rate=0.02,
        end_rate=0.12,
        noticed_weekly_rate=None,
        max_ramp_working_days=None,
    )
    result = run(reference_data, scenarios)
    schedule = scenarios.laser_nozzle_degradation.wear(SEED, FIRST_DAY)

    trials: Counter[date] = Counter()
    worn: Counter[date] = Counter()
    for cycle in result.cycles:
        if cycle.scrap_cause == ScrapCause.NOZZLE_WEAR:
            assert cycle.machine_id == LASER
        if cycle.machine_id == LASER and cycle.scrap_cause in (
            None,
            ScrapCause.NOZZLE_WEAR,
        ):
            trials[local_day(cycle.at)] += 1
            worn[local_day(cycle.at)] += cycle.scrap_cause == ScrapCause.NOZZLE_WEAR

    assert worn[FIRST_DAY] == 0  # before day 2
    assert len(trials) == 10  # every working day jobs were released on
    for day, n in trials.items():
        p = schedule.rate_on(day)
        expected = n * p
        spread = 3 * math.sqrt(n * p * (1 - p))
        assert abs(worn[day] - expected) <= spread, (day, n, p, worn[day])
    assert schedule.rate_on(date(2026, 7, 7)) == pytest.approx(0.02)
    assert schedule.rate_on(date(2026, 7, 11)) == pytest.approx(0.12)
    assert schedule.rate_on(date(2026, 7, 20)) == pytest.approx(0.12)  # holds


def test_a_bad_week_is_noticed_on_monday_and_the_nozzle_changed(reference_data):
    result = run(reference_data, nozzle(start_day=1, ramp_days=1, start_rate=0.1))

    assert [e.event for e in result.events] == ["ramp_start", "peak", "nozzle_changed"]
    changed = result.events[-1]
    assert changed.local_date == date(2026, 7, 13)  # the second Monday
    assert "nozzle changed" in changed.detail

    # The fix is a MAINTENANCE stop on the laser that ends when the wear does.
    stop = [
        s
        for s in result.machine_states
        if s.machine_id == LASER
        and s.state == MachineState.DOWN
        and s.reason == "MAINTENANCE"
        and s.end == changed.at
    ]
    assert len(stop) == 1
    planted = [
        s
        for s in result.machine_states
        if (s.machine_id, s.reason, s.job_num, s.op_seq)
        == (LASER, "MAINTENANCE", stop[0].job_num, stop[0].op_seq)
        and local_day(s.start) == changed.local_date
    ]
    minutes = sum((s.end - s.start).total_seconds() for s in planted) / 60
    assert 30 <= minutes <= 90

    # Week one really was over 5%, and nothing is scrapped for wear after.
    week_one = [
        c
        for c in result.cycles
        if c.machine_id == LASER and local_day(c.at) <= date(2026, 7, 10)
    ]
    assert sum(not c.good for c in week_one) / len(week_one) > 0.05
    nozzle_scrap = [
        c.at for c in result.cycles if c.scrap_cause == ScrapCause.NOZZLE_WEAR
    ]
    assert nozzle_scrap and max(nozzle_scrap) < changed.at


def test_a_week_under_the_threshold_goes_unnoticed(reference_data):
    """2% on top of the laser's usual ~1.6% stays under the 5% that gets a look."""
    result = run(
        reference_data,
        nozzle(
            start_day=1,
            ramp_days=1,
            start_rate=0.02,
            end_rate=0.02,
            max_ramp_working_days=None,
        ),
    )

    assert [e.event for e in result.events] == ["ramp_start", "peak"]
    assert max(
        c.at for c in result.cycles if c.scrap_cause == ScrapCause.NOZZLE_WEAR
    ) > datetime(2026, 7, 16, tzinfo=PLANT_TZ)


def test_nobody_noticing_ends_at_the_cap(reference_data):
    result = run(
        reference_data,
        nozzle(
            start_day=1,
            ramp_days=1,
            start_rate=0.1,
            noticed_weekly_rate=None,
            max_ramp_working_days=3,
        ),
    )

    capped = result.events[-1]
    assert [e.event for e in result.events] == ["ramp_start", "peak", "ramp_capped"]
    assert capped.local_date == date(2026, 7, 8)  # the third working day
    assert capped.at == datetime(2026, 7, 9, tzinfo=PLANT_TZ)
    worn_days = {
        local_day(c.at)
        for c in result.cycles
        if c.scrap_cause == ScrapCause.NOZZLE_WEAR
    }
    assert max(worn_days) == date(2026, 7, 8)


def test_a_ramp_after_the_window_leaves_no_events(reference_data, plain):
    result = run(reference_data, nozzle(start_day=41))

    assert result.events == []
    assert what_happened(result) == what_happened(plain)
