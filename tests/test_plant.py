import time as wall_clock
from datetime import date, time
from itertools import pairwise

import pytest

from crestline.reference import ReferenceData
from crestline.sim import Plant, PlantConfig, SimulationResult, generate_jobs, simulate
from crestline.sim.clock import SimClock
from crestline.sim.config import DowntimeConfig, DowntimeKind, VariationConfig
from crestline.sim.records import (
    BUSY_STATES,
    DowntimeReason,
    JobStatus,
    MachineState,
    ScrapCause,
)
from crestline.sim.shifts import ShiftCalendar

FIRST_DAY = date(2026, 7, 6)  # a Monday
LAST_RELEASE = date(2026, 7, 10)  # release for one week...
LAST_DAY = date(2026, 7, 17)  # ...and run for two, so nearly everything completes
SEED = 42

NO_DOWNTIME = DowntimeConfig(mean_run_minutes_between_events=None)
NO_VARIATION = VariationConfig(
    setup_sigma=0, machine_cycle_sigma=0, manual_cycle_sigma=0
)


@pytest.fixture(scope="module")
def result(reference_data: ReferenceData) -> SimulationResult:
    jobs = generate_jobs(reference_data, FIRST_DAY, LAST_RELEASE, SEED)
    return simulate(reference_data, jobs, FIRST_DAY, LAST_DAY, SEED)


@pytest.fixture(scope="module")
def exact_result(reference_data: ReferenceData) -> SimulationResult:
    """Same jobs, but no downtime and no variation: times land on standard."""
    jobs = generate_jobs(reference_data, FIRST_DAY, LAST_RELEASE, SEED)
    config = PlantConfig(downtime=NO_DOWNTIME, variation=NO_VARIATION)
    return simulate(reference_data, jobs, FIRST_DAY, LAST_DAY, SEED, config=config)


def ops_by_job(result: SimulationResult) -> dict[str, list]:
    grouped: dict[str, list] = {}
    for op in result.operations:
        grouped.setdefault(op.job_num, []).append(op)
    return grouped


def test_same_seed_produces_identical_results(reference_data, result):
    jobs = generate_jobs(reference_data, FIRST_DAY, LAST_RELEASE, SEED)
    again = simulate(reference_data, jobs, FIRST_DAY, LAST_DAY, SEED)

    assert again == result


def test_jobs_complete_in_routing_order(reference_data, result):
    routing = {
        (p.part_id, p.rev): [op.op_seq for op in p.routing]
        for p in reference_data.parts
    }

    for job in result.jobs:
        ops = ops_by_job(result)[job.job_num]
        assert [op.op_seq for op in ops] == routing[(job.part_id, job.rev)][: len(ops)]
        if job.status == JobStatus.COMPLETE:
            assert len(ops) == len(routing[(job.part_id, job.rev)])
        for previous, current in pairwise(ops):
            assert previous.completed_at is not None
            assert current.queued_at >= previous.completed_at
            assert current.setup_started_at >= current.queued_at
            assert current.run_started_at >= current.setup_started_at


def test_machines_never_run_outside_shifts(result):
    clock = SimClock.starting_on(FIRST_DAY)
    calendar = ShiftCalendar(clock, PlantConfig().calendar, FIRST_DAY, LAST_DAY)

    busy = [s for s in result.machine_states if s.state in BUSY_STATES]
    assert busy
    for state in busy:
        start = int((state.start - clock.start_utc).total_seconds())
        end = int((state.end - clock.start_utc).total_seconds())
        window = calendar.window_at(start)
        assert window is not None, f"{state} starts outside a shift window"
        assert end <= window.end, f"{state} runs past the end of its window"
        assert state.shift == window.shift


def test_machine_state_intervals_tile_the_timeline(result):
    calendar_end = SimClock.starting_on(FIRST_DAY).seconds_at(
        date(2026, 7, 18), time(0)
    )
    for machine_id in {s.machine_id for s in result.machine_states}:
        states = [s for s in result.machine_states if s.machine_id == machine_id]
        assert states == sorted(states, key=lambda s: s.start)
        assert states[0].start == SimClock.starting_on(FIRST_DAY).start_utc
        for previous, current in pairwise(states):
            assert current.start == previous.end
        assert (states[-1].end - states[0].start).total_seconds() == calendar_end


def test_quantity_flows_through_the_routing(result):
    for job in result.jobs:
        ops = ops_by_job(result)[job.job_num]
        assert ops[0].qty_in == job.qty_released
        for previous, current in pairwise(ops):
            assert previous.qty_in == previous.qty_good + previous.qty_scrap
            assert current.qty_in == previous.qty_good
        if job.status == JobStatus.COMPLETE:
            assert job.qty_completed == ops[-1].qty_good
            assert job.qty_scrapped == sum(op.qty_scrap for op in ops)


def test_cycles_match_operation_counts(result):
    counts: dict[tuple[str, int], list[int]] = {}
    for cycle in result.cycles:
        good, scrap = counts.setdefault((cycle.job_num, cycle.op_seq), [0, 0])
        counts[(cycle.job_num, cycle.op_seq)] = [
            good + cycle.good,
            scrap + (not cycle.good),
        ]
        assert cycle.scrap_cause in {None, ScrapCause.RANDOM, ScrapCause.SETUP_SCRAP}
        assert cycle.good == (cycle.scrap_cause is None)

    for op in result.operations:
        if op.completed_at is not None:
            assert counts[(op.job_num, op.op_seq)] == [op.qty_good, op.qty_scrap]
            assert op.machine_id in {
                c.machine_id for c in result.cycles if c.job_num == op.job_num
            }


def test_setup_scrap_only_hits_the_first_piece(result):
    first_cycle_seen: set[tuple[str, int]] = set()
    for cycle in result.cycles:  # cycles are appended in production order per op
        key = (cycle.job_num, cycle.op_seq)
        if cycle.scrap_cause == ScrapCause.SETUP_SCRAP:
            assert key not in first_cycle_seen
        first_cycle_seen.add(key)
    assert any(c.scrap_cause == ScrapCause.SETUP_SCRAP for c in result.cycles)


def test_brake_operations_use_whichever_brake_frees_up(result):
    brake_machines = {
        op.machine_id for op in result.operations if op.work_center == "BRAKE"
    }
    assert brake_machines == {"PB-01", "PB-02"}


def test_operator_matches_machine_and_shift(reference_data, result):
    employees = {e.employee_id: e for e in reference_data.employees}
    work_center_of = {
        wc.machine_id: wc.work_center for wc in reference_data.work_centers
    }

    busy = [
        s
        for s in result.machine_states
        if s.state in BUSY_STATES and s.reason != DowntimeReason.NO_OPERATOR
    ]
    for state in busy:
        employee = employees[state.employee_id]
        assert employee.work_center == work_center_of[state.machine_id]
        assert employee.shift == state.shift

    # The two brake operators float: each of them runs both brakes over the window.
    brake_pairs = {
        (s.employee_id, s.machine_id)
        for s in busy
        if work_center_of[s.machine_id] == "BRAKE"
    }
    for employee_id in {e for e, _ in brake_pairs}:
        assert {m for e, m in brake_pairs if e == employee_id} == {"PB-01", "PB-02"}


def test_downtime_is_random_and_switchable(reference_data, result, exact_result):
    down = [s for s in result.machine_states if s.state == MachineState.DOWN]
    assert down
    assert {s.reason for s in down} <= {r.value for r in DowntimeReason}
    limits = {k.reason: k.max_minutes for k in PlantConfig().downtime.kinds}
    manual = {
        wc.machine_id for wc in reference_data.work_centers if wc.type == "manual"
    }
    for state in down:
        assert (state.end - state.start).total_seconds() <= limits[state.reason] * 60
        assert state.machine_id not in manual

    assert not [s for s in exact_result.machine_states if s.state == MachineState.DOWN]


def test_no_operator_downtime_has_nobody_on_the_machine(reference_data):
    jobs = generate_jobs(reference_data, FIRST_DAY, LAST_RELEASE, SEED)
    only_no_operator = DowntimeConfig(
        kinds=[
            DowntimeKind(
                reason=DowntimeReason.NO_OPERATOR,
                weight=1,
                min_minutes=5,
                max_minutes=20,
            )
        ]
    )
    config = PlantConfig(downtime=only_no_operator)
    result = simulate(reference_data, jobs, FIRST_DAY, LAST_DAY, SEED, config=config)

    down = [s for s in result.machine_states if s.state == MachineState.DOWN]
    assert down
    assert all(s.employee_id is None for s in down)
    # Everywhere else, someone is on the machine.
    assert all(
        s.employee_id is not None
        for s in result.machine_states
        if s.state in {MachineState.SETUP, MachineState.RUNNING}
    )


def test_without_variation_times_land_on_standard(exact_result):
    seconds_by_op_and_state: dict[tuple[str, int, MachineState], float] = {}
    for state in exact_result.machine_states:
        if state.state in BUSY_STATES:
            key = (state.job_num, state.op_seq, state.state)
            seconds_by_op_and_state[key] = (
                seconds_by_op_and_state.get(key, 0)
                + (state.end - state.start).total_seconds()
            )

    for op in exact_result.operations:
        if op.completed_at is None:
            continue
        setup = seconds_by_op_and_state.get(
            (op.job_num, op.op_seq, MachineState.SETUP), 0
        )
        run = seconds_by_op_and_state.get(
            (op.job_num, op.op_seq, MachineState.RUNNING), 0
        )
        assert setup == op.std_setup_min * 60
        assert run == pytest.approx(
            op.qty_in * op.std_run_min_per_piece * 60, abs=op.qty_in
        )


def test_setup_multiplier_hook_scales_setup_time(reference_data):
    jobs = generate_jobs(reference_data, FIRST_DAY, LAST_RELEASE, SEED)
    config = PlantConfig(downtime=NO_DOWNTIME, variation=NO_VARIATION)

    def slow_pb02_second_shift(machine_id: str, shift: str, _day: date) -> float:
        return 2.0 if (machine_id, shift) == ("PB-02", "2nd") else 1.0

    result = Plant(
        reference_data,
        jobs.jobs,
        FIRST_DAY,
        LAST_DAY,
        SEED,
        config,
        slow_pb02_second_shift,
    ).run()

    setup_seconds: dict[tuple[str, int], float] = {}
    affected: set[tuple[str, int]] = set()
    for state in result.machine_states:
        if state.state == MachineState.SETUP:
            key = (state.job_num, state.op_seq)
            setup_seconds[key] = (
                setup_seconds.get(key, 0) + (state.end - state.start).total_seconds()
            )
            if (state.machine_id, state.shift) == ("PB-02", "2nd"):
                affected.add(key)
    assert affected

    all_pb02_2nd = {
        (s.job_num, s.op_seq)
        for s in result.machine_states
        if s.state == MachineState.SETUP
        and (s.job_num, s.op_seq) in affected
        and (s.machine_id, s.shift) != ("PB-02", "2nd")
    }
    for op in result.operations:
        key = (op.job_num, op.op_seq)
        if (
            key in affected and key not in all_pb02_2nd
        ):  # setup entirely on PB-02 2nd shift
            assert setup_seconds[key] == 2 * op.std_setup_min * 60
        elif key not in affected and op.completed_at is not None:
            assert setup_seconds.get(key, 0) == op.std_setup_min * 60


def test_ninety_days_runs_in_under_a_minute(reference_data, capsys):
    first_day, last_day = date(2026, 7, 3), date(2026, 9, 30)
    jobs = generate_jobs(reference_data, first_day, last_day, SEED)

    started = wall_clock.perf_counter()
    result = simulate(reference_data, jobs, first_day, last_day, SEED)
    elapsed = wall_clock.perf_counter() - started

    assert elapsed < 60

    # The plant keeps up: jobs released with two weeks to go all but finish.
    settled = [j for j in result.jobs if j.release_date <= date(2026, 9, 16)]
    completed = [j for j in settled if j.status == JobStatus.COMPLETE]
    assert len(completed) / len(settled) > 0.95

    busy: dict[str, float] = {}
    available: dict[str, float] = {}
    for state in result.machine_states:
        seconds = (state.end - state.start).total_seconds()
        if state.state in BUSY_STATES:
            busy[state.machine_id] = busy.get(state.machine_id, 0) + seconds
        if state.state != MachineState.PLANNED_STOP:
            available[state.machine_id] = available.get(state.machine_id, 0) + seconds
    with capsys.disabled():
        print(
            f"\n90 days: {len(result.jobs)} jobs, {len(result.cycles)} cycles in {elapsed:.1f}s; "
            + ", ".join(
                f"{m} {busy.get(m, 0) / available[m]:.0%}" for m in sorted(available)
            )
        )
