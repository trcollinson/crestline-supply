"""The brakes and the polled counters, with every transport mess off."""

from bisect import bisect_right
from datetime import datetime, timedelta
from itertools import pairwise

import polars as pl
import pytest

from crestline.mess import MessConfig
from crestline.sim.records import MachineState, StopReason
from crestline.sim.systems import machines

SEED = 42
# M1's machines: the rollover and nothing else.
M1 = MessConfig(mess_level=0).machines.model_copy(update={"counter_rollover_bits": 16})


def when(message) -> datetime:
    return message.at


@pytest.fixture(scope="module")
def clocks(plant_run) -> dict[str, machines.DriftingClock]:
    return machines.clocks(M1, SEED, machines.machine_window(plant_run.truth))


@pytest.fixture(scope="module")
def pb01(plant_run, reference_data, clocks) -> list[machines.Message]:
    return list(
        machines.pb01_messages(plant_run.truth, reference_data, clocks["PB-01"], SEED)
    )


def truth_states(plant_run, machine_id: str) -> pl.DataFrame:
    return plant_run.truth["truth_machine_states"].filter(
        pl.col("machine_id") == machine_id
    )


def pieces_on(plant_run, machine_id: str) -> int:
    cycles = plant_run.truth["truth_cycles"]
    return cycles.filter(pl.col("machine_id") == machine_id).height


def counts(plant_run, clocks, machine_id, mess=M1, **kwargs) -> list[machines.Message]:
    return list(
        machines.polled_counter_messages(
            machine_id, plant_run.truth, mess, clocks["GW02"], SEED, **kwargs
        )
    )


@pytest.mark.parametrize("machine_id", ["PB-02", "PEM-01"])
def test_the_legacy_counters_are_polled_every_30_seconds_around_the_clock(
    plant_run, clocks, machine_id
):
    messages = counts(plant_run, clocks, machine_id)
    states = truth_states(plant_run, machine_id)
    window = states["ended_at"].max() - states["started_at"].min()
    dev, topic, _ = machines.DEVICES[machine_id]

    assert len(messages) == window / timedelta(seconds=30)
    for previous, current in pairwise(messages):
        assert when(current) - when(previous) == timedelta(seconds=30)
    assert {m.topic for m in messages} == {topic}
    assert all(list(m.payload) == ["dev", "cnt", "gw_ts"] for m in messages)
    assert all(m.payload["dev"] == dev for m in messages)


@pytest.mark.parametrize("machine_id", ["PB-02", "PEM-01"])
def test_the_counter_rolls_over_at_16_bits(plant_run, clocks, machine_id):
    messages = counts(plant_run, clocks, machine_id, counter_start=65_000)
    values = [m.payload["cnt"] for m in messages]

    assert max(values) <= 65_535
    assert any(later < earlier for earlier, later in pairwise(values))
    strokes = sum((later - earlier) % 2**16 for earlier, later in pairwise(values))
    # The first poll is at local midnight on the first day, before any work.
    assert strokes == pieces_on(plant_run, machine_id)


def test_the_counter_without_rollover_just_keeps_counting(plant_run, clocks):
    messages = counts(
        plant_run,
        clocks,
        "PB-02",
        mess=M1.model_copy(update={"counter_rollover_bits": None}),
        counter_start=65_000,
    )
    values = [m.payload["cnt"] for m in messages]

    assert values == sorted(values)
    assert values[-1] == 65_000 + pieces_on(plant_run, "PB-02")


def test_a_power_cycle_puts_the_counter_back_to_zero(plant_run, clocks):
    states = truth_states(plant_run, "PB-02")
    resets = machines.power_cycles("PB-02", states, rate=1.0, seed=SEED)
    working_days = {
        row["started_at"].astimezone(machines.counters.PLANT_TZ).date()
        for row in states.filter(pl.col("state") != MachineState.PLANNED_STOP)
        .select("started_at")
        .iter_rows(named=True)
    }
    assert len(resets) == len(working_days) == 10  # one a day at rate 1
    running = states.filter(pl.col("state") == MachineState.RUNNING)
    stops = states.filter(
        (pl.col("state") == MachineState.PLANNED_STOP)
        & pl.col("reason").is_in([StopReason.BREAK.value, StopReason.MEAL.value])
    )
    for at in resets:  # never while running: at 1st-shift start or a break
        assert not running.filter(
            (pl.col("started_at") < at) & (pl.col("ended_at") > at)
        ).height
        at_shift_start = states.filter(
            (pl.col("started_at") == at)
            & (pl.col("state") != MachineState.PLANNED_STOP)
        ).height
        at_a_break = stops.filter(pl.col("started_at") == at).height
        assert at_shift_start or at_a_break

    messages = counts(plant_run, clocks, "PB-02", resets=resets)
    values = [m.payload["cnt"] for m in messages]
    polls = [when(m) for m in messages]
    for at in resets:
        i = bisect_right(polls, at) - 1  # the first poll at or after the reset
        assert polls[i] == at and values[i] == 0
    # Strokes after the last reset count up from zero; nothing is lost.
    after = sum(1 for at in strokes_on(plant_run, "PB-02") if at > resets[-1])
    assert values[-1] == after

    assert machines.power_cycles("PB-02", states, rate=0.0, seed=SEED) == []


def test_the_counter_wraps_before_its_first_power_cycle(plant_run, clocks):
    states = truth_states(plant_run, "PB-02")
    resets = machines.power_cycles("PB-02", states, rate=0.5, seed=SEED)
    assert resets
    values = [
        m.payload["cnt"] for m in counts(plant_run, clocks, "PB-02", resets=resets)
    ]
    polls = [when(m) for m in counts(plant_run, clocks, "PB-02")]
    first_reset = (
        polls.index(resets[0]) if resets[0] in polls else bisect_right(polls, resets[0])
    )

    wrapped = next(i for i in range(1, len(values)) if values[i] < values[i - 1])
    assert values[wrapped - 1] >= 65_000 and wrapped < first_reset
    assert values[first_reset] == 0 and values[first_reset - 1] < 65_000  # the reset


def strokes_on(plant_run, machine_id: str) -> list[datetime]:
    cycles = plant_run.truth["truth_cycles"]
    return sorted(cycles.filter(pl.col("machine_id") == machine_id)["completed_at"])


def test_pb01_counts_every_piece_and_publishes_at_most_once_a_second(plant_run, pb01):
    cyc = [m.payload["CYC_CNT_2"] for m in pb01]
    times = [when(m) for m in pb01]

    assert cyc == sorted(cyc)
    assert cyc[-1] - cyc[0] == pieces_on(plant_run, "PB-01")
    assert len(set(times)) == len(times)
    assert {m.topic for m in pb01} == {machines.PB01_TOPIC}
    assert all(
        list(m.payload) == ["ts", "N7:12", "CYC_CNT_2", "B3:0/4", "PRG"] for m in pb01
    )
    assert all(m.payload["ts"] == machines.message.millis(when(m)) for m in pb01)


def test_pb01_shows_the_mode_the_operator_left_it_in(plant_run, pb01):
    times = [when(m) for m in pb01]

    def registers_at(moment: datetime) -> dict:
        return pb01[bisect_right(times, moment) - 1].payload

    states = list(truth_states(plant_run, "PB-01").iter_rows(named=True))
    seen_idle_after_run = False
    for previous, state in pairwise(states):
        registers = registers_at(state["started_at"])
        if state["state"] == MachineState.SETUP:
            assert (registers["N7:12"], registers["B3:0/4"]) == (machines.SETUP, True)
        elif state["state"] == MachineState.RUNNING:
            assert (registers["N7:12"], registers["B3:0/4"]) == (machines.RUN, False)
        elif state["reason"] == StopReason.NO_SHIFT:
            assert registers["N7:12"] == machines.IDLE
        elif (
            state["state"] == MachineState.IDLE
            and previous["state"] == MachineState.RUNNING
        ):
            # Waiting for the next job, but still in run mode: no strokes.
            assert registers["N7:12"] == machines.RUN
            seen_idle_after_run = True
    assert seen_idle_after_run


def test_pb01_program_follows_the_job_on_the_machine(plant_run, pb01):
    part_of_job = dict(
        plant_run.truth["truth_jobs"].select("job_num", "part_id").iter_rows()
    )
    times = [when(m) for m in pb01]
    setups = truth_states(plant_run, "PB-01").filter(
        pl.col("state") == MachineState.SETUP
    )
    assert setups.height > 0
    for setup in setups.iter_rows(named=True):
        registers = pb01[bisect_right(times, setup["started_at"]) - 1].payload
        assert registers["PRG"] == machines.brake_program(part_of_job[setup["job_num"]])
