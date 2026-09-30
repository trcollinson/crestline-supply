from bisect import bisect_right
from datetime import datetime, timedelta
from itertools import pairwise

import polars as pl
import pytest

from crestline.mess import MachinesMess
from crestline.sim.records import MachineState, StopReason
from crestline.sim.systems import machines

SEED = 42


def when(message) -> datetime:
    return datetime.fromisoformat(message["published_at"])


@pytest.fixture(scope="module")
def pb01(plant_run, reference_data) -> list[dict]:
    return list(machines.pb01_messages(plant_run.truth, reference_data, SEED))


def truth_states(plant_run, machine_id: str) -> pl.DataFrame:
    return plant_run.truth["truth_machine_states"].filter(
        pl.col("machine_id") == machine_id
    )


def pieces_on(plant_run, machine_id: str) -> int:
    cycles = plant_run.truth["truth_cycles"]
    return cycles.filter(pl.col("machine_id") == machine_id).height


def test_pb02_is_polled_every_30_seconds_around_the_clock(plant_run):
    messages = list(machines.pb02_messages(plant_run.truth, MachinesMess(), SEED))
    states = truth_states(plant_run, "PB-02")
    window = states["ended_at"].max() - states["started_at"].min()

    assert len(messages) == window / timedelta(seconds=30)
    for previous, current in pairwise(messages):
        assert when(current) - when(previous) == timedelta(seconds=30)
    assert {m["topic"] for m in messages} == {machines.PB02_TOPIC}
    assert all(set(m["payload"]) == {"dev", "cnt", "gw_ts"} for m in messages)


def test_pb02_counter_rolls_over_at_16_bits(plant_run):
    messages = machines.pb02_messages(
        plant_run.truth, MachinesMess(), SEED, counter_start=65_000
    )
    counts = [m["payload"]["cnt"] for m in messages]

    assert max(counts) <= 65_535
    assert any(later < earlier for earlier, later in pairwise(counts))
    strokes = sum((later - earlier) % 2**16 for earlier, later in pairwise(counts))
    # The first poll is at local midnight on the first day, before any work.
    assert strokes == pieces_on(plant_run, "PB-02")


def test_pb02_counter_without_rollover_just_keeps_counting(plant_run):
    messages = machines.pb02_messages(
        plant_run.truth,
        MachinesMess(counter_rollover_bits=None),
        SEED,
        counter_start=65_000,
    )
    counts = [m["payload"]["cnt"] for m in messages]

    assert counts == sorted(counts)
    assert counts[-1] == 65_000 + pieces_on(plant_run, "PB-02")


def test_pb01_counts_every_piece_and_publishes_at_most_once_a_second(plant_run, pb01):
    counts = [m["payload"]["CYC_CNT_2"] for m in pb01]
    times = [when(m) for m in pb01]

    assert counts == sorted(counts)
    assert counts[-1] - counts[0] == pieces_on(plant_run, "PB-01")
    assert len(set(times)) == len(times)
    assert {m["topic"] for m in pb01} == {machines.PB01_TOPIC}
    assert all(
        list(m["payload"]) == ["ts", "N7:12", "CYC_CNT_2", "B3:0/4", "PRG"]
        for m in pb01
    )


def test_pb01_shows_the_mode_the_operator_left_it_in(plant_run, pb01):
    times = [when(m) for m in pb01]

    def registers_at(moment: datetime) -> dict:
        return pb01[bisect_right(times, moment) - 1]["payload"]

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
        registers = pb01[bisect_right(times, setup["started_at"]) - 1]["payload"]
        assert registers["PRG"] == machines.brake_program(part_of_job[setup["job_num"]])
