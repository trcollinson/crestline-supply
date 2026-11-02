"""PC-LINE over GW03: the process stream, with every transport mess off."""

from datetime import time, timedelta
from itertools import pairwise

import polars as pl
import pytest

from crestline.sim.clock import PLANT_TZ
from crestline.sim.records import MachineState
from crestline.sim.systems import machines
from crestline.sim.systems.machines import powder

SEED = 42


@pytest.fixture(scope="module")
def samples(plant_run) -> list[machines.Message]:
    return list(machines.pcline_messages(plant_run.truth, SEED))


def test_sampled_every_five_seconds_day_and_night(plant_run, samples):
    start, end = machines.machine_window(plant_run.truth)

    assert len(samples) == (end - start) / powder.SAMPLE
    assert all(b.at - a.at == powder.SAMPLE for a, b in pairwise(samples))
    assert {m.topic for m in samples} == {powder.TOPIC}
    assert all(
        list(m.payload) == ["t", "z1", "z2", "z3", "z4", "spd", "hook_cnt"]
        for m in samples
    )
    assert all(m.payload["t"] == powder.epoch_ms(m.at) for m in samples)


def test_one_hook_per_piece(plant_run, samples):
    hooks = [m.payload["hook_cnt"] for m in samples]
    pieces = (
        plant_run.truth["truth_cycles"].filter(pl.col("machine_id") == "PC-LINE").height
    )

    assert hooks == sorted(hooks)
    assert hooks[-1] - hooks[0] == pieces


def test_the_conveyor_runs_while_the_line_is_working(plant_run, samples):
    states = plant_run.truth["truth_machine_states"].filter(
        pl.col("machine_id") == "PC-LINE"
    )
    rows = states.select("state", "started_at", "ended_at").rows()
    i = 0
    seen = set()
    for m in samples:
        while rows[i][2] <= m.at:
            i += 1
        running = rows[i][0] in powder.RUNNING_STATES
        assert m.payload["spd"] == (12.0 if running else 0.0)
        seen.add(rows[i][0])
    assert {MachineState.RUNNING, MachineState.DOWN, MachineState.PLANNED_STOP} <= seen


def test_the_ovens_are_at_setpoint_on_shift_and_cold_after_a_weekend(samples):
    def zones_at(local_day_index: int, local: time) -> list[float]:
        day = samples[0].at.astimezone(PLANT_TZ).date() + timedelta(
            days=local_day_index
        )
        m = next(
            s
            for s in samples
            if s.at.astimezone(PLANT_TZ).replace(tzinfo=None)
            >= (__import__("datetime").datetime.combine(day, local))
        )
        return [m.payload[f"z{z}"] for z in range(1, 5)]

    assert all(395 <= z <= 405 for z in zones_at(0, time(12, 0)))  # Monday noon
    assert all(
        380 <= z <= 405 for z in zones_at(0, time(6, 0))
    )  # warmed up by 1st shift
    assert all(z < 100 for z in zones_at(6, time(12, 0)))  # Sunday noon
    assert all(
        z < 120 for z in zones_at(7, time(4, 0))
    )  # Monday before the ovens are lit
