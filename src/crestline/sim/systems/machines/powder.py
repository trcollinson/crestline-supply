"""PC-LINE, the powder coat line, over process gateway GW03 (spec §4.3).

The line is a conveyor through four oven zones. GW03 samples it every five
seconds, day and night: the four zone temperatures, the line speed, and
the hook counter. No parts, no jobs: the line knows hooks and temperatures.
It is the plant's highest-volume tag source, which is why it is here even
though M3's evals don't use it.

The ovens are lit at 05:00 on working days and warm up to their 400 °F
setpoints before 1st shift; they are shut off at 23:00 and cool overnight.
While lit, each zone wanders a degree or two around its setpoint. The
conveyor runs while the line is working (idle, setting up, or running) and
stops for a stoppage or a break. The hook counter goes up by one per piece
hung, since the plant model has run POWDER one piece at a time since M1.
GW03 keeps NTP time, so `t` is true time. No scenario touches the ovens
in M3 (the zone 3 drift is M8's).
"""

import math
from collections.abc import Iterator, Mapping
from datetime import UTC, datetime, time, timedelta

import polars as pl

from crestline.rng import make_rng

from ...clock import PLANT_TZ
from ...records import MachineState, StopReason
from .laser import epoch_ms
from .message import Message

TOPIC = "crestline/p10/gw03/pcline/process"
DEVICE = "PC-LINE"
SAMPLE = timedelta(seconds=5)

SETPOINT_F = 400.0
AMBIENT_F = 75.0
OVEN_ON = time(5, 0)  # local, on working days
OVEN_OFF = time(23, 0)
WARM_UP_TAU = timedelta(minutes=15)
COOL_DOWN_TAU = timedelta(minutes=90)
NOISE_SD_F = 1.5
NOISE_PERSISTENCE = 0.9  # per sample
LINE_SPEED_FT_MIN = 12.0
RUNNING_STATES = {MachineState.IDLE, MachineState.SETUP, MachineState.RUNNING}
HOOK_COUNT_START_RANGE = (50_000, 150_000)
ZONES = 4


def pcline_messages(truth: Mapping[str, pl.DataFrame], seed: int) -> Iterator[Message]:
    rng = make_rng(seed, stream="pcline")
    hooks = rng.randint(*HOOK_COUNT_START_RANGE)
    states = (
        truth["truth_machine_states"]
        .filter(pl.col("machine_id") == DEVICE)
        .sort("started_at")
    )
    rows = states.select("state", "started_at", "ended_at").rows()
    pieces = sorted(
        truth["truth_cycles"].filter(pl.col("machine_id") == DEVICE)["completed_at"]
    )
    lit = _oven_schedule(states)

    at, window_end = states["started_at"].min(), states["ended_at"].max()
    assert at is not None and window_end is not None
    temps = [AMBIENT_F] * ZONES
    noise = [0.0] * ZONES
    shock = math.sqrt(1 - NOISE_PERSISTENCE**2) * NOISE_SD_F
    warm = 1 - math.exp(-SAMPLE / WARM_UP_TAU)
    cool = 1 - math.exp(-SAMPLE / COOL_DOWN_TAU)
    row = piece = span = origin = 0
    while at < window_end:
        while rows[row][2] <= at:
            row += 1
        while piece < len(pieces) and pieces[piece] <= at:
            piece += 1
        while span < len(lit) and lit[span][1] <= at:
            span += 1
        oven_on = span < len(lit) and lit[span][0] <= at
        target, pull = (SETPOINT_F, warm) if oven_on else (AMBIENT_F, cool)
        payload: dict[str, object] = {"t": epoch_ms(at)}
        for z in range(ZONES):
            temps[z] += (target - temps[z]) * pull
            noise[z] = NOISE_PERSISTENCE * noise[z] + shock * rng.gauss(0, 1)
            payload[f"z{z + 1}"] = round(temps[z] + noise[z], 1)
        payload["spd"] = LINE_SPEED_FT_MIN if rows[row][0] in RUNNING_STATES else 0.0
        payload["hook_cnt"] = hooks + piece
        yield Message(TOPIC, at, payload, origin)
        origin += 1
        at += SAMPLE


def _oven_schedule(states: pl.DataFrame) -> list[tuple[datetime, datetime]]:
    """When the ovens are lit: 05:00 to 23:00 local on every working day."""
    working_days = sorted(
        {
            row["started_at"].astimezone(PLANT_TZ).date()
            for row in states.iter_rows(named=True)
            if not (
                row["state"] == MachineState.PLANNED_STOP
                and row["reason"] == StopReason.NO_SHIFT
            )
        }
    )
    return [
        (
            datetime.combine(day, OVEN_ON, tzinfo=PLANT_TZ).astimezone(UTC),
            datetime.combine(day, OVEN_OFF, tzinfo=PLANT_TZ).astimezone(UTC),
        )
        for day in working_days
    ]
