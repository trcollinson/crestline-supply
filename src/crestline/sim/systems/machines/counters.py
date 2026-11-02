"""PB-02 and PEM-01, the legacy machines behind gateway GW02 (spec §4.3).

Each has nothing but a stroke counter. GW02 polls it every 30 seconds, day
and night, and publishes the value with the gateway's own timestamp. The
counter is 16 bits wide, so it rolls over from 65,535 to 0, and it is not
retained through a power cycle: switch the machine off and on and it reads
0 again. The gateway can't tell the two apart; the pipeline has to.

Power cycles happen on a seeded share of working days
(`machines.counter_reset_rate`): mostly when the machine is switched on at
the start of 1st shift, sometimes at a break or meal. Never while running,
so the answer key stays true. Where the counter stands when the window
opens is seeded too, close enough to the top that it wraps before the
first power cycle, so a 90-day backfill always holds both a rollover and a
reset for the pipeline to tell apart.
"""

from bisect import bisect_right
from collections.abc import Iterator, Mapping, Sequence
from datetime import datetime, timedelta

import polars as pl

from crestline.mess import MachinesMess
from crestline.rng import make_rng

from ...clock import PLANT_TZ
from ...records import MachineState, StopReason
from .clocks import DriftingClock
from .message import Message

POLL = timedelta(seconds=30)
GATEWAY_TIMESTAMP = "%Y-%m-%dT%H:%M:%SZ"

# Device name on the wire, topic, and RNG stream per machine.
DEVICES = {
    "PB-02": ("PB02", "crestline/p10/gw02/pb02/cnt", "pb02"),
    "PEM-01": ("PEM01", "crestline/p10/gw02/pem01/cnt", "pem01"),
}

# Where each counter happens to be when the backfill window opens, if no
# power cycle moves it: PB-02 makes about 225 pieces a working day and PEM-01
# about 400, so these wrap a few weeks into a 90-day backfill.
COUNTER_START_RANGE = {"PB-02": (56_000, 63_000), "PEM-01": (45_000, 55_000)}

# Three power cycles in four happen when the machine is switched on in the
# morning; the rest at a break or meal.
MORNING_SHARE = 0.75


def power_cycles(
    machine_id: str, states: pl.DataFrame, rate: float, seed: int
) -> list[datetime]:
    """When the machine was switched off and on, from a seeded draw per working day."""
    rng = make_rng(seed, stream=f"resets/{machine_id}")
    shift_start: dict[object, datetime] = {}
    breaks: dict[object, list[datetime]] = {}
    mine = states.filter(pl.col("machine_id") == machine_id).sort("started_at")
    for row in mine.iter_rows(named=True):
        day = row["started_at"].astimezone(PLANT_TZ).date()
        if row["state"] == MachineState.PLANNED_STOP:
            if row["reason"] in (StopReason.BREAK, StopReason.MEAL):
                breaks.setdefault(day, []).append(row["started_at"])
            continue
        shift_start.setdefault(day, row["started_at"])

    cycles = []
    for day in sorted(shift_start):
        if rng.random() >= rate:
            continue
        if rng.random() < MORNING_SHARE or not breaks.get(day):
            cycles.append(shift_start[day])
        else:
            cycles.append(rng.choice(breaks[day]))
    return cycles


def polled_counter_messages(
    machine_id: str,
    truth: Mapping[str, pl.DataFrame],
    mess: MachinesMess,
    clock: DriftingClock,
    seed: int,
    resets: Sequence[datetime] = (),
    counter_start: int | None = None,
) -> Iterator[Message]:
    dev, topic, stream = DEVICES[machine_id]
    rng = make_rng(seed, stream=stream)
    drawn = rng.randint(*COUNTER_START_RANGE[machine_id])
    bits = mess.counter_rollover_bits
    states = truth["truth_machine_states"].filter(pl.col("machine_id") == machine_id)
    strokes = sorted(
        truth["truth_cycles"].filter(pl.col("machine_id") == machine_id)["completed_at"]
    )
    resets = sorted(resets)

    start = drawn if counter_start is None else counter_start
    if counter_start is None and bits is not None and resets:
        before = bisect_right(strokes, resets[0])
        if before >= 2:
            start = (2**bits - before // 2) % 2**bits

    poll, window_end = states["started_at"].min(), states["ended_at"].max()
    counted, base = 0, start
    next_reset = origin = 0
    while poll < window_end:
        while counted < len(strokes) and strokes[counted] <= poll:
            counted += 1
        while next_reset < len(resets) and resets[next_reset] <= poll:
            # Back to zero at that instant: only the strokes since then count.
            base = -bisect_right(strokes, resets[next_reset])
            next_reset += 1
        cnt = base + counted
        if bits is not None:
            cnt %= 2**bits
        yield Message(
            topic,
            poll,
            {
                "dev": dev,
                "cnt": cnt,
                "gw_ts": clock.device_time(poll).strftime(GATEWAY_TIMESTAMP),
            },
            origin,
        )
        origin += 1
        poll += POLL
