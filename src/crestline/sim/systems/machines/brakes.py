"""PB-01, the mid-age press brake behind edge gateway GW01 (spec §4.3).

GW01 publishes PB-01's controller registers whenever one changes, at most
once a second:

- `N7:12`: mode, 0 idle, 1 setup, 2 run, 3 fault
- `CYC_CNT_2`: stroke counter, one per piece (32 bits; it never wraps)
- `B3:0/4`: guard door open
- `PRG`: the brake program loaded, e.g. 1042 for program P1042

`ts` is PB-01's own clock, which drifts. The brake does not know which job
is on it; that has to be worked out from MES scans (M1's margin query).
"""

from collections.abc import Iterator, Mapping
from itertools import groupby
from operator import itemgetter

import polars as pl

from crestline.reference import ReferenceData
from crestline.rng import make_rng

from ...records import DowntimeReason, MachineState, StopReason
from .clocks import DriftingClock
from .message import Message, millis

PB01_TOPIC = "crestline/p10/gw01/pb01/data"

IDLE, SETUP, RUN, FAULT = 0, 1, 2, 3

# What PB-01's mode register shows during each true machine state. The
# controller reports the mode the operator left the machine in, not whether
# it is making parts, so most stops leave it where it was (not listed here):
# a brake waiting on material, or sitting between jobs, still says "run" and
# simply stops counting. A real fault trips it to 3, a tooling change means
# switching to setup, and it is switched off for maintenance and at the end
# of the day.
MODE_FOR_STATE = {MachineState.SETUP: SETUP, MachineState.RUNNING: RUN}
MODE_FOR_DOWNTIME = {
    DowntimeReason.JAM: FAULT,
    DowntimeReason.PROGRAM_ERROR: FAULT,
    DowntimeReason.TOOLING: SETUP,
    DowntimeReason.MAINTENANCE: IDLE,
}
MODE_FOR_STOP = {StopReason.NO_SHIFT: IDLE}

# The guard door is opened whenever someone has their hands in the machine,
# and the interlock means it is always closed while the brake is running.
DOOR_OPEN_DOWNTIME = {
    DowntimeReason.JAM,
    DowntimeReason.TOOLING,
    DowntimeReason.MAINTENANCE,
}

PB01_COUNTER_START_RANGE = (100_000, 900_000)


def brake_program(part_id: str) -> int:
    """The brake program number for a part: BRK-1042 runs program P1042."""
    return int(part_id.split("-")[1])


def pb01_messages(
    truth: Mapping[str, pl.DataFrame],
    reference: ReferenceData,
    clock: DriftingClock,
    seed: int,
) -> Iterator[Message]:
    rng = make_rng(seed, stream="pb01")
    count = rng.randint(*PB01_COUNTER_START_RANGE)
    brake_parts = sorted(
        p.part_id
        for p in reference.parts
        if any(op.work_center == "BRAKE" for op in p.routing)
    )
    program = brake_program(rng.choice(brake_parts))  # whatever ran last
    part_of_job = dict(truth["truth_jobs"].select("job_num", "part_id").iter_rows())

    states = truth["truth_machine_states"].filter(pl.col("machine_id") == "PB-01")
    strokes = truth["truth_cycles"].filter(pl.col("machine_id") == "PB-01")
    events = sorted(
        [(row["started_at"], "state", row) for row in states.iter_rows(named=True)]
        + [(at, "stroke", None) for at in strokes["completed_at"]],
        key=itemgetter(0),
    )

    mode, door_open = IDLE, False
    loaded: tuple[str, int] | None = None
    last_registers: tuple[int, int, bool, int] | None = None
    origin = 0
    # Everything that changes in the same second goes out as one message.
    for at, same_second in groupby(events, key=itemgetter(0)):
        for _, kind, row in same_second:
            if kind == "stroke":
                count += 1
                continue
            mode = _mode(row["state"], row["reason"], mode)
            door_open = _door_open(row["state"], row["reason"], door_open)
            job_op = (row["job_num"], row["op_seq"])
            if row["job_num"] is not None and job_op != loaded:
                loaded = job_op
                program = brake_program(part_of_job[row["job_num"]])

        registers = (mode, count, door_open, program)
        if registers != last_registers:
            last_registers = registers
            yield Message(
                PB01_TOPIC,
                at,
                {
                    "ts": millis(clock.device_time(at)),
                    "N7:12": mode,
                    "CYC_CNT_2": count,
                    "B3:0/4": door_open,
                    "PRG": program,
                },
                origin,
            )
            origin += 1


def _mode(state: str, reason: str | None, current: int) -> int:
    if state == MachineState.DOWN:
        return MODE_FOR_DOWNTIME.get(reason, current)
    if state == MachineState.PLANNED_STOP:
        return MODE_FOR_STOP.get(reason, current)
    return MODE_FOR_STATE.get(state, current)


def _door_open(state: str, reason: str | None, current: bool) -> bool:
    if state == MachineState.RUNNING:
        return False
    if state == MachineState.SETUP:
        return True
    if state == MachineState.DOWN and reason in DOOR_OPEN_DOWNTIME:
        return True
    return current
