"""The press brake controllers and their gateways (spec §4.3).

**PB-01** (mid-age) publishes through edge gateway GW01 whenever one of its
controller registers changes, at most once a second:

- `N7:12`: mode, 0 idle, 1 setup, 2 run, 3 fault
- `CYC_CNT_2`: stroke counter, one per piece
- `B3:0/4`: guard door open
- `PRG`: the brake program loaded, e.g. 1042 for program P1042

**PB-02** (legacy) has nothing but a counter. Gateway GW02 polls it every
30 seconds, day and night, and publishes the value with the gateway's own
timestamp. The counter is 16 bits wide, so it rolls over from 65,535 to 0.

Neither brake knows which job is on it. That has to be worked out by lining
machine activity up with MES scans, which is the point of M1's margin query.

Each output line is one message as the broker saw it: topic, publish time,
and payload.
"""

import json
from collections.abc import Iterable, Iterator, Mapping
from datetime import datetime, timedelta
from itertools import groupby
from operator import itemgetter
from pathlib import Path

import polars as pl

from crestline.mess import MachinesMess
from crestline.reference import ReferenceData
from crestline.rng import make_rng

from ..records import DowntimeReason, MachineState, StopReason

PB01_TOPIC = "crestline/p10/gw01/pb01/data"
PB02_TOPIC = "crestline/p10/gw02/pb02/cnt"
PB02_POLL = timedelta(seconds=30)

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

# Where PB-02's counter happens to be when the backfill window opens. PB-02
# makes about 225 pieces a working day, so starting between 56,000 and
# 63,000 means the 16-bit counter rolls over two to nine weeks into a 90-day
# backfill, and the pipeline has to handle it.
PB02_COUNTER_START_RANGE = (56_000, 63_000)
PB01_COUNTER_START_RANGE = (100_000, 900_000)  # 32-bit; it never wraps


def brake_program(part_id: str) -> int:
    """The brake program number for a part: BRK-1042 runs program P1042."""
    return int(part_id.split("-")[1])


def pb01_messages(
    truth: Mapping[str, pl.DataFrame], reference: ReferenceData, seed: int
) -> Iterator[dict[str, object]]:
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
            yield _message(
                PB01_TOPIC,
                at,
                {
                    "ts": _millis(at),
                    "N7:12": mode,
                    "CYC_CNT_2": count,
                    "B3:0/4": door_open,
                    "PRG": program,
                },
            )


def pb02_messages(
    truth: Mapping[str, pl.DataFrame],
    mess: MachinesMess,
    seed: int,
    counter_start: int | None = None,
) -> Iterator[dict[str, object]]:
    rng = make_rng(seed, stream="pb02")
    start = (
        counter_start
        if counter_start is not None
        else rng.randint(*PB02_COUNTER_START_RANGE)
    )
    bits = mess.counter_rollover_bits
    states = truth["truth_machine_states"].filter(pl.col("machine_id") == "PB-02")
    strokes = sorted(
        truth["truth_cycles"].filter(pl.col("machine_id") == "PB-02")["completed_at"]
    )

    poll, window_end = states["started_at"].min(), states["ended_at"].max()
    counted = 0
    while poll < window_end:
        while counted < len(strokes) and strokes[counted] <= poll:
            counted += 1
        cnt = start + counted
        if bits is not None:
            cnt %= 2**bits
        yield _message(
            PB02_TOPIC,
            poll,
            {"dev": "PB02", "cnt": cnt, "gw_ts": poll.strftime("%Y-%m-%dT%H:%M:%SZ")},
        )
        poll += PB02_POLL


def write_brake_streams(
    truth: Mapping[str, pl.DataFrame],
    reference: ReferenceData,
    mess: MachinesMess,
    seed: int,
    out_dir: Path,
) -> dict[Path, int]:
    out_dir.mkdir(parents=True, exist_ok=True)
    return {
        path: _write_jsonl(messages, path)
        for path, messages in [
            (out_dir / "pb01.jsonl", pb01_messages(truth, reference, seed)),
            (out_dir / "pb02.jsonl", pb02_messages(truth, mess, seed)),
        ]
    }


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


def _message(topic: str, at: datetime, payload: dict[str, object]) -> dict[str, object]:
    return {"topic": topic, "published_at": _millis(at), "payload": payload}


def _millis(at: datetime) -> str:
    return at.strftime("%Y-%m-%dT%H:%M:%S.") + f"{at.microsecond // 1000:03d}Z"


def _write_jsonl(messages: Iterable[dict[str, object]], path: Path) -> int:
    rows = 0
    with path.open("w", encoding="utf-8") as f:
        for message in messages:
            f.write(json.dumps(message, separators=(",", ":")) + "\n")
            rows += 1
    return rows
