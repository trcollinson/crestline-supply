"""LSR-01, the fiber laser, over edge node EDGE-LASER: Sparkplug B (spec §4.3).

The modern generation. The edge node publishes protobuf payloads under
`spBv1.0/Crestline/...`: an NBIRTH when the node connects (with `bdSeq`, the
birth/death sequence that ties the later NDEATH to it), a DBIRTH when the
laser is switched on (every metric, with its name, alias, and value), then
DDATA whenever a metric changes, carrying only the alias of each metric the
way most edge nodes are configured, so a reader needs the last DBIRTH to
know what alias 2 is. Every message after the NBIRTH gets the next `seq`,
0 to 255 and round again, which is how a reader notices a gap.

Unlike PB-01's controller, which shows the mode the operator left it in,
a modern laser controller reports what it is really doing: idle, setting
up, cutting, in alarm (with a code and a text), or on hold. Its counters
survive a power cycle. `PartCount` climbs by one per piece and `SheetCount`
by one per sheet nested; `CutTime_s` is the last piece's cut time.

The laser is switched off after 2nd shift and on before 1st (DDEATH and
DBIRTH); the edge node itself stays up, so a network outage is the node's
death and rebirth (`gateways.py`), with the data from the outage replayed
afterwards flagged historical. While the laser is on, the node republishes
every metric once a minute as a heartbeat. Timestamps come from the laser's
own clock, which drifts a little.
"""

from collections.abc import Iterator, Mapping
from dataclasses import replace
from datetime import UTC, datetime, time, timedelta
from itertools import groupby
from operator import itemgetter

import polars as pl

from crestline.reference import ReferenceData
from crestline.rng import make_rng
from crestline_machines import sparkplug as spb

from ...clock import PLANT_TZ
from ...records import DowntimeReason, MachineState, StopReason
from .clocks import DriftingClock
from .message import Message, SparkplugPayload

NAMESPACE = "spBv1.0/Crestline"
NODE = "EDGE-LASER"
DEVICE = "LSR-01"
NODE_KINDS = ("NBIRTH", "NDEATH")
BIRTHS_AND_DEATHS = ("NBIRTH", "NDEATH", "DBIRTH", "DDEATH")

POWER_ON = time(5, 50)  # local, on working days
POWER_OFF = time(23, 5)
HEARTBEAT = timedelta(seconds=60)
NEST_UTILIZATION = 0.7  # the share of a sheet that comes out as parts
PART_COUNT_START_RANGE = (1_000_000, 3_000_000)
PIECES_PER_SHEET_AT_START = 8

# The controller's State metric.
IDLE, SETUP, RUN, ALARM, HOLD = 0, 1, 2, 3, 4
STATE_FOR = {
    MachineState.IDLE: IDLE,
    MachineState.SETUP: SETUP,
    MachineState.RUNNING: RUN,
    MachineState.PLANNED_STOP: IDLE,
}
STATE_FOR_DOWNTIME = {
    DowntimeReason.JAM: ALARM,
    DowntimeReason.PROGRAM_ERROR: ALARM,
    DowntimeReason.MICROSTOP: ALARM,
    DowntimeReason.TOOLING: SETUP,  # a nozzle or lens change is done in setup
    DowntimeReason.MAINTENANCE: IDLE,
    DowntimeReason.MATERIAL_WAIT: HOLD,
    DowntimeReason.QUALITY_HOLD: HOLD,
    DowntimeReason.NO_OPERATOR: HOLD,
}
# What the controller says when it is in alarm; one is drawn per event.
ALARMS = {
    DowntimeReason.JAM: [(2101, "HEAD COLLISION"), (2104, "PART TIP-UP")],
    DowntimeReason.PROGRAM_ERROR: [
        (3301, "NC PROGRAM ERROR"),
        (3305, "PROGRAM NOT FOUND"),
    ],
    DowntimeReason.MICROSTOP: [
        (1201, "PIERCE SENSE TIMEOUT"),
        (1207, "ASSIST GAS PRESSURE LOW"),
        (1210, "SAFETY DOOR OPEN"),
    ],
}
NO_ALARM = (0, "")

# alias -> (name, datatype). Aliases are fixed in the edge node's config.
METRICS = {
    1: ("State", spb.INT32),
    2: ("PartCount", spb.UINT64),
    3: ("SheetCount", spb.UINT64),
    4: ("ProgramName", spb.STRING),
    5: ("AlarmCode", spb.INT32),
    6: ("AlarmText", spb.STRING),
    7: ("CutTime_s", spb.FLOAT),
}
STATE, PART_COUNT, SHEET_COUNT, PROGRAM, ALARM_CODE, ALARM_TEXT, CUT_TIME = range(1, 8)

EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
GATEWAY_ORIGIN = -1  # messages the session makes, not the laser


def topic(kind: str) -> str:
    base = f"{NAMESPACE}/{kind}/{NODE}"
    return base if kind in NODE_KINDS else f"{base}/{DEVICE}"


def kind_of(message: Message) -> str:
    return message.topic.split("/")[2]


def program_name(part_id: str, rev: str, material_id: str) -> str:
    """BRK-1042 rev C on CRS-14 runs BRK1042C_14GA.nc."""
    gauge = material_id.split("-")[-1]
    return f"{part_id.replace('-', '')}{rev}_{gauge}GA.nc"


def epoch_ms(at: datetime) -> int:
    return (at - EPOCH) // timedelta(milliseconds=1)


class LaserNode:
    """The edge node's session: bdSeq, seq, the laser's current values.

    `alive` and `values` are what the laser is doing as the stream is built,
    which runs ahead of the gateway: by the time the gateway sees a message,
    the node has already moved to it. A rebirth on reconnect must describe
    the laser as it was at that instant, so the stream calls `settle()`
    before each instant's changes, and the rebirth reads the settled copy.
    """

    def __init__(self, bd_seq: int, clock: DriftingClock) -> None:
        self.bd_seq = bd_seq
        self.seq = 0
        self.alive = False
        self.values: dict[int, object] = {}
        self.clock = clock
        self.settled_alive = False
        self.settled_values: dict[int, object] = {}

    def settle(self) -> None:
        """Remember the laser as it is now, before the next instant's changes."""
        self.settled_alive = self.alive
        self.settled_values = dict(self.values)

    # -- building messages ---------------------------------------------------

    def _ms(self, at: datetime) -> int:
        return epoch_ms(self.clock.device_time(at))

    def _message(
        self, kind: str, at: datetime, metrics: list[spb.Metric], origin: int
    ) -> Message:
        return Message(
            topic(kind),
            at,
            SparkplugPayload(self._ms(at), None, tuple(metrics)),
            origin,
        )

    def nbirth(self, at: datetime, origin: int = GATEWAY_ORIGIN) -> Message:
        ms = self._ms(at)
        return self._message(
            "NBIRTH",
            at,
            [
                spb.Metric(spb.INT64, self.bd_seq, ms, name="bdSeq"),
                spb.Metric(spb.BOOLEAN, False, ms, name="Node Control/Rebirth"),
            ],
            origin,
        )

    def ndeath(self, at: datetime) -> Message:
        return self._message(
            "NDEATH",
            at,
            [spb.Metric(spb.INT64, self.bd_seq, self._ms(at), name="bdSeq")],
            GATEWAY_ORIGIN,
        )

    def dbirth(
        self,
        at: datetime,
        origin: int = GATEWAY_ORIGIN,
        values: dict[int, object] | None = None,
    ) -> Message:
        ms = self._ms(at)
        values = self.values if values is None else values
        return self._message(
            "DBIRTH",
            at,
            [
                spb.Metric(datatype, values[alias], ms, name=name, alias=alias)
                for alias, (name, datatype) in METRICS.items()
            ],
            origin,
        )

    def ddeath(self, at: datetime, origin: int) -> Message:
        return self._message("DDEATH", at, [], origin)

    def ddata(self, at: datetime, aliases: list[int], origin: int) -> Message:
        ms = self._ms(at)
        return self._message(
            "DDATA",
            at,
            [
                spb.Metric(METRICS[alias][1], self.values[alias], ms, alias=alias)
                for alias in aliases
            ],
            origin,
        )

    # -- the gateway's session hooks (gateways.SparkplugSession) ---------------

    def stamp(self, message: Message, historical: bool = False) -> Message:
        """Give a message its `seq` as it is published; a rebirth starts at 0."""
        kind = kind_of(message)
        payload = message.payload
        assert isinstance(payload, SparkplugPayload)
        if kind == "NBIRTH":
            self.seq = 0
            seq: int | None = 0
        elif kind == "NDEATH":
            seq = None
        else:
            self.seq = (self.seq + 1) % 256
            seq = self.seq
        metrics = payload.metrics
        if historical:
            metrics = tuple(replace(m, is_historical=True) for m in metrics)
        return replace(message, payload=replace(payload, seq=seq, metrics=metrics))

    def is_birth_or_death(self, message: Message) -> bool:
        return kind_of(message) in BIRTHS_AND_DEATHS

    def outage_started(self, at: datetime) -> list[Message]:
        return [self.ndeath(at)]

    def reconnected(self, at: datetime) -> list[Message]:
        self.bd_seq = (self.bd_seq + 1) % 256
        births = [self.nbirth(at)]
        if self.settled_alive:
            births.append(self.dbirth(at, values=self.settled_values))
        return births


def laser_stream(
    truth: Mapping[str, pl.DataFrame],
    reference: ReferenceData,
    clock: DriftingClock,
    seed: int,
) -> tuple[LaserNode, Iterator[Message]]:
    """The node and its clean message stream, which the gateway consumes."""
    rng = make_rng(seed, stream="lsr01")
    part_count = rng.randint(*PART_COUNT_START_RANGE)
    node = LaserNode(rng.randint(0, 255), clock)
    parts = {(p.part_id, p.rev): p for p in reference.parts}
    sheet_weight = {m.material_id: m.sheet_weight_lb for m in reference.materials}
    loaded = rng.choice(sorted(parts))
    node.values = {
        STATE: IDLE,
        PART_COUNT: part_count,
        SHEET_COUNT: part_count // PIECES_PER_SHEET_AT_START,
        PROGRAM: program_name(*loaded, parts[loaded].material_id),
        ALARM_CODE: NO_ALARM[0],
        ALARM_TEXT: NO_ALARM[1],
        CUT_TIME: 0.0,
    }
    job_part = {
        row["job_num"]: (row["part_id"], row["rev"])
        for row in truth["truth_jobs"]
        .select("job_num", "part_id", "rev")
        .iter_rows(named=True)
    }
    states = truth["truth_machine_states"].filter(pl.col("machine_id") == DEVICE)
    cycles = truth["truth_cycles"].filter(pl.col("machine_id") == DEVICE)
    window_start = states["started_at"].min()
    assert window_start is not None

    def stream() -> Iterator[Message]:
        # Event order within one second: state changes, then pieces, then the
        # power switch (which never coincides with the others anyway).
        events = sorted(
            [
                (row["started_at"], 0, "state", row)
                for row in states.iter_rows(named=True)
            ]
            + [(at, 1, "cycle", None) for at in cycles["completed_at"]]
            + [(at, 2, kind, None) for at, kind in _power_schedule(states)],
            key=itemgetter(0, 1),
        )
        origin = 0
        node.settle()
        yield node.nbirth(window_start, origin)
        origin += 1
        next_heartbeat: datetime | None = None
        last_cut_at = window_start
        on_sheet = 0.0  # weight cut from the sheet in the laser
        nonlocal loaded

        for at, same_second in groupby(events, key=itemgetter(0)):
            # Nothing has changed since the last instant, and the heartbeats
            # before this one change nothing: a reconnect before `at` sees this.
            node.settle()
            while next_heartbeat is not None and next_heartbeat <= at:
                yield node.ddata(next_heartbeat, list(METRICS), origin)
                origin += 1
                next_heartbeat += HEARTBEAT
            changed: set[int] = set()
            for _, _, kind, row in same_second:
                if kind == "on":
                    node.alive = True
                    yield node.dbirth(at, origin)
                    origin += 1
                    next_heartbeat = at + HEARTBEAT
                elif kind == "off":
                    node.alive = False
                    next_heartbeat = None
                    yield node.ddeath(at, origin)
                    origin += 1
                elif kind == "state":
                    changed |= _apply_state(node, row, rng)
                    if row["state"] == MachineState.RUNNING:
                        last_cut_at = at
                    if row["state"] == MachineState.SETUP:
                        part = job_part[row["job_num"]]
                        if part != loaded:
                            loaded = part
                            node.values[PROGRAM] = program_name(
                                *part, parts[part].material_id
                            )
                            node.values[SHEET_COUNT] += (
                                1  # a new program starts on a fresh sheet
                            )
                            on_sheet = 0.0
                            changed |= {PROGRAM, SHEET_COUNT}
                else:  # a piece
                    node.values[PART_COUNT] += 1
                    node.values[CUT_TIME] = round((at - last_cut_at).total_seconds(), 1)
                    last_cut_at = at
                    weight = parts[loaded].weight_lb
                    if (
                        on_sheet + weight
                        > sheet_weight[parts[loaded].material_id] * NEST_UTILIZATION
                    ):
                        node.values[SHEET_COUNT] += 1
                        on_sheet = 0.0
                        changed.add(SHEET_COUNT)
                    on_sheet += weight
                    changed |= {PART_COUNT, CUT_TIME}
            if changed and node.alive:
                yield node.ddata(at, sorted(changed), origin)
                origin += 1

    return node, stream()


def _apply_state(node: LaserNode, row: Mapping[str, object], rng) -> set[int]:
    """Set State, AlarmCode, AlarmText for a truth interval; return what changed."""
    state = row["state"]
    reason = row["reason"]
    if state == MachineState.DOWN:
        new_state = STATE_FOR_DOWNTIME[reason]
        alarm = rng.choice(ALARMS[reason]) if reason in ALARMS else NO_ALARM
    else:
        new_state = STATE_FOR[state]
        alarm = NO_ALARM
    changed = set()
    for alias, value in (
        (STATE, new_state),
        (ALARM_CODE, alarm[0]),
        (ALARM_TEXT, alarm[1]),
    ):
        if node.values[alias] != value:
            node.values[alias] = value
            changed.add(alias)
    return changed


def _power_schedule(states: pl.DataFrame) -> list[tuple[datetime, str]]:
    """Switched on before 1st shift and off after 2nd, on working days."""
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
    schedule = []
    for day in working_days:
        for moment, kind in ((POWER_ON, "on"), (POWER_OFF, "off")):
            schedule.append(
                (datetime.combine(day, moment, tzinfo=PLANT_TZ).astimezone(UTC), kind)
            )
    return schedule
