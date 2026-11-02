"""The machines and their gateways: what each generation publishes (spec §4.3).

Four generations of equipment, four ways of getting data out:

- **LSR-01** (modern): Sparkplug B over edge node EDGE-LASER (`laser.py`)
- **PB-01** (mid-age): controller registers as flat JSON via GW01 (`brakes.py`)
- **PB-02 and PEM-01** (legacy): a 16-bit counter polled via GW02 (`counters.py`)
- **PC-LINE** (process): temperatures, speed, and a hook count via GW03 (`powder.py`)

Each device's clean stream is derived from the answer key (machine states
and pieces), stamped by a clock that may drift (`clocks.py`), then passed
through its gateway (`gateways.py`), which is where outages, duplicates,
and malformed messages happen. `message.py` writes what left the gateway
to `out/machines/<device>.jsonl`, one line per message, in publish order.

Every imperfection is a knob in `mess.yaml` on its own random stream, so
turning one on leaves every other file of the backfill byte-identical.
The Sparkplug B schema lives in `interfaces/machines` (`crestline_machines`),
which the pipelines share; the simulator uses it to encode, they to decode.
"""

from collections.abc import Iterator, Mapping
from datetime import datetime
from pathlib import Path

import polars as pl

from crestline.mess import MachinesMess
from crestline.reference import ReferenceData
from crestline.rng import make_rng

from .brakes import FAULT, IDLE, PB01_TOPIC, RUN, SETUP, brake_program, pb01_messages
from .clocks import DriftingClock
from .counters import DEVICES, POLL, polled_counter_messages, power_cycles
from .gateways import EDGE_LASER, GW01, GW02, GW03, deliver, outage_schedule
from .laser import LaserNode, laser_stream
from .message import Message, Sent, SparkplugPayload, in_publish_order, write_stream
from .powder import pcline_messages

PB02_TOPIC = DEVICES["PB-02"][1]
PEM01_TOPIC = DEVICES["PEM-01"][1]
CLOCKS = ("LSR-01", "PB-01", "GW02")

__all__ = [
    "CLOCKS",
    "DEVICES",
    "EDGE_LASER",
    "FAULT",
    "GW01",
    "GW02",
    "GW03",
    "IDLE",
    "PB01_TOPIC",
    "PB02_TOPIC",
    "PEM01_TOPIC",
    "POLL",
    "RUN",
    "SETUP",
    "DriftingClock",
    "LaserNode",
    "Message",
    "Sent",
    "SparkplugPayload",
    "brake_program",
    "clocks",
    "deliver",
    "in_publish_order",
    "laser_stream",
    "machine_streams",
    "machine_window",
    "outage_schedule",
    "pb01_messages",
    "pcline_messages",
    "polled_counter_messages",
    "power_cycles",
    "write_machine_streams",
    "write_stream",
]


def machine_window(truth: Mapping[str, pl.DataFrame]) -> tuple[datetime, datetime]:
    states = truth["truth_machine_states"]
    start, end = states["started_at"].min(), states["ended_at"].max()
    assert start is not None and end is not None
    return start, end


def clocks(
    mess: MachinesMess, seed: int, window: tuple[datetime, datetime]
) -> dict[str, DriftingClock]:
    """One drifting clock per device or gateway that stamps a payload."""
    return {
        name: DriftingClock(
            mess.clock_drift_seconds.seconds(name),
            *window,
            make_rng(seed, stream=f"clock/{name}"),
        )
        for name in CLOCKS
    }


def machine_streams(
    truth: Mapping[str, pl.DataFrame],
    reference: ReferenceData,
    mess: MachinesMess,
    seed: int,
    devices: tuple[str, ...] = ("lsr01", "pb01", "pb02", "pem01", "pcline"),
) -> dict[str, Iterator[Sent]]:
    """Each device's messages as they left its gateway, keyed by file stem."""
    window = machine_window(truth)
    clock = clocks(mess, seed, window)
    states = truth["truth_machine_states"]
    streams: dict[str, Iterator[Sent]] = {}
    for device in devices:
        if device == "lsr01":
            node, messages = laser_stream(truth, reference, clock["LSR-01"], seed)
            streams[device] = deliver(
                messages, EDGE_LASER, device, mess, seed, window, node
            )
        elif device == "pb01":
            messages = pb01_messages(truth, reference, clock["PB-01"], seed)
            streams[device] = deliver(messages, GW01, device, mess, seed, window)
        elif device in ("pb02", "pem01"):
            machine_id = "PB-02" if device == "pb02" else "PEM-01"
            resets = power_cycles(machine_id, states, mess.counter_reset_rate, seed)
            messages = polled_counter_messages(
                machine_id, truth, mess, clock["GW02"], seed, resets
            )
            streams[device] = deliver(messages, GW02, device, mess, seed, window)
        elif device == "pcline":
            streams[device] = deliver(
                pcline_messages(truth, seed), GW03, device, mess, seed, window
            )
        else:
            raise ValueError(f"no such device: {device}")
    return streams


def write_machine_streams(
    truth: Mapping[str, pl.DataFrame],
    reference: ReferenceData,
    mess: MachinesMess,
    seed: int,
    out_dir: Path,
) -> dict[Path, int]:
    out_dir.mkdir(parents=True, exist_ok=True)
    return {
        out_dir / f"{device}.jsonl": write_stream(stream, out_dir / f"{device}.jsonl")
        for device, stream in machine_streams(truth, reference, mess, seed).items()
    }
