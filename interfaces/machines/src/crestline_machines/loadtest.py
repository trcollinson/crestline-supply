"""A synthetic gateway for measuring what 1,000 tags cost in AWS (M3 step 9).

Crestline's own machines are about twenty tags, too few to price a real
plant from. This makes a gateway with 1,000 tags, each changing once a
second, and sends them through IoT Core in two shapes at once:

- **Mode A, a message per tag change**: 1,000 small messages a second
  (`{"ts": ..., "tag": "T0001", "v": 401.23}`, about 45 bytes), the way a
  gateway publishes when every change goes out on its own.
- **Mode B, one batch a second**: every tag's value in one message (about
  16 KB), the store-and-forward shape.

Each mode has its own rule and Firehose stream, tagged with its own `layer`,
so the bill splits them even though they run in the same hour. Mode A needs
several connections: IoT Core accepts 100 publishes a second per connection,
so its tags are shared out over `A_CONNECTIONS` of them, all under the one
thing and certificate (`LOADTEST-GW-00`, `-01`, ...).

The values are a seeded random walk, so two runs send the same bytes.
"""

import json
import random
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime

from .devices import LOADTEST_RULES, LOADTEST_TOPICS
from .publisher import Client

TAGS = 1_000
A_CONNECTIONS = 12  # 1,000 a second over 12 is 84 each, under IoT Core's 100
SEED = 42


def tag_name(index: int) -> str:
    return f"T{index + 1:04d}"


def basic_ingest(mode: str, topic: str) -> str:
    return f"$aws/rules/{LOADTEST_RULES[mode]}/{topic}"


class Tags:
    """1,000 values that each wander a little every second."""

    def __init__(self, count: int = TAGS, seed: int = SEED) -> None:
        self.rng = random.Random(seed)
        self.values = [round(self.rng.uniform(0, 500), 2) for _ in range(count)]

    def step(self) -> None:
        self.values = [round(v + self.rng.gauss(0, 0.5), 2) for v in self.values]


def a_message(ts_ms: int, index: int, value: float) -> tuple[str, bytes]:
    topic = LOADTEST_TOPICS["a"].format(tag=tag_name(index))
    payload = {"ts": ts_ms, "tag": tag_name(index), "v": value}
    return basic_ingest("a", topic), json.dumps(payload, separators=(",", ":")).encode()


def b_message(ts_ms: int, values: list[float]) -> tuple[str, bytes]:
    payload = {"ts": ts_ms, "tags": {tag_name(i): v for i, v in enumerate(values)}}
    return (
        basic_ingest("b", LOADTEST_TOPICS["b"]),
        json.dumps(payload, separators=(",", ":")).encode(),
    )


@dataclass
class Sent:
    messages: int = 0
    bytes: int = 0


@dataclass
class LoadtestManifest:
    name: str
    seconds: int
    started_at: str = ""
    finished_at: str = ""
    sent: dict[str, Sent] = field(default_factory=dict)  # mode -> what went out
    acknowledged: dict[str, int] = field(default_factory=dict)  # client -> PUBACKs
    refused: dict[str, int] = field(default_factory=dict)
    disconnects: dict[str, list[str]] = field(default_factory=dict)
    late_seconds: int = 0  # seconds whose messages couldn't all go out on time

    def to_json(self) -> str:
        return json.dumps(
            {
                "name": self.name,
                "seconds": self.seconds,
                "started_at": self.started_at,
                "finished_at": self.finished_at,
                "sent": {mode: vars(s) for mode, s in self.sent.items()},
                "acknowledged": self.acknowledged,
                "refused": self.refused,
                "disconnects": self.disconnects,
                "late_seconds": self.late_seconds,
            },
            indent=2,
        )


def run(
    a_clients: list[Client],
    b_client: Client | None,
    *,
    name: str,
    seconds: int,
    tags: int = TAGS,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
    wall_ms: Callable[[], int] = lambda: int(time.time() * 1000),
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> LoadtestManifest:
    """Send `seconds` seconds of both modes (mode A only if `b_client` is None, and so on)."""
    manifest = LoadtestManifest(name, seconds, now().isoformat())
    manifest.sent = {
        mode: Sent() for mode, on in (("a", a_clients), ("b", b_client)) if on
    }
    values = Tags(tags)
    began = clock()
    for second in range(seconds):
        values.step()
        ts = wall_ms()
        if b_client is not None:
            topic, payload = b_message(ts, values.values)
            b_client.send(topic, payload, [])
            manifest.sent["b"].messages += 1
            manifest.sent["b"].bytes += len(payload)
        for index, value in enumerate(values.values if a_clients else []):
            # Spread the second's changes evenly across it.
            wait = began + second + index / tags - clock()
            if wait > 0:
                sleep(wait)
            topic, payload = a_message(ts, index, value)
            a_clients[index % len(a_clients)].send(topic, payload, [])
            manifest.sent["a"].messages += 1
            manifest.sent["a"].bytes += len(payload)
        if clock() > began + second + 1:
            manifest.late_seconds += 1
        wait = began + second + 1 - clock()
        if wait > 0:
            sleep(wait)
    manifest.finished_at = now().isoformat()
    return manifest


def settle(
    manifest: LoadtestManifest, clients: Mapping[str, Client], timeout: float
) -> None:
    """Wait for every acknowledgement and note each connection's drops."""
    for name, client in clients.items():
        manifest.acknowledged[name], manifest.refused[name] = client.acknowledged(
            timeout
        )
        manifest.disconnects[name] = client.disconnects()
