"""The replay publisher: the backfill's machine files, sent to AWS IoT Core (spec §9).

Plays a stretch of plant time back through the gateways the way they first
sent it: each message in `published_at` order, from its own gateway's
connection and certificate, `--speed` times faster than it happened. It is
deliberately dumb. Everything a gateway did in transit (drift, duplicates,
outages and their late replays, corrupted messages) is already in the files,
so the publisher decides nothing: it sends every line it is given, once,
byte for byte as it left the gateway.

- **Payloads**: a JSON gateway's object goes out as the compact text it
  sent; a message GW01 corrupted goes out as the broken text that arrived;
  a Sparkplug payload goes out as its protobuf bytes.
- **MQTT 5, QoS 1**: every message is acknowledged, and an
  acknowledgement's reason code says whether IoT Core took it. Two user
  properties ride along: `published_at` (the plant time it left the
  gateway, for drift correction) and `replay` (this run's name).
- **Basic Ingest**: each message goes to `$aws/rules/<rule>/<topic>`.
- **No more than `max_rate` a second per connection.** IoT Core allows 100;
  GW03's replay after an outage is 5 ms apart, so a flood slows down rather
  than being refused. The schedule slips behind while it drains.

`replay()` takes the gateways' clients as arguments, so it runs the same
against IoT Core or against a fake in the tests; `connect()` makes the real
ones. Afterwards it returns a manifest: per device, how many were sent and
how many IoT Core acknowledged, which `crestline machines account` compares
with what landed in the raw bucket.
"""

import base64
import heapq
import json
import math
import ssl
import threading
import time
from collections.abc import Callable, Iterable, Iterator, Mapping
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from datetime import time as clock_time
from pathlib import Path
from typing import Protocol
from zoneinfo import ZoneInfo

import certifi

from .devices import BY_NAME, DEVICES, PUBLISHED_AT, REPLAY, Device

PLANT_TZ = ZoneInfo("America/Denver")
MAX_RATE = 90.0  # messages a second per connection; IoT Core's limit is 100
ACK_TIMEOUT = 30.0
RECONNECT_TIMEOUT = 300.0  # how long a send waits for a dropped connection to return


@dataclass(frozen=True, slots=True)
class Outgoing:
    device: Device
    topic: str
    published_at: str  # exactly as the file has it
    payload: bytes

    @property
    def at(self) -> datetime:
        return datetime.fromisoformat(self.published_at)


def plant_days(first: date, last: date) -> tuple[datetime, datetime]:
    """Plant-local days `first` to `last` inclusive, as a UTC [start, end)."""
    start = datetime.combine(first, clock_time(), tzinfo=PLANT_TZ)
    end = datetime.combine(last + timedelta(days=1), clock_time(), tzinfo=PLANT_TZ)
    return start.astimezone(UTC), end.astimezone(UTC)


def payload_bytes(device: Device, payload: object) -> bytes:
    """A file's payload as the bytes the gateway sent."""
    if isinstance(payload, dict):
        return json.dumps(payload, separators=(",", ":")).encode()
    if not isinstance(payload, str):
        raise TypeError(f"{device.name}: unexpected payload {payload!r}")
    if device.sparkplug:
        return base64.b64decode(payload, validate=True)
    return payload.encode()  # a message corrupted in transit: the broken text


def read_device(
    machines_dir: Path, device: Device, start: datetime, end: datetime
) -> Iterator[Outgoing]:
    """One device's messages published in [start, end), in file (publish) order."""
    with (machines_dir / f"{device.name}.jsonl").open(encoding="utf-8") as lines:
        for line in lines:
            row = json.loads(line)
            at = datetime.fromisoformat(row["published_at"])
            if at < start:
                continue
            if at >= end:
                return
            yield Outgoing(
                device,
                row["topic"],
                row["published_at"],
                payload_bytes(device, row["payload"]),
            )


def outgoing(
    machines_dir: Path,
    start: datetime,
    end: datetime,
    devices: Iterable[Device] = DEVICES,
) -> Iterator[Outgoing]:
    """Every device's messages in [start, end), merged in `published_at` order.

    Ties keep the order of the device list, then of each file, so a replay
    of the same window always sends the same sequence.
    """
    streams = [read_device(machines_dir, d, start, end) for d in devices]
    return heapq.merge(*streams, key=lambda m: m.at)


class Client(Protocol):
    """What the publisher needs of an MQTT 5 connection."""

    def send(
        self, topic: str, payload: bytes, properties: list[tuple[str, str]]
    ) -> None: ...
    def acknowledged(self, timeout: float) -> tuple[int, int]: ...
    def disconnects(self) -> list[str]: ...


@dataclass
class Tally:
    sent: int = 0
    first_published_at: str | None = None
    last_published_at: str | None = None


@dataclass
class Manifest:
    replay: str
    start: str
    end: str
    speed: float
    started_at: str = ""
    finished_at: str = ""
    devices: dict[str, Tally] = field(default_factory=dict)
    # Per gateway: PUBACKs that succeeded, PUBACKs with an error code, and
    # every time the connection dropped, with its reason.
    acknowledged: dict[str, int] = field(default_factory=dict)
    refused: dict[str, int] = field(default_factory=dict)
    disconnects: dict[str, list[str]] = field(default_factory=dict)

    def to_json(self) -> str:
        return json.dumps(
            {
                "replay": self.replay,
                "start": self.start,
                "end": self.end,
                "speed": self.speed,
                "started_at": self.started_at,
                "finished_at": self.finished_at,
                "devices": {name: vars(tally) for name, tally in self.devices.items()},
                "acknowledged": self.acknowledged,
                "refused": self.refused,
                "disconnects": self.disconnects,
            },
            indent=2,
        )


def replay_name(now: datetime) -> str:
    return now.astimezone(UTC).strftime("%Y%m%dT%H%M%SZ")


def replay(
    messages: Iterable[Outgoing],
    clients: Mapping[str, Client],
    *,
    name: str,
    start: datetime,
    end: datetime,
    speed: float,
    max_rate: float = MAX_RATE,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> Manifest:
    """Send `messages` at `speed` times the pace they were first published."""
    manifest = Manifest(
        name, start.isoformat(), end.isoformat(), speed, now().isoformat()
    )
    began = clock()
    # A gateway's pool of connections shares the load, so it may send that
    # many times faster.
    spacing = {
        gateway: 1.0 / (max_rate * getattr(client, "connections", 1))
        for gateway, client in clients.items()
    }
    next_free = {gateway: began for gateway in clients}
    for message in messages:
        gateway = message.device.gateway
        due = began + (message.at - start).total_seconds() / speed
        due = max(due, next_free[gateway])
        wait = due - clock()
        if wait > 0:
            sleep(wait)
        clients[gateway].send(
            message.device.basic_ingest_topic(message.topic),
            message.payload,
            [(PUBLISHED_AT, message.published_at), (REPLAY, name)],
        )
        next_free[gateway] = max(clock(), due) + spacing[gateway]
        tally = manifest.devices.setdefault(message.device.name, Tally())
        tally.sent += 1
        tally.first_published_at = tally.first_published_at or message.published_at
        tally.last_published_at = message.published_at
    for gateway, client in clients.items():
        manifest.acknowledged[gateway], manifest.refused[gateway] = client.acknowledged(
            ACK_TIMEOUT
        )
        manifest.disconnects[gateway] = client.disconnects()
    manifest.finished_at = now().isoformat()
    return manifest


# -- the real connections -------------------------------------------------------


class PahoClient:
    """One gateway's MQTT 5 connection to IoT Core, over TLS with its certificate.

    If the connection drops, paho's network thread reconnects on its own, as
    a gateway would, and a message sent meanwhile waits for it to come back.
    Every drop is recorded with its reason. A QoS 1 message in flight at a
    drop is sent again on reconnect, so it may land twice, or not at all if
    the session was lost: the replay accounting finds either.
    """

    def __init__(
        self, gateway: str, endpoint: str, cert_dir: Path, client_id: str | None = None
    ) -> None:
        import paho.mqtt.client as mqtt
        from paho.mqtt.packettypes import PacketTypes
        from paho.mqtt.properties import Properties

        self._properties = lambda: Properties(PacketTypes.PUBLISH)
        self._no_connection = mqtt.MQTT_ERR_NO_CONN
        self._connected = threading.Event()
        self._disconnects: list[str] = []
        self._pending = 0
        self.succeeded = 0
        self.failed = 0
        client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            # The policy lets a gateway connect only as itself (the load test's
            # gateway opens several connections, LOADTEST-GW-00, -01, ...).
            client_id=client_id or gateway,
            protocol=mqtt.MQTTv5,
        )
        client.tls_set(
            ca_certs=certifi.where(),
            certfile=str(cert_dir / "certificate.pem"),
            keyfile=str(cert_dir / "private.key"),
            tls_version=ssl.PROTOCOL_TLS_CLIENT,
        )
        client.max_inflight_messages_set(100)
        client.on_publish = self._on_publish
        connected = []

        def on_connect(c, userdata, flags, reason, props) -> None:
            connected.append(reason)
            if not reason.is_failure:
                self._connected.set()

        def on_disconnect(c, userdata, flags, reason, props) -> None:
            self._connected.clear()
            at = datetime.now(UTC).isoformat(timespec="seconds")
            self._disconnects.append(f"{at} {reason}")

        client.on_connect = on_connect
        client.on_disconnect = on_disconnect
        client.reconnect_delay_set(min_delay=1, max_delay=30)
        client.connect(endpoint, 8883)
        client.loop_start()
        deadline = time.monotonic() + 30
        while not connected and time.monotonic() < deadline:
            time.sleep(0.05)
        if not connected or connected[0].is_failure:
            client.loop_stop()
            raise ConnectionError(
                f"{gateway} could not connect: {connected[:1] or 'timed out'}"
            )
        self._client = client

    def _on_publish(self, client, userdata, mid, reason_code, properties) -> None:
        if reason_code.is_failure:
            self.failed += 1
        else:
            self.succeeded += 1

    def send(
        self, topic: str, payload: bytes, properties: list[tuple[str, str]]
    ) -> None:
        props = self._properties()
        props.UserProperty = properties
        info = self._client.publish(topic, payload, qos=1, properties=props)
        if info.rc == self._no_connection:
            # Dropped: wait for paho to reconnect, then send it again.
            if not self._connected.wait(RECONNECT_TIMEOUT):
                raise ConnectionError(f"no connection for {RECONNECT_TIMEOUT:.0f} s")
            info = self._client.publish(topic, payload, qos=1, properties=props)
        if info.rc != 0:
            raise ConnectionError(f"publish to {topic} failed locally: rc {info.rc}")
        self._pending += 1

    def disconnects(self) -> list[str]:
        return list(self._disconnects)

    def acknowledged(self, timeout: float) -> tuple[int, int]:
        """Wait for every PUBACK; returns (succeeded, refused)."""
        deadline = time.monotonic() + timeout
        while (
            self.succeeded + self.failed < self._pending and time.monotonic() < deadline
        ):
            time.sleep(0.05)
        drops = len(self._disconnects)
        self._client.disconnect()
        self._client.loop_stop()
        del self._disconnects[drops:]  # our own disconnect isn't a drop
        return self.succeeded, self.failed


class Pool:
    """A gateway's connections, taking turns: one, unless a replay needs more.

    IoT Core takes 100 publishes a second per connection. A 90-day replay
    at 2,000x needs about 400 a second from GW03 alone, so the replay opens
    a few connections per gateway (GW03, GW03-1, ...) and deals messages out
    in turn. That changes the order they arrive in, which nothing reads: the
    pipeline orders by published_at.
    """

    def __init__(self, clients: list[Client]) -> None:
        self.clients = clients
        self.connections = len(clients)
        self._turn = 0

    def send(
        self, topic: str, payload: bytes, properties: list[tuple[str, str]]
    ) -> None:
        self.clients[self._turn].send(topic, payload, properties)
        self._turn = (self._turn + 1) % self.connections

    def acknowledged(self, timeout: float) -> tuple[int, int]:
        counts = [c.acknowledged(timeout) for c in self.clients]
        return sum(a for a, _ in counts), sum(r for _, r in counts)

    def disconnects(self) -> list[str]:
        return [
            f"connection {n}: {drop}"
            for n, c in enumerate(self.clients)
            for drop in c.disconnects()
        ]


def connections_needed(
    machines_dir: Path,
    start: datetime,
    end: datetime,
    devices: Iterable[Device],
    speed: float,
    per_connection: float = 75.0,
) -> dict[str, int]:
    """Connections per gateway, so each carries `per_connection` a second at most on average."""
    seconds = (end - start).total_seconds() / speed
    sent: dict[str, int] = {}
    for device in devices:
        count = sum(1 for _ in read_device(machines_dir, device, start, end))
        sent[device.gateway] = sent.get(device.gateway, 0) + count
    return {g: max(1, math.ceil(n / seconds / per_connection)) for g, n in sent.items()}


def connect(
    endpoint: str, certs_dir: Path, connections: Mapping[str, int]
) -> dict[str, Client]:
    """A pool per gateway: GW03, then GW03-1, GW03-2, ... if it needs more."""
    return {
        g: Pool(
            [
                PahoClient(g, endpoint, certs_dir / g, g if n == 0 else f"{g}-{n}")
                for n in range(count)
            ]
        )
        for g, count in connections.items()
    }


def devices_named(names: Iterable[str] | None) -> list[Device]:
    return list(DEVICES) if not names else [BY_NAME[n] for n in names]
