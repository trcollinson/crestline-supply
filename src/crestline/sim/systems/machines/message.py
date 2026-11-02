"""One message as the broker saw it, and the file it goes into.

Every machine stream is a JSONL file with one line per message: `topic`,
`published_at` (when it left the gateway, by the broker's clock), and
`payload`. The payload is the JSON object the gateway sent (GW01, GW02,
GW03), a base64 string of the protobuf bytes for Sparkplug B (EDGE-LASER),
or, when GW01 corrupted a message, the truncated text exactly as it arrived.

Messages are generated in the order the device produced them, but they
don't leave the gateway in that order: an outage holds a buffer back, a
duplicate goes out a moment later. `write_stream` puts the lines in
`published_at` order without holding a whole file in memory: the gateway
layer promises never to emit a message published before the true time it
is at (`emitted_at`), so everything published before that time can be
written out.
"""

import base64
import heapq
import json
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from crestline_machines.sparkplug import Metric, encode_payload


@dataclass(frozen=True, slots=True)
class SparkplugPayload:
    """A Sparkplug B payload before it is encoded; `seq` is set at publish time."""

    timestamp_ms: int
    seq: int | None
    metrics: tuple[Metric, ...]


Payload = dict[str, object] | SparkplugPayload | str


@dataclass(frozen=True, slots=True)
class Message:
    """What a device produced, at true UTC time `at`; `origin` counts them."""

    topic: str
    at: datetime
    payload: Payload
    origin: int


@dataclass(frozen=True, slots=True)
class Sent:
    """What left the gateway: `copy` is 1 for a duplicate, 0 otherwise."""

    topic: str
    published_at: datetime
    payload: Payload
    origin: int
    copy: int
    emitted_at: datetime  # the gateway's true time when it emitted this


def millis(at: datetime) -> str:
    return at.strftime("%Y-%m-%dT%H:%M:%S.") + f"{at.microsecond // 1000:03d}Z"


def json_text(payload: dict[str, object]) -> str:
    """A JSON gateway's payload as it goes over the wire."""
    return json.dumps(payload, separators=(",", ":"))


def serialize(sent: Sent) -> str:
    payload = sent.payload
    if isinstance(payload, SparkplugPayload):
        raw = encode_payload(payload.timestamp_ms, payload.seq, list(payload.metrics))
        payload = base64.b64encode(raw).decode("ascii")
    line = {
        "topic": sent.topic,
        "published_at": millis(sent.published_at),
        "payload": payload,
    }
    return json.dumps(line, separators=(",", ":"))


def in_publish_order(sent: Iterable[Sent]) -> Iterator[Sent]:
    """The messages in `published_at` order, streamed.

    Holds back only what a later message could still overtake: the gateway
    promises every message it emits is published no earlier than its
    `emitted_at`, and that `emitted_at` never goes backwards.
    """
    heap: list[tuple[datetime, int, Sent]] = []
    last_emitted: datetime | None = None
    for pushed, message in enumerate(sent):
        if last_emitted is not None and message.emitted_at < last_emitted:
            raise ValueError(f"gateway went back in time at {message.emitted_at}")
        if message.published_at < message.emitted_at:
            raise ValueError(f"published before emitted: {message}")
        last_emitted = message.emitted_at
        heapq.heappush(heap, (message.published_at, pushed, message))
        while heap and heap[0][0] < message.emitted_at:
            yield heapq.heappop(heap)[2]
    while heap:
        yield heapq.heappop(heap)[2]


def write_stream(sent: Iterable[Sent], path: Path) -> int:
    """Write the messages in `published_at` order; return how many."""
    rows = 0
    with path.open("w", encoding="utf-8") as f:
        for message in in_publish_order(sent):
            f.write(serialize(message) + "\n")
            rows += 1
    return rows
