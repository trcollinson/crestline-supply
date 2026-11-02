"""What a gateway does to messages in transit (spec §4.3).

A device produces a clean stream of messages; the gateway between it and
the broker is where the transport messes happen, one knob each in
`mess.yaml`:

- **Outages** (`outages_per_week`, `outage_minutes`): the network drops for
  a while. What happens to the messages produced meanwhile depends on the
  gateway's generation. GW01 buffers them and, on reconnect, drains the
  buffer newest-first in a flood. EDGE-LASER (Sparkplug B) buffers too; on
  reconnect it is born again (NBIRTH with a new bdSeq, then DBIRTH) and
  replays the buffer oldest-first flagged historical. GW02 has no buffer:
  the polls are lost. GW03 buffers and replays in order. In every case the
  broker publishes a death for a Sparkplug node once its keep-alive runs
  out.
- **Duplicates** (`duplicate_rate`): a QoS 1 redelivery, the same bytes
  again a moment later (same `seq` for Sparkplug).
- **Malformed JSON** (`malformed_json_rate`, GW01 only): the message is cut
  short in transit and arrives as text that will not parse.

The duplicate and malformed decisions are drawn once per clean message,
whether or not an outage later swallows it, so switching outages on or off
leaves every other decision in place. Each gateway draws its outage
schedule from its own stream, so PB-02 and PEM-01, both behind GW02, lose
the same polls.
"""

from collections.abc import Iterable, Iterator
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from typing import Literal, Protocol

from crestline.mess import MachinesMess
from crestline.rng import make_rng

from .message import Message, Sent, json_text

DUPLICATE_DELAY_SECONDS = (0.2, 2.0)
MALFORMED_CUT = (0.2, 0.9)  # the share of the text that gets through
REBIRTH_SPACING = timedelta(milliseconds=50)


@dataclass(frozen=True, slots=True)
class GatewayProfile:
    name: str
    on_outage: Literal["replay_newest_first", "replay_oldest_first", "lose"]
    flood_ms: int = 0  # spacing of replayed messages after reconnect
    malformed: bool = False  # only GW01 corrupts messages in transit
    keepalive_s: int = 60  # how long the broker waits before publishing a death


GW01 = GatewayProfile("GW01", "replay_newest_first", flood_ms=20, malformed=True)
GW02 = GatewayProfile("GW02", "lose")
GW03 = GatewayProfile("GW03", "replay_oldest_first", flood_ms=5)
EDGE_LASER = GatewayProfile("EDGE-LASER", "replay_oldest_first", flood_ms=10)


class SparkplugSession(Protocol):
    """What the Sparkplug node does at the gateway's session boundaries."""

    def stamp(self, message: Message, historical: bool = False) -> Message: ...
    def is_birth_or_death(self, message: Message) -> bool: ...
    def outage_started(self, at: datetime) -> list[Message]: ...
    def reconnected(self, at: datetime) -> list[Message]: ...


def outage_schedule(
    gateway: str, mess: MachinesMess, seed: int, window: tuple[datetime, datetime]
) -> list[tuple[datetime, datetime]]:
    """Each outage's start and end, from the gateway's own seeded stream."""
    rng = make_rng(seed, stream=f"gateway/{gateway}/outages")
    start, end = window
    per_second = mess.outages_per_week / timedelta(weeks=1).total_seconds()
    if per_second == 0:
        return []
    low, high = mess.outage_minutes
    outages: list[tuple[datetime, datetime]] = []
    at = start
    while True:
        at += timedelta(seconds=rng.expovariate(per_second))
        if at >= end:
            return outages
        until = at + timedelta(minutes=rng.uniform(low, high))
        if not outages or at >= outages[-1][1]:  # one at a time
            outages.append((at, until))


def deliver(
    messages: Iterable[Message],
    profile: GatewayProfile,
    device: str,
    mess: MachinesMess,
    seed: int,
    window: tuple[datetime, datetime],
    session: SparkplugSession | None = None,
) -> Iterator[Sent]:
    """The device's messages as they left the gateway, in emission order.

    `device` names the stream for the per-message draws, so two devices
    behind one gateway draw their own duplicates. The output is not in
    `published_at` order (a replayed buffer comes late); `write_stream`
    sorts it.
    """
    dups = make_rng(seed, stream=f"gateway/{profile.name}/{device}/duplicates")
    bad = make_rng(seed, stream=f"gateway/{profile.name}/{device}/malformed")
    boundaries = [
        (at, kind)
        for start, end in outage_schedule(profile.name, mess, seed, window)
        for at, kind in ((start, "start"), (end, "end"))
    ]
    offline = False
    buffer: list[tuple[Message, float | None, float | None]] = []

    def send(
        message: Message,
        published_at: datetime,
        emitted_at: datetime,
        delay: float | None,
        cut: float | None,
        historical: bool = False,
    ) -> Iterator[Sent]:
        if session is not None:
            message = session.stamp(message, historical)
        payload = message.payload
        if cut is not None and isinstance(payload, dict):
            text = json_text(payload)
            payload = text[: max(1, int(len(text) * cut))]
        sent = Sent(message.topic, published_at, payload, message.origin, 0, emitted_at)
        yield sent
        if delay is not None:
            yield replace(
                sent, published_at=published_at + timedelta(seconds=delay), copy=1
            )

    def cross(at: datetime, kind: str) -> Iterator[Sent]:
        nonlocal offline
        if kind == "start":
            offline = True
            if session is not None:
                for death in session.outage_started(at):
                    yield from send(
                        death,
                        at + timedelta(seconds=profile.keepalive_s),
                        at,
                        None,
                        None,
                    )
            return
        offline = False
        published = at
        if session is not None:
            for birth in session.reconnected(at):
                yield from send(birth, published, at, None, None)
                published += REBIRTH_SPACING
        replay = buffer if profile.on_outage == "replay_oldest_first" else buffer[::-1]
        for message, delay, cut in replay:
            yield from send(message, published, at, delay, cut, historical=True)
            published += timedelta(milliseconds=profile.flood_ms)
        buffer.clear()

    def pass_boundaries(up_to: datetime) -> Iterator[Sent]:
        while boundaries and boundaries[0][0] <= up_to:
            yield from cross(*boundaries.pop(0))

    for message in messages:
        yield from pass_boundaries(message.at)
        delay = (
            dups.uniform(*DUPLICATE_DELAY_SECONDS)
            if dups.random() < mess.duplicate_rate
            else None
        )
        cut = None
        if profile.malformed and bad.random() < mess.malformed_json_rate:
            cut = bad.uniform(*MALFORMED_CUT)
        if offline:
            # Births and deaths that fall inside an outage are never seen by
            # the broker; the rebirth on reconnect says what is alive now.
            if profile.on_outage != "lose" and (
                session is None or not session.is_birth_or_death(message)
            ):
                buffer.append((message, delay, cut))
            continue
        yield from send(message, message.at, message.at, delay, cut)
    yield from pass_boundaries(window[1])
