"""What a gateway does in transit: duplicates, outages, malformed messages."""

import json
from datetime import timedelta
from itertools import pairwise

import polars as pl
import pytest

from crestline.mess import MessConfig
from crestline.sim.systems import machines
from crestline.sim.systems.machines import gateways, laser

SEED = 42
CLEAN = MessConfig(mess_level=0).machines.model_copy(
    update={"counter_rollover_bits": 16}
)


def with_(**changes):
    return CLEAN.model_copy(update=changes)


@pytest.fixture(scope="module")
def window(plant_run):
    return machines.machine_window(plant_run.truth)


@pytest.fixture(scope="module")
def pb02(plant_run, window) -> list[machines.Message]:
    clock = machines.clocks(CLEAN, SEED, window)["GW02"]
    return list(
        machines.polled_counter_messages("PB-02", plant_run.truth, CLEAN, clock, SEED)
    )


@pytest.fixture(scope="module")
def pb01(plant_run, reference_data, window) -> list[machines.Message]:
    clock = machines.clocks(CLEAN, SEED, window)["PB-01"]
    return list(machines.pb01_messages(plant_run.truth, reference_data, clock, SEED))


def deliver(
    messages, profile, device, mess, window, session=None
) -> list[machines.Sent]:
    return list(
        machines.deliver(iter(messages), profile, device, mess, SEED, window, session)
    )


def test_a_duplicate_is_the_same_message_again_a_moment_later(pb02, window):
    sent = deliver(pb02, gateways.GW02, "pb02", with_(duplicate_rate=0.05), window)
    originals = [s for s in sent if s.copy == 0]
    copies = [s for s in sent if s.copy == 1]
    by_origin = {s.origin: s for s in originals}

    assert len(originals) == len(pb02)
    expected = 0.05 * len(pb02)
    assert abs(len(copies) - expected) < 3 * (expected * 0.95) ** 0.5
    for copy in copies:
        original = by_origin[copy.origin]
        assert copy.payload == original.payload and copy.topic == original.topic
        assert (
            timedelta(seconds=0.2)
            <= copy.published_at - original.published_at
            <= timedelta(seconds=2)
        )
    assert not [
        s for s in deliver(pb02, gateways.GW02, "pb02", CLEAN, window) if s.copy
    ]


def test_outages_do_not_move_the_duplicate_draws(pb02, window):
    quiet = deliver(pb02, gateways.GW02, "pb02", with_(duplicate_rate=0.05), window)
    stormy = deliver(
        pb02,
        gateways.GW02,
        "pb02",
        with_(duplicate_rate=0.05, outages_per_week=20),
        window,
    )
    lost = {s.origin for s in quiet} - {s.origin for s in stormy}

    assert lost  # GW02 really lost polls
    assert {s.origin for s in stormy if s.copy} == {
        s.origin for s in quiet if s.copy
    } - lost


def test_gw01_corrupts_a_share_of_its_messages_in_transit(pb01, window):
    sent = deliver(pb01, gateways.GW01, "pb01", with_(malformed_json_rate=0.2), window)
    bad = [s for s in sent if isinstance(s.payload, str)]

    assert 0.1 * len(pb01) < len(bad) < 0.3 * len(pb01)
    for s in bad:
        with pytest.raises(json.JSONDecodeError):
            json.loads(s.payload)
        assert s.payload.startswith('{"ts":')
    assert len({s.origin for s in sent}) == len(
        pb01
    )  # the garbage replaces the message
    assert not [
        s
        for s in deliver(pb01, gateways.GW01, "pb01", CLEAN, window)
        if isinstance(s.payload, str)
    ]


def test_gw02_has_no_buffer_so_an_outage_loses_the_polls(pb02, window):
    mess = with_(outages_per_week=20)
    outages = machines.outage_schedule("GW02", mess, SEED, window)
    sent = deliver(pb02, gateways.GW02, "pb02", mess, window)
    published = {s.origin for s in sent}

    assert outages and all(
        end - start >= timedelta(minutes=2) for start, end in outages
    )
    assert all(
        start < end <= next_start for (start, end), (next_start, _) in pairwise(outages)
    )
    for m in pb02:
        inside = any(start <= m.at < end for start, end in outages)
        assert (m.origin not in published) == inside
    assert machines.outage_schedule("GW02", CLEAN, SEED, window) == []


def test_gw01_buffers_and_drains_newest_first_in_a_flood(pb01, window):
    mess = with_(outages_per_week=30)
    outages = machines.outage_schedule("GW01", mess, SEED, window)
    sent = deliver(pb01, gateways.GW01, "pb01", mess, window)
    by_origin = {s.origin: s for s in sent}

    assert len(sent) == len(pb01)
    replayed = False
    for start, end in outages:
        buffered = [m for m in pb01 if start <= m.at < end]
        if len(buffered) < 2:
            continue
        replayed = True
        published = [by_origin[m.origin].published_at for m in buffered]
        assert all(p >= end for p in published)
        assert published == sorted(published, reverse=True)  # newest first
        assert all(a - b == timedelta(milliseconds=20) for a, b in pairwise(published))
    assert replayed
    assert all(a.emitted_at <= b.emitted_at for a, b in pairwise(sent))
    assert all(s.published_at >= s.emitted_at for s in sent)


def test_gw03_replays_its_buffer_in_order(plant_run, window):
    mess = with_(outages_per_week=30)
    samples = list(machines.pcline_messages(plant_run.truth, SEED))[: 17280 * 3]
    outages = [
        o
        for o in machines.outage_schedule("GW03", mess, SEED, window)
        if o[1] < samples[-1].at
    ]
    sent = deliver(
        samples,
        gateways.GW03,
        "pcline",
        mess,
        (window[0], samples[-1].at + timedelta(seconds=5)),
    )
    by_origin = {s.origin: s for s in sent}

    assert outages and len(sent) == len(samples)
    for start, end in outages:
        published = [
            by_origin[m.origin].published_at for m in samples if start <= m.at < end
        ]
        assert published == sorted(published) and published[0] >= end
        assert all(b - a == timedelta(milliseconds=5) for a, b in pairwise(published))


def test_the_sparkplug_node_dies_is_reborn_and_replays_history(
    plant_run, reference_data, window
):
    mess = with_(outages_per_week=30)
    clock = machines.clocks(CLEAN, SEED, window)["LSR-01"]
    node, messages = machines.laser_stream(plant_run.truth, reference_data, clock, SEED)
    outages = machines.outage_schedule("EDGE-LASER", mess, SEED, window)
    sent = deliver(messages, gateways.EDGE_LASER, "lsr01", mess, window, node)
    kinds = [laser.kind_of(s) for s in sent]

    assert kinds.count("NDEATH") == len(outages)
    assert kinds.count("NBIRTH") == len(outages) + 1
    deaths = [s for s in sent if laser.kind_of(s) == "NDEATH"]
    births = [s for s in sent if laser.kind_of(s) == "NBIRTH"]
    for (start, end), death, birth in zip(outages, deaths, births[1:]):
        assert death.published_at == start + timedelta(seconds=60)
        assert birth.published_at == end and birth.payload.seq == 0
    bd = [next(m.value for m in b.payload.metrics if m.name == "bdSeq") for b in births]
    assert all(later == (earlier + 1) % 256 for earlier, later in pairwise(bd))
    dead_bd = [
        next(m.value for m in d.payload.metrics if m.name == "bdSeq") for d in deaths
    ]
    assert dead_bd == bd[:-1]  # a death names the birth it ends

    historical = [s for s in sent if any(m.is_historical for m in s.payload.metrics)]
    assert historical and all(laser.kind_of(s) == "DDATA" for s in historical)
    assert all(s.published_at >= s.emitted_at for s in sent)
    live_after_rebirth = [
        s
        for s in sent
        if laser.kind_of(s) == "DBIRTH" and s.origin == laser.GATEWAY_ORIGIN
    ]
    assert live_after_rebirth  # reborn while the laser was on
    for start, end in outages:
        inside = [s for s in sent if start <= s.published_at < end]
        assert all(
            laser.kind_of(s) == "NDEATH" for s in inside
        )  # nothing else gets through


@pytest.fixture(scope="module")
def laser_reborn(plant_run, reference_data, window):
    """The laser through many outages, and the same laser's clean stream."""
    mess = with_(outages_per_week=30)
    clock = machines.clocks(CLEAN, SEED, window)["LSR-01"]
    _, clean = machines.laser_stream(plant_run.truth, reference_data, clock, SEED)
    node, messages = machines.laser_stream(plant_run.truth, reference_data, clock, SEED)
    sent = deliver(messages, gateways.EDGE_LASER, "lsr01", mess, window, node)
    rebirths = [
        (birth, nxt)
        for birth, nxt in pairwise(sent)
        if laser.kind_of(birth) == "NBIRTH" and birth.published_at > window[0]
    ]
    return list(clean), rebirths


def test_a_rebirth_says_the_laser_is_on_only_when_it_is(laser_reborn):
    # The stream is built ahead of the gateway: by the time the gateway sees
    # the 05:50 power-on, the node already says the laser is on, and a
    # reconnect at 04:05 used to send a DBIRTH for a laser that was off.
    clean, rebirths = laser_reborn
    power = [
        (m.at, laser.kind_of(m) == "DBIRTH")
        for m in clean
        if laser.kind_of(m) in ("DBIRTH", "DDEATH")
    ]
    assert len(rebirths) > 50
    seen_on = seen_off = False
    for birth, nxt in rebirths:
        on = [is_on for at, is_on in power if at < birth.published_at]
        was_on = bool(on) and on[-1]
        reborn = laser.kind_of(nxt) == "DBIRTH" and nxt.origin == laser.GATEWAY_ORIGIN
        assert reborn == was_on, birth.published_at
        seen_on, seen_off = seen_on or was_on, seen_off or not was_on
    assert seen_on and seen_off


def test_a_rebirth_carries_the_values_of_its_own_instant(laser_reborn):
    # ...and a daytime rebirth used to carry the next piece's PartCount.
    clean, rebirths = laser_reborn

    def part_count(message) -> int | None:
        return next(
            (m.value for m in message.payload.metrics if m.alias == laser.PART_COUNT),
            None,
        )

    counts = [(m.at, part_count(m)) for m in clean if part_count(m) is not None]
    checked = 0
    for _, dbirth in rebirths:
        if laser.kind_of(dbirth) != "DBIRTH" or dbirth.origin != laser.GATEWAY_ORIGIN:
            continue  # reborn while off: the next birth is the laser's own power-on
        before = [count for at, count in counts if at < dbirth.published_at]
        assert part_count(dbirth) == before[-1], dbirth.published_at
        checked += 1
    assert checked > 20


def test_the_writer_orders_by_published_at_and_is_deterministic(pb01, window, tmp_path):
    mess = with_(duplicate_rate=0.05, outages_per_week=30, malformed_json_rate=0.05)
    paths = []
    for run in ("a", "b"):
        path = tmp_path / f"{run}.jsonl"
        machines.write_stream(
            machines.deliver(iter(pb01), gateways.GW01, "pb01", mess, SEED, window),
            path,
        )
        paths.append(path)
    lines = [json.loads(line) for line in paths[0].read_text().splitlines()]
    stamps = [line["published_at"] for line in lines]

    assert stamps == sorted(stamps)
    assert paths[0].read_bytes() == paths[1].read_bytes()
    assert len(lines) > len(pb01)


def test_the_writer_refuses_a_gateway_that_goes_back_in_time(pb02, window, tmp_path):
    sent = deliver(pb02[:3], gateways.GW02, "pb02", CLEAN, window)
    backwards = [sent[1], sent[0]]

    with pytest.raises(ValueError, match="back in time"):
        machines.write_stream(iter(backwards), tmp_path / "x.jsonl")


def test_a_machine_stream_is_the_whole_pipeline(plant_run, reference_data):
    streams = machines.machine_streams(
        plant_run.truth, reference_data, CLEAN, SEED, ("pb02", "pem01")
    )

    pb02 = list(streams["pb02"])
    assert pl.DataFrame({"cnt": [s.payload["cnt"] for s in pb02]}).height == len(pb02)
    assert all(s.copy == 0 for s in pb02)
