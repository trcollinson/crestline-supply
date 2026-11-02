"""The replay publisher (crestline_machines.publisher), against a fake IoT Core."""

import base64
import json
from datetime import UTC, date, datetime, timedelta
from itertools import pairwise

import pytest

from crestline_machines import publisher
from crestline_machines import sparkplug as spb
from crestline_machines.devices import BY_NAME

START = datetime(2026, 7, 7, 6, 0, tzinfo=UTC)  # midnight in Denver


class FakeClient:
    def __init__(self, sent: list) -> None:
        self.sent = sent

    def send(self, topic, payload, properties):
        self.sent.append((topic, payload, dict(properties)))

    def acknowledged(self, timeout):
        return len(self.sent), 0

    def disconnects(self):
        return []


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0
        self.sends: list[float] = []

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


def line(topic: str, at: datetime, payload) -> str:
    published = at.isoformat(timespec="milliseconds").replace("+00:00", "Z")
    return json.dumps({"topic": topic, "published_at": published, "payload": payload})


@pytest.fixture
def machines_dir(tmp_path):
    sparkplug = base64.b64encode(
        spb.encode_payload(1, 3, [spb.Metric(spb.UINT64, 7, 1, alias=2)])
    ).decode()
    files = {
        "pb01": [
            line(
                "crestline/p10/gw01/pb01/data",
                START - timedelta(seconds=1),
                {"ts": "early"},
            ),
            line(
                "crestline/p10/gw01/pb01/data",
                START + timedelta(seconds=10),
                {"ts": "a", "N7:12": 2},
            ),
            line(
                "crestline/p10/gw01/pb01/data",
                START + timedelta(seconds=40),
                '{"ts":"2026-07-0',
            ),
        ],
        "pb02": [
            line(
                "crestline/p10/gw02/pb02/cnt",
                START + timedelta(seconds=30),
                {"dev": "PB02", "cnt": 1},
            ),
        ],
        "pem01": [],
        "pcline": [
            line(
                "crestline/p10/gw03/pcline/process", START + timedelta(days=2), {"t": 0}
            ),
        ],
        "lsr01": [
            line(
                "spBv1.0/Crestline/DDATA/EDGE-LASER/LSR-01",
                START + timedelta(seconds=20),
                sparkplug,
            ),
        ],
    }
    for name, lines in files.items():
        (tmp_path / f"{name}.jsonl").write_text("".join(f"{x}\n" for x in lines))
    return tmp_path


def test_plant_days_are_local_days():
    start, end = publisher.plant_days(date(2026, 7, 7), date(2026, 7, 8))
    assert (start, end) == (START, START + timedelta(days=2))
    # The day the clocks fall back has 25 hours.
    start, end = publisher.plant_days(date(2026, 11, 1), date(2026, 11, 1))
    assert end - start == timedelta(hours=25)


def test_messages_come_in_publish_order_within_the_window(machines_dir):
    end = START + timedelta(days=2)
    messages = list(publisher.outgoing(machines_dir, START, end))
    assert [m.device.name for m in messages] == ["pb01", "lsr01", "pb02", "pb01"]
    assert [m.at for m in messages] == sorted(m.at for m in messages)


def test_payloads_go_out_as_the_bytes_the_gateway_sent(machines_dir):
    messages = list(publisher.outgoing(machines_dir, START, START + timedelta(days=1)))
    pb01, laser, _, broken = messages
    assert pb01.payload == b'{"ts":"a","N7:12":2}'  # compact, as GW01 writes it
    assert (
        broken.payload == b'{"ts":"2026-07-0'
    )  # corrupted in transit, sent as it arrived
    decoded = spb.decode_payload(laser.payload)  # protobuf, not base64
    assert decoded["seq"] == 3 and decoded["metrics"][0]["long_value"] == 7
    with pytest.raises(TypeError):
        publisher.payload_bytes(BY_NAME["pb01"], 42)


def test_each_gateway_sends_its_own_devices_through_basic_ingest(machines_dir):
    sent = {g: [] for g in ("EDGE-LASER", "GW01", "GW02", "GW03")}
    clients = {g: FakeClient(s) for g, s in sent.items()}
    clock = FakeClock()
    manifest = publisher.replay(
        publisher.outgoing(machines_dir, START, START + timedelta(days=1)),
        clients,
        name="R1",
        start=START,
        end=START + timedelta(days=1),
        speed=10,
        clock=clock,
        sleep=clock.sleep,
    )
    assert [t for t, _, _ in sent["GW01"]] == [
        "$aws/rules/crestline_pb01/crestline/p10/gw01/pb01/data"
    ] * 2
    assert sent["GW02"][0][0] == "$aws/rules/crestline_pb02/crestline/p10/gw02/pb02/cnt"
    assert sent["EDGE-LASER"][0][0].startswith("$aws/rules/crestline_lsr01/spBv1.0/")
    assert sent["GW03"] == []
    _, _, properties = sent["GW02"][0]
    assert properties == {"published_at": "2026-07-07T06:00:30.000Z", "replay": "R1"}
    assert {n: t.sent for n, t in manifest.devices.items()} == {
        "pb01": 2,
        "lsr01": 1,
        "pb02": 1,
    }
    assert manifest.acknowledged == {"EDGE-LASER": 1, "GW01": 2, "GW02": 1, "GW03": 0}
    # At 10x, the last message (40 s in) goes out 4 s after the start.
    assert clock.now == pytest.approx(4.0)


def test_a_flood_is_held_to_the_connection_s_rate(tmp_path):
    flood = [
        line(
            "crestline/p10/gw03/pcline/process",
            START + timedelta(milliseconds=5 * n),
            {"t": n},
        )
        for n in range(50)
    ]
    (tmp_path / "pcline.jsonl").write_text("".join(f"{x}\n" for x in flood))
    times: list[float] = []
    clock = FakeClock()

    class Timed(FakeClient):
        def send(self, topic, payload, properties):
            times.append(clock.now)

    publisher.replay(
        publisher.outgoing(
            tmp_path, START, START + timedelta(days=1), [BY_NAME["pcline"]]
        ),
        {"GW03": Timed([])},
        name="R2",
        start=START,
        end=START + timedelta(days=1),
        speed=1,
        max_rate=10,
        clock=clock,
        sleep=clock.sleep,
    )
    gaps = [b - a for a, b in pairwise(times)]
    assert len(times) == 50
    assert min(gaps) == pytest.approx(0.1)  # 10 a second, not 200


def test_the_manifest_is_json_with_every_count(machines_dir):
    clock = FakeClock()
    manifest = publisher.replay(
        publisher.outgoing(machines_dir, START, START + timedelta(days=1)),
        {g: FakeClient([]) for g in ("EDGE-LASER", "GW01", "GW02", "GW03")},
        name="R3",
        start=START,
        end=START + timedelta(days=1),
        speed=100,
        clock=clock,
        sleep=clock.sleep,
    )
    data = json.loads(manifest.to_json())
    assert data["replay"] == "R3"
    assert data["devices"]["pb01"] == {
        "sent": 2,
        "first_published_at": "2026-07-07T06:00:10.000Z",
        "last_published_at": "2026-07-07T06:00:40.000Z",
    }
    assert data["refused"] == {"EDGE-LASER": 0, "GW01": 0, "GW02": 0, "GW03": 0}
    assert data["disconnects"] == {"EDGE-LASER": [], "GW01": [], "GW02": [], "GW03": []}


def test_a_pool_takes_turns_and_lets_its_gateway_send_faster(tmp_path):
    flood = [
        line(
            "crestline/p10/gw03/pcline/process",
            START + timedelta(milliseconds=5 * n),
            {"t": n},
        )
        for n in range(40)
    ]
    (tmp_path / "pcline.jsonl").write_text("".join(f"{x}\n" for x in flood))
    connections = [FakeClient([]) for _ in range(4)]
    pool = publisher.Pool(connections)
    clock = FakeClock()
    manifest = publisher.replay(
        publisher.outgoing(
            tmp_path, START, START + timedelta(days=1), [BY_NAME["pcline"]]
        ),
        {"GW03": pool},
        name="R4",
        start=START,
        end=START + timedelta(days=1),
        speed=1,
        max_rate=10,
        clock=clock,
        sleep=clock.sleep,
    )
    assert [len(c.sent) for c in connections] == [10, 10, 10, 10]
    assert clock.now == pytest.approx(39 / 40)  # 40 a second over four connections
    assert manifest.acknowledged == {"GW03": 40}


def test_connections_are_sized_for_the_speed(machines_dir):
    end = START + timedelta(days=1)
    # 2 pb01 messages in a day at 86,400x is 2 a second: one connection does.
    assert publisher.connections_needed(
        machines_dir, START, end, [BY_NAME["pb01"]], 86_400
    ) == {"GW01": 1}
    # Squeezed into a hundredth of a second, that is 200 a second: three connections.
    assert publisher.connections_needed(
        machines_dir, START, end, [BY_NAME["pb01"]], 86_400 * 100
    ) == {"GW01": 3}
