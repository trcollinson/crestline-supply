"""machines.sql on a handful of hand-written gateway messages (M3 step 5).

Each test plants one imperfection the way a gateway delivers it, in the
wire format of spec §4.3, and checks what the views make of it: a QoS 1
redelivery, a drifting clock with a buffer replayed late and newest first,
a corrupted message, a 16-bit rollover next to a power-cycle reset, a
Sparkplug DDATA that names its metrics by alias only, and a heartbeat that
lands on the same instant as a change.
"""

import base64
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb
import pytest
from test_dst import statements_for

from crestline_machines import sparkplug as spb
from crestline_machines.iot_rule import write_rule_records

REPO_ROOT = Path(__file__).resolve().parent.parent
QUERIES = REPO_ROOT / "queries"
SOURCES = [
    "pb01_data",
    "pb02_counts",
    "pem01_counts",
    "pcline_process",
    "lsr01_messages",
    "lsr01_metrics",
]
T0 = datetime(2026, 7, 6, 16, 0, tzinfo=UTC)  # 10:00 in Denver
PB01_DRIFT = timedelta(seconds=100)
LASER_DRIFT = timedelta(seconds=-4)


def at(minutes: float) -> datetime:
    return T0 + timedelta(minutes=minutes)


def iso(moment: datetime) -> str:
    return moment.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def line(topic: str, published: datetime, payload) -> str:
    return json.dumps(
        {"topic": topic, "published_at": iso(published), "payload": payload}
    )


def pb01(true_at: datetime, published: datetime, count: int, mode: int = 2) -> str:
    payload = {
        "ts": iso(true_at + PB01_DRIFT),
        "N7:12": mode,
        "CYC_CNT_2": count,
        "B3:0/4": False,
        "PRG": 1042,
    }
    return line("crestline/p10/gw01/pb01/data", published, payload)


def poll(
    device: str, true_at: datetime, count: int, published: datetime | None = None
) -> str:
    payload = {
        "dev": device.upper(),
        "cnt": count,
        "gw_ts": true_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    return line(f"crestline/p10/gw02/{device}/cnt", published or true_at, payload)


def pcline(true_at: datetime, speed: float, hooks: int) -> str:
    payload = {
        "t": int(true_at.timestamp() * 1000),
        "z1": 400.0,
        "z2": 400.0,
        "z3": 400.0,
        "z4": 400.0,
        "spd": speed,
        "hook_cnt": hooks,
    }
    return line("crestline/p10/gw03/pcline/process", true_at, payload)


def laser(
    kind: str,
    true_at: datetime,
    seq: int | None,
    metrics: list[spb.Metric],
    published: datetime | None = None,
) -> str:
    ms = int((true_at + LASER_DRIFT).timestamp() * 1000)
    topic = f"spBv1.0/Crestline/{kind}/EDGE-LASER" + (
        "" if kind.startswith("N") else "/LSR-01"
    )
    payload = base64.b64encode(spb.encode_payload(ms, seq, metrics)).decode()
    return line(topic, published or true_at, payload)


def metric(
    alias: int,
    value,
    true_at: datetime,
    *,
    named: bool = False,
    historical: bool = False,
) -> spb.Metric:
    name, datatype = {1: ("State", spb.INT32), 2: ("PartCount", spb.UINT64)}[alias]
    ms = int((true_at + LASER_DRIFT).timestamp() * 1000)
    return spb.Metric(
        datatype,
        value,
        ms,
        name=name if named else None,
        alias=alias,
        is_historical=historical,
    )


@pytest.fixture(scope="module")
def con(tmp_path_factory) -> duckdb.DuckDBPyConnection:
    out = tmp_path_factory.mktemp("out")
    machines = out / "machines"
    machines.mkdir()
    files = {
        # PB-01's clock runs 100 s fast. Straight through at 10:00 and 10:01;
        # the 10:01 message delivered twice; a message cut short in transit;
        # then an outage: the strokes at 10:05 and 10:06 are held and
        # flooded out newest first at 10:20; straight through again at 10:21.
        "pb01": [
            pb01(at(0), at(0), 1000),
            pb01(at(1), at(1), 1001),
            pb01(at(1), at(1) + timedelta(seconds=1.5), 1001),
            json.dumps(
                {
                    "topic": "crestline/p10/gw01/pb01/data",
                    "published_at": iso(at(2)),
                    "payload": '{"ts":"2026-07-06T16:0',
                }
            ),
            pb01(at(6), at(20), 1003),
            pb01(at(5), at(20) + timedelta(milliseconds=20), 1002),
            pb01(at(21), at(21), 1004),
        ],
        # PB-02 rolls over: 65,535 then 1 is 2 strokes.
        "pb02": [
            poll("pb02", at(m / 2), c) for m, c in enumerate([65534, 65535, 1, 2])
        ],
        # PEM-01 is power cycled: 31,208 then 4 is 4 strokes, not 34,332. One
        # poll is delivered twice.
        "pem01": [
            poll("pem01", at(m / 2), c) for m, c in enumerate([31207, 31208, 4, 5])
        ]
        + [poll("pem01", at(1.5), 5, published=at(1.5) + timedelta(seconds=0.4))],
        # PC-LINE's conveyor stops for a minute.
        "pcline": [
            pcline(at(m / 12), 0.0 if 12 <= m < 24 else 12.0, 500 + m)
            for m in range(36)
        ],
        "lsr01": [
            laser("NBIRTH", at(0), 0, [spb.Metric(spb.INT64, 7, 0, name="bdSeq")]),
            laser(
                "DBIRTH",
                at(0),
                1,
                [metric(1, 0, at(0), named=True), metric(2, 500, at(0), named=True)],
            ),
            laser("DDATA", at(1), 2, [metric(1, 2, at(1)), metric(2, 501, at(1))]),
            # A heartbeat (every metric, unchanged) and a piece in the same
            # millisecond: the heartbeat went first (seq 3), holding 501.
            laser("DDATA", at(2), 3, [metric(1, 2, at(2)), metric(2, 501, at(2))]),
            laser("DDATA", at(2), 4, [metric(2, 502, at(2))]),
            # Into alarm at 10:03, and the piece at 10:04 waits out an outage:
            # replayed at 10:10, flagged historical.
            laser("DDATA", at(3), 5, [metric(1, 3, at(3))]),
            laser(
                "DDATA",
                at(4),
                6,
                [
                    metric(1, 2, at(4), historical=True),
                    metric(2, 503, at(4), historical=True),
                ],
                published=at(10),
            ),
            laser("DDEATH", at(12), 7, []),
        ],
    }
    for device, lines in files.items():
        (machines / f"{device}.jsonl").write_text("\n".join(lines) + "\n")
    write_rule_records(machines / "lsr01.jsonl", out / "decoded" / "lsr01.jsonl")

    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    con.execute("SET file_search_path = ?", [str(out)])
    for statement in statements_for(QUERIES / "sources.sql", SOURCES):
        con.execute(statement)
    con.execute((QUERIES / "machines.sql").read_text(encoding="utf-8"))
    return con


def naive(moment: datetime) -> datetime:
    return moment.replace(tzinfo=None)


def readings(con, machine: str) -> list[tuple]:
    return con.execute(
        "select occurred_at_utc, counter from machine_readings where machine_id = ? order by occurred_at_utc",
        [machine],
    ).fetchall()


def test_a_redelivered_message_is_kept_once(con):
    counts = dict(
        con.sql(
            "select machine_id, count(*) from machine_messages group by 1"
        ).fetchall()
    )
    assert counts["PB-01"] == 5  # 7 lines, less the copy and the corrupted one
    assert counts["PEM-01"] == 4


def test_a_drifting_clock_is_corrected_and_a_late_buffer_put_back_in_order(con):
    assert readings(con, "PB-01") == [
        (naive(at(0)), 1000),
        (naive(at(1)), 1001),
        (naive(at(5)), 1002),  # flooded at 10:20, newest first, put back at 10:05
        (naive(at(6)), 1003),
        (naive(at(21)), 1004),
    ]
    late = con.sql(
        "select came_straight_through, clock_offset_ms from machine_readings where machine_id = 'PB-01' and counter in (1002, 1003)"
    ).fetchall()
    assert late == [(False, 100_000), (False, 100_000)]


def test_one_stroke_per_count_whatever_order_they_arrived_in(con):
    cycles = con.sql(
        "select cycles from machine_counts where machine_id = 'PB-01' order by counted_at_utc"
    ).fetchall()
    assert [c for (c,) in cycles] == [0, 1, 1, 1, 1]


def test_a_rollover_is_told_from_a_reset(con):
    rows = con.sql(
        "select machine_id, cycles, counted_as from machine_counts where machine_id in ('PB-02', 'PEM-01') order by machine_id, counted_at_utc"
    ).fetchall()
    assert rows == [
        ("PB-02", 0, "count"),
        ("PB-02", 1, "count"),
        ("PB-02", 2, "rollover"),  # 65,535 -> 1
        ("PB-02", 1, "count"),
        ("PEM-01", 0, "count"),
        ("PEM-01", 1, "count"),
        ("PEM-01", 4, "reset"),  # 31,208 -> 4: switched off and on
        ("PEM-01", 1, "count"),
    ]


def test_sparkplug_aliases_are_named_from_the_birth(con):
    # Every laser piece counted once: the heartbeat's repeat of 501 in the same
    # millisecond as 502 is no step back, and the replayed 503 lands at 10:04.
    rows = con.sql(
        "select counted_at_utc, cycles, counted_as from machine_counts where machine_id = 'LSR-01' and cycles > 0 order by 1"
    ).fetchall()
    assert rows == [
        (naive(at(1)), 1, "count"),
        (naive(at(2)), 1, "count"),
        (naive(at(4)), 1, "count"),
    ]


def test_states_become_intervals(con):
    laser = con.sql(
        "select state_code, started_at_utc, ended_at_utc from machine_states where machine_id = 'LSR-01' order by 2"
    ).fetchall()
    assert laser == [
        (0, naive(at(0)), naive(at(1))),
        (2, naive(at(1)), naive(at(3))),
        (
            3,
            naive(at(3)),
            naive(at(4)),
        ),  # in alarm until the replayed message says it ran again
        (2, naive(at(4)), naive(at(12))),
        (-1, naive(at(12)), None),  # switched off
    ]
    line = con.sql(
        "select state_code, started_at_utc, ended_at_utc from machine_states where machine_id = 'PC-LINE' order by 2"
    ).fetchall()
    assert line == [
        (1, naive(at(0)), naive(at(1))),
        (0, naive(at(1)), naive(at(2))),  # conveyor stopped for a minute
        (1, naive(at(2)), None),
    ]


def test_cycles_add_up_by_minute_and_by_plant_day(con):
    rows = con.sql("select * from machine_cycles_by_day").fetchall()
    by_day = {(machine, day.isoformat()): cycles for machine, day, cycles in rows}
    assert by_day == {
        ("LSR-01", "2026-07-06"): 3,
        ("PB-01", "2026-07-06"): 4,
        ("PB-02", "2026-07-06"): 4,
        ("PEM-01", "2026-07-06"): 6,
        ("PC-LINE", "2026-07-06"): 35,
    }
    by_minute = con.sql(
        "select sum(cycles) from machine_cycles_by_minute where machine_id = 'PB-01'"
    ).fetchone()
    assert by_minute == (4,)
