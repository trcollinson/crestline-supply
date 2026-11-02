"""LSR-01 over EDGE-LASER: Sparkplug B, with every transport mess off."""

import base64
import json
from bisect import bisect_right
from datetime import UTC, datetime, timedelta
from itertools import pairwise

import polars as pl
import pytest

from crestline.mess import MessConfig
from crestline.sim.clock import PLANT_TZ
from crestline.sim.records import DowntimeReason, MachineState
from crestline.sim.systems import machines
from crestline.sim.systems.machines import laser
from crestline_machines import sparkplug as spb

SEED = 42
M1 = MessConfig(mess_level=0).machines.model_copy(update={"counter_rollover_bits": 16})


def kind(message) -> str:
    return message.topic.split("/")[2]


def metric(message, alias: int):
    return next(m.value for m in message.payload.metrics if m.alias == alias)


def carrying(stream, alias: int):
    return [
        m
        for m in stream
        if kind(m) in ("DBIRTH", "DDATA")
        and any(x.alias == alias for x in m.payload.metrics)
    ]


@pytest.fixture(scope="module")
def stream(plant_run, reference_data) -> list[machines.Message]:
    clock = machines.clocks(M1, SEED, machines.machine_window(plant_run.truth))[
        "LSR-01"
    ]
    _, messages = machines.laser_stream(plant_run.truth, reference_data, clock, SEED)
    return list(messages)


@pytest.fixture(scope="module")
def sent(plant_run, reference_data) -> list[machines.Sent]:
    return list(
        machines.machine_streams(plant_run.truth, reference_data, M1, SEED, ("lsr01",))[
            "lsr01"
        ]
    )


def truth_states(plant_run) -> pl.DataFrame:
    return plant_run.truth["truth_machine_states"].filter(
        pl.col("machine_id") == "LSR-01"
    )


def test_node_topics_carry_no_device_and_device_topics_do(stream):
    topics = {kind(m): m.topic for m in stream}

    assert topics["NBIRTH"] == "spBv1.0/Crestline/NBIRTH/EDGE-LASER"
    assert topics["DBIRTH"] == "spBv1.0/Crestline/DBIRTH/EDGE-LASER/LSR-01"
    assert topics["DDATA"] == "spBv1.0/Crestline/DDATA/EDGE-LASER/LSR-01"
    assert topics["DDEATH"] == "spBv1.0/Crestline/DDEATH/EDGE-LASER/LSR-01"
    assert "NDEATH" not in topics  # only an outage kills the node


def test_the_laser_is_switched_on_before_first_shift_and_off_after_second(
    plant_run, stream
):
    births = [m.at.astimezone(PLANT_TZ) for m in stream if kind(m) == "DBIRTH"]
    deaths = [m.at.astimezone(PLANT_TZ) for m in stream if kind(m) == "DDEATH"]

    assert (
        kind(stream[0]) == "NBIRTH"
        and stream[0].at == machines.machine_window(plant_run.truth)[0]
    )
    assert len(births) == len(deaths) == 10  # the working days of the two-week fixture
    assert all(b.time() == laser.POWER_ON and b.weekday() < 5 for b in births)
    assert all(d.time() == laser.POWER_OFF and d.weekday() < 5 for d in deaths)
    assert all(d > b for b, d in zip(births, deaths))


def test_births_name_every_metric_and_data_carries_aliases_only(stream):
    births = [m for m in stream if kind(m) == "DBIRTH"]
    data = [m for m in stream if kind(m) == "DDATA"]

    for birth in births:
        assert [(m.alias, m.name, m.datatype) for m in birth.payload.metrics] == [
            (alias, name, datatype) for alias, (name, datatype) in laser.METRICS.items()
        ]
    assert all(
        m.name is None and m.alias in laser.METRICS
        for d in data
        for m in d.payload.metrics
    )
    assert all(
        m.seq is None for m in [d.payload for d in data]
    )  # the gateway stamps seq


def test_part_count_climbs_by_one_per_piece(plant_run, stream):
    counts = [metric(m, laser.PART_COUNT) for m in carrying(stream, laser.PART_COUNT)]
    pieces = (
        plant_run.truth["truth_cycles"].filter(pl.col("machine_id") == "LSR-01").height
    )

    assert counts == sorted(counts)
    assert counts[-1] - counts[0] == pieces
    assert all(later - earlier <= 1 for earlier, later in pairwise(counts))


def test_state_and_alarms_follow_what_the_laser_is_really_doing(plant_run, stream):
    times = [m.at for m in stream]

    def value_at(moment: datetime, alias: int):
        i = bisect_right(times, moment) - 1
        while i >= 0:
            m = stream[i]
            if kind(m) in ("DBIRTH", "DDATA") and any(
                x.alias == alias for x in m.payload.metrics
            ):
                return metric(m, alias)
            i -= 1
        raise AssertionError("no value yet")

    seen = set()
    for row in truth_states(plant_run).iter_rows(named=True):
        local = row["started_at"].astimezone(PLANT_TZ).time()
        if not (laser.POWER_ON <= local < laser.POWER_OFF):
            continue  # the laser is off
        at = row["started_at"]
        if row["state"] == MachineState.DOWN:
            expected = laser.STATE_FOR_DOWNTIME[row["reason"]]
            if row["reason"] in laser.ALARMS:
                codes = {code for code, _ in laser.ALARMS[row["reason"]]}
                assert value_at(at, laser.ALARM_CODE) in codes
                assert value_at(at, laser.ALARM_TEXT) != ""
                seen.add(row["reason"])
            else:
                assert value_at(at, laser.ALARM_CODE) == 0
        else:
            expected = laser.STATE_FOR[row["state"]]
            assert (value_at(at, laser.ALARM_CODE), value_at(at, laser.ALARM_TEXT)) == (
                0,
                "",
            )
        assert value_at(at, laser.STATE) == expected, row
    assert DowntimeReason.JAM in seen or DowntimeReason.MICROSTOP in seen


def test_a_heartbeat_republishes_every_metric_each_minute_while_the_laser_is_on(stream):
    births = [m.at for m in stream if kind(m) == "DBIRTH"]
    deaths = [m.at for m in stream if kind(m) == "DDEATH"]
    heartbeats = [
        m
        for m in stream
        if kind(m) == "DDATA" and len(m.payload.metrics) == len(laser.METRICS)
    ]

    expected = sum(int((d - b) / laser.HEARTBEAT) for b, d in zip(births, deaths))
    assert len(heartbeats) == expected
    for hb in heartbeats:
        birth = max(b for b in births if b <= hb.at)
        assert (hb.at - birth) % laser.HEARTBEAT == timedelta(0)
    # A heartbeat can fall on the instant the laser is switched off, not after.
    assert all(
        not (d < m.at < b) for m in heartbeats for d, b in zip(deaths, births[1:])
    )


def test_the_program_is_loaded_at_setup_and_named_for_part_rev_and_gauge(
    plant_run, reference_data, stream
):
    times = [m.at for m in stream]
    parts = {(p.part_id, p.rev): p for p in reference_data.parts}
    jobs = {
        r["job_num"]: (r["part_id"], r["rev"])
        for r in plant_run.truth["truth_jobs"].iter_rows(named=True)
    }
    setups = truth_states(plant_run).filter(pl.col("state") == MachineState.SETUP)

    assert laser.program_name("BRK-1042", "C", "CRS-14") == "BRK1042C_14GA.nc"
    for setup in setups.iter_rows(named=True):
        i = bisect_right(times, setup["started_at"]) - 1
        program = next(
            metric(stream[j], laser.PROGRAM)
            for j in range(i, -1, -1)
            if kind(stream[j]) in ("DBIRTH", "DDATA")
            and any(m.alias == laser.PROGRAM for m in stream[j].payload.metrics)
        )
        part = parts[jobs[setup["job_num"]]]
        assert program == laser.program_name(part.part_id, part.rev, part.material_id)


def test_sheet_count_and_cut_time(stream):
    sheets = [metric(m, laser.SHEET_COUNT) for m in carrying(stream, laser.SHEET_COUNT)]
    cuts = [
        m
        for m in stream
        if kind(m) == "DDATA"
        and any(x.alias == laser.CUT_TIME for x in m.payload.metrics)
    ]

    assert sheets == sorted(sheets) and sheets[-1] > sheets[0]
    assert all(
        0 < metric(m, laser.CUT_TIME) < 600 for m in cuts if len(m.payload.metrics) < 7
    )


def test_the_gateway_stamps_seq_from_zero_at_the_birth_and_wraps_at_255(sent):
    assert kind(sent[0]) == "NBIRTH" and sent[0].payload.seq == 0
    seq = 0
    wrapped = False
    for message in sent[1:]:
        if kind(message) == "NDEATH":
            assert message.payload.seq is None
            continue
        assert message.payload.seq == (seq + 1) % 256
        wrapped |= message.payload.seq == 0
        seq = message.payload.seq
    assert wrapped
    assert {kind(m) for m in sent} == {"NBIRTH", "DBIRTH", "DDATA", "DDEATH"}


def test_the_file_holds_base64_protobuf_that_decodes(sent, tmp_path):
    path = tmp_path / "lsr01.jsonl"
    rows = machines.write_stream(iter(sent), path)
    lines = [json.loads(line) for line in path.read_text().splitlines()]

    assert rows == len(lines) == len(sent)
    birth = next(line for line in lines if "/DBIRTH/" in line["topic"])
    decoded = spb.decode_payload(base64.b64decode(birth["payload"]))
    assert [m["name"] for m in decoded["metrics"]] == [
        name for name, _ in laser.METRICS.values()
    ]
    assert decoded["seq"] == 1
    assert decoded["timestamp"] == laser.epoch_ms(
        datetime.fromisoformat(birth["published_at"])
    )
    assert datetime.fromisoformat(birth["published_at"]).tzinfo == UTC
