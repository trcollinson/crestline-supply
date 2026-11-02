"""The local stand-in for the Sparkplug IoT rule (`crestline_machines.iot_rule`).

It must land what AWS's `decode(*, 'proto', ...)` lands: protobuf's JSON
mapping, with camelCase names, 64-bit integers as strings, and unset fields
left out (spec §9, from the step 0 spike).
"""

import base64
import json
import os
from pathlib import Path

from crestline_machines import sparkplug as spb
from crestline_machines.iot_rule import rule_records, write_rule_records

FIXTURE = Path(__file__).resolve().parent / "fixtures" / "sparkplug" / "ddata.bin"


def machine_line(payload) -> str:
    return json.dumps(
        {
            "topic": "spBv1.0/Crestline/DDATA/EDGE-LASER/LSR-01",
            "published_at": "2026-07-06T16:00:00.000Z",
            "payload": payload,
        }
    )


def test_a_record_carries_the_raw_bytes_and_the_rule_s_json_mapping():
    payload_b64 = base64.b64encode(FIXTURE.read_bytes()).decode()
    [record] = rule_records([machine_line(payload_b64)])

    assert record["topic"] == "spBv1.0/Crestline/DDATA/EDGE-LASER/LSR-01"
    assert record["published_at"] == "2026-07-06T16:00:00.000Z"
    assert record["payload_b64"] == payload_b64
    decoded = record["decoded"]
    assert isinstance(decoded["timestamp"], str)  # uint64: a string, as in the rule
    assert isinstance(decoded["seq"], str)
    snake_case = {"long_value", "int_value", "is_historical"}
    for metric in decoded["metrics"]:
        assert "datatype" in metric
        assert not snake_case & set(metric)  # the rule's names are camelCase


def test_values_travel_in_the_field_their_datatype_names():
    payload = spb.encode_payload(
        1_783_000_000_000,
        5,
        [
            spb.Metric(spb.UINT64, 2_338_250, 1_783_000_000_000, alias=2),
            spb.Metric(spb.INT32, 3, 1_783_000_000_000, alias=1, is_historical=True),
            spb.Metric(spb.STRING, "BRK1042C_14GA.nc", 1_783_000_000_000, alias=4),
        ],
    )
    [record] = rule_records([machine_line(base64.b64encode(payload).decode())])

    part_count, state, program = record["decoded"]["metrics"]
    assert part_count["longValue"] == "2338250"
    assert "isHistorical" not in part_count
    assert state["intValue"] == 3 and state["isHistorical"] is True
    assert program["stringValue"] == "BRK1042C_14GA.nc"
    assert "name" not in part_count  # DDATA by alias only


def test_a_payload_that_will_not_decode_is_dropped_like_the_rule_drops_it():
    lines = [
        machine_line("not base64!"),
        machine_line(base64.b64encode(b"\xff\xff\xff").decode()),
        machine_line({"an": "object"}),
    ]
    assert list(rule_records(lines)) == []


def test_the_decoded_file_is_written_once_and_redone_when_its_source_changes(tmp_path):
    source = tmp_path / "lsr01.jsonl"
    dest = tmp_path / "decoded" / "lsr01.jsonl"
    source.write_text(
        machine_line(base64.b64encode(FIXTURE.read_bytes()).decode()) + "\n"
    )

    assert write_rule_records(source, dest) == 1
    assert write_rule_records(source, dest) == 0  # already up to date

    later = dest.stat().st_mtime + 10
    os.utime(source, (later, later))
    assert write_rule_records(source, dest) == 1
    assert not dest.with_suffix(".partial").exists()
