"""The Sparkplug B schema package: encode, decode, and the spike's sample."""

from pathlib import Path

import pytest

from crestline_machines import sparkplug as spb

FIXTURE = Path(__file__).parent / "fixtures" / "sparkplug" / "ddata.bin"
AT = 1_782_995_467_412


def test_every_datatype_round_trips():
    metrics = [
        spb.Metric(spb.INT32, 2, AT, name="State", alias=1),
        spb.Metric(spb.UINT64, 184_233, AT, name="PartCount", alias=2),
        spb.Metric(spb.UINT64, 2**64 - 1, AT, alias=3),
        spb.Metric(spb.STRING, "BRK1042C_14GA.nc", AT, alias=4),
        spb.Metric(spb.FLOAT, 12.5, AT, alias=5, is_historical=True),
        spb.Metric(spb.BOOLEAN, False, AT, name="Node Control/Rebirth"),
        spb.Metric(spb.INT64, 200, AT, name="bdSeq"),
    ]

    decoded = spb.decode_payload(spb.encode_payload(AT, 17, metrics))

    assert decoded["timestamp"] == AT
    assert decoded["seq"] == 17
    assert decoded["metrics"] == [
        {"name": "State", "alias": 1, "datatype": 3, "timestamp": AT, "int_value": 2},
        {"name": "PartCount", "alias": 2, "datatype": 8, "timestamp": AT, "long_value": 184_233},
        {"alias": 3, "datatype": 8, "timestamp": AT, "long_value": 2**64 - 1},
        {"alias": 4, "datatype": 12, "timestamp": AT, "string_value": "BRK1042C_14GA.nc"},
        {"alias": 5, "datatype": 9, "timestamp": AT, "is_historical": True, "float_value": 12.5},
        {"name": "Node Control/Rebirth", "datatype": 11, "timestamp": AT, "boolean_value": False},
        {"name": "bdSeq", "datatype": 4, "timestamp": AT, "long_value": 200},
    ]  # fmt: skip


def test_an_ndeath_carries_no_seq():
    death = spb.encode_payload(AT, None, [spb.Metric(spb.INT64, 3, AT, name="bdSeq")])
    data = spb.encode_payload(AT, 0, [spb.Metric(spb.INT64, 3, AT, name="bdSeq")])

    assert "seq" not in spb.decode_payload(death)
    assert spb.decode_payload(data)["seq"] == 0


def test_a_metric_needs_a_name_or_an_alias_and_a_known_datatype():
    with pytest.raises(ValueError, match="name or an alias"):
        spb.Metric(spb.INT32, 1, AT)
    with pytest.raises(ValueError, match="datatype"):
        spb.Metric(16, b"", AT, name="DataSet")


def test_the_spikes_ddata_decodes_to_what_was_published():
    """The 121 bytes M3 step 0 pushed through an IoT rule's decode()."""
    decoded = spb.decode_payload(FIXTURE.read_bytes())

    assert decoded["seq"] == 17
    by_key = {m.get("name", m.get("alias")): m for m in decoded["metrics"]}
    assert by_key["PartCount"]["long_value"] == 184_233
    assert by_key["ProgramName"]["string_value"] == "BRK1042C_14GA.nc"
    assert by_key["State"]["int_value"] == 2
    assert by_key[7]["long_value"] == 18_446_744_073_709_551_000  # alias only, exact
    assert spb.encode_payload(
        decoded["timestamp"],
        decoded["seq"],
        [
            spb.Metric(m["datatype"], m.get("long_value", m.get("string_value", m.get("int_value"))),
                       m["timestamp"], name=m.get("name"), alias=m.get("alias"))
            for m in decoded["metrics"]
        ],
    ) == FIXTURE.read_bytes()  # fmt: skip
