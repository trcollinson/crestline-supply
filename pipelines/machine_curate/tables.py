"""The curated machine tables: one Iceberg table per device (M3 step 8).

Each table holds every message IoT Core landed for its device, typed, with
the columns named the way the source views name them (`device_ts_utc`,
`gateway_ts_utc`, `published_at_utc`, ...), so Athena's sources.sql only
has to choose the replay. Nothing is deduplicated here: a QoS 1
redelivery is a message that arrived, and machines.sql drops it the same
way on DuckDB and Athena.

A row's key, `message_id`, is the SHA-256 of the line Firehose wrote. A
device's topic and publish time don't identify a message (the laser's
heartbeat and a data message share a millisecond hundreds of times in 90
days), but a whole landed line does: it carries the replay, the arrival
time, and the payload. So merging the same file twice inserts nothing new.

Each column is (name, Iceberg type, the Athena expression that reads it),
where `line` is one record as Firehose wrote it. The records are the rule's output:
the JSON gateways' fields flattened beside `topic`, `published_at`,
`replay`, and `received_at_utc` (epoch ms); the laser's `payload_b64` and
`decoded`.
"""

CURATED_DATABASE = "crestline_curated"
RAW_TABLE = "raw_machines_iot"  # in the raw Glue database, over source=machines-iot/


def _text(path: str) -> str:
    return f"json_extract_scalar(line, '{path}')"


def _iso(path: str) -> str:
    """An ISO 8601 UTC text field as a plain timestamp (the clock is in the name)."""
    return (
        f"cast(from_iso8601_timestamp({_text(path)}) at time zone 'UTC' as timestamp)"
    )


def _epoch_ms(path: str) -> str:
    return f"cast(from_unixtime(cast({_text(path)} as bigint) / 1000.0) at time zone 'UTC' as timestamp)"


ENVELOPE = [
    ("message_id", "string", "to_hex(sha256(to_utf8(line)))"),
    ("topic", "string", _text("$.topic")),
    ("published_at_utc", "timestamp", _iso("$.published_at")),
    ("received_at_utc", "timestamp", _epoch_ms("$.received_at_utc")),
    ("replay", "string", _text("$.replay")),
]

COUNTER = [
    ("gateway_ts_utc", "timestamp", _iso("$.gw_ts")),
    ("dev", "string", _text("$.dev")),
    ("cnt", "int", f"cast({_text('$.cnt')} as integer)"),
]

PAYLOAD = {
    # PB-01's register addresses aren't JSON-path identifiers: quote them.
    "pb01": [
        ("device_ts_utc", "timestamp", _iso("$.ts")),
        ("mode", "int", f"cast({_text('$["N7:12"]')} as integer)"),
        ("cyc_cnt", "long", f"cast({_text('$.CYC_CNT_2')} as bigint)"),
        ("guard_open", "boolean", f"cast({_text('$["B3:0/4"]')} as boolean)"),
        ("program", "int", f"cast({_text('$.PRG')} as integer)"),
    ],
    "pb02": COUNTER,
    "pem01": COUNTER,
    "pcline": [
        ("gateway_ts_utc", "timestamp", _epoch_ms("$.t")),
        ("zone1_f", "double", f"cast({_text('$.z1')} as double)"),
        ("zone2_f", "double", f"cast({_text('$.z2')} as double)"),
        ("zone3_f", "double", f"cast({_text('$.z3')} as double)"),
        ("zone4_f", "double", f"cast({_text('$.z4')} as double)"),
        ("line_speed_ft_min", "double", f"cast({_text('$.spd')} as double)"),
        ("hook_cnt", "long", f"cast({_text('$.hook_cnt')} as bigint)"),
    ],
    # The rule's decode() output stays JSON text; the source views unnest it.
    "lsr01": [
        ("payload_b64", "string", _text("$.payload_b64")),
        ("decoded", "string", "json_format(json_extract(line, '$.decoded'))"),
    ],
}

COLUMNS = {device: ENVELOPE + columns for device, columns in PAYLOAD.items()}
TABLES = {device: f"machine_{device}" for device in PAYLOAD}
KEY = "message_id"
