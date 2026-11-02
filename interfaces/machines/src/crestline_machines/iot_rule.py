"""What the Sparkplug IoT rule makes of LSR-01's messages, done locally.

In AWS, the IoT rule for EDGE-LASER keeps each payload as base64 and also
decodes it with `decode(*, 'proto', ...)` against the Sparkplug B schema
(spec §9). Locally there is no rule, so this module writes the same record
from the backfill's `lsr01.jsonl`, one line per message:

    {"topic": ..., "published_at": ..., "payload_b64": ..., "decoded": {...}}

`decoded` is protobuf's own JSON mapping (`MessageToDict`), the mapping the
rule uses too: camelCase field names (`longValue`, `isHistorical`), unset
fields left out, and 64-bit integers as strings (`"seq": "17"`). Writing
the same shape on both sides lets the SQL that reads it stay the same.

The rule drops a payload it can't parse without a trace; so does this
(none of LSR-01's are malformed: only GW01 corrupts messages in transit).
"""

import base64
import binascii
import json
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Any

from google.protobuf.json_format import MessageToDict
from google.protobuf.message import DecodeError

from . import sparkplug_b_pb2 as pb


def rule_records(lines: Iterable[str]) -> Iterator[dict[str, Any]]:
    """Each machine-file line as the IoT rule would land it in S3."""
    for line in lines:
        message = json.loads(line)
        payload_b64 = message["payload"]
        if not isinstance(payload_b64, str):
            continue
        payload = pb.Payload()
        try:
            payload.ParseFromString(base64.b64decode(payload_b64, validate=True))
        except (binascii.Error, DecodeError):
            continue
        yield {
            "topic": message["topic"],
            "published_at": message["published_at"],
            "payload_b64": payload_b64,
            "decoded": MessageToDict(payload),
        }


def write_rule_records(source: Path, dest: Path) -> int:
    """Decode `source` into `dest`, unless `dest` is already newer; returns lines written."""
    if dest.exists() and dest.stat().st_mtime >= source.stat().st_mtime:
        return 0
    dest.parent.mkdir(parents=True, exist_ok=True)
    partial = dest.with_suffix(".partial")
    count = 0
    with (
        source.open(encoding="utf-8") as lines,
        partial.open("w", encoding="utf-8") as out,
    ):
        for record in rule_records(lines):
            out.write(json.dumps(record, separators=(",", ":")) + "\n")
            count += 1
    partial.replace(dest)
    return count
