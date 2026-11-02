"""Sparkplug B payloads, encoded and decoded (spec §4.3).

A Sparkplug B message is a protobuf `Payload`: a timestamp, a sequence
number, and a list of metrics, each with a name or an alias, a datatype, a
timestamp, and one value field chosen by the datatype. Births carry names
and aliases; data messages usually carry aliases only, which is why a
consumer has to keep the last birth to make sense of them.

Only the handful of datatypes Crestline's laser uses are mapped here. The
generated `sparkplug_b_pb2` module is the whole schema; this module is the
thin layer that keeps the rest of the code from touching protobuf directly.
"""

from dataclasses import dataclass
from typing import Any

from . import sparkplug_b_pb2 as pb

# Sparkplug B `DataType` values (the enum in sparkplug_b.proto).
INT32 = 3
INT64 = 4
UINT64 = 8
FLOAT = 9
BOOLEAN = 11
STRING = 12

# Which protobuf value field each datatype travels in.
VALUE_FIELD = {
    INT32: "int_value",
    INT64: "long_value",
    UINT64: "long_value",
    FLOAT: "float_value",
    BOOLEAN: "boolean_value",
    STRING: "string_value",
}


@dataclass(frozen=True, slots=True)
class Metric:
    """One metric in a payload. `name` or `alias` may be None, not both."""

    datatype: int
    value: Any
    timestamp_ms: int
    name: str | None = None
    alias: int | None = None
    is_historical: bool = False

    def __post_init__(self) -> None:
        if self.name is None and self.alias is None:
            raise ValueError("a metric needs a name or an alias")
        if self.datatype not in VALUE_FIELD:
            raise ValueError(f"unsupported Sparkplug datatype {self.datatype}")


def encode_payload(timestamp_ms: int, seq: int | None, metrics: list[Metric]) -> bytes:
    """Serialize a payload. `seq` is None for an NDEATH, which carries none."""
    payload = pb.Payload()
    payload.timestamp = timestamp_ms
    if seq is not None:
        payload.seq = seq
    for metric in metrics:
        m = payload.metrics.add()
        if metric.name is not None:
            m.name = metric.name
        if metric.alias is not None:
            m.alias = metric.alias
        m.datatype = metric.datatype
        m.timestamp = metric.timestamp_ms
        if metric.is_historical:
            m.is_historical = True
        setattr(m, VALUE_FIELD[metric.datatype], metric.value)
    return payload.SerializeToString()


def decode_payload(data: bytes) -> dict[str, Any]:
    """Parse a payload into plain Python: the wire's field names, snake_case.

    Each metric is a dict with whichever of `name` and `alias` it carried,
    `datatype`, `timestamp`, `is_historical` (only when set), and its value
    under the field it travelled in (`long_value`, `string_value`, ...), the
    way protobuf's own JSON mapping presents it, but with Python numbers.
    """
    payload = pb.Payload()
    payload.ParseFromString(data)
    out: dict[str, Any] = {"timestamp": payload.timestamp}
    if payload.HasField("seq"):
        out["seq"] = payload.seq
    metrics = []
    for m in payload.metrics:
        metric: dict[str, Any] = {}
        if m.HasField("name"):
            metric["name"] = m.name
        if m.HasField("alias"):
            metric["alias"] = m.alias
        metric["datatype"] = m.datatype
        metric["timestamp"] = m.timestamp
        if m.is_historical:
            metric["is_historical"] = True
        field = m.WhichOneof("value")
        if field is not None:
            metric[field] = getattr(m, field)
        metrics.append(metric)
    out["metrics"] = metrics
    return out
