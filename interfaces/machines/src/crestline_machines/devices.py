"""The plant's machine devices and the gateways they reach AWS through (spec §4.3, §9).

One list, read by everything that has to agree on it: the CDK stack (an
IoT thing per gateway, a rule and a Firehose stream per device), the replay
publisher (which certificate sends which file), and the replay accounting
(which raw folder holds which file's messages).

Each device gets its own rule, rather than one per generation, because a
Firehose stream writes one fixed prefix and GW02 carries two devices: PB-02
and PEM-01 have to land under different `entity=` folders.

A gateway publishes through AWS IoT Core's Basic Ingest: to
`$aws/rules/<rule>/<topic>`, which goes straight to the rule and skips the
message broker (and its per-message charge). `topic()` in the rule still
returns the original topic.
"""

from dataclasses import dataclass
from pathlib import Path

SPARKPLUG_NAMESPACE = "spBv1.0/Crestline"


@dataclass(frozen=True, slots=True)
class Device:
    name: str  # the backfill's file stem and the raw folder's entity
    gateway: str  # the IoT thing that publishes it
    topic_filter: str  # the rule's FROM clause
    sparkplug: bool = False  # protobuf, decoded by the rule; otherwise JSON

    @property
    def rule(self) -> str:
        """The IoT rule's name: letters, digits, and underscores only."""
        return f"crestline_{self.name}"

    def basic_ingest_topic(self, topic: str) -> str:
        return f"$aws/rules/{self.rule}/{topic}"


DEVICES = (
    # The node's own topics (NBIRTH, NDEATH) have no device level, so the
    # filter is the whole namespace rather than .../EDGE-LASER/LSR-01.
    Device("lsr01", "EDGE-LASER", f"{SPARKPLUG_NAMESPACE}/#", sparkplug=True),
    Device("pb01", "GW01", "crestline/p10/gw01/pb01/data"),
    Device("pb02", "GW02", "crestline/p10/gw02/pb02/cnt"),
    Device("pem01", "GW02", "crestline/p10/gw02/pem01/cnt"),
    Device("pcline", "GW03", "crestline/p10/gw03/pcline/process"),
)
GATEWAYS = tuple(dict.fromkeys(d.gateway for d in DEVICES))


def policy_name(gateway: str) -> str:
    """The IoT policy a gateway's certificate is attached to."""
    return f"crestline-{gateway}"


BY_NAME = {d.name: d for d in DEVICES}

# Where the replay lands in the raw bucket (spec §9). The backfill's own
# machine files are `source=machines`; the two never share a prefix.
RAW_SOURCE = "source=machines-iot"
ERRORS_PREFIX = f"{RAW_SOURCE}/errors"  # a rule's error action: one object per message
FIREHOSE_ERRORS_PREFIX = (
    f"{RAW_SOURCE}/firehose-errors"  # Firehose's: one line per record
)

# The Sparkplug B schema as a protobuf descriptor set, built from the
# vendored proto with protoc and committed (like the generated Python), and
# where the rule's decode() reads it in the stack's schema bucket.
DESCRIPTOR_FILE = Path(__file__).resolve().parents[2] / "proto" / "sparkplug_b.desc"
DESCRIPTOR_KEY = "descriptors/sparkplug_b.desc"

# What the rules read from each message's MQTT 5 user properties: the
# plant time it left the gateway (the backfill's `published_at`, which
# drift correction needs; `timestamp()` is the replay's own wall clock), and
# the replay it belongs to, so one replay can be counted apart from another.
PUBLISHED_AT = "published_at"
REPLAY = "replay"


# The load test (M3 step 9): one synthetic gateway with a rule per mode.
# Mode A sends one message per tag change; mode B one batch per second.
LOADTEST_GATEWAY = "LOADTEST-GW"
LOADTEST_RULES = {"a": "crestline_loadtest_a", "b": "crestline_loadtest_b"}
LOADTEST_TOPICS = {
    "a": "crestline/loadtest/LOADTEST-GW/tag/{tag}",
    "b": "crestline/loadtest/LOADTEST-GW/batch",
}
LOADTEST_FILTERS = {
    "a": "crestline/loadtest/+/tag/+",
    "b": "crestline/loadtest/+/batch",
}
