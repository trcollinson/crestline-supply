"""Account for a machine replay, message by message (M3 step 7).

Every message the publisher sent has to be somewhere: delivered to the raw
bucket by its device's rule and Firehose stream, written by the rule's
error action, or dropped by the rules engine as JSON that won't parse
(GW01 corrupts a few in transit), which leaves no trace but the rule's
`ParseError` metric. This counts all three and compares them with the
publisher's manifest, device by device.

A message can also land twice. The publisher sends at QoS 1, which is "at
least once": a message in flight when a gateway's connection drops is sent
again on reconnect, and if the first copy got through, both land (M3 step
10's fresh-clone replay: GW02 and GW03 each dropped once, and 3 PEM-01 polls
and 1 PC-LINE sample arrived twice). A copy is the same line again but for
IoT Core's own `received_at_utc`: the same publish time and payload. The
backfill's files never repeat that pair (their own duplicates are resent
later, so they have a publish time of their own), so a repeat in raw is a
redelivery, and it is counted, not mistaken for a message. A replay is
accounted for only if, for every device, sent = raw - redelivered + errors
+ parse errors exactly, and IoT Core acknowledged every gateway's messages
and refused none. A resent message can be acknowledged twice, so a gateway
needs at least as many acknowledgements as messages, not exactly as many.

Raw lines are counted by the `replay` field the rule copied from the
message's user properties, so replays on the same day don't mix. An error
object doesn't carry it; errors are counted in the replay's own days, which
is exact as long as one replay runs at a time; so are the parse errors, read
from CloudWatch over the replay's own minutes.

This is data-platform code: it never reads the answer key or imports the
simulator.
"""

import hashlib
import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

import boto3

from crestline_machines.devices import (
    DEVICES,
    ERRORS_PREFIX,
    FIREHOSE_ERRORS_PREFIX,
    RAW_SOURCE,
    REPLAY,
)

REPLAYS_DIR = Path("replays")  # under the backfill's output folder
RECEIVED_AT = "received_at_utc"  # the rule's timestamp(): differs between copies


@dataclass(frozen=True)
class DeviceCount:
    device: str
    sent: int
    raw: int
    redelivered: int
    errors: int
    parse_errors: int

    @property
    def accounted(self) -> bool:
        delivered = self.raw - self.redelivered
        return self.sent == delivered + self.errors + self.parse_errors


@dataclass(frozen=True)
class Account:
    replay: str
    devices: list[DeviceCount]
    acknowledged: dict[str, int]
    refused: dict[str, int]
    sent_by_gateway: dict[str, int]

    @property
    def ok(self) -> bool:
        return all(d.accounted for d in self.devices) and all(
            self.acknowledged.get(g, 0) >= n and not self.refused.get(g, 0)
            for g, n in self.sent_by_gateway.items()
        )

    def report(self) -> str:
        lines = [
            f"Replay {self.replay}",
            "  device       sent       raw  redelivered    errors  parse errors   accounted",
        ]
        for d in self.devices:
            lines.append(
                f"  {d.device:<8} {d.sent:>8,} {d.raw:>9,} {d.redelivered:>12,} {d.errors:>9,} "
                f"{d.parse_errors:>13,}   {'yes' if d.accounted else 'NO'}"
            )
        lines.append("  gateway      sent  acknowledged  refused")
        for gateway, sent in self.sent_by_gateway.items():
            lines.append(
                f"  {gateway:<10} {sent:>6,} {self.acknowledged.get(gateway, 0):>13,} "
                f"{self.refused.get(gateway, 0):>8,}"
            )
        lines.append("Accounted for." if self.ok else "NOT accounted for.")
        return "\n".join(lines)


def latest_manifest(out_dir: Path) -> Path:
    manifests = sorted((out_dir / REPLAYS_DIR).glob("*.json"))
    if not manifests:
        raise FileNotFoundError(f"no replay manifests in {out_dir / REPLAYS_DIR}")
    return manifests[-1]


def replay_days(manifest: dict) -> list[date]:
    """The UTC days its messages could have landed: from start to a day after it finished."""
    first = datetime.fromisoformat(manifest["started_at"]).date()
    last = datetime.fromisoformat(manifest["finished_at"]).date() + timedelta(days=1)
    return [first + timedelta(days=n) for n in range((last - first).days + 1)]


def account(manifest_path: Path, bucket: str, s3=None, cloudwatch=None) -> Account:
    s3 = s3 or boto3.client("s3")
    cloudwatch = cloudwatch or boto3.client("cloudwatch")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    name = manifest["replay"]
    days = replay_days(manifest)
    counts = []
    sent_by_gateway: dict[str, int] = {}
    for device in DEVICES:
        sent = manifest["devices"].get(device.name, {}).get("sent", 0)
        sent_by_gateway[device.gateway] = sent_by_gateway.get(device.gateway, 0) + sent
        seen: set[bytes] = set()  # every line of the replay so far, as a digest
        raw = redelivered = 0
        for day in days:
            prefix = f"{RAW_SOURCE}/entity={device.name}/ingest_date={day}/"
            for key in _keys(s3, bucket, prefix):
                for line in _lines_of_replay(s3, bucket, key, name):
                    raw += 1
                    copy = _without_arrival(line)
                    if copy in seen:
                        redelivered += 1
                    seen.add(copy)
        errors = sum(
            len(
                _keys(
                    s3,
                    bucket,
                    f"{ERRORS_PREFIX}/entity={device.name}/ingest_date={day}/",
                )
            )
            for day in days
        ) + sum(
            _lines(s3, bucket, key)
            for day in days
            for key in _keys(
                s3,
                bucket,
                f"{FIREHOSE_ERRORS_PREFIX}/entity={device.name}/ingest_date={day}/",
            )
        )
        parse_errors = _parse_errors(cloudwatch, device.rule, manifest)
        counts.append(
            DeviceCount(device.name, sent, raw, redelivered, errors, parse_errors)
        )
    return Account(
        name, counts, manifest["acknowledged"], manifest["refused"], sent_by_gateway
    )


def _parse_errors(cloudwatch, rule: str, manifest: dict) -> int:
    """Messages the rule's engine dropped as unparseable JSON during the replay."""
    start = datetime.fromisoformat(manifest["started_at"]).replace(
        second=0, microsecond=0
    )
    end = datetime.fromisoformat(manifest["finished_at"]) + timedelta(minutes=5)
    points = cloudwatch.get_metric_statistics(
        Namespace="AWS/IoT",
        MetricName="ParseError",
        Dimensions=[{"Name": "RuleName", "Value": rule}],
        StartTime=start,
        EndTime=end,
        Period=60,
        Statistics=["Sum"],
    )["Datapoints"]
    return int(sum(point["Sum"] for point in points))


def _keys(s3, bucket: str, prefix: str) -> list[str]:
    pages = s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix)
    return [obj["Key"] for page in pages for obj in page.get("Contents", [])]


def _lines(s3, bucket: str, key: str) -> int:
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")
    return sum(1 for line in body.splitlines() if line)


def _lines_of_replay(s3, bucket: str, key: str, name: str) -> list[dict]:
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")
    records = (json.loads(line) for line in body.splitlines() if line)
    return [record for record in records if record.get(REPLAY) == name]


def _without_arrival(record: dict) -> bytes:
    """A line's digest without the time IoT Core received it: equal for copies."""
    fields = {k: v for k, v in record.items() if k != RECEIVED_AT}
    return hashlib.sha256(json.dumps(fields, sort_keys=True).encode()).digest()
