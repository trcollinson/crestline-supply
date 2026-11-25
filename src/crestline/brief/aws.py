"""The morning brief in Lambda (M5 step 6), started at 05:30 plant time.

One run: work out which morning it is, get that morning's facts from Athena
(the brief layer, `crestline_brief`), write the brief with the model through
the number check (write.py, the same code the CLI runs), put the Markdown,
an HTML page, and the log in the brief bucket, and email the text through
SNS.

**Which morning.** The data ends on Sep 30, so the real date has nothing to
report. The stack replays the plant instead, the way the ERP API replays its
history: `CRESTLINE_BRIEF_CLOCK_START` is the plant morning the brief writes
on the weekday `CRESTLINE_BRIEF_CLOCK_ANCHOR`, and each weekday after it is
the next plant morning, until the data's last (`CRESTLINE_BRIEF_LAST_MORNING`),
where it stays. With no clock, every run writes the last morning. An event
can name the morning itself: `{"morning": "2026-08-20"}`.

**Earlier mornings' facts** (the ledger's "new" or "open since") are kept in
the bucket, `brief/facts/<morning>.json`, one file per morning, written the
first time a morning is computed and read ever after. A cold store would
compute up to 20 earlier mornings at about a minute each, past Lambda's
limit, so `crestline brief seed` fills it from a local run of the same SQL
(step 3 showed DuckDB's facts equal Athena's, cell for cell).

**Proving the fallback.** `{"plant_orphan": true}` appends a sentence with a
number no fact holds to the model's reply before the check; the template
must go out, and the log says why.

Data-platform code: it never reads the answer key or imports the simulator.
"""

import os
import tempfile
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

from crestline.athena import Athena
from crestline.brief import delivery
from crestline.brief.facts import athena_facts
from crestline.brief.morning import MorningFacts
from crestline.brief.write import write

FACTS_PREFIX = "brief/facts/"
PLANTED_ORPHAN = (
    "Planted to test the fallback: LSR-01 scrapped 99.9% of its pieces. "
    "[scrap:LSR-01:scrap_pct]"
)


@dataclass(frozen=True)
class Settings:
    bucket: str
    topic_arn: str
    workgroup: str
    raw_database: str
    brief_database: str
    model: str
    last_morning: date
    clock_start: date | None
    clock_anchor: date | None

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> "Settings":
        env = os.environ if env is None else env

        def day(name: str) -> date | None:
            return date.fromisoformat(env[name]) if env.get(name) else None

        return cls(
            bucket=env["CRESTLINE_BRIEF_BUCKET"],
            topic_arn=env["CRESTLINE_BRIEF_TOPIC_ARN"],
            workgroup=env["CRESTLINE_ATHENA_WORKGROUP"],
            raw_database=env["CRESTLINE_RAW_DATABASE"],
            brief_database=env["CRESTLINE_BRIEF_DATABASE"],
            model=env.get("CRESTLINE_BRIEF_MODEL", "sonnet"),
            last_morning=day("CRESTLINE_BRIEF_LAST_MORNING") or date(2026, 10, 1),
            clock_start=day("CRESTLINE_BRIEF_CLOCK_START"),
            clock_anchor=day("CRESTLINE_BRIEF_CLOCK_ANCHOR"),
        )


def plant_morning(today: date, s: Settings) -> date:
    """The plant morning the replay is on, for a real date.

    The clock moves one plant weekday for each real weekday from the anchor
    on (a weekend moves neither), and stops at the last morning with data.
    """
    if s.clock_start is None or s.clock_anchor is None:
        return s.last_morning
    weekdays = sum(
        1
        for n in range(max((today - s.clock_anchor).days, 0))
        if (s.clock_anchor + timedelta(days=n)).weekday() < 5
    )
    morning = s.clock_start
    for _ in range(weekdays):
        morning += timedelta(days=3 if morning.weekday() == 4 else 1)
    return min(morning, s.last_morning)


def s3_cached(
    compute: Callable[[date], MorningFacts], s3, bucket: str
) -> Callable[[date], MorningFacts]:
    """`compute`, keeping each morning's facts in the bucket (morning.cached's twin)."""

    def facts_for(morning: date) -> MorningFacts:
        key = f"{FACTS_PREFIX}{morning.isoformat()}.json"
        try:
            body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        except ClientError as error:
            if error.response["Error"]["Code"] not in ("NoSuchKey", "404"):
                raise
            found = compute(morning)
            s3.put_object(
                Bucket=bucket,
                Key=key,
                Body=found.to_json().encode(),
                ContentType="application/json",
            )
            return found
        return MorningFacts.from_json(body.decode("utf-8"))

    return facts_for


def run(
    event: dict,
    settings: Settings,
    s3,
    sns,
    compute: Callable[[date], MorningFacts],
    bedrock=None,
    today: date | None = None,
) -> dict:
    """One brief, delivered; what went out and why."""
    # At 05:30 in Denver it is 11:30 or 12:30 UTC: the same calendar date.
    today = today or datetime.now(UTC).date()
    morning = (
        date.fromisoformat(event["morning"])
        if event.get("morning")
        else plant_morning(today, settings)
    )
    model = event.get("model", settings.model)
    facts_for = s3_cached(compute, s3, settings.bucket)
    with tempfile.TemporaryDirectory() as scratch:
        written = write(
            morning,
            facts_for,
            Path(scratch),
            model=None if model == "none" else model,
            bedrock=bedrock,
            planted=PLANTED_ORPHAN if event.get("plant_orphan") else None,
        )
        stem = f"brief/{morning.isoformat()}"
        title = f"Crestline morning brief, {morning:%A} {morning.isoformat()}"
        s3.put_object(
            Bucket=settings.bucket,
            Key=f"{stem}.md",
            Body=written.text.encode(),
            ContentType="text/markdown; charset=utf-8",
        )
        s3.put_object(
            Bucket=settings.bucket,
            Key=f"{stem}.html",
            Body=delivery.page(written.text, title).encode(),
            ContentType="text/html; charset=utf-8",
        )
        log_key = f"brief/logs/{written.log.name}"
        s3.put_object(
            Bucket=settings.bucket,
            Key=log_key,
            Body=written.log.read_bytes(),
            ContentType="application/x-ndjson",
        )
    cited_copy = f"s3://{settings.bucket}/{stem}.html"
    sns.publish(
        TopicArn=settings.topic_arn,
        Subject=title,
        Message=delivery.plain_text(written.text, cited_copy),
    )
    return {
        "today": today.isoformat(),
        "morning": morning.isoformat(),
        "version": written.version,
        "reason": written.reason,
        "brief": f"s3://{settings.bucket}/{stem}.md",
        "log": f"s3://{settings.bucket}/{log_key}",
    }


def handler(event: dict, context: object) -> dict:
    """The Lambda's entry point: the stack's settings, real AWS clients."""
    settings = Settings.from_env()
    athena = Athena(
        database=settings.raw_database,
        workgroup=settings.workgroup,
        raw_bucket="",  # the brief layer's statements name no bucket
        brief_database=settings.brief_database,
    )
    queries = Path(os.environ.get("LAMBDA_TASK_ROOT", ".")) / "queries"

    def compute(morning: date) -> MorningFacts:
        return MorningFacts.from_frame(
            athena_facts(athena, morning, queries).frame, morning
        )

    result = run(
        event or {}, settings, boto3.client("s3"), boto3.client("sns"), compute
    )
    print(result)  # the function's log, next to write()'s own in the bucket
    return result
