"""The brief's Lambda (M5 step 6), offline: the replay clock, the facts kept
in the bucket, one run delivered, the planted orphan, and the two formats."""

import json
from datetime import date

import pytest
from botocore.exceptions import ClientError
from test_brief_write import SMALL, FakeBedrock, small_facts

from crestline.brief import aws, delivery
from crestline.brief.morning import MorningFacts

SETTINGS = aws.Settings(
    bucket="brief-bucket",
    topic_arn="arn:aws:sns:us-east-1:000000000000:brief",
    workgroup="crestline",
    raw_database="crestline_raw",
    brief_database="crestline_brief",
    model="sonnet",
    last_morning=date(2026, 10, 1),
    clock_start=date(2026, 8, 12),  # a Wednesday
    clock_anchor=date(2026, 11, 23),  # a Monday
)


@pytest.mark.parametrize(
    ("today", "morning"),
    [
        (date(2026, 11, 20), date(2026, 8, 12)),  # before the anchor: the start
        (date(2026, 11, 23), date(2026, 8, 12)),  # the anchor
        (date(2026, 11, 24), date(2026, 8, 13)),
        (date(2026, 11, 26), date(2026, 8, 17)),  # Wed's brief, then Thu, Fri, Mon
        (date(2026, 11, 27), date(2026, 8, 18)),
        (date(2026, 11, 28), date(2026, 8, 19)),  # a weekend run is Monday's brief
        (date(2026, 11, 30), date(2026, 8, 19)),  # and Monday's run sends it
        (date(2027, 6, 1), date(2026, 10, 1)),  # the data's last morning, held
    ],
)
def test_the_clock_moves_a_plant_weekday_per_real_weekday(today, morning):
    assert aws.plant_morning(today, SETTINGS) == morning


def test_without_a_clock_every_run_is_the_last_morning():
    still = aws.Settings(
        **{**SETTINGS.__dict__, "clock_start": None, "clock_anchor": None}
    )
    assert aws.plant_morning(date(2026, 11, 24), still) == date(2026, 10, 1)


class FakeS3:
    def __init__(self, objects: dict[str, bytes] | None = None):
        self.objects = dict(objects or {})

    def get_object(self, Bucket, Key):
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
        body = self.objects[Key]
        return {"Body": type("Body", (), {"read": lambda self: body})()}

    def put_object(self, Bucket, Key, Body, ContentType):
        self.objects[Key] = Body


class FakeSns:
    def __init__(self):
        self.sent: list[dict] = []

    def publish(self, **message):
        self.sent.append(message)


def test_facts_are_computed_once_then_read_from_the_bucket():
    computed: list[date] = []

    def compute(morning: date) -> MorningFacts:
        computed.append(morning)
        return small_facts(morning)

    s3 = FakeS3()
    facts_for = aws.s3_cached(compute, s3, "brief-bucket")
    first = facts_for(SMALL.morning)
    again = facts_for(SMALL.morning)
    assert computed == [SMALL.morning]
    assert first == again == SMALL
    assert "brief/facts/2026-08-20.json" in s3.objects


PROSE = (
    "**New:** LSR-01 scrap jumped to 10.7% from a 2.6% baseline. "
    "[scrap:LSR-01:scrap_pct]\n\n14 jobs finished. [output:plant:jobs_finished]"
)


def deliver(event: dict, bedrock=None) -> tuple[dict, FakeS3, FakeSns]:
    s3, sns = FakeS3(), FakeSns()
    result = aws.run(
        event, SETTINGS, s3, sns, small_facts, bedrock or FakeBedrock(PROSE)
    )
    return result, s3, sns


def test_a_run_writes_the_brief_its_page_and_log_and_emails_it():
    result, s3, sns = deliver({"morning": "2026-08-20"})
    assert result["version"] == "model"
    assert s3.objects["brief/2026-08-20.md"].decode() == PROSE
    assert b'<span class="cite">' in s3.objects["brief/2026-08-20.html"]
    [log] = [k for k in s3.objects if k.startswith("brief/logs/")]
    kinds = [json.loads(line)["type"] for line in s3.objects[log].splitlines()]
    assert kinds[0] == "brief" and kinds[-1] == "delivered"
    [email] = sns.sent
    assert email["Subject"] == "Crestline morning brief, Thursday 2026-08-20"
    assert "[scrap:" not in email["Message"]
    assert "LSR-01 scrap jumped to 10.7% from a 2.6% baseline." in email["Message"]
    assert "s3://brief-bucket/brief/2026-08-20.html" in email["Message"]


def test_a_planted_orphan_sends_the_template_and_the_log_says_why():
    result, s3, sns = deliver({"morning": "2026-08-20", "plant_orphan": True})
    assert result["version"] == "template"
    assert "failed the number check" in result["reason"]
    [log] = [k for k in s3.objects if k.startswith("brief/logs/")]
    records = [json.loads(line) for line in s3.objects[log].splitlines()]
    assert [r["text"] for r in records if r["type"] == "planted"] == [
        aws.PLANTED_ORPHAN
    ]
    [model_check] = [
        r for r in records if r["type"] == "check" and r["version"] == "model"
    ]
    assert model_check["orphans"][0]["text"] == "99.9%"
    # The model's own words are logged as it wrote them.
    [reply] = [r for r in records if r["type"] == "model_reply"]
    assert "99.9%" not in reply["text"]
    assert "Planted" not in sns.sent[0]["Message"]


def test_the_clock_names_the_morning_when_the_event_doesnt():
    s3, sns = FakeS3(), FakeSns()
    result = aws.run(
        {}, SETTINGS, s3, sns, small_facts, FakeBedrock(PROSE), today=date(2026, 11, 24)
    )
    assert result["morning"] == "2026-08-13"


# --- The two formats ----------------------------------------------------------

BRIEF = """\
# Morning brief, Thursday 2026-08-20

## What changed

- **New:** LSR-01 scrapped 10.7% [scrap:LSR-01:scrap_pct] [scrap:LSR-01:pieces]
- PB-02 2nd: 6.1 points [margin:PB-02 2nd:gap_points, margin:PB-02 2nd:jobs]

Stops on *PC-LINE*: 149 minutes. [stops:PC-LINE:stop_minutes][stops:PC-LINE:stops]
"""


def test_the_email_drops_every_citation_and_the_bold():
    text = delivery.plain_text(BRIEF, "s3://b/brief/2026-08-20.html")
    assert "[" not in text and "**" not in text
    assert "- New: LSR-01 scrapped 10.7%" in text
    assert "- PB-02 2nd: 6.1 points\n" in text
    assert "Stops on *PC-LINE*: 149 minutes." in text
    assert text.endswith(
        "Every number, with its source: s3://b/brief/2026-08-20.html\n"
    )


def test_the_page_keeps_citations_small_and_escapes_text():
    page = delivery.page(BRIEF + "\nR&D <test>\n", "Crestline morning brief")
    assert "<h2>What changed</h2>" in page
    assert "<li><strong>New:</strong> LSR-01 scrapped 10.7%" in page
    assert page.count('class="cite"') == 3
    assert "<em>PC-LINE</em>" in page
    assert "R&amp;D &lt;test&gt;" in page
