"""Writing the brief (M5 step 4): the ledger, the template, and the model's
version through the check, with a stand-in for Bedrock.

The ledger's tests plant a chain of mornings: an alert that opens, stays
open, closes, and opens again. The template's runs on real mornings of M5's
backfill, where it must pass the number check every time. The model's
tests check what goes out: the model's brief if it passes, the template if
it fails or the call does, with the reason in the log.
"""

import json
from datetime import date, timedelta
from pathlib import Path

import pytest
from botocore.exceptions import ClientError

from crestline.brief import facts, ledger, narrate, template
from crestline.brief.check import check
from crestline.brief.morning import Fact, MorningFacts, cached
from crestline.brief.write import write

REPO_ROOT = Path(__file__).resolve().parent.parent

# --- The ledger -------------------------------------------------------------------

# Ten plant days, Monday to Friday, two weeks. The brief read on each plant
# day's morning is about the one before.
PLANT_DAYS = [date(2026, 8, 3) + timedelta(days=d) for d in range(12) if d % 7 < 5]


def morning_with(plant_day: date | None, *alerts: str) -> MorningFacts:
    """Facts holding just these alerts."""
    rows = tuple(
        Fact(f"anomaly:{a}:rule", "anomaly", a, "rule", 1.0, "points", 0.5)
        for a in alerts
    )
    after = plant_day + timedelta(days=1) if plant_day else date(2026, 8, 3)
    return MorningFacts(after, plant_day, rows)


def chain(fired: dict[str, list[date]]) -> dict[date, MorningFacts]:
    """Each plant day's brief, keyed by its morning (the next plant day, or
    the day after), with the alerts that fired on it; and the first one,
    with no data before it."""
    briefs = {PLANT_DAYS[0]: morning_with(None)}
    for day, next_day in zip(PLANT_DAYS, [*PLANT_DAYS[1:], None], strict=True):
        on = [alert for alert, days in fired.items() if day in days]
        brief = morning_with(day, *on)
        briefs[next_day or brief.morning] = brief
    return briefs


def alert_statuses(fired: dict[str, list[date]], max_days: int = 20):
    briefs = chain(fired)
    today = briefs[max(briefs)]
    return ledger.statuses(today, briefs.__getitem__, max_days)


def test_an_alert_seen_first_today_is_new():
    found = alert_statuses({"PEM-01": [PLANT_DAYS[-1]]})
    assert found["anomaly:PEM-01:rule"].new
    assert found["anomaly:PEM-01:rule"].describe() == "New"


def test_an_alert_open_every_day_since_is_still_open_since_its_first():
    found = alert_statuses({"LSR-01": PLANT_DAYS[4:]})["anomaly:LSR-01:rule"]
    assert not found.new
    assert found.since == PLANT_DAYS[4]
    assert found.describe() == f"Still open since {PLANT_DAYS[4].isoformat()}"


def test_an_alert_that_closed_and_opened_again_counts_from_the_reopening():
    days = PLANT_DAYS[1:4] + PLANT_DAYS[6:]  # off on days 4 and 5
    found = alert_statuses({"PC-LINE": days})["anomaly:PC-LINE:rule"]
    assert found.since == PLANT_DAYS[6]


def test_the_walk_stops_at_its_limit_and_at_the_first_day_on_record():
    every_day = {"PB-02 2nd": PLANT_DAYS}
    capped = alert_statuses(every_day, max_days=3)["anomaly:PB-02 2nd:rule"]
    assert capped.at_least and capped.since == PLANT_DAYS[-4]
    assert capped.describe().startswith("Open since at least")

    whole = alert_statuses(every_day)["anomaly:PB-02 2nd:rule"]
    assert whole.first_on_record and whole.since == PLANT_DAYS[0]


def test_alerts_are_kept_apart():
    found = alert_statuses({"LSR-01": PLANT_DAYS[2:], "PEM-01": PLANT_DAYS[-1:]})
    assert found["anomaly:LSR-01:rule"].since == PLANT_DAYS[2]
    assert found["anomaly:PEM-01:rule"].new


# --- The template on real mornings ------------------------------------------------

MORNINGS = [
    date(2026, 7, 6),  # the first Monday: little or no baseline yet
    date(2026, 7, 13),
    date(2026, 8, 13),
    date(2026, 8, 17),
    date(2026, 8, 20),
    date(2026, 8, 24),
    date(2026, 10, 1),
]


@pytest.fixture(scope="module")
def facts_for(m5_out, tmp_path_factory):
    """Facts over M5's backfill, kept, so the ledger computes each morning once."""
    with pytest.MonkeyPatch.context() as mp:
        mp.chdir(REPO_ROOT)
        connection = facts.connect(m5_out, MORNINGS[0])

    def compute(morning: date) -> MorningFacts:
        with pytest.MonkeyPatch.context() as mp:
            mp.chdir(REPO_ROOT)
            return MorningFacts.from_frame(facts.facts(connection, morning), morning)

    return cached(compute, tmp_path_factory.mktemp("facts"))


@pytest.mark.parametrize("morning", MORNINGS, ids=str)
def test_the_template_passes_the_number_check(facts_for, morning):
    today = facts_for(morning)
    alerts = ledger.statuses(today, facts_for, max_days=3)
    text = template.render(today, alerts)
    result = check(text, today, ledger.dates(alerts))
    assert result.passed, "\n".join(str(o) for o in result.orphans)
    assert result.numbers > 0
    # Every fact it was given is stated.
    assert result.cited == set(today.by_id)


def test_the_template_leads_with_the_nozzle_and_pb02(facts_for):
    today = facts_for(date(2026, 8, 20))
    text = template.render(today, ledger.statuses(today, facts_for, max_days=3))
    changed = text.split("## What changed")[1].split("## Output")[0]
    assert "LSR-01 scrapped 10.7%" in changed
    assert "PB-02 2nd shift: the ERP's margin" in changed


# --- The model, through the check -------------------------------------------------


class FakeBedrock:
    """Converse's reply shape, with a text of the test's choosing."""

    def __init__(
        self, text: str = "", stop: str = "end_turn", error: Exception | None = None
    ):
        self.text, self.stop, self.error = text, stop, error
        self.sent: list[dict] = []

    def converse(self, **request):
        self.sent.append(request)
        if self.error:
            raise self.error
        return {
            "output": {
                "message": {"role": "assistant", "content": [{"text": self.text}]}
            },
            "stopReason": self.stop,
            "usage": {"inputTokens": 4321, "outputTokens": 321, "totalTokens": 4642},
        }


SMALL = MorningFacts(
    date(2026, 8, 20),
    date(2026, 8, 19),
    (
        Fact("anomaly:LSR-01:scrap_above_baseline", "anomaly", "LSR-01",
             "scrap_above_baseline", 15.1, "standard errors", 3.0),
        Fact("scrap:LSR-01:scrap_pct", "scrap", "LSR-01", "scrap_pct", 10.7, "percent", 2.6),
        Fact("scrap:LSR-01:pieces", "scrap", "LSR-01", "pieces", 875, "pieces"),
        Fact("scrap:LSR-01:scrapped", "scrap", "LSR-01", "scrapped", 94, "pieces"),
        Fact("output:plant:jobs_finished", "output", "plant", "jobs_finished", 14, "jobs"),
        Fact("context:baseline:working_days", "context", "baseline", "working_days", 20,
             "working days"),
    ),
)  # fmt: skip


def small_facts(morning: date) -> MorningFacts:
    return SMALL if morning == SMALL.morning else MorningFacts(morning, None, ())


def log_records(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines()]


def test_a_model_brief_that_passes_goes_out(tmp_path):
    prose = (
        "**New:** LSR-01 scrap jumped to 10.7% from a 2.6% baseline. "
        "[scrap:LSR-01:scrap_pct]\n\n14 jobs finished. [output:plant:jobs_finished]"
    )
    bedrock = FakeBedrock(prose)
    written = write(SMALL.morning, small_facts, tmp_path, "sonnet", bedrock)

    assert written.version == "model"
    assert (tmp_path / "2026-08-20.md").read_text() == prose
    sent = bedrock.sent[0]
    assert sent["modelId"] == narrate.MODELS["sonnet"]
    assert sent["inferenceConfig"]["temperature"] == 0
    assert "toolConfig" not in sent
    user = json.loads(sent["messages"][0]["content"][0]["text"])
    assert {f["id"] for f in user["facts"]} == set(SMALL.by_id)
    assert user["facts"][0]["status"] == "New"
    # An alert's threshold isn't sent as a baseline; a rate's baseline is.
    assert user["facts"][0]["threshold"] == 3.0 and "baseline" not in user["facts"][0]
    assert user["facts"][1]["baseline"] == 2.6

    kinds = [r["type"] for r in log_records(written.log)]
    assert kinds == ["brief", "check", "request", "model_reply", "check", "delivered"]


def test_citations_with_markdown_escaped_underscores_are_read_as_written(tmp_path):
    # Sonnet sometimes escapes the underscores in a fact ID (M5 step 8);
    # Markdown renders `scrap\\_pct` as `scrap_pct`, so it's the same citation.
    prose = "LSR-01 scrap was 10.7%. [scrap:LSR-01:scrap\\_pct]"
    written = write(SMALL.morning, small_facts, tmp_path, "sonnet", FakeBedrock(prose))

    assert written.version == "model"
    assert written.text == "LSR-01 scrap was 10.7%. [scrap:LSR-01:scrap_pct]"
    replies = [r for r in log_records(written.log) if r["type"] == "model_reply"]
    assert replies[0]["text"] == prose  # the log keeps what the model wrote


def test_a_model_brief_with_an_orphan_is_replaced_by_the_template(tmp_path):
    prose = "LSR-01 scrapped 10.7%, about 4 times its usual. [scrap:LSR-01:scrap_pct]"
    written = write(SMALL.morning, small_facts, tmp_path, "nova", FakeBedrock(prose))

    assert written.version == "template"
    assert written.text == template.render(SMALL, written.alerts)
    assert "failed the number check (1 orphans)" in written.reason
    checks = [r for r in log_records(written.log) if r["type"] == "check"]
    assert checks[1]["version"] == "model" and not checks[1]["passed"]
    assert checks[1]["orphans"][0]["text"] == "4"


@pytest.mark.parametrize(
    ("bedrock", "reason"),
    [
        (
            FakeBedrock(
                error=ClientError(
                    {"Error": {"Code": "AccessDeniedException"}}, "Converse"
                )
            ),
            "the model failed: ClientError",
        ),
        (
            FakeBedrock("LSR-01 scrapped", stop="max_tokens"),
            "stopped early (max_tokens)",
        ),
    ],
    ids=["error", "cut off"],
)
def test_no_usable_model_brief_sends_the_template(tmp_path, bedrock, reason):
    written = write(SMALL.morning, small_facts, tmp_path, "sonnet", bedrock)
    assert written.version == "template"
    assert reason in written.reason
    assert log_records(written.log)[-1]["version"] == "template"


def test_without_a_model_the_template_goes_out_and_bedrock_is_never_called(tmp_path):
    bedrock = FakeBedrock("never")
    written = write(SMALL.morning, small_facts, tmp_path, None, bedrock)
    assert written.version == "template" and not bedrock.sent


def test_the_prompt_holds_no_free_text():
    """Only the facts and fixed words go to the model: no reason text, no
    operator notes (M4's planted instructions lived there)."""
    sent = narrate.request("sonnet", SMALL, {})
    user = json.loads(sent["messages"][0]["content"][0]["text"])
    assert set(user) == {"morning", "plant_day", "data_as_of", "margin_window",
                         "alerts", "metrics", "facts"}  # fmt: skip
    for row in user["facts"]:
        assert set(row) <= {"id", "value", "unit", "baseline", "threshold", "status"}
        assert isinstance(row["value"], int | float)
