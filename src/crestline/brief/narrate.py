"""The model writes the brief up (M5 step 4): one Converse call, no tools.

The model gets the facts as JSON and nothing else: no MES free text (an
operator's note is where M4's planted instructions lived), no tools,
no data it could fetch. It writes prose; the number check (check.py)
decides whether that prose goes out.

Temperature 0, so the same facts give (nearly) the same brief. The system
prompt is short on purpose and names the rule the check enforces: use the
numbers as given or rounded, compute nothing, cite each fact you state.
"""

import json
import time
from dataclasses import dataclass
from typing import Any

import boto3
from botocore.config import Config

from crestline.brief.ledger import AlertStatus
from crestline.brief.morning import Fact, MorningFacts, formatted

# The two models M4 measured (spec §9); `crestline_agent` has the same pair.
MODELS = {
    "sonnet": "us.anthropic.claude-sonnet-4-6",
    "nova": "us.amazon.nova-2-lite-v1:0",
}
INFERENCE = {"maxTokens": 4096, "temperature": 0}

SYSTEM_PROMPT = (
    "You write the morning brief a plant manager at Crestline Supply, a metal "
    "fabrication plant, reads before first shift. The user message is the "
    "facts, as JSON. Lead with what changed (the alerts, new ones first, each "
    "with its status as given), then output, margin, stops, and scrap, in short "
    "plain sentences or bullets, in Markdown, short enough to read before the "
    "shift starts (state what matters, not every fact). Never use a table: it is "
    "also sent as a plain-text email. Use only numbers from the facts, written "
    "as digits, as given or rounded; never compute a new one (no differences, "
    "ratios, sums, counts, or 'twice'). Put no numbers in headings. End each "
    "sentence or bullet that states a number with the IDs of every fact it "
    "uses, in square brackets, e.g. [scrap:LSR-01:scrap_pct]: a number cited "
    "anywhere else doesn't count. Say what the facts say and nothing more: no "
    "causes, advice, or actions."
)
# What changed in the prompt, and why (M5 step 6, from step 5's backtest):
# Sonnet put counts in headings and baselines in bullets citing other facts,
# drew tables 15 times, and called a continuing alert new. PROMPT_VERSION is
# logged with every request.
PROMPT_VERSION = 2

# What each metric means, for the model; the IDs say which machine.
METRICS = {
    "pieces": "pieces made (good plus scrapped); its baseline is the usual day's",
    "good": "good pieces",
    "scrapped": "pieces scrapped",
    "scans": "operator scans on and off jobs",
    "jobs_finished": "jobs whose last operation finished on the plant day",
    "jobs": "jobs the ERP closed in the margin window",
    "erp_margin_pct": "margin as the ERP costs it (standard hours)",
    "rebuilt_margin_pct": "margin rebuilt from the machines' real setup and run times",
    "gap_points": "ERP margin minus rebuilt margin, in percentage points",
    "setup_vs_standard": "real setup time as a multiple of the standard",
    "stops": "stops the machine data shows (short ones left out, as the log leaves them out)",
    "stop_minutes": "minutes of those stops; its baseline is the usual day's",
    "stops_not_logged": "of those stops, how many no operator wrote down",
    "logged_stops": "stops in the operators' downtime log",
    "logged_minutes": "minutes in the operators' downtime log",
    "scrap_pct": "scrap rate on the plant day; its baseline is the rate over the baseline window",
    "standard_errors": "how far the rate is from its baseline, in standard errors",
    "working_days": "how many working days every baseline covers",
}
ALERT_NOTE = (
    "Alerts (IDs starting 'anomaly:'): the value is what tripped the rule and the "
    "threshold is what it beat (not a baseline); status says whether it is new or "
    "since when it has fired every day."
)


@dataclass(frozen=True)
class Narration:
    model: str
    text: str
    stop_reason: str
    usage: dict[str, int]
    ms: int


def model_id(name: str) -> str:
    return MODELS.get(name, name)


def bedrock_client(session: boto3.Session | None = None):
    session = session or boto3.Session()
    return session.client(
        "bedrock-runtime",
        config=Config(
            read_timeout=300, retries={"mode": "adaptive", "max_attempts": 10}
        ),
    )


def prompt(facts: MorningFacts, alerts: dict[str, AlertStatus]) -> dict[str, Any]:
    """The user message: the morning, its plant day, and every fact."""
    return {
        "morning": f"{facts.morning:%A} {facts.morning.isoformat()}",
        "plant_day": facts.plant_day
        and f"{facts.plant_day:%A} {facts.plant_day.isoformat()}",
        "data_as_of": "05:30 plant time this morning",
        "margin_window": facts.margin_window
        and {
            "jobs_closed_from": facts.margin_window[0].isoformat(),
            "to": facts.margin_window[1].isoformat(),
            "note": "the ERP closes a job in its nightly labor batch, so margin "
            "runs a day behind the floor",
        },
        "alerts": ALERT_NOTE,
        "metrics": METRICS,
        "facts": [
            {
                "id": fact.fact_id,
                "value": formatted(fact.value, fact.unit),
                "unit": fact.unit,
                # An alert's baseline column is its rule's threshold: named
                # so, or a model reads it as the machine's usual (M5 step 4).
                **(
                    {_baseline_name(fact): formatted(fact.baseline, fact.unit)}
                    if fact.baseline is not None
                    else {}
                ),
                **(
                    {"status": alerts[fact.fact_id].describe()}
                    if fact.fact_id in alerts
                    else {}
                ),
            }
            for fact in facts.facts
        ],
    }


def _baseline_name(fact: Fact) -> str:
    return "threshold" if fact.family == "anomaly" else "baseline"


def request(model: str, facts: MorningFacts, alerts: dict[str, AlertStatus]) -> dict:
    """The Converse request, as sent and as logged."""
    return {
        "modelId": model_id(model),
        "system": [{"text": SYSTEM_PROMPT}],
        "messages": [
            {
                "role": "user",
                "content": [{"text": json.dumps(prompt(facts, alerts), indent=1)}],
            }
        ],
        "inferenceConfig": INFERENCE,
    }


def narrate(bedrock, sent: dict) -> Narration:
    """Send the request; the brief is the reply's text."""
    started = time.perf_counter()
    reply = bedrock.converse(**sent)
    text = "\n".join(
        block["text"]
        for block in reply["output"]["message"]["content"]
        if "text" in block
    ).strip()
    return Narration(
        model=sent["modelId"],
        text=text,
        stop_reason=reply["stopReason"],
        usage={k: v for k, v in reply["usage"].items() if isinstance(v, int)},
        ms=round((time.perf_counter() - started) * 1000),
    )
