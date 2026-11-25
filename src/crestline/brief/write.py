"""Write one morning's brief (M5 step 4): facts, alerts, prose, the check.

1. The facts as of the morning (cached, so earlier briefs' facts are reused).
2. Each alert new or open since when (ledger.py).
3. The template version, which must pass the number check: if it doesn't,
   the template has a bug, and nothing goes out.
4. With a model: its version, through the same check. It goes out if it
   passes; if it fails, or the model can't be reached, the template does,
   and the log says why.

`planted` is a sentence appended to the model's reply before the check, for
proving the fallback where it runs (M5 step 6: a Lambda invoked with a
planted orphan must deliver the template); the log records it apart from
what the model wrote.

Everything is logged to `<out>/brief/logs/<UTC>-<morning>-<model>.jsonl`, as
M4's agent sessions are: what was sent (system prompt, inference settings,
the whole user message), what came back (text, stop reason, tokens, time),
each check's result with every orphan, and what went out. The brief that
went out is `<out>/brief/<morning>.md`.
"""

import json
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from datetime import UTC, date, datetime
from pathlib import Path

from botocore.exceptions import BotoCoreError, ClientError

from crestline.brief import ledger, narrate, template
from crestline.brief.check import CheckResult, check, unescape_citations
from crestline.brief.morning import MorningFacts


@dataclass
class Written:
    morning: date
    text: str  # what went out
    version: str  # "model" or "template"
    reason: str  # why that version
    template_check: CheckResult
    model_check: CheckResult | None = None
    narration: narrate.Narration | None = None
    log: Path | None = None
    path: Path | None = None
    alerts: dict[str, ledger.AlertStatus] = field(default_factory=dict)


class BriefLog:
    """Append-only JSONL, each record stamped with its UTC time."""

    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path

    def write(self, kind: str, **record) -> None:
        stamped = {"type": kind, "at": _now(), **record}
        with self.path.open("a", encoding="utf-8") as out:
            out.write(json.dumps(stamped, ensure_ascii=False, default=str) + "\n")


def write(
    morning: date,
    facts_for: Callable[[date], MorningFacts],
    brief_dir: Path,
    model: str | None = None,
    bedrock=None,
    planted: str | None = None,
) -> Written:
    """The brief for `morning`, written to `brief_dir`; `model` None for the template."""
    today = facts_for(morning)
    alerts = ledger.statuses(today, facts_for)
    allowed_dates = ledger.dates(alerts)
    log = BriefLog(_log_path(brief_dir / "logs", morning, model or "template"))
    log.write(
        "brief",
        morning=morning.isoformat(),
        plant_day=today.plant_day,
        model=model and narrate.model_id(model),
        prompt_version=narrate.PROMPT_VERSION,
        alerts={i: status.describe() for i, status in alerts.items()},
        facts=json.loads(today.to_json()),
    )

    draft = template.render(today, alerts)
    template_check = check(draft, today, allowed_dates)
    _log_check(log, "template", template_check)
    if not template_check.passed:
        raise RuntimeError(
            "the template failed its own number check:\n"
            + "\n".join(str(o) for o in template_check.orphans)
        )
    written = Written(morning, draft, "template", "no model asked", template_check)
    written.alerts = alerts

    if model:
        sent = narrate.request(model, today, alerts)
        log.write("request", **sent)
        try:
            narration = narrate.narrate(bedrock or narrate.bedrock_client(), sent)
        except (BotoCoreError, ClientError) as error:  # no prose: the template goes out
            log.write("model_error", error=f"{type(error).__name__}: {error}")
            written.reason = f"the model failed: {type(error).__name__}"
        else:
            log.write(
                "model_reply",
                text=narration.text,
                stop_reason=narration.stop_reason,
                usage=narration.usage,
                ms=narration.ms,
            )
            # The log keeps the reply as the model wrote it; the check and
            # the delivered brief read its citations unescaped.
            narration = replace(narration, text=unescape_citations(narration.text))
            if planted:
                log.write("planted", text=planted)
                narration = replace(narration, text=f"{narration.text}\n\n{planted}")
            written.narration = narration
            written.model_check = check(narration.text, today, allowed_dates)
            _log_check(log, "model", written.model_check)
            if narration.stop_reason != "end_turn":
                written.reason = f"the model stopped early ({narration.stop_reason})"
            elif not written.model_check.passed:
                written.reason = (
                    f"the model's brief failed the number check "
                    f"({len(written.model_check.orphans)} orphans)"
                )
            else:
                written.text, written.version = narration.text, "model"
                written.reason = "the model's brief passed the number check"

    written.path = brief_dir / f"{morning.isoformat()}.md"
    written.path.parent.mkdir(parents=True, exist_ok=True)
    written.path.write_text(written.text, encoding="utf-8")
    log.write(
        "delivered", version=written.version, reason=written.reason, path=written.path
    )
    written.log = log.path
    return written


def _log_check(log: BriefLog, version: str, result: CheckResult) -> None:
    log.write(
        "check",
        version=version,
        passed=result.passed,
        numbers=result.numbers,
        cited=sorted(result.cited),
        orphans=[o.__dict__ for o in result.orphans],
    )


def _log_path(folder: Path, morning: date, model: str) -> Path:
    """A new file per brief, numbered if two start in one second."""
    folder.mkdir(parents=True, exist_ok=True)
    base = f"{datetime.now(UTC):%Y%m%dT%H%M%SZ}-{morning.isoformat()}-{model}"
    for n in range(1, 1000):
        path = folder / (f"{base}.jsonl" if n == 1 else f"{base}-{n}.jsonl")
        try:
            path.open("x").close()
            return path
        except FileExistsError:
            continue
    raise RuntimeError(f"no free log name for {base}")


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")
