"""The brief without a model (M5 step 4): fixed sections, the facts as they are.

This is what goes out when no model is asked, when the model can't be
reached, and when the model's brief fails the number check. It is plain on
purpose: every line is one or two facts in a fixed sentence, each followed
by the IDs of the facts it states, so the check passes it on every morning
(a test runs it on every morning of the backfill).

What changed comes first: the alerts, new ones before old ones. Then the
four families in the order a plant manager asks about them.
"""

from crestline.brief.ledger import AlertStatus
from crestline.brief.morning import CONTEXT_FACT_ID, Fact, MorningFacts, formatted

# Work centers in routing order (spec §2.5), machines likewise.
WORK_CENTERS = ["LASER", "DEBURR", "BRAKE", "PEM", "POWDER", "ASSY"]
MACHINES = ["LSR-01", "DB-01", "PB-01", "PB-02", "PEM-01", "PC-LINE", "AS-01"]


def render(facts: MorningFacts, alerts: dict[str, AlertStatus]) -> str:
    morning = facts.morning
    title = f"# Morning brief, {morning:%A} {morning.isoformat()}"
    if facts.plant_day is None:
        return f"{title}\n\nNo plant day yet: nobody had worked before this morning.\n"
    by_id = facts.by_id
    lines = [
        title,
        "",
        (
            f"About {facts.plant_day:%A} {facts.plant_day.isoformat()}, from the data "
            "as it stood at 05:30 this morning. Each baseline is the last "
            f"{_say(by_id[CONTEXT_FACT_ID])}. [{CONTEXT_FACT_ID}]"
        ),
        "",
        "## What changed",
        "",
        *_alerts(facts, alerts),
        "",
        "## Output",
        "",
        *_output(by_id),
        "",
        "## Margin",
        "",
        *_margin(facts, by_id),
        "",
        "## Stops",
        "",
        "Stops the machines show, against the operators' log.",
        "",
        *_stops(by_id),
        "",
        "## Scrap",
        "",
        *_scrap(by_id),
    ]
    return "\n".join(lines) + "\n"


def _alerts(facts: MorningFacts, alerts: dict[str, AlertStatus]) -> list[str]:
    if not facts.anomaly_ids:
        return ["Nothing crossed a threshold."]
    by_id = facts.by_id
    ordered = sorted(
        facts.anomaly_ids, key=lambda i: (not alerts[i].new, alerts[i].since, i)
    )
    return [f"- **{alerts[i].describe()}:** {_alert(by_id[i], by_id)}" for i in ordered]


def _alert(alert: Fact, by_id: dict[str, Fact]) -> str:
    subject, metric = alert.subject, alert.metric
    if metric == "scrap_above_baseline":
        rate = by_id[f"scrap:{subject}:scrap_pct"]
        return (
            f"{subject} scrapped {_say(rate)} against its baseline of "
            f"{_say(rate, rate.baseline)}, {_say(alert)} above it (threshold: "
            f"{_say(alert, alert.baseline)}). {_cite(alert, rate)}"
        )
    if metric == "erp_margin_above_rebuilt":
        erp, rebuilt, jobs = (
            by_id[f"margin:{subject}:{name}"]
            for name in ("erp_margin_pct", "rebuilt_margin_pct", "jobs")
        )
        return (
            f"{subject} shift: the ERP's margin, {_say(erp)}, is {_say(alert)} above "
            f"the rebuilt margin, {_say(rebuilt)}, on {_say(jobs)} closed (threshold: "
            f"{_say(alert, alert.baseline)}). {_cite(alert, erp, rebuilt, jobs)}"
        )
    if metric == "stop_minutes_above_usual":
        minutes = by_id[f"stops:{subject}:stop_minutes"]
        return (
            f"{subject} was stopped {_say(alert)}; its usual is "
            f"{_say(minutes, minutes.baseline)} (threshold: "
            f"{_say(alert, alert.baseline)}). {_cite(alert, minutes)}"
        )
    if metric == "output_below_usual":
        pieces = by_id[f"output:{subject}:pieces"]
        return (
            f"{subject} made {_say(alert)}; its usual is "
            f"{_say(pieces, pieces.baseline)} (threshold: "
            f"{_say(alert, alert.baseline)}). {_cite(alert, pieces)}"
        )
    return f"{alert.fact_id}: {_say(alert)}. {_cite(alert)}"


def _output(by_id: dict[str, Fact]) -> list[str]:
    finished = by_id["output:plant:jobs_finished"]
    lines = [f"- Jobs finished: {_n(finished)}. {_cite(finished)}"]
    for center in _present(WORK_CENTERS, "output", by_id, "pieces"):
        pieces, good, scrapped, scans = (
            by_id[f"output:{center}:{name}"]
            for name in ("pieces", "good", "scrapped", "scans")
        )
        usual = f" (usual {_n(pieces, pieces.baseline)})" if pieces.baseline else ""
        lines.append(
            f"- {center}: {_say(pieces)}{usual}, {_n(good)} good and "
            f"{_n(scrapped)} scrapped, on {_say(scans)}. "
            f"{_cite(pieces, good, scrapped, scans)}"
        )
    return lines


def _margin(facts: MorningFacts, by_id: dict[str, Fact]) -> list[str]:
    subjects = sorted({fact.subject for fact in facts.family("margin")})
    closed = [s for s in subjects if by_id[f"margin:{s}:jobs"].value > 0]
    if not closed:
        zero = [by_id[f"margin:{s}:jobs"] for s in subjects]
        return [f"No jobs closed in the ERP yet. {_cite(*zero)}".strip()]
    first, last = facts.margin_window
    lines = [
        (
            f"Jobs the ERP closed from {first.isoformat()} to {last.isoformat()}. The "
            "ERP closes a job in its nightly labor batch, so this runs a day behind the "
            "floor."
        ),
        "",
    ]
    # Each brake and shift, then every job closed.
    for subject in [s for s in closed if s != "all jobs"] + ["all jobs"]:
        jobs = by_id[f"margin:{subject}:jobs"]
        erp, rebuilt, gap, setups = (
            by_id.get(f"margin:{subject}:{name}")
            for name in (
                "erp_margin_pct",
                "rebuilt_margin_pct",
                "gap_points",
                "setup_vs_standard",
            )
        )
        parts = [_say(jobs)]
        if erp:
            parts.append(f"ERP margin {_say(erp)}")
        if rebuilt:
            parts.append(
                f"rebuilt {_say(rebuilt)}"
                + (f" (ERP {_say(gap)} {_side(gap)})" if gap else "")
            )
        if setups:
            parts.append(f"setups at {_say(setups)} standard")
        name = "All jobs" if subject == "all jobs" else f"{subject} shift"
        stated = [f for f in (jobs, erp, rebuilt, gap, setups) if f]
        lines.append(f"- {name}: {', '.join(parts)}. {_cite(*stated)}")
    return lines


def _stops(by_id: dict[str, Fact]) -> list[str]:
    lines = []
    for machine in _present(MACHINES, "stops", by_id, "stops"):
        stops, minutes, missing, logged, logged_minutes = (
            by_id[f"stops:{machine}:{name}"]
            for name in (
                "stops", "stop_minutes", "stops_not_logged", "logged_stops",
                "logged_minutes",
            )
        )  # fmt: skip
        usual = f" (usual {_n(minutes, minutes.baseline)})" if minutes.baseline else ""
        lines.append(
            f"- {machine}: {_say(stops)}, {_say(minutes)}{usual}; the log has "
            f"{_say(logged)}, {_say(logged_minutes)}; not in the log: {_n(missing)}. "
            f"{_cite(stops, minutes, logged, logged_minutes, missing)}"
        )
    return lines or ["No machine data."]


def _scrap(by_id: dict[str, Fact]) -> list[str]:
    lines = []
    for machine in _present(MACHINES, "scrap", by_id, "pieces"):
        pieces, scrapped = (
            by_id[f"scrap:{machine}:{name}"] for name in ("pieces", "scrapped")
        )
        rate = by_id.get(f"scrap:{machine}:scrap_pct")
        errors = by_id.get(f"scrap:{machine}:standard_errors")
        if rate is None:
            lines.append(
                f"- {machine}: {_n(scrapped)} of {_say(pieces)} scrapped. {_cite(scrapped, pieces)}"
            )
            continue
        against = (
            f", baseline {_say(rate, rate.baseline)}, {_say(errors)} {_side(errors)} it"
            if rate.baseline is not None and errors is not None
            else ", no baseline yet"
        )
        lines.append(
            f"- {machine}: {_say(rate)} ({_n(scrapped)} of {_say(pieces)}){against}. "
            f"{_cite(*[f for f in (rate, scrapped, pieces, errors) if f])}"
        )
    return lines or ["No scans."]


def _present(
    order: list[str], family: str, by_id: dict[str, Fact], metric: str
) -> list[str]:
    """The subjects of `order` that have this family's facts, in that order,
    then any others."""
    subjects = {
        i.split(":")[1]
        for i in by_id
        if i.startswith(f"{family}:") and i.endswith(f":{metric}")
    }
    return [s for s in order if s in subjects] + sorted(subjects - set(order))


def _n(fact: Fact, value: float | None = None) -> str:
    """A count's number alone, where the sentence says what it counts."""
    return f"{abs(formatted(fact.value if value is None else value, fact.unit)):,}"


def _say(fact: Fact, value: float | None = None) -> str:
    """A fact's value (or its baseline) with its unit, as the check reads it."""
    number = abs(formatted(fact.value if value is None else value, fact.unit))
    unit = fact.unit
    if unit == "percent":
        return f"{number:.1f}%"
    if unit == "ratio":
        return f"{number:.2f}×"
    if unit in ("points", "standard errors"):
        return f"{number:.1f} {unit}"
    text = f"{number:,}"
    word = unit[:-1] if number == 1 and unit.endswith("s") else unit
    return f"{text} {word}"


def _side(fact: Fact) -> str:
    return "above" if fact.value >= 0 else "below"


def _cite(*facts: Fact) -> str:
    return " ".join(f"[{fact.fact_id}]" for fact in facts)
