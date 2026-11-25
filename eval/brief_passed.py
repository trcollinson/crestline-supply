"""The briefs that passed the number check, read for what it can't see (M5 step 9).

The number check (crestline/brief/check.py) ties every number in a model's
brief to a fact cited beside it, and reads nothing else. A brief can pass it
and still mislead: an alert called new that has been open for weeks, "all
machines within normal range" on a morning with an alert, one machine's number
cited correctly but written against another's name, a cause the facts don't
give. This file reads the backtest's briefs that went out under the final
prompt (`nova-v2`, `sonnet-v2`) for those:

    uv run python eval/brief_passed.py statuses [--out out-m5]   # alert labels against the ledger, by script
    uv run python eval/brief_passed.py score [--out out-m5]      # the hand readings, checked against the logs
    uv run python eval/brief_passed.py sample [--out out-m5] [--to F]   # every misleading brief and 10 clean ones, facts beside

The readings are in brief_passed_review.yaml, one entry per brief that went
out, keyed to its log: each finding quotes the brief as the log holds it, and
`score` fails if a quote isn't there word for word, a brief is missing or read
twice, or an entry names a log that isn't the brief's newest.

Verdicts: `wrong` (a fact contradicts it), `invented` (a claim no fact
supports), `cause` (a cause, explanation, advice, or action the facts don't
give; the prompt forbids them), `status` (an alert called new when it has
been open, or the reverse, or a wrong "since" date): each of these is
misleading. `minor` is worth a note and misleads no one (an alert named
without its status, the plant day called "today").

Reads the backtest's logs and facts, not the answer key.
"""

import argparse
import json
import random
import re
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

import yaml

import brief_scoring
from crestline.brief.check import unescape_citations

MODELS = ("nova-v2", "sonnet-v2")  # the final prompt's two backtest runs
REVIEW = Path(__file__).with_name("brief_passed_review.yaml")
MISLEADING = ("wrong", "invented", "cause", "status")
VERDICTS = (*MISLEADING, "minor")


@dataclass(frozen=True)
class Brief:
    model: str
    morning: str
    log: Path
    text: str  # as delivered: the model's reply, citations unescaped
    given: dict  # the user message the model was sent: the morning and its facts

    @property
    def alerts(self) -> dict[str, str]:
        return {
            f["id"]: f["status"]
            for f in self.given["facts"]
            if f["id"].startswith("anomaly:") and "status" in f
        }


def passed_briefs(out_dir: Path) -> list[Brief]:
    """Every brief that went out under the final prompt, from its newest log."""
    went_out = brief_scoring.narration(out_dir)
    found = []
    for model in MODELS:
        mornings = (
            went_out[model]
            .filter(went_out[model]["version"] == "model")["morning"]
            .to_list()
        )
        newest: dict[str, Path] = {}
        for path in sorted(
            (out_dir / "backtest/briefs" / model / "logs").glob("*.jsonl")
        ):
            newest[json.loads(path.open().readline())["morning"]] = path
        for morning in mornings:
            records = [json.loads(line) for line in newest[morning].open()]
            reply = next(r for r in records if r["type"] == "model_reply")
            request = next(r for r in records if r["type"] == "request")
            found.append(
                Brief(
                    model=model,
                    morning=morning,
                    log=newest[morning],
                    text=unescape_citations(reply["text"]),
                    given=json.loads(request["messages"][0]["content"][0]["text"]),
                )
            )
    return found


# --- Alert statuses, by script ----------------------------------------------------

MONTHS = [
    "jan",
    "feb",
    "mar",
    "apr",
    "may",
    "jun",
    "jul",
    "aug",
    "sep",
    "oct",
    "nov",
    "dec",
]
OPEN_WORDS = re.compile(
    r"still open|open since|since at least|continu|ongoing|still firing|remains? open|open alert",
    re.IGNORECASE,
)


def dates_in(text: str) -> list[str]:
    """ISO dates and written ones ('Aug 13', '13 August') as YYYY-MM-DD."""
    found = re.findall(r"20\d\d-\d\d-\d\d", text)
    month = r"(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\.?"
    for name, day in re.findall(rf"\b{month} (\d{{1,2}})\b", text, re.IGNORECASE):
        found.append(f"2026-{MONTHS.index(name.lower()) + 1:02d}-{int(day):02d}")
    for day, name in re.findall(rf"\b(\d{{1,2}}) {month}", text, re.IGNORECASE):
        found.append(f"2026-{MONTHS.index(name.lower()) + 1:02d}-{int(day):02d}")
    return found


def label(text: str) -> str | None:
    """'new', 'open', 'both', or None: what a line calls an alert."""
    text = re.sub(r"\bno new\b|\bnew ones first\b", "", text, flags=re.IGNORECASE)
    new, still = (
        bool(re.search(r"\bnew\b", text, re.IGNORECASE)),
        bool(OPEN_WORDS.search(text)),
    )
    return {(True, False): "new", (False, True): "open", (True, True): "both"}.get(
        (new, still)
    )


def alert_labels(brief: Brief) -> list[dict]:
    """Each alert's first mention (the alerts come first), its label and date, against the ledger.

    The label is the line's own, else the bold line or heading above it. A
    verdict of `unclear` (no label, or both) is for a person to read.
    """
    lines = brief.text.splitlines()
    rows = []
    for alert, status in brief.alerts.items():
        subject = alert.split(":")[1]
        expected = "new" if status == "New" else "open"
        since = (dates_in(status) or [None])[0]
        heading, mention = "", None
        for i, line in enumerate(lines):
            if line.startswith("#"):
                heading = line
            if alert in line or (
                subject in line
                and re.search(
                    r"alert|threshold|above (baseline|usual|rebuilt)",
                    line,
                    re.IGNORECASE,
                )
            ):
                above = next(
                    (
                        lines[j]
                        for j in range(i - 1, max(i - 4, -1), -1)
                        if re.match(r"^\*\*[^*]+\*\*:?\s*$", lines[j])
                    ),
                    "",
                )
                mention = (line, above, heading)
                break
        if mention is None:
            rows.append(
                {"alert": alert, "status": status, "verdict": "not named", "line": ""}
            )
            continue
        line, above, heading = mention
        found = label(line) or label(above) or label(heading)
        dates = dates_in(line) or dates_in(above)
        if found in (None, "both"):
            verdict = "unclear"
        elif found != expected:
            verdict = "mislabelled"
        elif expected == "open" and since and dates and since not in dates:
            verdict = "wrong date"
        elif expected == "open" and since and not dates:
            verdict = "right, no date"
        else:
            verdict = "right"
        rows.append(
            {"alert": alert, "status": status, "verdict": verdict, "line": line.strip()}
        )
    return rows


def statuses(out_dir: Path) -> list[dict]:
    return [
        {"model": b.model, "morning": b.morning, **row}
        for b in passed_briefs(out_dir)
        for row in alert_labels(b)
    ]


# --- The hand readings ------------------------------------------------------------


def review(path: Path = REVIEW) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def checked(out_dir: Path, path: Path = REVIEW) -> tuple[list[Brief], list[dict]]:
    """The readings, refused unless each is a brief that went out, quoted from its log."""
    briefs = {(b.model, b.morning): b for b in passed_briefs(out_dir)}
    readings = review(path)
    if readings["out"] != out_dir.name:
        raise SystemExit(f"{path.name} reads {readings['out']}, not {out_dir.name}")
    entries = readings["briefs"]
    keys = [(e["model"], str(e["morning"])) for e in entries]
    problems = [f"read twice: {k}" for k, n in Counter(keys).items() if n > 1]
    problems += [f"not read: {k}" for k in briefs if k not in keys]
    for entry, key in zip(entries, keys):
        brief = briefs.get(key)
        if brief is None:
            problems.append(f"no such brief went out: {key}")
            continue
        if entry["log"] != brief.log.name:
            problems.append(
                f"{key}: log {entry['log']} isn't the newest, {brief.log.name}"
            )
        for finding in entry.get("findings") or []:
            if finding["verdict"] not in VERDICTS:
                problems.append(f"{key}: verdict {finding['verdict']!r}")
            if finding["quote"] not in brief.text:
                problems.append(f"{key}: not in the brief: {finding['quote'][:80]!r}")
    if problems:
        raise SystemExit(f"{path.name}:\n  " + "\n  ".join(problems))
    return list(briefs.values()), entries


def misleading(entry: dict) -> list[dict]:
    return [f for f in entry.get("findings") or [] if f["verdict"] in MISLEADING]


def summary(out_dir: Path) -> str:
    _, entries = checked(out_dir)
    lines = ["# The briefs that went out, read by hand", ""]
    for model in MODELS:
        mine = [e for e in entries if e["model"] == model]
        bad = [e for e in mine if misleading(e)]
        verdicts = Counter(f["verdict"] for e in mine for f in e.get("findings") or [])
        lines.append(
            f"- **{model}**: {len(bad)} of {len(mine)} briefs that went out hold at "
            f"least one misleading statement; findings by verdict: "
            + (
                ", ".join(f"{v} {verdicts[v]}" for v in VERDICTS if verdicts[v])
                or "none"
            )
        )
    rows = statuses(out_dir)
    tally = Counter((r["model"], r["verdict"]) for r in rows)
    lines += [
        "",
        "Alert statuses, by script (each alert's first mention against the ledger):",
        "",
    ]
    for model in MODELS:
        lines.append(
            f"- **{model}**: "
            + ", ".join(f"{v} {n}" for (m, v), n in sorted(tally.items()) if m == model)
        )
    lines += [
        "",
        "Every misleading statement:",
        "",
        "| model | morning | verdict | quote | fact | note |",
        "|---|---|---|---|---|---|",
    ]
    for e in entries:
        for f in misleading(e):
            quote = f["quote"].replace("|", "\\|").replace("\n", " ")
            lines.append(
                f"| {e['model']} | {e['morning']} | {f['verdict']} | {quote} | {f['fact']} | {f['note']} |"
            )
    return "\n".join(lines) + "\n"


# --- A sample to check by hand -------------------------------------------------------


def facts_table(brief: Brief, subjects: set[str] | None = None) -> str:
    """The facts the model was given; with `subjects`, only those machines' (and their shifts')."""
    rows = [
        "| fact | value | baseline / threshold | unit | status |",
        "|---|---|---|---|---|",
    ]
    for f in brief.given["facts"]:
        subject = f["id"].split(":")[1]
        if subjects is not None and not any(
            subject == s or subject.startswith(s + " ") for s in subjects
        ):
            continue
        base = f.get(
            "baseline", f"threshold {f['threshold']}" if "threshold" in f else ""
        )
        rows.append(
            f"| {f['id']} | {f['value']} | {base} | {f['unit']} | {f.get('status', '')} |"
        )
    return "\n".join(rows)


def named(brief: Brief, findings: list[dict]) -> set[str]:
    """The machines and work centers a reading's findings name (in the quote or the fact)."""
    subjects = {f["id"].split(":")[1].split(" ")[0] for f in brief.given["facts"]}
    text = " ".join(f["quote"] + " " + f["fact"] for f in findings)
    return {s for s in subjects if s in text}


def _header(b: Brief) -> list[str]:
    g = b.given
    window = g.get("margin_window") or {}
    return [
        f"### {b.model}, {g['morning']} (plant day {g['plant_day']})",
        "",
        (
            f"Log `{b.log.name}`; margin window: jobs closed "
            f"{window.get('jobs_closed_from')} to {window.get('to')}."
        ),
        "",
    ]


def _findings(entry: dict) -> list[str]:
    return [
        f'- *{f["verdict"]}*: "{f["quote"]}" ({f["fact"]}). {f["note"]}'
        for f in entry.get("findings") or []
    ] or ["- none"]


def _quoted(text: str) -> list[str]:
    return ["> " + line if line else ">" for line in text.splitlines()]


def sample(out_dir: Path, clean: int = 10, seed: int = 42) -> str:
    """Every brief read as misleading, then `clean` drawn from the rest.

    A misleading brief shows its findings and the facts for the machines they
    name, with the whole brief and every fact folded below; a clean one shows
    the whole brief and every fact, since checking it means reading all of it.
    """
    briefs, entries = checked(out_dir)
    by_key = {(b.model, b.morning): b for b in briefs}
    bad = [e for e in entries if misleading(e)]
    rest = [e for e in entries if not misleading(e)]
    drawn = sorted(
        random.Random(seed).sample(rest, min(clean, len(rest))),
        key=lambda e: (e["model"], str(e["morning"])),
    )
    parts = [
        "# Passed briefs: a sample to check",
        "",
        (
            f"The final prompt's briefs that passed the number check (seed 42's backtest), "
            f"read for what the check can't see. Part 1: every brief read as misleading "
            f"({len(bad)}), each with its findings and the facts for the machines they name "
            f"(the whole brief and all its facts folded below). Part 2: {len(drawn)} drawn "
            f"at random (seed {seed}) from the {len(rest)} read as clean, each whole, with "
            "every fact the model was given. Minor findings are listed too; they mislead "
            "no one. Verdicts and the rules behind them: eval/brief_passed_review.yaml."
        ),
        "",
        f"## Part 1: read as misleading ({len(bad)})",
        "",
    ]
    for e in bad:
        b = by_key[(e["model"], str(e["morning"]))]
        parts += [
            *_header(b),
            "**Findings:**",
            "",
            *_findings(e),
            "",
            "**Facts for the machines named:**",
            "",
            facts_table(b, named(b, misleading(e))),
            "",
            "<details><summary>The brief as delivered</summary>",
            "",
            *_quoted(b.text),
            "",
            "</details>",
            "",
            "<details><summary>Every fact the model was given</summary>",
            "",
            facts_table(b),
            "",
            "</details>",
            "",
        ]
    parts += [f"## Part 2: read as clean, {len(drawn)} at random", ""]
    for e in drawn:
        b = by_key[(e["model"], str(e["morning"]))]
        parts += [
            *_header(b),
            "**Findings (minor only):**",
            "",
            *_findings(e),
            "",
            "**The brief as delivered:**",
            "",
            *_quoted(b.text),
            "",
            "**The facts the model was given:**",
            "",
            facts_table(b),
            "",
        ]
    return "\n".join(parts)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("statuses", "score", "sample"):
        p = sub.add_parser(name)
        p.add_argument("--out", type=Path, default=Path("out-m5"))
        if name == "sample":
            p.add_argument("--to", type=Path, help="write the sample here")
            p.add_argument("--clean", type=int, default=10)
            p.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    if args.command == "statuses":
        rows = statuses(args.out)
        print(Counter((r["model"], r["verdict"]) for r in rows))
        for r in rows:
            if r["verdict"] not in ("right", "right, no date"):
                print(
                    f"{r['model']} {r['morning']} {r['alert']} ({r['status']}): {r['verdict']}\n    {r['line']}"
                )
    elif args.command == "score":
        print(summary(args.out))
    else:
        text = sample(args.out, args.clean, args.seed)
        if args.to:
            args.to.write_text(text, encoding="utf-8")
            print(f"{args.to}: {len(text.split())} words")
        else:
            print(text)


if __name__ == "__main__":
    main()
