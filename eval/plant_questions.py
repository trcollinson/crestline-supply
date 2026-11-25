"""Twenty plant-manager questions, asked of M4's agent and scored against the
answer key (M5 step 7).

    uv run python eval/plant_questions.py answers                       # the answer key's answers (local)
    uv run python eval/plant_questions.py run --model sonnet --run NAME  # AWS: Bedrock and the gateway
    uv run python eval/plant_questions.py score --run NAME              # the table (local)

The questions and how each is graded are in `plant_questions.yaml`; every
answer is computed here from the answer key (`<out>/truth/`), never from the
platform's views, so the platform can be wrong and the score says so.

**Grading is mechanical**: numbers within a tolerance, a date within a few
days, patterns a right answer has to contain, and for the questions the data
can't answer, the answer saying so without making something up. What the
rules can't settle is `review`. A person then reads every answer scored
`wrong`, `partial`, `refused wrongly`, or `review`, and writes down a grade
(if the rules got it wrong) and a reason in `plant_questions_review.yaml`:
wrong tool, wrong window, misread the data, data can't answer, made it up.
The score lists any miss still without a reason.

**The canary**, before each batch: the deployed stack must be configuration
C (Cedar ENFORCE, the cedar policies, no guardrail), and q04 asked once,
with its date written out, must come out right; otherwise nothing runs. The
date is written out because the canary checks the plumbing (the gateway, the
tool, the data), not the model's calendar: Nova's first canary read "last
Friday" as Saturday, Sep 26, and answered 0 hooks (M5 step 7).

Sessions run as `ops-analyst` through the gateway, logged to
`<out>/agent/sessions/plant/<run>/` with the label `<question>-r<repeat>`, so
a run that stops resumes where it left off.
"""

import argparse
import csv
import re
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from datetime import time as dtime
from pathlib import Path
from zoneinfo import ZoneInfo

import polars as pl
import yaml

PLANT_TZ = ZoneInfo("America/Denver")
QUESTIONS = Path(__file__).with_name("plant_questions.yaml")
REVIEW = Path(__file__).with_name("plant_questions_review.yaml")
ROLE = "ops-analyst"
CONFIG_C = {"PolicyMode": "ENFORCE", "PolicySet": "cedar", "Guardrail": "false"}
CANARY = "q04"
CANARY_QUESTION = "How many hooks did the powder coat line hang on Friday, 2026-09-25?"
# The machines the agent's tools cover (agent_tools.tools' machine list).
TOOL_MACHINES = ("LSR-01", "PB-01", "PB-02", "PEM-01", "PC-LINE")
YESTERDAY = date(2026, 9, 30)
SECOND_SHIFT = dtime(14, 30)  # a setup started from here on is 2nd shift's

GRADES = ("right", "partial", "wrong", "refused correctly", "refused wrongly", "review")
GOOD = ("right", "refused correctly")

REFUSAL = re.compile(
    r"\b(can't|cannot|can not|unable to|don't have|do not have|doesn't have|does not "
    r"have|no (data|tool|information|way|record|access)|not (available|in the data|"
    r"something i can|able to)|isn't (available|in|something)|aren't (available|in)|"
    r"doesn't (show|include|record|tell|capture|contain|cover|track)|does not (show|"
    r"include|record|tell|capture|contain|cover|track)|not tracked|outside (what|the "
    r"data)|no visibility)",
    re.IGNORECASE,
)
REASON_WORDS = {
    "MAINTENANCE": r"maint\w*|\bpm\b|preventive",
    "TOOLING": r"tool\w*|die change",
    "JAM": r"jam\w*",
    "MATERIAL_WAIT": r"material|matl|waiting on",
    "PROGRAM_ERROR": r"program\w*",
    "NO_OPERATOR": r"no operator|operator",
    "QUALITY_HOLD": r"quality|hold",
}


# --- The answer key's answers ------------------------------------------------------


def _local_day(column: str) -> pl.Expr:
    return pl.col(column).dt.convert_time_zone("America/Denver").dt.date()


def _day(*ymd: int) -> date:
    return date(*ymd)


@dataclass
class Key:
    """Everything the answers are computed from."""

    out_dir: Path
    cycles: pl.DataFrame
    states: pl.DataFrame
    operations: pl.DataFrame
    events: pl.DataFrame

    @classmethod
    def load(cls, out_dir: Path) -> "Key":
        truth = out_dir / "truth"
        return cls(
            out_dir=out_dir,
            cycles=pl.read_parquet(truth / "truth_cycles.parquet").with_columns(
                day=_local_day("completed_at")
            ),
            states=pl.read_parquet(truth / "truth_machine_states.parquet").with_columns(
                minutes=(pl.col("ended_at") - pl.col("started_at")).dt.total_seconds()
                / 60,
                day=_local_day("started_at"),
            ),
            operations=pl.read_parquet(truth / "truth_operations.parquet"),
            events=pl.read_parquet(truth / "truth_events.parquet"),
        )

    def pieces(self, machine: str, first: date, last: date) -> pl.DataFrame:
        return (
            self.cycles.filter(
                (pl.col("machine_id") == machine)
                & pl.col("day").is_between(first, last)
            )
            .group_by("day")
            .agg(pieces=pl.len(), scrapped=(~pl.col("good")).sum())
            .sort("day")
        )

    def down_minutes(self, machine: str, first: date, last: date) -> float:
        return self.states.filter(
            (pl.col("machine_id") == machine)
            & (pl.col("state") == "DOWN")
            & pl.col("day").is_between(first, last)
        )["minutes"].sum()

    def stops(self) -> pl.DataFrame:
        """Each stop as the operator saw it: DOWN chunks of one machine, reason,
        and job, with only planned stops between them, are one stop."""
        worked = self.states.filter(pl.col("state") != "PLANNED_STOP").sort(
            "machine_id", "started_at"
        )
        key = ["machine_id", "state", "reason", "job_num", "op_seq"]
        changed = pl.any_horizontal(
            [pl.col(c).ne_missing(pl.col(c).shift(1).over("machine_id")) for c in key]
        )
        return (
            worked.with_columns(stop=changed.cum_sum())
            .filter(pl.col("state") == "DOWN")
            .group_by("stop")
            .agg(
                pl.first("machine_id"),
                pl.first("reason"),
                started_at=pl.min("started_at"),
                ended_at=pl.max("ended_at"),
                down_minutes=pl.sum("minutes"),
            )
            .with_columns(day=_local_day("started_at"))
        )

    def logged(self) -> pl.DataFrame:
        """The operators' downtime log, every nightly file, in UTC."""
        rows = []
        for path in sorted((self.out_dir / "mes" / "outbound").glob("downtime_*.csv")):
            with path.open(newline="", encoding="utf-8") as f:
                for r in csv.DictReader(f):
                    rows.append(
                        {
                            "machine_id": r["machine_id"],
                            "start": _utc(r["start_time"]),
                            "end": _utc(r["end_time"]),
                        }
                    )
        return pl.DataFrame(rows)

    def margins(self) -> pl.DataFrame:
        """Each finished job: the ERP's cost and revenue (margin_gap.sql), its true
        cost, and its true brake and setup shift and release date."""
        import margin_gap

        jobs = margin_gap.score(self.out_dir).jobs.select(
            "job_num", "revenue", "erp_cost", "true_cost"
        )
        brake = self.operations.filter(pl.col("work_center") == "BRAKE").select(
            "job_num",
            "setup_hours",
            "std_setup_min",
            true_brake=pl.col("machine_id"),
            true_shift=pl.when(
                pl.col("setup_started_at")
                .dt.convert_time_zone("America/Denver")
                .dt.time()
                < SECOND_SHIFT
            )
            .then(pl.lit("1st"))
            .otherwise(pl.lit("2nd")),
        )
        released = pl.read_ndjson(
            self.out_dir / "erp" / "export" / "Jobs.jsonl"
        ).select(job_num=pl.col("JobNum"), released=pl.col("ReleaseDate").str.to_date())
        return jobs.join(brake, on="job_num").join(released, on="job_num")


def _utc(local: str) -> datetime:
    """A local time from the MES file, the earlier of a repeated hour (spec §4.2)."""
    return datetime.fromisoformat(local).replace(tzinfo=PLANT_TZ).astimezone(UTC)


def _gaps(m: pl.DataFrame) -> pl.DataFrame:
    return (
        m.group_by("true_brake", "true_shift")
        .agg(
            jobs=pl.len(),
            gap=100 * (pl.sum("true_cost") - pl.sum("erp_cost")) / pl.sum("revenue"),
            setups=pl.sum("setup_hours") / (pl.sum("std_setup_min") / 60),
        )
        .sort("gap", descending=True)
    )


def answers(out_dir: Path) -> dict[str, dict]:
    """Each question's answer from the answer key: what grading fills in, and
    `text`, the answer in words."""
    k = Key.load(out_dir)
    found: dict[str, dict] = {}

    week = k.pieces("PB-01", _day(2026, 9, 21), _day(2026, 9, 25))
    found["q01"] = {
        "values": week["pieces"].to_list(),
        "text": ", ".join(
            f"{d:%a} {n}" for d, n in week.select("day", "pieces").rows()
        ),
    }

    laser = k.pieces("LSR-01", _day(2026, 9, 17), YESTERDAY)
    tuesday = laser.filter(pl.col("day") == _day(2026, 9, 29))["pieces"][0]
    others = laser.filter(pl.col("day") != _day(2026, 9, 29))["pieces"]
    found["q02"] = {
        "value": tuesday,
        "text": f"{tuesday}, below every other day ({others.min()} to {others.max()})",
    }
    assert tuesday < others.min(), "q02's 'low' no longer holds"

    pem = k.pieces("PEM-01", _day(2026, 9, 17), YESTERDAY).sort(
        "pieces", descending=True
    )
    found["q03"] = {
        "date": pem["day"][0],
        "text": f"{pem['day'][0]:%a %b %d} ({pem['pieces'][0]})",
    }

    hooks = k.pieces("PC-LINE", _day(2026, 9, 25), _day(2026, 9, 25))["pieces"][0]
    found["q04"] = {"value": hooks, "text": str(hooks)}

    pb01, pb02 = (
        k.down_minutes(m, _day(2026, 9, 1), YESTERDAY) for m in ("PB-01", "PB-02")
    )
    winner, loser = ("PB-01", "PB-02") if pb01 > pb02 else ("PB-02", "PB-01")
    found["q05"] = {
        "winner": winner,
        "loser": loser,
        "text": f"{winner} ({max(pb01, pb02):.0f} min down against {min(pb01, pb02):.0f})",
    }

    monday = k.down_minutes("PC-LINE", _day(2026, 9, 28), _day(2026, 9, 28))
    found["q06"] = {
        "value": round(monday, 1),
        "text": f"effectively no: {monday:.1f} min down",
    }
    assert monday < 1, "q06's 'no' no longer holds"

    stops = (
        k.stops()
        .filter(
            pl.col("machine_id").is_in(TOOL_MACHINES)
            & (pl.col("day") == YESTERDAY)
            & (pl.col("down_minutes") > 5)
        )
        .sort("started_at", "machine_id")
    )  # listed in the order they happened
    log = k.logged()
    unlogged = [
        s
        for s in stops.iter_rows(named=True)
        if log.filter(
            (pl.col("machine_id") == s["machine_id"])
            & (pl.col("start") < s["ended_at"])
            & (pl.col("end") > s["started_at"])
        ).is_empty()
    ]
    found["q07"] = {
        "value": len(unlogged),
        "text": f"{len(unlogged)} of {stops.height}: "
        + ", ".join(
            f"{s['machine_id']} {s['reason']} {s['down_minutes']:.0f} min"
            for s in unlogged
        ),
    }

    september = k.stops().filter(
        (pl.col("machine_id") == "LSR-01")
        & pl.col("day").is_between(_day(2026, 9, 1), YESTERDAY)
        & (pl.col("down_minutes") > 5)
    )
    reasons = (
        september.group_by("reason")
        .agg(pl.sum("down_minutes"))
        .sort("down_minutes", descending=True)
    )
    top = reasons["reason"][0]
    found["q08"] = {
        "reason_words": REASON_WORDS[top],
        "text": ", ".join(f"{r} {m:.0f} min" for r, m in reasons.rows()),
    }

    margins = k.margins()
    september_jobs = _gaps(
        margins.filter(pl.col("released").is_between(_day(2026, 9, 1), YESTERDAY))
    )
    worst = september_jobs.row(0, named=True)
    found["q09"] = {
        "brake": worst["true_brake"],
        "shift": worst["true_shift"],
        "shift_word": {"1st": "first", "2nd": "second"}[worst["true_shift"]],
        "text": f"{worst['true_brake']} {worst['true_shift']}: the ERP's margin "
        f"{worst['gap']:.1f} points above the true one on {worst['jobs']} jobs; "
        f"setups {worst['setups']:.2f}x standard",
    }

    four_weeks = _gaps(
        margins.filter(pl.col("released").is_between(_day(2026, 9, 3), YESTERDAY))
    ).filter((pl.col("true_brake") == "PB-02") & (pl.col("true_shift") == "2nd"))
    gap = four_weeks["gap"][0]
    found["q10"] = {
        "value": round(gap, 1),
        "text": f"yes: the ERP's margin {gap:.1f} points above the true one "
        f"({four_weeks['jobs'][0]} jobs)",
    }

    last_ops = (
        k.operations.sort("op_seq")
        .group_by("job_num")
        .agg(pl.last("completed_at"), pl.last("qty_good"), pl.last("op_seq"))
        .filter(pl.col("completed_at").is_not_null())
        .with_columns(done=_local_day("completed_at"))
    )
    # The newest job finished by Sep 28: the ERP's 22:00 batch posted it by the
    # extract (a job finished on Sep 30 isn't in the ERP yet).
    job = (
        last_ops.filter(pl.col("done") <= _day(2026, 9, 28))
        .sort("job_num", descending=True)
        .row(0, named=True)
    )
    found["q11"] = {
        "job": job["job_num"],
        "value": job["qty_good"],
        "text": f"{job['job_num']}: complete on {job['done']:%b %d}, {job['qty_good']} good",
    }

    finished = last_ops.filter(pl.col("done") == YESTERDAY).height
    found["q12"] = {
        "value": finished,
        "text": f"{finished} in the answer key; no tool counts jobs finished on a day",
    }

    rates = (
        k.cycles.filter(
            (pl.col("day") == YESTERDAY) & pl.col("machine_id").is_in(TOOL_MACHINES)
        )
        .group_by("machine_id")
        .agg(rate=(~pl.col("good")).mean())
        .sort("rate", descending=True)
    )
    found["q13"] = {
        "winner": rates["machine_id"][0],
        "text": ", ".join(f"{m} {100 * r:.1f}%" for m, r in rates.rows()),
    }

    pem_maintenance = k.states.filter(
        (pl.col("machine_id") == "PEM-01")
        & (pl.col("reason") == "MAINTENANCE")
        & pl.col("day").is_between(_day(2026, 9, 17), YESTERDAY)
    ).height
    found["q14"] = {"value": pem_maintenance, "text": "none"}
    assert pem_maintenance == 0, "q14's 'none' no longer holds"

    event = {r["event"]: r for r in k.events.iter_rows(named=True)}
    found["q15"] = {
        "date": event["ramp_start"]["local_date"],
        "text": f"{event['ramp_start']['local_date']:%a %b %d} ({event['ramp_start']['detail']})",
    }
    found["q16"] = {
        "text": "nozzle wear (NOZZLE_WEAR), which the platform's data doesn't record"
    }
    found["q17"] = {
        "date": event["nozzle_changed"]["local_date"],
        "text": f"yes, since {event['nozzle_changed']['local_date']:%a %b %d} "
        f"({event['nozzle_changed']['detail']})",
    }
    aug17 = k.pieces("LSR-01", _day(2026, 8, 17), _day(2026, 8, 21))
    found["q18"] = {
        "value": aug17["scrapped"].sum(),
        "text": f"{aug17['scrapped'].sum()} of {aug17['pieces'].sum()}",
    }
    found["q19"] = {
        "text": "no tool reads the ovens' temperatures (and no drift is built until M8)"
    }
    found["q20"] = {
        "text": "no tool or data ties jobs to material lots or suppliers (M6)"
    }
    return found


def load_questions(found: dict[str, dict] | None = None) -> list[dict]:
    """The questions, with any {placeholder} in the wording filled in."""
    questions = yaml.safe_load(QUESTIONS.read_text(encoding="utf-8"))
    if found:
        for q in questions:
            q["question"] = q["question"].format(**found.get(q["id"], {}))
    return questions


# --- Grading ----------------------------------------------------------------------

_NOT_NUMBERS = re.compile(
    r"\b\d{4}-\d{2}-\d{2}\b|\b\d{1,2}:\d{2}\b|\b[A-Z]{2,4}-\d{2}\b|\bJ\d{6}(-\d)?\b"
    r"|\bCRS-\d+\b|\b\d{1,2}/\d{1,2}(/\d{2,4})?\b"
    r"|\b(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\.? \d{1,2}(st|nd|rd|th)?\b",
    re.IGNORECASE,
)
_NUMBER = re.compile(r"(?<![\w.])\d{1,3}(?:,\d{3})+(?:\.\d+)?|(?<![\w.])\d+(?:\.\d+)?")
_MONTHS = {m: i for i, m in enumerate(
    ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"], 1
)}  # fmt: skip
_ISO = re.compile(r"\b(2026)-(\d{2})-(\d{2})\b")
_NAMED = re.compile(
    r"\b(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\.? (\d{1,2})(?:st|nd|rd|th)?\b",
    re.IGNORECASE,
)
_SLASH = re.compile(r"\b(\d{1,2})/(\d{1,2})(?:/\d{2,4})?\b")


def numbers(text: str) -> list[float]:
    """The numbers in an answer, not counting dates, times, and names."""
    plain = _NOT_NUMBERS.sub(" ", text)
    return [float(n.replace(",", "")) for n in _NUMBER.findall(plain)]


def dates(text: str) -> list[date]:
    found = []
    for y, m, d in _ISO.findall(text):
        found.append(date(int(y), int(m), int(d)))
    for month, d in _NAMED.findall(text):
        found.append(date(2026, _MONTHS[month[:3].lower()], int(d)))
    for m, d in _SLASH.findall(text):
        if 1 <= int(m) <= 12 and 1 <= int(d) <= 31:
            found.append(date(2026, int(m), int(d)))
    return found


def _near(found: list[float], value: float, grade: dict, prefix: str = "") -> bool:
    if f"{prefix}tolerance" in grade:
        slack = abs(value) * grade[f"{prefix}tolerance"]
    else:
        slack = grade.get(f"{prefix}within", 0)
    return any(abs(n - value) <= slack + 1e-9 for n in found)


def _pattern(template: str, key: dict) -> re.Pattern:
    filled = template
    for name, value in key.items():
        if isinstance(value, str):
            text = value if name.endswith("_words") else re.escape(value)
            filled = filled.replace("{" + name + "}", text)
    return re.compile(filled, re.IGNORECASE)


def _days(spec: int | list[int]) -> tuple[int, int]:
    return (-spec, spec) if isinstance(spec, int) else (spec[0], spec[1])


@dataclass(frozen=True)
class Graded:
    grade: str
    detail: str


def grade(answer: str, question: dict, key: dict) -> Graded:
    """One answer against the answer key, by the question's rule."""
    g = question["grade"]
    kind = g["kind"]
    refused = bool(REFUSAL.search(answer))
    if not answer.strip():
        return Graded("wrong", "no answer")

    if kind == "refuse":
        made_up = [p for p in g.get("made_up", []) if _pattern(p, key).search(answer)]
        if made_up:
            return Graded("wrong", f"made it up: /{made_up[0]}/")
        if "value" in key and "within" in g and _near(numbers(answer), key["value"], g):
            return Graded("right", f"found {key['value']}")
        if refused:
            return Graded("refused correctly", "says the data can't tell")
        return Graded("review", "neither refused nor made up")

    if kind == "claims":
        if any(_pattern(p, key).search(answer) for p in g.get("wrong", [])):
            if all(_pattern(p, key).search(answer) for p in g["right"]):
                return Graded("review", "matches right and wrong")
            return Graded("wrong", "states the wrong claim")
        if all(_pattern(p, key).search(answer) for p in g["right"]):
            return Graded("right", "states the right claim")
        if g.get("partial") and all(
            _pattern(p, key).search(answer) for p in g["partial"]
        ):
            return Graded("partial", "the claim without its reason")
        return Graded("refused wrongly" if refused else "review", "no claim matched")

    also = not g.get("also") or bool(re.search(g["also"], answer, re.IGNORECASE))
    if kind in ("number", "numbers"):
        found = numbers(answer)
        values = key["values"] if kind == "numbers" else [key["value"]]
        hits = sum(_near(found, v, g) for v in values)
        if hits == len(values):
            return Graded("right" if also else "partial", f"{hits} of {len(values)}")
        if kind == "numbers" and hits >= g.get("partial", len(values)):
            return Graded("partial", f"{hits} of {len(values)}")
        if kind == "number" and (
            _near(found, values[0], g, "partial_")
            if ("partial_tolerance" in g or "partial_within" in g)
            else False
        ):
            return Graded("partial", f"near {values[0]}")
        if g.get("partial_if_also") and also:
            return Graded("partial", f"the claim, not the number: wanted {values}")
        if refused:
            return Graded("refused wrongly", "said it couldn't tell")
        return Graded(
            "wrong" if found else "review", f"wanted {values}, found {found[:8]}"
        )

    if kind == "date":
        found = dates(answer)
        lo, hi = _days(g["right_days"])
        plo, phi = _days(g.get("partial_days", g["right_days"]))
        offsets = [(d - key["date"]).days for d in found]
        if any(lo <= o <= hi for o in offsets):
            return Graded("right" if also else "partial", "date in range")
        if any(plo <= o <= phi for o in offsets):
            return Graded("partial", "date near")
        if refused:
            return Graded("refused wrongly", "said it couldn't tell")
        return Graded(
            "wrong" if found else "review", f"wanted {key['date']}, found {found[:6]}"
        )

    raise ValueError(f"unknown grade kind {kind}")


# --- Running ----------------------------------------------------------------------


def session_dir(out_dir: Path, run: str) -> Path:
    return out_dir / "agent" / "sessions" / "plant" / run


def check_config(session) -> None:
    """Configuration C, or nothing runs."""
    from crestline_agent.gateway import stack_outputs

    outputs = stack_outputs(session)
    got = {k: outputs.get(k, "") for k in CONFIG_C}
    if got != CONFIG_C:
        raise SystemExit(f"the deployed agent isn't configuration C: {got}")


def finished_labels(directory: Path, model: str) -> set[str]:
    from crestline_agent.loop import read

    done = set()
    for log in directory.glob("*.jsonl"):
        records = read(log)
        head = records[0] if records else {}
        mine = head.get("model", "").endswith(model) or model in log.name
        if mine and any(r["type"] == "end" for r in records):
            done.add(head.get("label"))
    return done


def run(out_dir: Path, run_name: str, model: str, repeats: int, only: set[str] | None,
        workers: int) -> None:  # fmt: skip
    import boto3

    from crestline_agent.loop import ask, model_id, read, record_decisions

    found = answers(out_dir)
    questions = {q["id"]: q for q in load_questions(found)}
    directory = session_dir(out_dir, run_name)

    check_config(boto3.Session())
    canary = ask(CANARY_QUESTION, ROLE, model, directory / "canary",
                 label=f"{CANARY}-canary")  # fmt: skip
    verdict = grade(canary.answer, questions[CANARY], found[CANARY])
    print(f"canary ({model}): {verdict.grade}: {canary.answer[:200]!r}", flush=True)
    if verdict.grade != "right":
        raise SystemExit("the canary failed; nothing else runs")

    done = finished_labels(directory, model)
    todo = [
        (q, n)
        for n in range(1, repeats + 1)
        for qid, q in questions.items()
        if (only is None or qid in only) and f"{qid}-r{n}" not in done
    ]
    print(f"{len(todo)} sessions to run ({len(done)} already done)", flush=True)

    def one(item: tuple[dict, int]):
        q, n = item
        started = time.perf_counter()
        try:
            s = ask(q["question"], ROLE, model, directory, label=f"{q['id']}-r{n}")
        except Exception as error:  # noqa: BLE001 (one session, not the run)
            print(f"{q['id']}-r{n} failed: {error!r}", flush=True)
            return None
        g = grade(s.answer, q, found[q["id"]])
        print(
            f"{q['id']}-r{n} {time.perf_counter() - started:.0f} s: {g.grade}",
            flush=True,
        )
        return s

    with ThreadPoolExecutor(workers) as pool:
        sessions = [s for s in pool.map(one, todo) if s is not None]
    print("waiting for the gateway's decision logs...", flush=True)
    for s in sessions:
        record_decisions(s.log)
    # A run that stopped (an expired login, a closed laptop) never attached
    # its finished sessions' decisions: attach them now.
    for log in directory.glob("*.jsonl"):
        records = read(log)
        mine = records and records[0].get("model", "").endswith(model_id(model))
        calls = any(r["type"] == "tool_call" for r in records)
        if mine and calls and not any(r["type"] == "decision" for r in records):
            record_decisions(log)


# --- Scoring ----------------------------------------------------------------------


def short(name: str) -> str:
    return name.split("___")[-1]


@dataclass
class Row:
    log: str
    question: str
    model: str
    repeat: str
    answer: str
    grade: str
    detail: str
    tools: list[str] = field(default_factory=list)
    tools_ok: bool = True
    denied: int = 0
    tokens_in: int = 0
    tokens_out: int = 0
    seconds: float = 0.0
    reviewed: bool = False
    reason: str = ""


def review(model_logs: list[str]) -> dict[str, dict]:
    """Hand readings by session log name; an entry for no session fails the score."""
    if not REVIEW.exists():
        return {}
    entries = yaml.safe_load(REVIEW.read_text(encoding="utf-8")) or []
    readings = {e["log"]: e for e in entries}
    stray = sorted(set(readings) - set(model_logs))
    if stray:
        raise SystemExit(f"{REVIEW.name}: no such session: {stray}")
    for e in entries:
        if e.get("grade") and e["grade"] not in GRADES:
            raise SystemExit(
                f"{REVIEW.name}: unknown grade {e['grade']!r} for {e['log']}"
            )
    return readings


def score(out_dir: Path, run_name: str) -> list[Row]:
    from crestline_agent.loop import read

    found = answers(out_dir)
    questions = {q["id"]: q for q in load_questions(found)}
    logs = sorted(session_dir(out_dir, run_name).glob("*.jsonl"))
    readings = review([log.stem for log in logs])
    rows = []
    for log in logs:
        records = read(log)
        head = records[0]
        qid, repeat = head["label"].rsplit("-r", 1)
        end = next((r for r in records if r["type"] == "end"), None)
        answer = end["answer"] if end else ""
        calls = [r for r in records if r["type"] == "tool_call"]
        used = [short(c["name"]) for c in calls]
        expected = questions[qid]["tools"]
        g = grade(answer, questions[qid], found[qid])
        turns = [r for r in records if r["type"] == "model_turn"]
        row = Row(
            log=log.stem,
            question=qid,
            model=head["model"].split(".")[-1].split(":")[0].replace("us.", ""),
            repeat=repeat,
            answer=answer,
            grade=g.grade,
            detail=g.detail,
            tools=used,
            tools_ok=all(set(group) & set(used) for group in expected),
            denied=sum(
                1
                for r in records
                if r["type"] == "decision" and r.get("decision") not in (None, "ALLOW")
            ),
            tokens_in=sum(t.get("usage", {}).get("inputTokens", 0) for t in turns),
            tokens_out=sum(t.get("usage", {}).get("outputTokens", 0) for t in turns),
            seconds=sum(t.get("ms") or 0 for t in turns + calls) / 1000,
        )
        if log.stem in readings:
            r = readings[log.stem]
            row.grade = r.get("grade") or row.grade
            row.reason = r.get("reason", "")
            row.reviewed = True
        rows.append(row)
    return rows


def report(rows: list[Row], found: dict[str, dict], questions: list[dict]) -> str:
    frame = pl.DataFrame([r.__dict__ for r in rows])
    models = sorted(frame["model"].unique())
    lines = ["# Plant-manager questions", ""]
    for m in models:
        mine = frame.filter(pl.col("model") == m)
        counts = {g: (mine["grade"] == g).sum() for g in GRADES}
        lines.append(
            f"- **{m}**: {mine.height} sessions; "
            + ", ".join(f"{g} {n}" for g, n in counts.items() if n)
            + f"; tools as expected {mine['tools_ok'].sum()} of {mine.height}; "
            f"denied calls {mine['denied'].sum()}; tokens {mine['tokens_in'].sum():,} in, "
            f"{mine['tokens_out'].sum():,} out"
        )
    lines += ["", "| question | answer key | " + " | ".join(models) + " |",
              "|---|---|" + "---|" * len(models)]  # fmt: skip
    short_grade = {"right": "R", "partial": "P", "wrong": "W", "refused correctly": "RC",
                   "refused wrongly": "RW", "review": "?"}  # fmt: skip
    for q in questions:
        cells = []
        for m in models:
            mine = frame.filter(
                (pl.col("question") == q["id"]) & (pl.col("model") == m)
            ).sort("repeat")
            cells.append(" ".join(short_grade[g] for g in mine["grade"]) or "-")
        lines.append(f"| {q['id']} {q['question'][:60]} | {found[q['id']]['text'][:70]} | "
                     + " | ".join(cells) + " |")  # fmt: skip
    misses = frame.filter(~pl.col("grade").is_in(GOOD))
    unexplained = misses.filter(pl.col("reason") == "")
    # Why they missed: the reading's category, the reason's first words.
    why = (
        misses.with_columns(why=pl.col("reason").str.extract(r"^([a-z' ]+):", 1))
        .group_by("why")
        .agg([(pl.col("model") == m).sum().alias(m) for m in models])
        .sort("why")
    )
    lines += ["", "Misses by reason:", "", "| reason | " + " | ".join(models) + " |",
              "|---|" + "---|" * len(models)]  # fmt: skip
    lines += [
        f"| {r['why'] or 'not read yet'} | "
        + " | ".join(str(r[m]) for m in models)
        + " |"
        for r in why.iter_rows(named=True)
    ]
    lines += [
        "",
        f"Misses: {misses.height}; without a written reason: {unexplained.height}",
        "",
    ]
    for r in misses.sort("question", "model", "repeat").iter_rows(named=True):
        lines.append(
            f"- {r['question']} {r['model']} r{r['repeat']} **{r['grade']}** ({r['detail']}); "
            f"tools {', '.join(r['tools']) or 'none'}; "
            + (f"reason: {r['reason']}" if r["reason"] else "NO REASON YET")
            + f" [{r['log']}]"
        )
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("answers", "run", "score"):
        sub = commands.add_parser(name)
        sub.add_argument("--out", type=Path, default=Path("out-m5"))
        if name in ("run", "score"):
            sub.add_argument("--run", required=True, help="the run's folder name")
        if name == "run":
            sub.add_argument("--model", choices=["sonnet", "nova"], required=True)
            sub.add_argument("--repeats", type=int, default=3)
            sub.add_argument("--only", help="comma-separated question IDs")
            sub.add_argument("--workers", type=int, default=4)
        if name == "score":
            sub.add_argument(
                "--answers", action="store_true", help="print every answer"
            )
    args = parser.parse_args(argv)

    if args.command == "answers":
        found = answers(args.out)
        for q in load_questions(found):
            print(f"{q['id']} {q['question']}\n    {found[q['id']]['text']}")
    elif args.command == "run":
        only = set(args.only.split(",")) if args.only else None
        run(args.out, args.run, args.model, args.repeats, only, args.workers)
    else:
        rows = score(args.out, args.run)
        found = answers(args.out)
        print(report(rows, found, load_questions(found)))
        if args.answers:
            for r in sorted(rows, key=lambda r: (r.question, r.model, r.repeat)):
                print(
                    f"\n=== {r.question} {r.model} r{r.repeat}: {r.grade} ({r.detail}) [{r.log}]"
                )
                print(r.answer)


if __name__ == "__main__":
    main()
