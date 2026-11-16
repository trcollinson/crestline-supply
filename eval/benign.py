"""Ask the agent the benign questions and check each session (M4 step 5).

    uv run python eval/benign.py [--model sonnet|nova] [--role ROLE] [--only q01,q07] [--config C]

Each question in `benign_questions.yaml` is one agent session through the
deployed gateway, logged to `out/agent/sessions/benign/<run>/`. A session
passes if:

- it ended with an answer (`end_turn`, some text), not out of turns;
- for each of the question's tool groups, it called one of those tools and
  the tool answered (the call wasn't refused by the gateway or the Lambda);
- the policy engine denied none of its calls (read from the gateway's logs).

That says the right tools were used, not that the answer is right; step 6
checks answers against the query's own result. Every answer is printed so
a person can read them. Calls Bedrock and the gateway in AWS: a run of the
20 costs a dollar or two in tokens.
"""

import argparse
import json
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path

import yaml
from botocore.exceptions import BotoCoreError, ClientError

from crestline_agent.loop import ask, read, record_decisions

QUESTIONS = Path(__file__).with_name("benign_questions.yaml")
SESSIONS = Path("out/agent/sessions/benign")


def load(path: Path = QUESTIONS) -> list[dict]:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def short(name: str) -> str:
    """`PlantData___downtime_events` -> `downtime_events`."""
    return name.split("___")[-1]


def answered(call: dict) -> bool:
    """The tool ran and didn't refuse its arguments."""
    if call["status"] != "success":
        return False
    texts = [c.get("text", "") for c in call["result_block"]["toolResult"]["content"]]
    try:
        return not any("error" in json.loads(t) for t in texts)
    except (json.JSONDecodeError, TypeError):
        return True  # plain text: whatever it says, the tool answered


def check(question: dict, records: list[dict]) -> list[str]:
    """What's wrong with one session; [] if it passed."""
    problems = []
    end = next((r for r in records if r["type"] == "end"), None)
    if end is None:
        return ["the session never ended"]
    if end["stop"] != "end_turn" or not end["answer"]:
        problems.append(f"no answer (stopped: {end['stop']})")
    calls = [r for r in records if r["type"] == "tool_call"]
    good = {short(c["name"]) for c in calls if answered(c)}
    for group in question["tools"]:
        if not good & set(group):
            problems.append(f"no answer from {' or '.join(group)}")
    for d in (r for r in records if r["type"] == "decision"):
        if d["decision"] is None:
            problems.append(f"no decision logged for {short(d['name'])}")
        elif d["decision"] != "ALLOW":
            problems.append(f"denied {short(d['name'])}: {d['policies'] or 'default'}")
    return problems


def role_for(question: dict, role: str) -> str:
    return role if question["role"] == "both" else question["role"]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--model", default="sonnet", help="sonnet, nova, or a model ID")
    parser.add_argument(
        "--role",
        default="ops-analyst",
        choices=["ops-analyst", "maintenance-planner"],
        help="who asks the questions either role may ask",
    )
    parser.add_argument("--only", help="comma-separated question IDs")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument(
        "--config",
        help="the configuration deployed (C, D, ...), kept in the run's name",
    )
    args = parser.parse_args()

    questions = load()
    if args.only:
        wanted = set(args.only.split(","))
        questions = [q for q in questions if q["id"] in wanted]
    run = f"{datetime.now(UTC):%Y%m%dT%H%M%SZ}-{args.model}-{args.role}"
    if args.config:
        run += f"-{args.config}"
    log_dir = SESSIONS / run

    def one(question: dict):
        started = time.perf_counter()
        try:
            session = ask(
                question["question"],
                role_for(question, args.role),
                args.model,
                log_dir,
                label=question["id"],
            )
        except (BotoCoreError, ClientError, OSError) as error:  # not the whole run
            print(f"{question['id']} failed: {error!r}", flush=True)
            return question, None, time.perf_counter() - started
        print(
            f"{question['id']} done in {time.perf_counter() - started:.0f} s",
            flush=True,
        )
        return question, session, time.perf_counter() - started

    with ThreadPoolExecutor(args.workers) as pool:
        done = list(pool.map(one, questions))
    failed = [q["id"] for q, session, _ in done if session is None]
    done = [d for d in done if d[1] is not None]

    print("waiting for the gateway's decision logs...")
    for _, session, _ in done:
        record_decisions(session.log)

    rows, passed = [], 0
    for question, session, seconds in done:
        records = read(session.log)
        problems = check(question, records)
        passed += not problems
        rows.append(
            {
                "id": question["id"],
                "role": role_for(question, args.role),
                "tools": [short(c["name"]) for c in session.tool_calls],
                "turns": session.turns,
                "tokens": session.usage,
                "seconds": round(seconds),
                "problems": problems,
                "log": str(session.log),
            }
        )
        print(f"\n=== {question['id']} ({rows[-1]['role']}): {question['question']}")
        print(f"tools: {', '.join(rows[-1]['tools']) or '(none)'}")
        print(session.answer)
        print("PASS" if not problems else f"FAIL: {'; '.join(problems)}")

    (log_dir / "summary.json").write_text(json.dumps(rows, indent=2) + "\n")
    print(f"\n{'id':<5} {'role':<20} {'turns':>5} {'in':>7} {'out':>6} {'s':>4}  tools")
    for r in rows:
        print(
            f"{r['id']:<5} {r['role']:<20} {r['turns']:>5} "
            f"{r['tokens']['inputTokens']:>7} {r['tokens']['outputTokens']:>6} "
            f"{r['seconds']:>4}  {', '.join(r['tools'])}"
            + ("" if not r["problems"] else f"  FAIL: {'; '.join(r['problems'])}")
        )
    total = len(rows) + len(failed)
    print(f"\n{passed} of {total} passed; logs in {log_dir}")
    if failed:
        print(f"sessions that raised: {', '.join(failed)}")


if __name__ == "__main__":
    main()
