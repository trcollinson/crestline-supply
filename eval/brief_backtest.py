"""The morning brief's backtest (M5 step 5): every morning in the window.

The brief runs as of each working morning, seeing only what had arrived by
05:30 (queries/as_of.sql), and the backtest asks of each what a plant
manager would: what did it flag, was it real, and how much sooner than the
Monday scrap report did it see the nozzle.

    uv run python eval/brief_backtest.py run --out out-m5          # facts, every morning (DuckDB)
    uv run python eval/brief_backtest.py narrate --out out-m5 --model sonnet   # AWS: Bedrock
    uv run python eval/brief_backtest.py score --out out-m5        # the tables (local)

`run` keeps each morning's facts where the brief keeps them
(`<out>/brief/facts/<morning>.json`) and, beside them, the rows its rules
read, unrounded (`<out>/backtest/rows/<morning>/<view>.parquet`): the
threshold sweep re-applies the rules to those. `narrate` writes each
morning's brief with a model, through the number check, to
`<out>/backtest/briefs/<model>/`, logs included. `score` reads those files
and the answer key; it runs no query against the platform and calls no
model, so the tables are reproducible from what `run` and `narrate` left.

DuckDB, not Athena: step 3's look-ahead test showed the two engines give
the same facts cell for cell, and a second seed is never uploaded.
"""

import argparse
import sys
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from datetime import date, timedelta
from pathlib import Path

from crestline import query
from crestline.brief.morning import MorningFacts, cached

# The views behind the rules, saved for each morning. brief_machine_stops is
# cut to the plant day (its stops are the ones a stop alert counted).
RULE_VIEWS = {
    "brief_scrap": "select * from brief_scrap",
    "brief_stops": "select * from brief_stops",
    "brief_output": "select * from brief_output",
    "brief_margin": "select * from brief_margin",
    "brief_margin_jobs": "select * from brief_margin_jobs",
    "brief_machine_stops": (
        "select s.* from brief_machine_stops as s"
        " join brief_plant_day as p on s.work_date = p.plant_day"
    ),
}


def window(out_dir: Path) -> tuple[date, date]:
    """The backfill's first day and the morning after its last (from the MES files)."""
    days = sorted(
        date.fromisoformat(f"{p.stem[6:10]}-{p.stem[10:12]}-{p.stem[12:14]}")
        for p in (out_dir / "mes" / "outbound").glob("labor_*.csv")
    )
    if not days:
        raise SystemExit(f"no MES labor files in {out_dir}/mes/outbound")
    return days[0], days[-1] + timedelta(days=1)


def mornings(out_dir: Path) -> list[date]:
    """Every morning a brief goes out: the window's first day (nothing has
    arrived yet, so the ledger's walk ends there), then each weekday after
    it, to the morning after the last day."""
    first, end = window(out_dir)
    days = [first + timedelta(days=d) for d in range(1, (end - first).days + 1)]
    return [first, *(d for d in days if d.weekday() < 5)]


def facts_dir(out_dir: Path) -> Path:
    return out_dir / "brief" / "facts"


def rows_dir(out_dir: Path) -> Path:
    return out_dir / "backtest" / "rows"


def briefs_dir(out_dir: Path, model: str, label: str = "") -> Path:
    """One narration run's briefs: `sonnet`, or `sonnet-v2` for a labelled
    rerun (M5 step 6 reran with the second prompt beside step 5's run)."""
    return out_dir / "backtest" / "briefs" / (f"{model}-{label}" if label else model)


# --- run: the facts and the rules' rows, every morning ----------------------------


def _run_chunk(out_dir: Path, chunk: list[date]) -> list[str]:
    """One process: a DuckDB layer of its own, moved from morning to morning."""
    from crestline.brief import facts

    con = facts.connect(out_dir, chunk[0])
    done = []
    for morning in chunk:
        frame = facts.facts(con, morning)
        found = MorningFacts.from_frame(frame, morning)
        facts_dir(out_dir).mkdir(parents=True, exist_ok=True)
        (facts_dir(out_dir) / f"{morning.isoformat()}.json").write_text(
            found.to_json(), encoding="utf-8"
        )
        folder = rows_dir(out_dir) / morning.isoformat()
        folder.mkdir(parents=True, exist_ok=True)
        for view, sql in RULE_VIEWS.items():
            con.sql(sql).pl().write_parquet(folder / f"{view}.parquet")
        done.append(f"{morning} plant day {found.plant_day}: {len(found.facts)} facts")
    return done


def run(out_dir: Path, workers: int) -> None:
    # Decode the laser's Sparkplug once here, not once per process.
    query.connect_sources(out_dir).close()
    every = mornings(out_dir)
    chunks = [every[i::workers] for i in range(workers)]
    with ProcessPoolExecutor(workers) as pool:
        for lines in pool.map(_run_chunk, [out_dir] * workers, chunks):
            print("\n".join(lines), flush=True)
    print(
        f"{len(every)} mornings: facts in {facts_dir(out_dir)}, rows in {rows_dir(out_dir)}"
    )


# --- narrate: each morning's brief, with a model ----------------------------------


def _not_computed(morning: date) -> MorningFacts:
    raise SystemExit(f"no facts for {morning}: run `brief_backtest.py run` first")


def narrate(
    out_dir: Path, model: str, workers: int, only: list[date] | None, label: str = ""
) -> None:
    from crestline.brief import narrate as narration
    from crestline.brief.write import write

    facts_for = cached(_not_computed, facts_dir(out_dir))
    bedrock = narration.bedrock_client()
    targets = [
        m for m in (only or mornings(out_dir)) if facts_for(m).plant_day is not None
    ]

    def one(morning: date) -> str:
        written = write(
            morning, facts_for, briefs_dir(out_dir, model, label), model, bedrock
        )
        usage = written.narration.usage if written.narration else {}
        return (
            f"{morning} {written.version:8} {usage.get('inputTokens', '-')}/"
            f"{usage.get('outputTokens', '-')} tokens: {written.reason}"
        )

    with ThreadPoolExecutor(workers) as pool:
        for line in pool.map(one, targets):
            print(line, flush=True)


# --- the command ------------------------------------------------------------------


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("run", "narrate", "score"):
        sub = commands.add_parser(name)
        sub.add_argument("--out", type=Path, default=Path("out-m5"))
        if name == "run":
            sub.add_argument("--workers", type=int, default=4)
        if name == "narrate":
            sub.add_argument("--model", choices=["sonnet", "nova"], required=True)
            sub.add_argument("--workers", type=int, default=4)
            sub.add_argument("--only", type=date.fromisoformat, nargs="*")
            sub.add_argument("--label", default="", help="a rerun's own folder")
        if name == "score":
            sub.add_argument("--report", type=Path, help="also write the report here")
    args = parser.parse_args(argv)

    if args.command == "run":
        run(args.out, args.workers)
    elif args.command == "narrate":
        narrate(args.out, args.model, args.workers, args.only, args.label)
    else:
        import brief_scoring

        text = brief_scoring.report(brief_scoring.score(args.out))
        print(text)
        if args.report:
            args.report.write_text(text, encoding="utf-8")


if __name__ == "__main__":
    sys.exit(main())
