"""One morning's facts as the brief's writers and its check use them (M5 step 4).

brief.sql hands back a table; the template, the model, and the number check
all want the same thing from it: each fact's ID, value, unit, and baseline,
the plant day the brief is about, and the dates its margin covers. This
module turns the table into that, saves it as JSON (the facts a brief was
written from are kept next to it), and reads it back.

Values are kept at the precision the SQL rounded them to, which the unit
says (`UNIT_DECIMALS`): the template prints them that way, and the check
lets prose round them further, never sharpen them.
"""

import json
import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import polars as pl

# How many decimals brief.sql gives each unit.
UNIT_DECIMALS = {
    "pieces": 0,
    "scans": 0,
    "jobs": 0,
    "stops": 0,
    "minutes": 0,
    "working days": 0,
    "percent": 1,
    "points": 1,
    "standard errors": 1,
    "ratio": 2,
}

# brief.sql measures each baseline over the 20 working days before the
# plant day, and the margin over the jobs closed in the last 20. A number
# the brief may state, so it is a fact too, though no query makes it.
BASELINE_WORKING_DAYS = 20
CONTEXT_FACT_ID = "context:baseline:working_days"

_MARGIN_WINDOW = re.compile(r"closed (\d{4}-\d{2}-\d{2}) to (\d{4}-\d{2}-\d{2})")


@dataclass(frozen=True)
class Fact:
    fact_id: str
    family: str
    subject: str
    metric: str
    value: float
    unit: str
    baseline: float | None = None


@dataclass(frozen=True)
class MorningFacts:
    """The facts for one morning; `plant_day` is None if no one had worked yet."""

    morning: date
    plant_day: date | None
    facts: tuple[Fact, ...]
    margin_window: tuple[date, date] | None = None

    @classmethod
    def from_frame(cls, frame: pl.DataFrame, morning: date) -> "MorningFacts":
        """From brief.sql's rows (DuckDB's or Athena's, where dates are text)."""
        rows = frame.to_dicts()
        plant_day = _as_date(rows[0]["plant_day"]) if rows else None
        window = next(
            (
                (date.fromisoformat(found[1]), date.fromisoformat(found[2]))
                for row in rows
                if row["family"] == "margin"
                and (found := _MARGIN_WINDOW.search(row["source_rows"] or ""))
            ),
            None,
        )
        facts = [
            Fact(
                fact_id=row["fact_id"],
                family=row["family"],
                subject=row["subject"],
                metric=row["metric"],
                value=float(row["value"]),
                unit=row["unit"],
                baseline=None if row["baseline"] is None else float(row["baseline"]),
            )
            for row in rows
        ]
        if facts:
            facts.append(
                Fact(
                    CONTEXT_FACT_ID,
                    "context",
                    "baseline",
                    "working_days",
                    float(BASELINE_WORKING_DAYS),
                    "working days",
                )
            )
        return cls(morning, plant_day, tuple(facts), window)

    @property
    def by_id(self) -> dict[str, Fact]:
        return {fact.fact_id: fact for fact in self.facts}

    @property
    def anomaly_ids(self) -> list[str]:
        return [fact.fact_id for fact in self.facts if fact.family == "anomaly"]

    def family(self, name: str) -> list[Fact]:
        return [fact for fact in self.facts if fact.family == name]

    def to_json(self) -> str:
        return json.dumps(
            {
                "morning": self.morning.isoformat(),
                "plant_day": self.plant_day and self.plant_day.isoformat(),
                "margin_window": self.margin_window
                and [day.isoformat() for day in self.margin_window],
                "facts": [fact.__dict__ for fact in self.facts],
            },
            indent=1,
        )

    @classmethod
    def from_json(cls, text: str) -> "MorningFacts":
        data = json.loads(text)
        return cls(
            date.fromisoformat(data["morning"]),
            data["plant_day"] and date.fromisoformat(data["plant_day"]),
            tuple(Fact(**fact) for fact in data["facts"]),
            data["margin_window"]
            and tuple(date.fromisoformat(day) for day in data["margin_window"]),
        )


def cached(
    get: Callable[[date], MorningFacts], folder: Path
) -> Callable[[date], MorningFacts]:
    """`get`, keeping each morning's facts in `folder/<morning>.json`.

    A morning's facts never change once its data has arrived, so a brief
    reads an earlier morning's from the file its own brief left, and only
    computes the ones no brief was written for.
    """

    def facts_for(morning: date) -> MorningFacts:
        path = folder / f"{morning.isoformat()}.json"
        if path.exists():
            return MorningFacts.from_json(path.read_text(encoding="utf-8"))
        found = get(morning)
        folder.mkdir(parents=True, exist_ok=True)
        path.write_text(found.to_json(), encoding="utf-8")
        return found

    return facts_for


def formatted(value: float, unit: str) -> int | float:
    """A value at its unit's precision: a whole number as an int."""
    decimals = UNIT_DECIMALS.get(unit, 2)
    rounded = round(value, decimals)
    return int(rounded) if decimals == 0 else rounded


def _as_date(value: object) -> date:
    return value if isinstance(value, date) else date.fromisoformat(str(value))
