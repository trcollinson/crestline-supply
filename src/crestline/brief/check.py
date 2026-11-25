"""The number check: every number in a brief must be one of its facts (M5 step 4).

A model writes the brief, and a model can state a number nobody measured: a
rate rounded the wrong way, a percentage it worked out itself, last week's
figure on this week's machine. M4 found that no policy reads the
answer, so this reads it. A brief fails if any number in it can't be traced
to the facts it was written from, and the template goes out instead.

The rules, in order:

1. Identifiers aren't numbers. Fact IDs cited in the text, machine names
   (`LSR-01`), and shift names (`1st`, `2nd`) are taken out first. A date
   must be one the brief is about (the morning, the plant day, the margin's
   window, the day an alert opened); the only time is 05:30, when the data
   was cut.
2. A number must match a fact cited in the same block: a paragraph, a list
   item, a heading, or a table row. So a right number on the wrong machine
   fails ("misattributed"), and so does a number with no citation near it.
3. Rounding: a number matches a fact's value or baseline if it is the value
   rounded half up to the number's own decimals, with no more decimals than
   the fact has. 10.7 may be "11%"; 875 may not be "about 900".
4. Units: a unit written after the number must be the fact's ("%" for a
   percent, "points", "minutes", "×" for a ratio, ...). A bare number may
   match any fact cited in its block.
5. Derived numbers fail: "twice", "half", "4× the baseline", a difference
   nobody measured. Small numbers spelled out ("three stops") are checked
   as digits.

What it can't see: the words around a number. Signs are ignored ("3.0
points below" matches -3.0), so the check proves every number was measured,
not that the sentence describes it the right way round.

Before any of this, `unescape_citations` reads a fact ID written with
Markdown's escaped underscores (`[stops:PB-01:stop\\_minutes]`, which renders
as `stop_minutes`) as the ID itself: Sonnet wrote them that way in 2 of 64
backtest briefs and in M5 step 8's live one, and each then failed on every
citation in it (85 orphans where 2 were real).
"""

import re
from dataclasses import dataclass, field
from datetime import date
from decimal import ROUND_HALF_UP, Decimal

from crestline.brief.morning import UNIT_DECIMALS, Fact, MorningFacts

_BRACKETED = re.compile(r"\[[^\]\n]*\]")

CUT_TIME = "05:30"  # the only time a brief may state: when its data was cut

# What may follow a number, and the unit it means. Longest first, so
# "percentage points" is points and "minutes" isn't "min" plus "utes".
_UNITS = [
    (r"percentage points?", "points"),
    (r"standard errors?", "standard errors"),
    (r"working days?", "working days"),
    (r"per ?cent", "percent"),
    (r"%", "percent"),
    (r"points?", "points"),
    (r"pts", "points"),
    (r"pp", "points"),
    (r"stop[- ]minutes?", "minutes"),
    (r"minutes?", "minutes"),
    (r"mins?", "minutes"),
    (r"pieces?", "pieces"),
    (r"parts?", "pieces"),
    (r"pcs", "pieces"),
    (r"jobs?", "jobs"),
    (r"stops?", "stops"),
    (r"scans?", "scans"),
    (r"SEs?", "standard errors"),
    (r"sigma", "standard errors"),
    (r"σ", "standard errors"),
    (r"×", "ratio"),
    (r"x", "ratio"),
    (r"days?", "working days"),
]
_UNIT = re.compile(
    r"\s*(" + "|".join(pattern for pattern, _ in _UNITS) + r")(?![A-Za-z])",
    re.IGNORECASE,
)
_NUMBER = re.compile(r"(?<![\w.])[-−+]?(?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?")

_WORDS = {
    "zero": 0, "two": 2, "three": 3, "four": 4, "five": 5, "six": 6,
    "seven": 7, "eight": 8, "nine": 9, "ten": 10, "eleven": 11,
    "twelve": 12, "thirteen": 13, "fourteen": 14, "fifteen": 15,
    "sixteen": 16, "seventeen": 17, "eighteen": 18, "nineteen": 19,
    "twenty": 20,
}  # fmt: skip
_NUMBER_WORD = re.compile(r"\b(" + "|".join(_WORDS) + r")\b", re.IGNORECASE)
# "one" is a number only when it counts something: "one stop", "one of".
_ONE = re.compile(
    r"\bone(?=\s+(?:of\b|" + "|".join(p for p, _ in _UNITS[:-3]) + r"))",
    re.IGNORECASE,
)
_DERIVED = re.compile(
    r"\b(twice|double[sd]?|doubling|triple[sd]?|tripling|quadruple[sd]?|"
    r"half|halved|\w+-?fold)\b",
    re.IGNORECASE,
)

_MACHINE = re.compile(r"\b[A-Z]{2,4}-\d{2}\b")
_IDENTIFIERS = re.compile(r"\b(?:1st|2nd|Plant 10|P10)\b")
_CITATION_LIKE = re.compile(r"[\[(`]\s*[a-z]+:[^\]\)`]*:[^\]\)`]*[\])`]")
_LIST_MARKER = re.compile(r"^(?:[-*+]|\d+[.)])\s+")

_MONTHS = {
    name: number
    for number, names in enumerate(
        [
            ("jan", "january"), ("feb", "february"), ("mar", "march"),
            ("apr", "april"), ("may",), ("jun", "june"), ("jul", "july"),
            ("aug", "august"), ("sep", "sept", "september"),
            ("oct", "october"), ("nov", "november"), ("dec", "december"),
        ],
        start=1,
    )
    for name in names
}  # fmt: skip
_ISO_DATE = re.compile(r"\b(\d{4})-(\d{2})-(\d{2})\b")
_NAMED_DATE = re.compile(
    r"\b(" + "|".join(sorted(_MONTHS, key=len, reverse=True)) + r")\.?\s+"
    r"(\d{1,2})(?:st|nd|rd|th)?(?:,?\s+(\d{4}))?\b",
    re.IGNORECASE,
)
_SLASH_DATE = re.compile(r"\b(\d{1,2})/(\d{1,2})(?:/(\d{2,4}))?\b")
_TIME = re.compile(r"\b(\d{1,2}):(\d{2})\b(?:\s*[ap]\.?m\.?)?", re.IGNORECASE)


@dataclass(frozen=True)
class Orphan:
    """A number in the brief that the facts don't account for."""

    text: str  # as written, with its unit
    reason: str  # orphan, misattributed, wrong unit, uncited, derived, date, time, unknown citation
    detail: str
    block: str  # where it is

    def __str__(self) -> str:
        return f"{self.text!r}: {self.reason} ({self.detail}) in: {self.block[:120]}"


@dataclass
class CheckResult:
    numbers: int = 0  # numbers found and checked
    orphans: list[Orphan] = field(default_factory=list)
    cited: set[str] = field(default_factory=set)

    @property
    def passed(self) -> bool:
        return not self.orphans


def unescape_citations(text: str) -> str:
    """`[a:b:c\\_d]` -> `[a:b:c_d]`: inside square brackets only, so prose is untouched."""
    return _BRACKETED.sub(lambda m: m[0].replace("\\_", "_"), text)


def check(
    text: str, facts: MorningFacts, dates: frozenset[date] = frozenset()
) -> CheckResult:
    """Check every number in `text` against `facts`.

    `dates` are dates the brief may state besides the morning, the plant
    day, and the margin's window: the days its open alerts were first seen.
    """
    allowed_dates = {facts.morning, *dates}
    if facts.plant_day:
        allowed_dates.add(facts.plant_day)
    if facts.margin_window:
        allowed_dates.update(facts.margin_window)
    by_id = facts.by_id
    ids = sorted(by_id, key=len, reverse=True)
    # The baseline window is a definition, not a measurement: in scope
    # everywhere, but only written with its unit ("20 working days").
    context = [fact for fact in facts.facts if fact.family == "context"]
    context_ids = {fact.fact_id for fact in context}

    result = CheckResult()
    for block in blocks(text):
        rest, cited = _take_citations(block, ids)
        result.cited |= cited
        rest = _take_unknown_citations(rest, block, result)
        rest = _take_dates(rest, block, facts.morning, allowed_dates, result)
        rest = _take_times(rest, block, result)
        rest = _IDENTIFIERS.sub(" ", _MACHINE.sub(" ", rest))
        for found in _DERIVED.finditer(rest):
            result.orphans.append(
                Orphan(found.group(0), "derived", "a comparison no fact states", block)
            )
        rest = _DERIVED.sub(" ", rest)
        rest = _ONE.sub("1", rest)
        rest = _NUMBER_WORD.sub(lambda m: str(_WORDS[m.group(1).lower()]), rest)
        for number in _NUMBER.finditer(rest):
            unit_found = _UNIT.match(rest, number.end())
            unit = _unit_name(unit_found.group(1)) if unit_found else None
            written = number.group(0) + (unit_found.group(0) if unit_found else "")
            result.numbers += 1
            orphan = _judge(
                number.group(0), unit, written, block,
                [by_id[i] for i in cited if i not in context_ids] + context,
                facts.facts,
            )  # fmt: skip
            if orphan:
                result.orphans.append(orphan)
    return result


def blocks(text: str) -> list[str]:
    """The text's blocks: paragraphs, list items, headings, and table rows,
    each on one line, list markers taken off."""
    found: list[str] = []
    current: list[str] = []
    for line in text.splitlines():
        stripped = line.strip()
        alone = stripped.startswith(("#", "|"))
        if current and (not stripped or alone or _LIST_MARKER.match(stripped)):
            found.append(" ".join(current))
            current = []
        if stripped:
            current.append(_LIST_MARKER.sub("", stripped))
        if alone:
            found.append(" ".join(current))
            current = []
    if current:
        found.append(" ".join(current))
    return found


def _take_citations(block: str, ids: list[str]) -> tuple[str, set[str]]:
    cited = set()
    for fact_id in ids:
        if fact_id in block:
            cited.add(fact_id)
            block = block.replace(fact_id, " ")
    return block, cited


def _take_unknown_citations(rest: str, block: str, result: CheckResult) -> str:
    for found in _CITATION_LIKE.finditer(rest):
        result.orphans.append(
            Orphan(found.group(0), "unknown citation", "no such fact", block)
        )
    return _CITATION_LIKE.sub(" ", rest)


def _take_dates(
    rest: str, block: str, morning: date, allowed: set[date], result: CheckResult
) -> str:
    def judge(found: re.Match, day: date | None) -> str:
        if day not in allowed:
            result.orphans.append(
                Orphan(found.group(0), "date", "not a day the brief is about", block)
            )
        return " "

    def iso(found: re.Match) -> str:
        return judge(found, _date(*map(int, found.groups())))

    def named(found: re.Match) -> str:
        month, day, year = found.groups()
        return judge(
            found, _date(int(year or morning.year), _MONTHS[month.lower()], int(day))
        )

    def slashed(found: re.Match) -> str:
        month, day, year = found.groups()
        year = (
            int(year) + 2000 if year and len(year) == 2 else int(year or morning.year)
        )
        return judge(found, _date(year, int(month), int(day)))

    rest = _ISO_DATE.sub(iso, rest)
    rest = _NAMED_DATE.sub(named, rest)
    return _SLASH_DATE.sub(slashed, rest)


def _take_times(rest: str, block: str, result: CheckResult) -> str:
    def judge(found: re.Match) -> str:
        hour, minute = found.group(1), found.group(2)
        if f"{int(hour):02d}:{minute}" != CUT_TIME:
            result.orphans.append(
                Orphan(found.group(0), "time", f"only {CUT_TIME} is a fact", block)
            )
        return " "

    return _TIME.sub(judge, rest)


def _judge(
    number: str,
    unit: str | None,
    written: str,
    block: str,
    cited: list[Fact],
    every: tuple[Fact, ...],
) -> Orphan | None:
    """None if the number is one of the cited facts; why not, if it isn't."""
    plain = number.replace(",", "").replace("−", "-").lstrip("+-")
    value = Decimal(plain)
    decimals = len(plain.split(".")[1]) if "." in plain else 0

    def matches(fact: Fact, any_unit: bool = False) -> bool:
        if fact.family == "context" and unit is None:
            return False  # "20" alone isn't the baseline window; "20 working days" is
        if not any_unit and unit is not None and unit != fact.unit:
            return False
        if decimals > UNIT_DECIMALS.get(fact.unit, 2):
            return False
        return any(
            candidate is not None and _rounded(candidate, decimals) == value
            for candidate in (fact.value, fact.baseline)
        )

    if any(matches(fact) for fact in cited):
        return None
    measured = [fact for fact in cited if fact.family != "context"]
    if unit is not None and (wrong := [f for f in measured if matches(f, True)]):
        return Orphan(
            written, "wrong unit", f"{wrong[0].fact_id} is in {wrong[0].unit}", block
        )
    elsewhere = [fact.fact_id for fact in every if fact not in cited and matches(fact)]
    if not measured:
        detail = f"matches {elsewhere[0]}" if elsewhere else "no fact matches"
        return Orphan(written, "uncited", detail, block)
    if elsewhere:
        return Orphan(
            written, "misattributed", f"matches {elsewhere[0]}, not cited here", block
        )
    return Orphan(written, "orphan", "no fact has this value", block)


def _rounded(value: float, decimals: int) -> Decimal:
    exponent = Decimal(1).scaleb(-decimals)
    return abs(Decimal(repr(value))).quantize(exponent, rounding=ROUND_HALF_UP)


def _unit_name(written: str) -> str:
    for pattern, name in _UNITS:
        if re.fullmatch(pattern, written, re.IGNORECASE):
            return name
    raise ValueError(written)


def _date(year: int, month: int, day: int) -> date | None:
    try:
        return date(year, month, day)
    except ValueError:
        return None
