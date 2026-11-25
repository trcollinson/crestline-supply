"""The number check (M5 step 4): it passes numbers the facts hold and catches
the ones they don't.

Each planted failure is one sentence a model could plausibly write: a number
nudged, a percentage nobody measured, a right number with the wrong unit or
on the wrong machine, a date or a time from nowhere, a comparison worked
out from two facts.
"""

from datetime import date

import pytest

from crestline.brief.check import blocks, check
from crestline.brief.morning import CONTEXT_FACT_ID, Fact, MorningFacts

MORNING = date(2026, 8, 20)
PLANT_DAY = date(2026, 8, 19)


def fact(fact_id: str, value: float, unit: str, baseline: float | None = None) -> Fact:
    family, subject, metric = fact_id.split(":")
    return Fact(fact_id, family, subject, metric, value, unit, baseline)


FACTS = MorningFacts(
    MORNING,
    PLANT_DAY,
    (
        fact("anomaly:LSR-01:scrap_above_baseline", 15.1, "standard errors", 3.0),
        fact("scrap:LSR-01:scrap_pct", 10.7, "percent", 2.6),
        fact("scrap:LSR-01:scrapped", 94, "pieces"),
        fact("scrap:LSR-01:pieces", 875, "pieces"),
        fact("scrap:PB-01:scrap_pct", 2.8, "percent", 1.8),
        fact("margin:PB-01 1st:gap_points", -0.8, "points"),
        fact("margin:PB-02 2nd:setup_vs_standard", 2.79, "ratio"),
        fact("margin:all jobs:jobs", 1285, "jobs"),
        fact("stops:PC-LINE:stops", 3, "stops"),
        fact("stops:PC-LINE:stops_not_logged", 2, "stops"),
        fact("stops:PC-LINE:stop_minutes", 149, "minutes", 12),
        fact(CONTEXT_FACT_ID, 20, "working days"),
    ),
    (date(2026, 7, 23), PLANT_DAY),
)

GOOD = """\
# Morning brief, Thursday 2026-08-20

About Wednesday, August 19, from the data as it stood at 05:30.

## What changed

- **New:** LSR-01 scrapped 10.7% of its pieces against a baseline of 2.6%, 15.1 standard errors above it. [anomaly:LSR-01:scrap_above_baseline] [scrap:LSR-01:scrap_pct]
- That is 94 of 875 pieces. [scrap:LSR-01:scrapped] [scrap:LSR-01:pieces]

PC-LINE stopped three times for 149 minutes, and two of those stops are not
in the log. [stops:PC-LINE:stops] [stops:PC-LINE:stop_minutes]
[stops:PC-LINE:stops_not_logged]

PB-02 2nd shift's setups ran 2.79x standard [margin:PB-02 2nd:setup_vs_standard];
PB-01 1st shift's ERP margin is 0.8 points below the rebuilt one
(margin:PB-01 1st:gap_points). Baselines are the last 20 working days.

| Jobs closed 2026-07-23 to 2026-08-19 | 1,285 | [margin:all jobs:jobs] |
"""


def orphans(text: str, dates=frozenset()) -> list[tuple[str, str]]:
    return [(o.text.strip(), o.reason) for o in check(text, FACTS, dates).orphans]


def test_a_brief_that_states_only_facts_passes():
    result = check(GOOD, FACTS)
    assert result.passed, [str(o) for o in result.orphans]
    assert result.numbers == 12  # dates, 05:30, machine and shift names are not numbers
    assert "margin:PB-01 1st:gap_points" in result.cited


@pytest.mark.parametrize(
    ("sentence", "found"),
    [
        # A number nudged.
        ("LSR-01 scrapped 10.9%. [scrap:LSR-01:scrap_pct]", ("10.9%", "orphan")),
        # A percentage nobody measured, next to a real one.
        (
            "LSR-01 scrapped 10.7%, 12% of it on 2nd shift. [scrap:LSR-01:scrap_pct]",
            ("12%", "orphan"),
        ),
        # The right number, the wrong unit.
        (
            "LSR-01's scrap is up 10.7 points. [scrap:LSR-01:scrap_pct]",
            ("10.7 points", "wrong unit"),
        ),
        # The right number on the wrong machine.
        ("PB-01 scrapped 10.7%. [scrap:PB-01:scrap_pct]", ("10.7%", "misattributed")),
        # A number with no citation near it.
        ("LSR-01 scrapped 10.7% yesterday.", ("10.7%", "uncited")),
        # Worked out from two facts.
        (
            "LSR-01 scrapped 4.1× its baseline. [scrap:LSR-01:scrap_pct]",
            ("4.1×", "orphan"),
        ),
        (
            "LSR-01's scrap more than quadrupled. [scrap:LSR-01:scrap_pct]",
            ("quadrupled", "derived"),
        ),
        (
            "PC-LINE stopped twice as long as usual. [stops:PC-LINE:stop_minutes]",
            ("twice", "derived"),
        ),
        # Spelled out, and wrong.
        ("PC-LINE had four stops. [stops:PC-LINE:stops]", ("4 stops", "orphan")),
        # Rounded too far, or sharpened.
        (
            "LSR-01 made about 900 pieces. [scrap:LSR-01:pieces]",
            ("900 pieces", "orphan"),
        ),
        ("LSR-01 scrapped 10.70%. [scrap:LSR-01:scrap_pct]", ("10.70%", "orphan")),
        # A date and a time from nowhere.
        (
            "The nozzle started wearing on August 12. [scrap:LSR-01:scrap_pct]",
            ("August 12", "date"),
        ),
        ("First shift starts at 06:00. [scrap:LSR-01:scrap_pct]", ("06:00", "time")),
        # A citation of a fact that doesn't exist.
        (
            "LSR-01 scrapped a lot. [scrap:LSR-01:scrap_rate]",
            ("[scrap:LSR-01:scrap_rate]", "unknown citation"),
        ),
        # The baseline window is only a fact with its unit.
        ("Baselines go back 20.", ("20", "uncited")),
    ],
    ids=lambda value: value if isinstance(value, str) else value[1],
)
def test_the_check_catches_a_number_the_facts_dont_hold(sentence, found):
    assert orphans(sentence) == [found]


@pytest.mark.parametrize(
    "sentence",
    [
        "LSR-01 scrapped 11%. [scrap:LSR-01:scrap_pct]",  # rounded to whole
        "LSR-01's baseline is 3%. [scrap:LSR-01:scrap_pct]",  # a baseline, rounded
        "PB-01 1st: ERP 1 point under rebuilt. [margin:PB-01 1st:gap_points]",
        "PB-01 1st: -0.8 points. [margin:PB-01 1st:gap_points]",
        "PB-02 2nd's setups took 2.8 times standard. [margin:PB-02 2nd:setup_vs_standard]",
        "2 of PC-LINE's 3 stops weren't logged. [stops:PC-LINE:stops_not_logged] [stops:PC-LINE:stops]",
        "The rule: more than 3.0 standard errors. [anomaly:LSR-01:scrap_above_baseline]",
        "PC-LINE: 149 stop-minutes. [stops:PC-LINE:stop_minutes]",
        "A day behind, as the ERP closes jobs at night.",  # no number at all
    ],
)
def test_the_check_allows_rounding_signs_and_baselines(sentence):
    assert orphans(sentence) == []


def test_one_counts_only_when_it_counts_something():
    assert orphans("No one logged one stop. [stops:PC-LINE:stops]") == [
        ("1 stop", "orphan")
    ]


def test_dates_the_ledger_names_are_allowed():
    sentence = "Open since 2026-08-13. [scrap:LSR-01:scrap_pct]"
    assert orphans(sentence) == [("2026-08-13", "date")]
    assert orphans(sentence, frozenset({date(2026, 8, 13)})) == []


def test_a_block_is_a_paragraph_a_list_item_a_heading_or_a_table_row():
    text = "# Title 1\nline one\ncontinues\n\n- item\n  wrapped\n1. numbered\n| a | b |\n| c | d |"
    assert blocks(text) == [
        "# Title 1",
        "line one continues",
        "item wrapped",
        "numbered",
        "| a | b |",
        "| c | d |",
    ]


def test_a_citation_covers_only_its_own_block():
    text = "LSR-01 scrapped 10.7%.\n\nSee [scrap:LSR-01:scrap_pct]."
    assert orphans(text) == [("10.7%", "uncited")]


def test_unescape_citations_touches_only_bracketed_text():
    from crestline.brief.check import unescape_citations

    text = "a\\_b [stops:PB-01:stop\\_minutes, scrap:LSR-01:scrap\\_pct] c\\_d"
    assert unescape_citations(text) == (
        "a\\_b [stops:PB-01:stop_minutes, scrap:LSR-01:scrap_pct] c\\_d"
    )
