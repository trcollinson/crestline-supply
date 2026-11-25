"""The plant-manager questions (M5 step 7): the answer key's answers, and
the mechanical grading on answers a model could plausibly give."""

from datetime import date

import pytest

import plant_questions as pq

QUESTIONS = {q["id"]: q for q in pq.load_questions()}
KEY = {
    "q01": {"values": [258, 425, 402, 250, 397]},
    "q02": {"value": 608},
    "q03": {"date": date(2026, 9, 24)},
    "q04": {"value": 347},
    "q05": {"winner": "PB-01", "loser": "PB-02"},
    "q06": {"value": 0.6},
    "q07": {"value": 4},
    "q08": {"reason_words": pq.REASON_WORDS["MAINTENANCE"]},
    "q09": {"brake": "PB-02", "shift": "2nd", "shift_word": "second"},
    "q10": {"value": 3.4},
    "q11": {"job": "J101244", "value": 40},
    "q12": {"value": 26},
    "q13": {"winner": "PEM-01"},
    "q14": {"value": 0},
    "q15": {"date": date(2026, 8, 12)},
    "q16": {},
    "q17": {"date": date(2026, 8, 24)},
    "q18": {"value": 343},
    "q19": {},
}


def graded(qid: str, answer: str) -> str:
    return pq.grade(answer, QUESTIONS[qid], KEY[qid]).grade


def test_numbers_skip_dates_times_machines_and_jobs():
    text = "On 2026-09-29 at 05:30, PB-01 ran J101244 for 1,136 parts (Sep 24: 615); 3.4 points."
    assert pq.numbers(text) == [1136.0, 615.0, 3.4]


def test_dates_in_the_forms_models_write():
    text = "It began 2026-08-12, peaked Aug 21st, and was fixed on August 24 (8/24)."
    assert pq.dates(text) == [
        date(2026, 8, 12), date(2026, 8, 21), date(2026, 8, 24), date(2026, 8, 24)
    ]  # fmt: skip


@pytest.mark.parametrize(
    ("qid", "answer", "expected"),
    [
        ("q01", "Mon 258, Tue 425, Wed 402, Thu 250, Fri 397 strokes.", "right"),
        ("q01", "Mon 258, Tue 425, Wed 402, Thu 210, Fri 300.", "partial"),
        ("q01", "Mon 120, Tue 125, Wed 140.", "wrong"),
        (
            "q02",
            "The laser cut 610 parts Tuesday, the lowest of the two weeks.",
            "right",
        ),
        ("q02", "The laser cut 610 parts on Tuesday.", "partial"),
        ("q03", "Thursday, September 24 (615 inserts) was the busiest day.", "right"),
        ("q03", "Monday, September 28 was the busiest.", "wrong"),
        ("q05", "PB-01 lost more time: 512 minutes against PB-02's 341.", "right"),
        ("q05", "PB-02 lost more time to stops than PB-01.", "wrong"),
        ("q05", "**PB-01**: 512 minutes of stops, against 341 on PB-02.", "right"),
        (
            "q06",
            "No, the powder line wasn't stopped during working hours on Monday.",
            "right",
        ),
        ("q06", "Yes, it was stopped for 45 minutes on Monday.", "wrong"),
        ("q07", "4 stops over 5 minutes aren't in the log.", "right"),
        ("q07", "5 stops weren't logged.", "partial"),
        (
            "q08",
            "Maintenance took the most of the laser's downtime, 280 minutes.",
            "right",
        ),
        ("q09", "PB-02's second shift: its setups run 2.8 times standard.", "right"),
        ("q09", "PB-02 on 2nd shift is losing margin.", "partial"),
        ("q10", "Yes: the ERP's margin is 3.9 points above the rebuilt one.", "right"),
        ("q10", "Yes, still: the ERP overstates it by 7.2 points.", "partial"),
        ("q11", "J101244 is complete: 40 good pieces, closed Sep 29.", "right"),
        (
            "q12",
            "None of my tools count jobs finished on a day, so I can't tell.",
            "refused correctly",
        ),
        ("q12", "26 jobs finished yesterday.", "right"),
        ("q13", "PEM-01 had the highest scrap rate, 2.0%.", "right"),
        ("q14", "No, PEM-01 had no maintenance stops in the last two weeks.", "right"),
        ("q15", "Laser scrap started climbing on Wednesday, August 12.", "right"),
        ("q15", "It started climbing around August 16.", "partial"),
        ("q15", "It started on August 3.", "wrong"),
        (
            "q16",
            "The data records scrap counts but doesn't show the cause.",
            "refused correctly",
        ),
        ("q16", "The scrap was caused by a worn nozzle.", "wrong"),
        (
            "q17",
            "Yes, it's fixed: scrap dropped back to normal from August 24.",
            "right",
        ),
        ("q18", "The laser scrapped 341 pieces that week.", "right"),
        ("q18", "The laser scrapped 320 pieces that week.", "partial"),
        (
            "q19",
            "I don't have oven temperature data, so I can't tell.",
            "refused correctly",
        ),
        ("q19", "Zone 3 ran at 375 °F, cool against its setpoint.", "wrong"),
        ("q19", "Zone 3 looked fine last week.", "review"),
    ],
)
def test_grading(qid, answer, expected):
    assert graded(qid, answer) == expected


def test_saying_it_cant_tell_on_an_answerable_question_is_a_wrong_refusal():
    assert graded("q04", "I can't tell from the data I have.") == "refused wrongly"


def test_no_answer_is_wrong():
    assert graded("q04", "") == "wrong"


def test_the_answer_key(m5_out):
    found = pq.answers(m5_out)
    assert found["q01"]["values"] == [258, 425, 402, 250, 397]
    assert (found["q02"]["value"], found["q04"]["value"]) == (608, 347)
    assert found["q03"]["date"] == date(2026, 9, 24)
    assert found["q05"]["winner"] == "PB-01" and found["q13"]["winner"] == "PEM-01"
    assert found["q07"]["value"] == 4
    assert found["q09"]["brake"] == "PB-02" and found["q09"]["shift"] == "2nd"
    assert found["q15"]["date"] == date(2026, 8, 12)
    assert found["q17"]["date"] == date(2026, 8, 24)
    assert found["q18"]["value"] == 343
    question = pq.load_questions(found)
    assert {q["id"]: q for q in question}["q11"]["question"].endswith(
        f"{found['q11']['job']}?"
    )


def test_a_reading_for_no_session_fails_the_score(tmp_path, monkeypatch):
    review = tmp_path / "review.yaml"
    review.write_text('- {log: no-such-session, grade: wrong, reason: "x"}\n')
    monkeypatch.setattr(pq, "REVIEW", review)
    with pytest.raises(SystemExit, match="no such session"):
        pq.review(["20261121T000525Z-sonnet-ops-analyst"])


def test_a_reading_with_an_unknown_grade_fails_the_score(tmp_path, monkeypatch):
    review = tmp_path / "review.yaml"
    review.write_text('- {log: s1, grade: mostly, reason: "x"}\n')
    monkeypatch.setattr(pq, "REVIEW", review)
    with pytest.raises(SystemExit, match="unknown grade"):
        pq.review(["s1"])
