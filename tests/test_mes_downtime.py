"""The MES downtime file: what people write down, and how."""

import re
from datetime import UTC, datetime, timedelta

import polars as pl
import pytest
from test_backfill import TRUTH_ONLY_WORDS

from crestline.mess import MesMess, MessConfig
from crestline.sim.clock import PLANT_TZ
from crestline.sim.records import DowntimeReason, MachineState, StopReason
from crestline.sim.systems import mes

SEED = 42
LOCAL_MINUTE = re.compile(r"^\d{4}-\d\d-\d\d \d\d:\d\d:00$")
EVERYTHING = MesMess(
    min_recorded_downtime_minutes=0, downtime_unrecorded_rate=0, free_text_noise="none"
)


def at(hour: int, minute: int = 0) -> datetime:
    return datetime(2026, 7, 6, hour, minute, tzinfo=UTC)


def row(state, reason, start, end, job="J1", employee="EMP-003", machine="PB-02"):
    return {
        "machine_id": machine,
        "state": state,
        "reason": reason,
        "started_at": start,
        "ended_at": end,
        "shift": "1st",
        "job_num": job,
        "op_seq": 30,
        "employee_id": employee,
    }


def test_a_stop_cut_by_a_break_is_one_stop_to_the_operator():
    states = pl.DataFrame(
        [
            row(MachineState.RUNNING, None, at(13), at(14)),
            row(MachineState.DOWN, DowntimeReason.JAM, at(14), at(14, 5)),
            row(
                MachineState.PLANNED_STOP,
                StopReason.BREAK,
                at(14, 5),
                at(14, 15),
                job=None,
                employee=None,
            ),
            row(MachineState.DOWN, DowntimeReason.JAM, at(14, 15), at(14, 20)),
            row(
                MachineState.DOWN, DowntimeReason.TOOLING, at(14, 20), at(14, 40)
            ),  # a new reason
            row(MachineState.RUNNING, None, at(14, 40), at(15)),
            row(
                MachineState.DOWN, DowntimeReason.TOOLING, at(15), at(15, 10)
            ),  # ran in between
            row(
                MachineState.DOWN,
                DowntimeReason.NO_OPERATOR,
                at(15, 10),
                at(15, 20),
                employee=None,
            ),
            row(MachineState.IDLE, None, at(15, 20), at(16), job=None, employee=None),
        ]
    )
    stops = mes.merge_stops(states)

    assert stops.select("reason", "started_at", "ended_at", "employee_id").rows() == [
        (DowntimeReason.JAM, at(14), at(14, 20), "EMP-003"),
        (DowntimeReason.TOOLING, at(14, 20), at(14, 40), "EMP-003"),
        (DowntimeReason.TOOLING, at(15), at(15, 10), "EMP-003"),
        (DowntimeReason.NO_OPERATOR, at(15, 10), at(15, 20), "EMP-003"),  # filled in
    ]


@pytest.fixture(scope="module")
def stops(plant_run) -> pl.DataFrame:
    return mes.merge_stops(plant_run.truth["truth_machine_states"])


@pytest.fixture(scope="module")
def recorded(plant_run, reference_data) -> pl.DataFrame:
    return mes.downtime_records(plant_run.truth, reference_data, MessConfig().mes, SEED)


def minutes(frame: pl.DataFrame) -> pl.Series:
    return (pl.col("ended_at") - pl.col("started_at")).dt.total_seconds() / 60


def test_short_stops_are_never_written_down_and_long_ones_always(stops, recorded):
    long_stops = stops.filter(minutes(stops) >= 30)
    between = stops.filter((minutes(stops) >= 5) & (minutes(stops) < 30))
    written = minutes(recorded)

    assert recorded.filter(written < 5).is_empty()
    assert stops.filter(minutes(stops) < 5).height > 0  # there were short stops to skip
    assert recorded.filter(written >= 30).height == long_stops.height > 0
    share = recorded.filter((written >= 5) & (written < 30)).height / between.height
    assert 0.3 < share < 0.9  # about 60% at level 2


def test_the_thresholds_and_the_rate_are_the_knobs(plant_run, reference_data, stops):
    everything = mes.downtime_records(plant_run.truth, reference_data, EVERYTHING, SEED)
    nothing_in_between = mes.downtime_records(
        plant_run.truth, reference_data, MesMess(downtime_unrecorded_rate=1), SEED
    )
    chatty = mes.downtime_records(
        plant_run.truth, reference_data, MesMess(downtime_unrecorded_rate=0), SEED
    )

    assert everything.height == stops.height
    assert nothing_in_between.height == stops.filter(minutes(stops) >= 30).height
    assert chatty.height == stops.filter(minutes(stops) >= 5).height


def test_the_file_columns_are_what_the_spec_says(recorded, reference_data):
    employees = {e.employee_id for e in reference_data.employees}
    work_center = {wc.machine_id: wc.work_center for wc in reference_data.work_centers}

    assert (
        recorded.columns[: len(mes.DOWNTIME_FILE_COLUMNS)] == mes.DOWNTIME_FILE_COLUMNS
    )
    assert recorded["record_type"].unique().to_list() == ["N"]
    assert recorded["entered_by"].is_in(list(employees)).all()
    assert all(LOCAL_MINUTE.match(t) for t in recorded["start_time"])
    assert all(LOCAL_MINUTE.match(t) for t in recorded["end_time"])
    assert all(
        work_center[m] == wc
        for m, wc in recorded.select("machine_id", "work_center").rows()
    )
    assert set(recorded["machine_id"]) == {
        "LSR-01",
        "PB-01",
        "PB-02",
        "PEM-01",
        "PC-LINE",
    }
    ids = recorded.sort("record_id")
    assert ids["record_id"].to_list() == list(
        range(mes.FIRST_DOWNTIME_RECORD_ID, mes.FIRST_DOWNTIME_RECORD_ID + ids.height)
    )
    assert ids["ended_at"].is_sorted()  # numbered as they ended


def test_times_are_typed_to_the_nearest_minute_in_plant_time(recorded):
    for started_at, start_time in recorded.select("started_at", "start_time").rows():
        local = (
            (started_at + timedelta(seconds=30))
            .astimezone(PLANT_TZ)
            .replace(second=0, microsecond=0)
        )
        assert start_time == local.strftime("%Y-%m-%d %H:%M:%S")


def test_reason_text_is_canonical_or_the_way_people_write(
    plant_run, reference_data, recorded
):
    canonical = mes.downtime_records(
        plant_run.truth, reference_data, MesMess(free_text_noise="none"), SEED
    )
    first_names = {e.name.split()[0] for e in reference_data.employees}

    assert set(canonical["reason_text"]) <= set(mes.CANONICAL_REASON.values())
    assert canonical.select("record_id", "start_time").equals(
        recorded.select("record_id", "start_time")
    )
    texts = recorded["reason_text"].to_list()
    assert len(set(texts)) > len(set(canonical["reason_text"]))
    assert any(t is None for t in texts)
    named = [t for t in texts if t and "see " in t]
    assert named and all(t.split("see ")[1] in first_names for t in named)
    assert not any(TRUTH_ONLY_WORDS.search(t) for t in texts if t)
    assert all(
        not any(TRUTH_ONLY_WORDS.search(p) for p in phrases)
        for phrases in mes.REASON_PHRASES.values()
    )


def test_nightly_files_hold_the_stops_that_ended_that_day(
    plant_run, recorded, tmp_path
):
    days = (plant_run.last_day - plant_run.first_day).days + 1
    written = mes.write_downtime_files(
        recorded,
        plant_run.first_day,
        plant_run.last_day,
        plant_run.extracted_at,
        tmp_path,
    )

    assert len(written) == days and sum(written.values()) == recorded.height
    for path, rows in written.items():
        lines = path.read_text(encoding="utf-8").splitlines()
        assert lines[0] == ",".join(mes.DOWNTIME_FILE_COLUMNS)
        assert len(lines) == rows + 1
        for line in lines[1:]:
            assert (
                line.split(",")[5][:10]
                == f"{path.stem[9:13]}-{path.stem[13:15]}-{path.stem[15:17]}"
            )
    assert (tmp_path / "downtime_20260711.csv").read_text().splitlines() == [
        ",".join(mes.DOWNTIME_FILE_COLUMNS)
    ]

    early = mes.write_downtime_files(
        recorded,
        plant_run.first_day,
        plant_run.last_day,
        datetime.combine(
            plant_run.last_day + timedelta(days=1), datetime.min.time(), tzinfo=PLANT_TZ
        ).astimezone(UTC)
        + timedelta(hours=1),
        tmp_path / "early",
    )
    assert len(early) == days - 1  # the last night's file hasn't dropped at 01:00
