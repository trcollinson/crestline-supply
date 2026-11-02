"""Every mess happens about as often as mess.yaml says (spec §5).

A seeded year of the plant, run in memory, and each mess counted from what
the plant's systems recorded (eval/mess_rates.py does the counting). At
level 2 every count falls within three binomial standard deviations of its
configured rate; switch a mess off, or double its rate, and its count falls
outside, so the check would notice a mess that quietly stopped, or ran hot.

A year, not the 90-day backfill: duplicate orders are rare enough (about 13
in 90 days) that a doubled rate still lands inside the 90-day band, and so
are counter resets (about 13 on one machine). A year holds about 5,200
orders and 260 working days, and every check sees doubling. The machine
messes that are common (duplicates, outages, malformed messages, unlogged
stops) are counted on the year's first 90 days, because a year of GW02
polls is two million messages; only PB-02's counter runs the whole year,
for its resets. The plant is simulated once; the MES, the ERP, and the
machine streams are rebuilt per setting, and only the part a setting
changes.
"""

from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta

import polars as pl
import pytest

import mess_rates
from crestline.mess import MessConfig
from crestline.sim import Scenarios, build_truth_tables, generate_jobs, simulate
from crestline.sim.backfill import backfill_window, extraction_time
from crestline.sim.systems import erp, machines, mes
from crestline.sim.systems.mes import PLANT_TZ

END = date(2026, 10, 1)
SLICE_DAYS = 90
LEVEL_2 = MessConfig()
RATE_KNOBS = [
    "mes.forgot_scan_off_rate",
    "erp.soft_delete_rate",
    "erp.job_split_rate",
    "machines.duplicate_rate",
    "machines.malformed_json_rate",
    "machines.outages_per_week",
    "machines.counter_reset_rate",
    "mes.downtime_unrecorded_rate",
]
SWITCHES = {
    "erp.lastmodified_skips: JobOperations.qty": "JobOperations.qty",
    "erp.lastmodified_skips: LaborDetails.post": "LaborDetails.post",
}


def as_delivered(sent: machines.Sent) -> mess_rates.Delivered:
    return sent.published_at, sent.topic, sent.payload


@pytest.fixture(scope="module")
def measure(reference_data) -> Callable[[MessConfig], dict[str, mess_rates.Rate]]:
    """Count every mess in a year's run with these settings, against level 2's rates."""
    first_day, last_day = backfill_window(365, END)
    jobs = generate_jobs(reference_data, first_day, last_day, seed=42)
    truth = build_truth_tables(
        simulate(
            reference_data, jobs, first_day, last_day, seed=42, scenarios=Scenarios()
        ),
        reference_data,
    )
    released_at = dict(truth["truth_jobs"].select("job_num", "released_at").iter_rows())
    expected = mess_rates.configured(LEVEL_2)
    cutoff = datetime.combine(
        first_day + timedelta(days=SLICE_DAYS), datetime.min.time(), tzinfo=PLANT_TZ
    ).astimezone(UTC)
    sliced = {
        "truth_jobs": truth["truth_jobs"],
        "truth_machine_states": truth["truth_machine_states"].filter(
            pl.col("started_at") < cutoff
        ),
        "truth_cycles": truth["truth_cycles"].filter(pl.col("completed_at") < cutoff),
    }
    memo: dict[str, dict[str, list]] = {}

    def plant_systems(mess_config: MessConfig) -> dict[str, list]:
        key = (
            "systems:"
            + mess_config.mes.model_dump_json()
            + mess_config.erp.model_dump_json()
        )
        if key in memo:
            return memo[key]
        scans = mes.scan_records(truth, reference_data, mess_config.mes, seed=42)
        changes = erp.build_erp(
            jobs,
            released_at,
            scans,
            reference_data,
            mess_config.erp,
            extraction_time(END),
            seed=42,
        )
        final = changes.as_of()
        local_scans = [
            (
                on.astimezone(PLANT_TZ).replace(tzinfo=None),
                off.astimezone(PLANT_TZ).replace(tzinfo=None),
            )
            for on, off in scans.select("scan_on", "scan_off").iter_rows()
        ]

        def versions(entity: str, key: tuple[str, ...]) -> list[tuple[tuple, str, str]]:
            # Each change is a row version; when it happened is its SysValidFrom.
            return [
                (
                    tuple(c.row[k] for k in key),
                    erp._timestamp(c.at),
                    c.row["LastModified"],
                )
                for c in changes.changes
                if c.entity == entity and not c.deleted and c.at is not None
            ]

        stops = mes.downtime_records(truth, reference_data, mess_config.mes, seed=42)
        recorded = {
            (machine, start, end)
            for machine, start, end in stops.select(
                "machine_id", "start_time", "end_time"
            ).rows()
        }
        memo[key] = {
            "mes.forgot_scan_off_rate": list(
                mess_rates.forgotten_scan_offs(local_scans)
            ),
            "erp.soft_delete_rate": list(
                mess_rates.duplicate_orders(
                    final["SalesOrders"], final["SalesOrderLines"]
                )
            ),
            "erp.job_split_rate": list(mess_rates.job_splits(final["Jobs"])),
            "erp.lastmodified_skips: JobOperations.qty": list(
                mess_rates.stale_stamps(
                    versions("JobOperations", ("JobNum", "OprSeq")), updates_only=True
                )
            ),
            "erp.lastmodified_skips: LaborDetails.post": list(
                mess_rates.stale_stamps(
                    versions("LaborDetails", ("LaborDtlSeq",)), updates_only=False
                )
            ),
            "mes.downtime_unrecorded_rate": list(
                mess_rates.unrecorded_downtime(
                    mess_rates.downtime_candidates(
                        truth["truth_machine_states"], mess_config, recorded
                    )
                )
            ),
        }
        return memo[key]

    def machine_systems(mess_config: MessConfig) -> dict[str, list]:
        key = "machines:" + mess_config.machines.model_dump_json()
        if key in memo:
            return memo[key]
        knobs = mess_config.machines
        streams = machines.machine_streams(
            sliced, reference_data, knobs, 42, ("pb01", "pem01", "lsr01")
        )
        delivered = {
            device: [as_delivered(s) for s in machines.in_publish_order(stream)]
            for device, stream in streams.items()
        }
        window = mess_rates.machine_window(delivered["pem01"])
        pb02_year = machines.machine_streams(
            truth, reference_data, knobs, 42, ("pb02",)
        )["pb02"]
        year = machines.machine_window(truth)
        memo[key] = {
            "machines.duplicate_rate": [
                t
                for device in delivered.values()
                for t in mess_rates.duplicate_messages(device)
            ],
            "machines.malformed_json_rate": list(
                mess_rates.malformed_messages(delivered["pb01"])
            ),
            "machines.outages_per_week": [
                *mess_rates.outage_trials(
                    mess_rates.gap_outages(delivered["pem01"]), window
                ),
                *mess_rates.outage_trials(
                    mess_rates.rebirth_outages(delivered["lsr01"]), window
                ),
            ],
            "machines.counter_reset_rate": [
                *mess_rates.counter_resets(
                    (as_delivered(s) for s in machines.in_publish_order(pb02_year)),
                    year,
                ),
                *mess_rates.counter_resets(delivered["pem01"], window),
            ],
        }
        return memo[key]

    def count(mess_config: MessConfig) -> dict[str, mess_rates.Rate]:
        trials = plant_systems(mess_config) | machine_systems(mess_config)
        return {
            knob: mess_rates.rates(knob, expected[knob], t)["all"]
            for knob, t in trials.items()
        }

    return count


def with_rate(knob: str, value: float) -> MessConfig:
    section, name = knob.split(".")
    return MessConfig(**{section: {name: value}})


def knob_value(mess: MessConfig, knob: str) -> float:
    section, name = knob.split(".")
    return getattr(getattr(mess, section), name)


def test_every_mess_happens_at_its_configured_rate_at_level_2(measure):
    measured = measure(LEVEL_2)
    assert set(measured) == set(RATE_KNOBS) | set(SWITCHES)
    for knob, rate in measured.items():
        assert rate.ok, (knob, rate.hits, rate.trials, rate.band)
        assert rate.trials > 250, knob


@pytest.mark.parametrize("knob", RATE_KNOBS)
def test_a_mess_switched_off_fails_its_check(measure, knob):
    rate = measure(with_rate(knob, 0))[knob]
    assert rate.hits == 0
    assert not rate.ok


@pytest.mark.parametrize("knob", RATE_KNOBS)
def test_a_mess_at_double_its_rate_fails_its_check(measure, knob):
    doubled = 2 * knob_value(LEVEL_2, knob)
    rate = measure(with_rate(knob, doubled))[knob]
    low, high = rate.band
    assert rate.hits > high, (knob, rate.hits, rate.band)
    assert not rate.ok
    assert low >= 0


@pytest.mark.parametrize("knob", SWITCHES)
def test_a_stamp_left_honest_fails_its_check(measure, knob):
    skips = [s for s in LEVEL_2.erp.lastmodified_skips if s != SWITCHES[knob]]
    rate = measure(MessConfig(erp={"lastmodified_skips": skips}))[knob]
    assert rate.hits == 0
    assert not rate.ok


def test_the_counts_read_the_interfaces_the_way_the_spec_describes():
    # (scan_on, scan_off) in plant local time, as the MES files write them.
    scans = [
        ("2026-07-06 14:35", "2026-07-06 23:00"),  # off at shift end
        ("2026-07-06 21:00", "2026-07-07 06:00"),  # forgotten
        ("2026-07-10 21:00", "2026-07-13 06:00"),  # forgotten over a weekend
        ("2026-07-07 06:00", "2026-07-07 09:00"),  # on at 06:00, off properly
    ]
    scans = [
        (datetime.fromisoformat(on), datetime.fromisoformat(off)) for on, off in scans
    ]
    assert list(mess_rates.forgotten_scan_offs(scans)) == [
        ("2026-07", 0, 1),
        ("2026-07", 1, 1),
        ("2026-07", 1, 1),
        ("2026-07", 0, 1),
    ]
    orders = [
        {"OrderNum": "S100001", "OrderDate": "2026-07-06"},
        {"OrderNum": "S900001", "OrderDate": "2026-07-06"},
    ]
    lines = [
        {"OrderNum": "S100001", "Deleted": False},
        {"OrderNum": "S900001", "Deleted": True},
    ]
    # One chance (the original), one duplicate.
    assert mess_rates.rates("x", 0.01, mess_rates.duplicate_orders(orders, lines))[
        "all"
    ] == mess_rates.Rate("x", 0.01, hits=1, trials=1)
    versions = [
        (("J1", 10), "2026-07-06T14:00:00Z", "2026-07-06T14:00:00Z"),  # created
        (("J1", 10), "2026-07-08T04:00:00Z", "2026-07-06T14:00:00Z"),  # stale update
        (("J1", 10), "2026-07-09T04:00:00Z", "2026-07-09T04:00:00Z"),  # honest update
    ]
    assert list(mess_rates.stale_stamps(versions, updates_only=True)) == [
        ("2026-07", 1, 1),
        ("2026-07", 0, 1),
    ]


def test_the_machine_counts_read_the_files_the_way_the_spec_describes():
    def at(seconds: float) -> datetime:
        return datetime.fromisoformat("2026-07-06T12:00:00Z") + timedelta(
            seconds=seconds
        )

    topic = "crestline/p10/gw02/pb02/cnt"
    polls = [
        (at(0), topic, {"dev": "PB02", "cnt": 65_530, "gw_ts": "a"}),
        (at(1.5), topic, {"dev": "PB02", "cnt": 65_530, "gw_ts": "a"}),  # a copy
        (at(30), topic, {"dev": "PB02", "cnt": 65_534, "gw_ts": "b"}),
        (at(60), topic, {"dev": "PB02", "cnt": 2, "gw_ts": "c"}),  # the rollover
        (at(90), topic, {"dev": "PB02", "cnt": 4, "gw_ts": "d"}),
        (
            at(600),
            topic,
            {"dev": "PB02", "cnt": 0, "gw_ts": "e"},
        ),  # after an outage, a reset
        (at(630), topic, {"dev": "PB02", "cnt": 1, "gw_ts": "f"}),
    ]
    assert list(mess_rates.duplicate_messages(polls)) == [
        ("2026-07", 0, 1),
        ("2026-07", 1, 0),
        *[("2026-07", 0, 1)] * 5,
    ]
    assert mess_rates.gap_outages(polls) == [at(90)]
    assert [t for t in mess_rates.counter_resets(polls, (at(0), at(630))) if t[1]] == [
        ("2026-07", 1, 0)
    ]
    assert list(mess_rates.outage_trials([at(90)], (at(0), at(3 * 3600)))) == [
        ("2026-07", 1, 0),
        *[("2026-07", 0, 1)] * 3,
    ]
    births = [
        (at(0), "spBv1.0/Crestline/NBIRTH/EDGE-LASER", "x"),
        (at(5), "spBv1.0/Crestline/DDATA/EDGE-LASER/LSR-01", "y"),
        (at(900), "spBv1.0/Crestline/NBIRTH/EDGE-LASER", "x"),
    ]
    assert mess_rates.rebirth_outages(births) == [at(900)]
    samples = [
        (
            at(s + late),
            "crestline/p10/gw03/pcline/process",
            {"t": int(at(s).timestamp() * 1000)},
        )
        for s, late in ((0, 0), (5, 0), (10, 300), (15, 295), (20, 0))
    ]
    samples.append((at(312), samples[2][1], samples[2][2]))  # a copy of the late one
    samples.append(
        (at(2000 + 400), samples[2][1], {"t": int(at(2000).timestamp() * 1000)})
    )
    by_publish_time = sorted(samples, key=lambda sample: sample[0])
    assert mess_rates.late_burst_outages(by_publish_time) == [at(10), at(2000)]
    assert list(
        mess_rates.malformed_messages([(at(0), "t", {"ts": 1}), (at(1), "t", '{"ts')])
    ) == [
        ("2026-07", 0, 1),
        ("2026-07", 1, 1),
    ]
