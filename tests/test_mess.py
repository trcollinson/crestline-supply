from datetime import time
from pathlib import Path

import pytest
from pydantic import ValidationError

from crestline.mess import ERP_ENTITIES, LEVELS, ClockDrift, MessConfig, load_mess
from crestline.sim.systems import erp

MESS_YAML = Path(__file__).resolve().parent.parent / "mess.yaml"


def test_mess_yaml_holds_the_m1_messes():
    mess = load_mess(MESS_YAML)

    assert mess.mes.forgot_scan_off_rate == 0.03
    assert mess.erp.labor_rounding_hours == 0.1
    assert mess.erp.labor_post_time == time(22, 0)
    assert mess.machines.counter_rollover_bits == 16
    assert mess == MessConfig()  # the defaults describe the same plant


def test_a_mess_that_is_not_built_yet_is_rejected(tmp_path):
    path = tmp_path / "mess.yaml"
    path.write_text("mes:\n  files:\n    encoding: cp1252\n")

    with pytest.raises(ValidationError):
        load_mess(path)


def test_an_unquoted_time_is_rejected(tmp_path):
    # YAML reads 22:00 without quotes as the base-60 number 1320.
    path = tmp_path / "mess.yaml"
    path.write_text("erp:\n  labor_post_time: 22:00\n")

    with pytest.raises(ValidationError, match="in quotes"):
        load_mess(path)


def test_every_m1_mess_can_be_switched_off(tmp_path):
    path = tmp_path / "mess.yaml"
    path.write_text(
        "mes:\n"
        "  forgot_scan_off_rate: 0\n"
        "erp:\n"
        "  labor_rounding_hours: 0\n"
        "  labor_post_time: null\n"
        "machines:\n"
        "  counter_rollover_bits: null\n"
    )
    mess = load_mess(path)

    assert mess.mes.forgot_scan_off_rate == 0
    assert mess.erp.labor_rounding_hours == 0
    assert mess.erp.labor_post_time is None
    assert mess.machines.counter_rollover_bits is None


def test_the_repo_mess_yaml_is_level_two_with_the_spec_values():
    mess = load_mess(MESS_YAML)

    assert (mess.mess_level, mess.seed) == (2, 42)
    assert mess == MessConfig(mess_level=2)
    assert mess.erp.token_ttl_seconds == 3600
    assert mess.erp.throttle_rps == 5
    assert mess.erp.random_500_rate == 0.005
    assert mess.erp.nextlink_drift is True
    assert mess.erp.lastmodified_skips == ["JobOperations.qty", "LaborDetails.post"]
    assert mess.erp.legacy_date_format_entities == ["LaborDetails"]
    assert mess.erp.numbers_as_strings is True
    assert (mess.erp.soft_delete_rate, mess.erp.job_split_rate) == (0.01, 0.02)


def test_level_zero_is_a_clean_plant():
    mess = MessConfig(mess_level=0)

    assert mess.mes.forgot_scan_off_rate == 0
    assert mess.mes.min_recorded_downtime_minutes == 0
    assert mess.mes.downtime_unrecorded_rate == 0
    assert mess.mes.free_text_noise == "none"
    machines = mess.machines
    assert machines.counter_rollover_bits is None
    assert machines.counter_reset_rate == 0
    assert machines.clock_drift_seconds == ClockDrift(lsr_01=0, pb_01=0, gw02=0)
    assert (machines.duplicate_rate, machines.outages_per_week) == (0, 0)
    assert machines.malformed_json_rate == 0
    erp_mess = mess.erp
    assert (erp_mess.token_ttl_seconds, erp_mess.throttle_rps) == (None, None)
    assert (erp_mess.random_500_rate, erp_mess.nextlink_drift) == (0, False)
    assert (erp_mess.numbers_as_strings, erp_mess.legacy_date_format_entities) == (
        False,
        [],
    )
    assert (erp_mess.lastmodified_skips, erp_mess.soft_delete_rate) == ([], 0)
    assert (erp_mess.job_split_rate, erp_mess.labor_rounding_hours) == (0, 0)
    assert erp_mess.labor_post_time is None


def test_level_one_halves_the_rates_and_level_three_doubles_them():
    two = MessConfig(mess_level=2)
    one, three = MessConfig(mess_level=1), MessConfig(mess_level=3)

    for rate in ("random_500_rate", "soft_delete_rate", "job_split_rate"):
        assert getattr(one.erp, rate) == getattr(two.erp, rate) / 2
        assert getattr(three.erp, rate) == getattr(two.erp, rate) * 2
    for rate in ("forgot_scan_off_rate", "downtime_unrecorded_rate"):
        assert getattr(one.mes, rate) == getattr(two.mes, rate) / 2
        assert getattr(three.mes, rate) == getattr(two.mes, rate) * 2
    for rate in (
        "counter_reset_rate",
        "duplicate_rate",
        "outages_per_week",
        "malformed_json_rate",
    ):
        assert getattr(one.machines, rate) == getattr(two.machines, rate) / 2
        assert getattr(three.machines, rate) == getattr(two.machines, rate) * 2
    assert (one.erp.throttle_rps, three.erp.throttle_rps) == (10, 2)
    assert three.erp.token_ttl_seconds == two.erp.token_ttl_seconds // 2
    for level in (one, two, three):  # the formats do not change with the level
        assert level.erp.numbers_as_strings
        assert level.erp.lastmodified_skips == two.erp.lastmodified_skips
        assert level.erp.labor_post_time == time(22, 0)
        assert level.machines.counter_rollover_bits == 16
        assert level.machines.clock_drift_seconds == two.machines.clock_drift_seconds
        assert level.machines.outage_minutes == (2, 45)
        assert level.mes.min_recorded_downtime_minutes == 5
        assert level.mes.always_recorded_downtime_minutes == 30
        assert level.mes.free_text_noise == "high"
    assert (
        LEVELS[2] == {}
    )  # level 2 is the field defaults, nothing else to keep in step


def test_keys_written_in_mess_yaml_override_the_level(tmp_path):
    path = tmp_path / "mess.yaml"
    path.write_text(
        "seed: 7\nmess_level: 0\nerp:\n  job_split_rate: 0.5\n  throttle_rps: 3\n"
    )
    mess = load_mess(path)

    assert mess.seed == 7
    assert (mess.erp.job_split_rate, mess.erp.throttle_rps) == (0.5, 3)
    assert mess.erp.soft_delete_rate == 0  # the rest is still level 0
    assert mess.erp.numbers_as_strings is False


def test_a_level_that_does_not_exist_is_rejected():
    with pytest.raises(ValidationError, match="mess_level"):
        MessConfig(mess_level=4)


def test_every_new_erp_mess_can_be_switched_off(tmp_path):
    path = tmp_path / "mess.yaml"
    path.write_text(
        "erp:\n"
        "  token_ttl_seconds: null\n"
        "  throttle_rps: null\n"
        "  random_500_rate: 0\n"
        "  nextlink_drift: false\n"
        "  numbers_as_strings: false\n"
        "  legacy_date_format_entities: []\n"
        "  lastmodified_skips: []\n"
        "  soft_delete_rate: 0\n"
        "  job_split_rate: 0\n"
    )

    assert load_mess(path).erp == MessConfig(mess_level=0).erp.model_copy(
        update={"labor_rounding_hours": 0.1, "labor_post_time": time(22, 0)}
    )


def test_every_m3_mess_can_be_switched_off(tmp_path):
    """The downtime file's knobs and every machine knob, from YAML, one by one."""
    path = tmp_path / "mess.yaml"
    path.write_text(
        "mes:\n"
        "  min_recorded_downtime_minutes: 0\n"
        "  downtime_unrecorded_rate: 0\n"
        "  free_text_noise: none\n"
        "machines:\n"
        "  counter_rollover_bits: null\n"
        "  counter_reset_rate: 0\n"
        "  clock_drift_seconds:\n"
        "    LSR-01: 0\n"
        "    PB-01: 0\n"
        "    GW02: 0\n"
        "  duplicate_rate: 0\n"
        "  outages_per_week: 0\n"
        "  malformed_json_rate: 0\n"
    )
    mess = load_mess(path)
    clean = MessConfig(mess_level=0)

    assert mess.machines == clean.machines
    assert mess.mes == clean.mes.model_copy(update={"forgot_scan_off_rate": 0.03})


def test_one_clock_can_be_made_exact_on_its_own(tmp_path):
    path = tmp_path / "mess.yaml"
    path.write_text("machines:\n  clock_drift_seconds:\n    PB-01: 0\n")
    drift = load_mess(path).machines.clock_drift_seconds

    assert (drift.seconds("PB-01"), drift.seconds("LSR-01"), drift.seconds("GW02")) == (
        0,
        5,
        20,
    )


def test_the_downtime_and_outage_ranges_must_make_sense(tmp_path):
    path = tmp_path / "mess.yaml"
    for text in (
        "mes:\n  min_recorded_downtime_minutes: 40\n  always_recorded_downtime_minutes: 30\n",
        "machines:\n  outage_minutes: [45, 2]\n",
        "machines:\n  outage_minutes: [0, 2]\n",
        "machines:\n  clock_drift_seconds:\n    GW03: 1\n",
        "mes:\n  free_text_noise: medium\n",
    ):
        path.write_text(text)
        with pytest.raises(ValidationError):
            load_mess(path)


def test_the_erp_mess_names_must_be_real(tmp_path):
    path = tmp_path / "mess.yaml"
    for text in (
        "erp:\n  legacy_date_format_entities: [Widgets]\n",
        "erp:\n  lastmodified_skips: [Jobs.qty]\n",
        "erp:\n  throttle_rps: 0\n",
    ):
        path.write_text(text)
        with pytest.raises(ValidationError):
            load_mess(path)


def test_the_entity_names_match_the_erp_the_simulator_builds():
    assert set(ERP_ENTITIES) == set(erp.ENTITIES)
