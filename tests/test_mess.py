from datetime import time
from pathlib import Path

import pytest
from pydantic import ValidationError

from crestline.mess import ERP_ENTITIES, LEVELS, MessConfig, load_mess
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
    path.write_text("mes:\n  free_text_noise: high\n")

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
    assert mess.machines.counter_rollover_bits is None
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
    assert one.mes.forgot_scan_off_rate == two.mes.forgot_scan_off_rate / 2
    assert three.mes.forgot_scan_off_rate == two.mes.forgot_scan_off_rate * 2
    assert (one.erp.throttle_rps, three.erp.throttle_rps) == (10, 2)
    assert three.erp.token_ttl_seconds == two.erp.token_ttl_seconds // 2
    for level in (one, two, three):  # the formats do not change with the level
        assert level.erp.numbers_as_strings
        assert level.erp.lastmodified_skips == two.erp.lastmodified_skips
        assert level.erp.labor_post_time == time(22, 0)
        assert level.machines.counter_rollover_bits == 16
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
