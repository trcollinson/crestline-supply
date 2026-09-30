from pathlib import Path

import pytest
from pydantic import ValidationError

from crestline.mess import MessConfig, load_mess

MESS_YAML = Path(__file__).resolve().parent.parent / "mess.yaml"


def test_mess_yaml_holds_the_m1_messes():
    mess = load_mess(MESS_YAML)

    assert mess.mes.forgot_scan_off_rate == 0.03
    assert mess.erp.labor_rounding_hours == 0.1
    assert mess.erp.labor_post_delay_hours == 24
    assert mess.machines.counter_rollover_bits == 16
    assert mess == MessConfig()  # the defaults describe the same plant


def test_a_mess_that_is_not_built_yet_is_rejected(tmp_path):
    path = tmp_path / "mess.yaml"
    path.write_text("erp:\n  throttle_rps: 5\n")

    with pytest.raises(ValidationError):
        load_mess(path)


def test_every_m1_mess_can_be_switched_off(tmp_path):
    path = tmp_path / "mess.yaml"
    path.write_text(
        "mes:\n"
        "  forgot_scan_off_rate: 0\n"
        "erp:\n"
        "  labor_rounding_hours: 0\n"
        "  labor_post_delay_hours: 0\n"
        "machines:\n"
        "  counter_rollover_bits: null\n"
    )
    mess = load_mess(path)

    assert mess.mes.forgot_scan_off_rate == 0
    assert mess.erp.labor_rounding_hours == 0
    assert mess.erp.labor_post_delay_hours == 0
    assert mess.machines.counter_rollover_bits is None
