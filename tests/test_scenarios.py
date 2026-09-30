from datetime import date
from pathlib import Path

import pytest
from pydantic import ValidationError

from crestline.sim import (
    PlantConfig,
    Scenarios,
    SimulationResult,
    generate_jobs,
    load_scenarios,
    simulate,
)
from crestline.sim.config import DowntimeConfig, VariationConfig
from crestline.sim.records import MachineState

REPO_ROOT = Path(__file__).resolve().parent.parent

FIRST_DAY = date(2026, 7, 6)
LAST_RELEASE = date(2026, 7, 10)
LAST_DAY = date(2026, 7, 17)
SEED = 42


def test_repo_scenarios_file_enables_the_pb02_story():
    scenarios = load_scenarios(REPO_ROOT / "scenarios.yaml")

    assert scenarios.pb02_second_shift_setups.enabled is True
    assert scenarios.pb02_second_shift_setups.setup_multiplier == 2.8


def test_unknown_scenario_id_is_rejected(tmp_path):
    bad = tmp_path / "scenarios.yaml"
    bad.write_text("laser_nozzle_degradation:\n  enabled: true\n")

    with pytest.raises(ValidationError, match="laser_nozzle_degradation"):
        load_scenarios(bad)


def test_multiplier_applies_only_to_pb02_on_second_shift():
    multiply = Scenarios().setup_multiplier()
    day = date(2026, 7, 6)

    assert multiply("PB-02", "2nd", day) == 2.8
    assert multiply("PB-02", "1st", day) == 1.0
    assert multiply("PB-01", "2nd", day) == 1.0
    assert multiply("LSR-01", "2nd", day) == 1.0

    off = Scenarios(pb02_second_shift_setups={"enabled": False}).setup_multiplier()
    assert off("PB-02", "2nd", day) == 1.0


def setup_ratio_by_machine_and_shift(
    result: SimulationResult,
) -> dict[tuple[str, str], float]:
    """Mean actual/standard setup time, for setups that fell entirely on one machine and shift."""
    std = {(op.job_num, op.op_seq): op.std_setup_min * 60 for op in result.operations}
    segments: dict[tuple[str, int], list] = {}
    for state in result.machine_states:
        if state.state == MachineState.SETUP:
            segments.setdefault((state.job_num, state.op_seq), []).append(state)

    ratios: dict[tuple[str, str], list[float]] = {}
    for key, states in segments.items():
        where = {(s.machine_id, s.shift) for s in states}
        if len(where) != 1:
            continue  # straddled a shift change; part of it ran at the other rate
        actual = sum((s.end - s.start).total_seconds() for s in states)
        ratios.setdefault(where.pop(), []).append(actual / std[key])
    return {where: sum(r) / len(r) for where, r in ratios.items()}


def test_toggling_the_scenario_off_returns_pb02_second_shift_to_standard(
    reference_data,
):
    jobs = generate_jobs(reference_data, FIRST_DAY, LAST_RELEASE, SEED)
    on = Scenarios()
    off = Scenarios(pb02_second_shift_setups={"enabled": False})

    with_story = setup_ratio_by_machine_and_shift(
        simulate(reference_data, jobs, FIRST_DAY, LAST_DAY, SEED, scenarios=on)
    )
    without_story = setup_ratio_by_machine_and_shift(
        simulate(reference_data, jobs, FIRST_DAY, LAST_DAY, SEED, scenarios=off)
    )

    # Random variation is on (default config), so "~1x" and "~2.8x" are ranges.
    assert 2.5 <= with_story[("PB-02", "2nd")] <= 3.1
    assert 0.9 <= without_story[("PB-02", "2nd")] <= 1.15
    for control in [("PB-02", "1st"), ("PB-01", "1st"), ("PB-01", "2nd")]:
        assert 0.9 <= with_story[control] <= 1.15
        assert 0.9 <= without_story[control] <= 1.15


def test_without_variation_pb02_second_shift_setups_are_exactly_2_8x(reference_data):
    jobs = generate_jobs(reference_data, FIRST_DAY, LAST_RELEASE, SEED)
    config = PlantConfig(
        downtime=DowntimeConfig(mean_run_minutes_between_events=None),
        variation=VariationConfig(
            setup_sigma=0, machine_cycle_sigma=0, manual_cycle_sigma=0
        ),
    )

    result = simulate(
        reference_data, jobs, FIRST_DAY, LAST_DAY, SEED, config, Scenarios()
    )

    ratios = setup_ratio_by_machine_and_shift(result)
    assert ratios[("PB-02", "2nd")] == pytest.approx(2.8, abs=0.01)
    assert ratios[("PB-02", "1st")] == 1.0
    assert ratios[("PB-01", "1st")] == 1.0
    assert ratios[("PB-01", "2nd")] == 1.0
