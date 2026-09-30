import pytest
from pydantic import ValidationError

from crestline.reference import ReferenceData


def test_load_reference_data_loads_every_table(reference_data):
    assert len(reference_data.customers) == 8
    assert len(reference_data.materials) == 4
    assert len(reference_data.work_centers) == 7
    assert len(reference_data.rates) == 6
    assert len(reference_data.parts) == 12
    assert len(reference_data.employees) == 14


def test_every_routing_op_points_to_a_real_work_center(reference_data):
    known_work_centers = {r.work_center for r in reference_data.rates}

    for part in reference_data.parts:
        assert part.routing, f"{part.part_id} rev {part.rev} has no routing operations"
        for op in part.routing:
            assert op.work_center in known_work_centers


def test_routing_op_with_unknown_work_center_is_rejected(reference_data):
    bad_part = reference_data.parts[0].model_copy(deep=True)
    bad_part.routing[0].work_center = "NOT_A_REAL_WORK_CENTER"

    with pytest.raises(ValidationError):
        ReferenceData(
            customers=reference_data.customers,
            materials=reference_data.materials,
            work_centers=reference_data.work_centers,
            rates=reference_data.rates,
            parts=[bad_part, *reference_data.parts[1:]],
            employees=reference_data.employees,
        )


def test_employees_split_across_shifts_and_real_work_centers(reference_data):
    known_work_centers = {r.work_center for r in reference_data.rates}

    shifts = {e.shift for e in reference_data.employees}
    assert shifts == {"1st", "2nd"}
    for employee in reference_data.employees:
        assert employee.work_center in known_work_centers
