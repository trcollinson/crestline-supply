"""Pydantic models for the static reference data in spec section 2."""

from typing import Literal

from pydantic import BaseModel, model_validator


class Customer(BaseModel):
    code: str
    name: str


class Material(BaseModel):
    material_id: str
    description: str
    cost_per_lb: float


class WorkCenterMachine(BaseModel):
    work_center: str
    machine_id: str
    description: str
    type: Literal["discrete_automated", "manual", "discrete", "continuous_process"]
    interface_generation: Literal[
        "sparkplug_b_mqtt",
        "flat_json_gw01",
        "polled_counter_gw02",
        "process_stream_gw03",
        "none",
    ]


class Rate(BaseModel):
    work_center: str
    machine_burden_per_hr: float
    labor_per_hr: float


class RoutingOp(BaseModel):
    part_id: str
    rev: str
    op_seq: int
    work_center: str
    setup_min: float
    run_min_per_piece: float


class Part(BaseModel):
    part_id: str
    rev: str
    family: str
    material_id: str
    weight_lb: float
    price: float
    routing: list[RoutingOp] = []


class Employee(BaseModel):
    employee_id: str
    name: str
    shift: Literal["1st", "2nd"]
    work_center: str


class ReferenceData(BaseModel):
    customers: list[Customer]
    materials: list[Material]
    work_centers: list[WorkCenterMachine]
    rates: list[Rate]
    parts: list[Part]
    employees: list[Employee]

    @model_validator(mode="after")
    def _check_cross_references(self) -> "ReferenceData":
        # `rates` has exactly one row per real work center, so it's the
        # canonical set that routings and employee assignments must land in.
        known_work_centers = {r.work_center for r in self.rates}
        known_materials = {m.material_id for m in self.materials}

        for part in self.parts:
            if not part.routing:
                raise ValueError(
                    f"{part.part_id} rev {part.rev} has no routing operations"
                )
            if part.material_id not in known_materials:
                raise ValueError(
                    f"{part.part_id} references unknown material {part.material_id}"
                )
            for op in part.routing:
                if op.work_center not in known_work_centers:
                    raise ValueError(
                        f"{part.part_id} rev {part.rev} op {op.op_seq} "
                        f"references unknown work center {op.work_center}"
                    )

        for employee in self.employees:
            if employee.work_center not in known_work_centers:
                raise ValueError(
                    f"{employee.employee_id} assigned to unknown work center {employee.work_center}"
                )

        return self
