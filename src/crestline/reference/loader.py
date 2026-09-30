"""CSV loading for the static reference data in `reference/`."""

import csv
from pathlib import Path

from .models import (
    Customer,
    Employee,
    Material,
    Part,
    Rate,
    ReferenceData,
    RoutingOp,
    WorkCenterMachine,
)


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def load_reference_data(reference_dir: Path) -> ReferenceData:
    """Load and cross-validate every reference table under `reference_dir`."""
    routing_ops = [
        RoutingOp(**row) for row in _read_csv(reference_dir / "routings.csv")
    ]
    routing_ops_by_part: dict[tuple[str, str], list[RoutingOp]] = {}
    for op in routing_ops:
        routing_ops_by_part.setdefault((op.part_id, op.rev), []).append(op)

    parts = []
    for row in _read_csv(reference_dir / "parts.csv"):
        ops = sorted(
            routing_ops_by_part.get((row["part_id"], row["rev"]), []),
            key=lambda op: op.op_seq,
        )
        parts.append(Part(**row, routing=ops))

    return ReferenceData(
        customers=[
            Customer(**row) for row in _read_csv(reference_dir / "customers.csv")
        ],
        materials=[
            Material(**row) for row in _read_csv(reference_dir / "materials.csv")
        ],
        work_centers=[
            WorkCenterMachine(**row)
            for row in _read_csv(reference_dir / "work_centers.csv")
        ],
        rates=[Rate(**row) for row in _read_csv(reference_dir / "rates.csv")],
        parts=parts,
        employees=[
            Employee(**row) for row in _read_csv(reference_dir / "employees.csv")
        ],
    )
