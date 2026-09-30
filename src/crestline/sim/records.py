"""What the simulation records: the answer key, in memory.

Plain dataclasses rather than pydantic models because there are hundreds of
thousands of cycle rows; the truth writer (M1 step 5) turns these into
Parquet. All timestamps are timezone-aware UTC.
"""

from dataclasses import dataclass, field
from datetime import date, datetime
from enum import StrEnum


class MachineState(StrEnum):
    IDLE = "IDLE"
    SETUP = "SETUP"
    RUNNING = "RUNNING"
    DOWN = "DOWN"
    PLANNED_STOP = "PLANNED_STOP"


BUSY_STATES = frozenset({MachineState.SETUP, MachineState.RUNNING, MachineState.DOWN})


class DowntimeReason(StrEnum):
    """Truth downtime reasons (spec §3.2)."""

    JAM = "JAM"
    TOOLING = "TOOLING"
    MATERIAL_WAIT = "MATERIAL_WAIT"
    MAINTENANCE = "MAINTENANCE"
    QUALITY_HOLD = "QUALITY_HOLD"
    NO_OPERATOR = "NO_OPERATOR"
    PROGRAM_ERROR = "PROGRAM_ERROR"
    MICROSTOP = "MICROSTOP"


class ScrapCause(StrEnum):
    """Truth scrap causes (spec §3.3)."""

    SETUP_SCRAP = "SETUP_SCRAP"
    NOZZLE_WEAR = "NOZZLE_WEAR"
    BAD_MATERIAL_FLATNESS = "BAD_MATERIAL_FLATNESS"
    OVEN_UNDERCURE = "OVEN_UNDERCURE"
    OPERATOR_ERROR = "OPERATOR_ERROR"
    RANDOM = "RANDOM"


class StopReason(StrEnum):
    """Why a machine is in PLANNED_STOP."""

    BREAK = "BREAK"
    MEAL = "MEAL"
    NO_SHIFT = "NO_SHIFT"


class JobStatus(StrEnum):
    OPEN = "OPEN"  # released, nothing started yet
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETE = "COMPLETE"


@dataclass(slots=True)
class JobRecord:
    job_num: str
    order_num: str
    part_id: str
    rev: str
    qty_ordered: int
    qty_released: int
    release_date: date
    due_date: date
    released_at: datetime
    status: JobStatus = JobStatus.OPEN
    started_at: datetime | None = None
    completed_at: datetime | None = None
    qty_completed: int = 0
    qty_scrapped: int = 0


@dataclass(slots=True)
class OperationRecord:
    job_num: str
    op_seq: int
    work_center: str
    machine_id: str
    std_setup_min: float
    std_run_min_per_piece: float
    qty_in: int
    queued_at: datetime
    setup_started_at: datetime
    run_started_at: datetime | None = None
    completed_at: datetime | None = None
    qty_good: int = 0
    qty_scrap: int = 0


@dataclass(frozen=True, slots=True)
class MachineStateRecord:
    """One row of the machine state interval table (spec §3.6)."""

    machine_id: str
    state: MachineState
    reason: str | None
    start: datetime
    end: datetime
    shift: str | None
    job_num: str | None
    op_seq: int | None
    employee_id: str | None


@dataclass(frozen=True, slots=True)
class CycleRecord:
    """One piece produced (spec §3.1): good or scrap, with the true cause."""

    machine_id: str
    job_num: str
    op_seq: int
    at: datetime
    good: bool
    scrap_cause: ScrapCause | None


@dataclass(slots=True)
class SimulationResult:
    jobs: list[JobRecord] = field(default_factory=list)
    operations: list[OperationRecord] = field(default_factory=list)
    machine_states: list[MachineStateRecord] = field(default_factory=list)
    cycles: list[CycleRecord] = field(default_factory=list)
