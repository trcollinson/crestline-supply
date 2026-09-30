"""Tunable plant behaviour, with defaults that describe Crestline as spec'd.

These are facts about the *plant* (how long breaks are, how often a brake
jams), not about its systems; interface imperfections live in `mess.yaml`.
Tests override pieces of this to isolate behaviour, for example turning
downtime and variation off to check that times land exactly on standard.
"""

from datetime import time
from typing import Literal

from pydantic import BaseModel, Field, model_validator

from .records import DowntimeReason, StopReason


class Break(BaseModel):
    start: time
    minutes: int
    kind: Literal[StopReason.BREAK, StopReason.MEAL]


class ShiftSpec(BaseModel):
    name: Literal["1st", "2nd"]
    start: time
    end: time
    breaks: list[Break]

    @model_validator(mode="after")
    def _breaks_fall_inside_shift_in_order(self) -> "ShiftSpec":
        cursor = self.start
        for brk in self.breaks:
            if brk.start < cursor:
                raise ValueError(
                    f"{self.name} shift breaks overlap or are out of order"
                )
            hour, minute = divmod(
                brk.start.hour * 60 + brk.start.minute + brk.minutes, 60
            )
            cursor = time(hour, minute)
        if cursor > self.end:
            raise ValueError(f"{self.name} shift break runs past the end of the shift")
        return self


def _default_shifts() -> list[ShiftSpec]:
    # Spec §2: 1st 06:00-14:30, 2nd 14:30-23:00. Each shift gets a 10-minute
    # break, a 30-minute meal, and a 10-minute break.
    return [
        ShiftSpec(
            name="1st",
            start=time(6, 0),
            end=time(14, 30),
            breaks=[
                Break(start=time(8, 0), minutes=10, kind=StopReason.BREAK),
                Break(start=time(10, 0), minutes=30, kind=StopReason.MEAL),
                Break(start=time(12, 30), minutes=10, kind=StopReason.BREAK),
            ],
        ),
        ShiftSpec(
            name="2nd",
            start=time(14, 30),
            end=time(23, 0),
            breaks=[
                Break(start=time(16, 30), minutes=10, kind=StopReason.BREAK),
                Break(start=time(18, 30), minutes=30, kind=StopReason.MEAL),
                Break(start=time(21, 0), minutes=10, kind=StopReason.BREAK),
            ],
        ),
    ]


class ShiftCalendarConfig(BaseModel):
    shifts: list[ShiftSpec] = Field(default_factory=_default_shifts)
    # Monday=0 .. Friday=4. The spec's occasional Saturday OT waits for M3.
    working_weekdays: list[int] = [0, 1, 2, 3, 4]


class VariationConfig(BaseModel):
    """Log-normal sigma applied to standard times; 0 means exactly standard."""

    setup_sigma: float = 0.20
    machine_cycle_sigma: float = 0.10
    manual_cycle_sigma: float = 0.25


class DowntimeKind(BaseModel):
    reason: DowntimeReason
    weight: float
    min_minutes: float
    max_minutes: float


def _default_downtime_kinds() -> list[DowntimeKind]:
    return [
        DowntimeKind(
            reason=DowntimeReason.MICROSTOP, weight=55, min_minutes=0.5, max_minutes=2
        ),
        DowntimeKind(
            reason=DowntimeReason.JAM, weight=15, min_minutes=3, max_minutes=15
        ),
        DowntimeKind(
            reason=DowntimeReason.TOOLING, weight=8, min_minutes=10, max_minutes=40
        ),
        DowntimeKind(
            reason=DowntimeReason.MATERIAL_WAIT, weight=8, min_minutes=5, max_minutes=30
        ),
        DowntimeKind(
            reason=DowntimeReason.PROGRAM_ERROR, weight=5, min_minutes=5, max_minutes=20
        ),
        DowntimeKind(
            reason=DowntimeReason.MAINTENANCE, weight=4, min_minutes=30, max_minutes=120
        ),
        DowntimeKind(
            reason=DowntimeReason.QUALITY_HOLD, weight=3, min_minutes=10, max_minutes=60
        ),
        DowntimeKind(
            reason=DowntimeReason.NO_OPERATOR, weight=2, min_minutes=5, max_minutes=20
        ),
    ]


class DowntimeConfig(BaseModel):
    """Random downtime on non-manual machines; `None` turns it off.

    Events arrive as a memoryless process on *running* time, so a machine
    that runs twice as much breaks twice as often. Manual stations (DEBURR,
    ASSY) get no downtime in M1.
    """

    mean_run_minutes_between_events: float | None = 90.0
    kinds: list[DowntimeKind] = Field(default_factory=_default_downtime_kinds)


class ScrapConfig(BaseModel):
    random_rate: float = 0.01  # spec §3.3 baseline, every piece everywhere
    setup_scrap_probability: float = 0.25  # first piece after a machine setup


class PlantConfig(BaseModel):
    calendar: ShiftCalendarConfig = Field(default_factory=ShiftCalendarConfig)
    variation: VariationConfig = Field(default_factory=VariationConfig)
    downtime: DowntimeConfig = Field(default_factory=DowntimeConfig)
    scrap: ScrapConfig = Field(default_factory=ScrapConfig)
