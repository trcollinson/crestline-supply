"""Hidden scenarios (spec §3.5): the stories the pipeline is supposed to find.

A scenario is a knob in `scenarios.yaml` that bends one part of the plant
model away from standard. The plant itself never knows *why* a setup ran
long; it only exposes seams (like `SetupMultiplier`) that scenarios plug
into. Keeping the two apart is what makes each story toggleable, which is
how we prove the pipeline found the effect rather than an artifact.

Two are built: `pb02_second_shift_setups` (M1), which is on, and
`laser_nozzle_degradation` (M5), which is off unless a scenarios file turns it
on, so the default backfill still reproduces every number published before
it. The oven drift (M8) and the bad lot (M6) arrive with their milestones.
"""

from collections.abc import Callable
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import ClassVar, Literal, Protocol

import yaml
from pydantic import BaseModel, ConfigDict, Field

from crestline.rng import make_rng

from .clock import PLANT_TZ
from .records import ScenarioEvent, ScrapCause

# Scales the wall-clock time of a setup chunk by (machine_id, shift, local
# date). The plant calls this once per chunk, so a setup that straddles the
# 14:30 shift change is slowed only for the part that fell on 2nd shift.
SetupMultiplier = Callable[[str, str, date], float]


class Pb02SecondShiftSetups(BaseModel):
    """The slow second shift: PB-02 setups on 2nd shift take 2.8× standard.

    ERP costs the job at standard hours, so the extra machine time never
    shows up there; the affected jobs look as profitable as any other.
    """

    machine_id: ClassVar[str] = "PB-02"
    shift: ClassVar[Literal["1st", "2nd"]] = "2nd"

    enabled: bool = True
    setup_multiplier: float = 2.8

    def setup_factor(self, machine_id: str, shift: str, _local_date: date) -> float:
        if self.enabled and machine_id == self.machine_id and shift == self.shift:
            return self.setup_multiplier
        return 1.0


class MachineWear(Protocol):
    """The plant's seam for something on a machine that wears out and gets fixed.

    The plant asks it about every piece it judged good, tells it about every
    piece made, and before each run of pieces asks whether a repair is due.
    It never learns what is wearing or why.
    """

    def plan(self, working_days: list[date]) -> None: ...
    def extra_scrap(self, machine_id: str, local_date: date) -> ScrapCause | None: ...
    def count_piece(self, machine_id: str, local_date: date, scrap: bool) -> None: ...
    def repair_due(self, machine_id: str, local_date: date) -> int | None: ...
    def repaired(self, machine_id: str, at: datetime, local_date: date) -> None: ...
    def events(self, last_day: date) -> list[ScenarioEvent]: ...


class LaserNozzleDegradation(BaseModel):
    """The degrading nozzle: LSR-01 scrap climbs until someone notices.

    From `start_day` (day 1 is the window's first day, so day 41 of a window
    starting Jul 3 is Wed Aug 12) a share of the laser's otherwise good pieces
    is scrapped as `NOZZLE_WEAR`, rising in a straight line from `start_rate`
    to `end_rate` over `ramp_days` calendar days and then holding there.

    Nobody watches scrap daily. On the first day in a new week that the laser
    runs, someone reads last week's scrap report; if LSR-01's scrap (every
    cause) was above `noticed_weekly_rate`, maintenance changes the nozzle
    before the next piece: a `MAINTENANCE` stop of `nozzle_change_minutes`,
    after which the wear is gone. `noticed_weekly_rate: null` means nobody
    ever notices. If the nozzle is still worn after `max_ramp_working_days`
    working days, the ramp ends there anyway (a guard, so a scenario can
    never run unbounded; the answer key says which ending happened).

    The draws come from their own `nozzle` stream, and only for pieces the
    plant has already judged good, so the plant's own draws are untouched
    until the first piece the nozzle scraps changes what happens downstream.
    """

    machine_id: ClassVar[str] = "LSR-01"

    enabled: bool = False
    start_day: int = Field(default=41, ge=1)
    ramp_days: int = Field(default=10, ge=1)
    start_rate: float = Field(default=0.01, ge=0, le=1)
    end_rate: float = Field(default=0.09, ge=0, le=1)
    noticed_weekly_rate: float | None = Field(default=0.05, gt=0, le=1)
    nozzle_change_minutes: tuple[float, float] = (30, 90)
    max_ramp_working_days: int | None = Field(default=15, ge=1)

    def wear(self, seed: int, first_day: date) -> "NozzleWear | None":
        """The scenario's state for one run, or None if it is switched off."""
        return NozzleWear(self, seed, first_day) if self.enabled else None


class NozzleWear:
    """One run of the nozzle scenario: the ramp, the weekly report, the fix."""

    def __init__(
        self, config: LaserNozzleDegradation, seed: int, first_day: date
    ) -> None:
        self.config = config
        self.rng = make_rng(seed, stream="nozzle")
        self.start = first_day + timedelta(days=config.start_day - 1)
        self.peak = self.start + timedelta(days=config.ramp_days - 1)
        self.last_worn_day: date | None = None  # the cap, set by plan()
        self.changed_at: datetime | None = None
        self.change_detail = ""
        self._pieces_by_week: dict[tuple[int, int], list[int]] = {}
        self._week_reviewed: tuple[int, int] | None = None

    def plan(self, working_days: list[date]) -> None:
        cap = self.config.max_ramp_working_days
        worn_days = [day for day in working_days if day >= self.start]
        if cap is not None and len(worn_days) >= cap:
            self.last_worn_day = worn_days[cap - 1]

    def rate_on(self, local_date: date) -> float:
        """The scheduled `NOZZLE_WEAR` rate on a day, if the nozzle were never changed."""
        if not self._scheduled(local_date):
            return 0.0
        config = self.config
        days_in = (local_date - self.start).days
        progress = (
            min(1.0, days_in / (config.ramp_days - 1)) if config.ramp_days > 1 else 1.0
        )
        return config.start_rate + (config.end_rate - config.start_rate) * progress

    def _scheduled(self, local_date: date) -> bool:
        return local_date >= self.start and (
            self.last_worn_day is None or local_date <= self.last_worn_day
        )

    def _worn(self, machine_id: str, local_date: date) -> bool:
        return (
            machine_id == self.config.machine_id
            and self.changed_at is None
            and self._scheduled(local_date)
        )

    def extra_scrap(self, machine_id: str, local_date: date) -> ScrapCause | None:
        if self._worn(machine_id, local_date) and (
            self.rng.random() < self.rate_on(local_date)
        ):
            return ScrapCause.NOZZLE_WEAR
        return None

    def count_piece(self, machine_id: str, local_date: date, scrap: bool) -> None:
        """Keep the laser's weekly scrap report (every cause), as the plant would."""
        if machine_id == self.config.machine_id:
            week = self._pieces_by_week.setdefault(_week(local_date), [0, 0])
            week[0] += 1
            week[1] += scrap

    def repair_due(self, machine_id: str, local_date: date) -> int | None:
        """Seconds of nozzle change, the first time the laser runs in a week that follows a bad one."""
        if machine_id != self.config.machine_id:
            return None
        this_week = _week(local_date)
        if this_week == self._week_reviewed:
            return None
        self._week_reviewed = this_week
        threshold = self.config.noticed_weekly_rate
        if threshold is None or not self._worn(machine_id, local_date):
            return None
        cut, scrap = self._pieces_by_week.get(
            _week(local_date - timedelta(days=7)), [0, 0]
        )
        if cut == 0 or scrap / cut <= threshold:
            return None
        self.change_detail = (
            f"last week's {machine_id} scrap was {scrap / cut:.1%} "
            f"({scrap} of {cut} pieces), over {threshold:.0%}: nozzle changed"
        )
        low, high = self.config.nozzle_change_minutes
        return max(1, round(self.rng.uniform(low, high) * 60))

    def repaired(self, machine_id: str, at: datetime, local_date: date) -> None:
        self.changed_at = at

    def events(self, last_day: date) -> list[ScenarioEvent]:
        """When the ramp started, peaked, and ended, for the answer key."""
        name = "laser_nozzle_degradation"
        machine = self.config.machine_id
        events: list[ScenarioEvent] = []
        if self.start > last_day:
            return events
        events.append(
            ScenarioEvent(
                name, "ramp_start", machine, _midnight_utc(self.start), self.start,
                f"NOZZLE_WEAR rate {self.config.start_rate:.0%}, rising to "
                f"{self.config.end_rate:.0%} on {self.peak}",
            )
        )  # fmt: skip
        ended = (
            self.changed_at
            if self.changed_at is not None
            else _midnight_utc(self.last_worn_day + timedelta(days=1))
            if self.last_worn_day is not None and self.last_worn_day < last_day
            else None
        )
        peak_at = _midnight_utc(self.peak)
        if self.peak <= last_day and (ended is None or peak_at < ended):
            events.append(
                ScenarioEvent(
                    name, "peak", machine, peak_at, self.peak,
                    f"NOZZLE_WEAR rate {self.config.end_rate:.0%}",
                )
            )  # fmt: skip
        if self.changed_at is not None:
            events.append(
                ScenarioEvent(
                    name, "nozzle_changed", machine, self.changed_at,
                    self.changed_at.astimezone(PLANT_TZ).date(), self.change_detail,
                )
            )  # fmt: skip
        elif ended is not None:
            assert self.last_worn_day is not None
            events.append(
                ScenarioEvent(
                    name, "ramp_capped", machine, ended, self.last_worn_day,
                    f"nobody noticed in {self.config.max_ramp_working_days} working days",
                )
            )  # fmt: skip
        return events


def _week(local_date: date) -> tuple[int, int]:
    year, week, _ = local_date.isocalendar()
    return year, week


def _midnight_utc(local_date: date) -> datetime:
    return datetime.combine(local_date, time(0), tzinfo=PLANT_TZ).astimezone(UTC)


class Scenarios(BaseModel):
    """Every scenario, as read from `scenarios.yaml`.

    Unknown keys are rejected so that a misspelled ID, or a scenario that is
    not built yet, fails loudly instead of silently doing nothing.
    """

    model_config = ConfigDict(extra="forbid")

    pb02_second_shift_setups: Pb02SecondShiftSetups = Field(
        default_factory=Pb02SecondShiftSetups
    )
    laser_nozzle_degradation: LaserNozzleDegradation = Field(
        default_factory=LaserNozzleDegradation
    )

    def setup_multiplier(self) -> SetupMultiplier:
        """Compose every scenario's effect on setup time into one plant hook."""

        def multiply(machine_id: str, shift: str, local_date: date) -> float:
            return self.pb02_second_shift_setups.setup_factor(
                machine_id, shift, local_date
            )

        return multiply


def load_scenarios(path: Path) -> Scenarios:
    with path.open(encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return Scenarios(**data)
