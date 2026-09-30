"""Hidden scenarios (spec §3.5): the stories the pipeline is supposed to find.

A scenario is a knob in `scenarios.yaml` that bends one part of the plant
model away from standard. The plant itself never knows *why* a setup ran
long; it only exposes seams (like `SetupMultiplier`) that scenarios plug
into. Keeping the two apart is what makes each story toggleable, which is
how we prove the pipeline found the effect rather than an artifact.

M1 ships the framework and `pb02_second_shift_setups`. Scenarios with a day
range (nozzle wear from day 41, oven drift on days 58-61, the bad lot on day
70) arrive with M3 and M6 and will add day windows then.
"""

from collections.abc import Callable
from datetime import date
from pathlib import Path
from typing import ClassVar, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field

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


class Scenarios(BaseModel):
    """Every scenario, as read from `scenarios.yaml`.

    Unknown keys are rejected so that a misspelled ID, or a scenario that is
    not built yet, fails loudly instead of silently doing nothing.
    """

    model_config = ConfigDict(extra="forbid")

    pb02_second_shift_setups: Pb02SecondShiftSetups = Field(
        default_factory=Pb02SecondShiftSetups
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
