"""Interface imperfections, read from `mess.yaml` (spec §5).

The plant simulation decides what really happened; these settings decide how
imperfectly each system records it. Every mess can be switched off on its
own, which is how the tests show that a pipeline handles it rather than
getting lucky.

This module lives outside `sim` on purpose: from M2 the interface services
(the ERP and MES APIs) read the same settings, and they must never import
the simulator.

Only the messes built so far are here. Spec §5 also has a top-level `seed`
and a `mess_level` preset (0 clean ... 3 hostile); both arrive in M2, when
there are enough messes for a level to mean something. Until then the seed
comes from the CLI. Unknown keys are rejected so that a mess that is not
built yet fails loudly instead of silently doing nothing.
"""

from pathlib import Path

import yaml
from pydantic import BaseModel, ConfigDict, Field


class MesMess(BaseModel):
    model_config = ConfigDict(extra="forbid")

    # Share of scans an operator never closes. A supervisor closes them at
    # 06:00 the next working morning, so the MES shows a 10-16 hour "run"
    # (longer over a weekend). 0 means every scan is closed on time.
    forgot_scan_off_rate: float = Field(default=0.03, ge=0, le=1)


class ErpMess(BaseModel):
    model_config = ConfigDict(extra="forbid")

    # MES labor hours are rounded to this increment when they post to ERP.
    # 0 keeps exact hours.
    labor_rounding_hours: float = Field(default=0.1, ge=0)
    # Hours between an operator's scan-off and the labor posting to ERP.
    # 0 posts it the moment the scan closes.
    labor_post_delay_hours: float = Field(default=24, ge=0)


class MachinesMess(BaseModel):
    model_config = ConfigDict(extra="forbid")

    # Width of the legacy polled counters behind gateway GW02 (PB-02): a
    # 16-bit counter rolls over from 65,535 to 0. None means it never wraps.
    counter_rollover_bits: int | None = Field(default=16, ge=1)


class MessConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mes: MesMess = Field(default_factory=MesMess)
    erp: ErpMess = Field(default_factory=ErpMess)
    machines: MachinesMess = Field(default_factory=MachinesMess)


def load_mess(path: Path) -> MessConfig:
    with path.open(encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return MessConfig(**data)
