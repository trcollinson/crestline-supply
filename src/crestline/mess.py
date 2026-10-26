"""Interface imperfections, read from `mess.yaml` (spec §5).

The plant simulation decides what really happened; these settings decide how
imperfectly each system records it. Every mess can be switched off on its
own, which is how the tests show that a pipeline handles it rather than
getting lucky.

This module lives outside `sim` on purpose: the interface services (the ERP
and MES APIs) read the same settings, and they must never import the
simulator.

`mess_level` picks a preset for every knob (0 clean, 1 mild, 2 realistic,
3 hostile); any key written in `mess.yaml` overrides the preset. The field
defaults below are level 2, the spec's values. `seed` is the default random
seed; a command-line `--seed` overrides it.

Only the messes built so far are here. Unknown keys are rejected so that a
mess that is not built yet fails loudly instead of silently doing nothing.
"""

from datetime import time
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

# The ERP's entities, as the API names them (the simulator's schema in
# `sim/systems/erp.py` is checked against this list by a test).
ERP_ENTITIES = (
    "WorkCenters",
    "Customers",
    "Parts",
    "PartRevisions",
    "SalesOrders",
    "SalesOrderLines",
    "Jobs",
    "JobOperations",
    "JobMaterials",
    "LaborDetails",
    "JobCosts",
)


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
    # Local time of the nightly labor batch. Each working day it posts the
    # earlier days' MES labor, once a supervisor has worked the exception
    # report, so a day's labor lands the next working night. None posts each
    # scan the moment it closes.
    labor_post_time: time | None = time(22, 0)

    # --- What the API does to a client (applied when it serves a page) ---

    # How long an access token lives, in seconds of the ERP's clock, so a long
    # extraction outlives it however fast a replay runs. None: never expires.
    token_ttl_seconds: int | None = Field(default=3600, gt=0)
    # Requests per second one API process accepts before answering 429 with
    # a Retry-After. None: no limit.
    throttle_rps: float | None = Field(default=5, gt=0)
    # Share of requests that fail with a bare 500 for no reason.
    random_500_rate: float = Field(default=0.005, ge=0, le=1)
    # True: @odata.nextLink pages with $skip, so rows can repeat or vanish
    # between pages when the ERP changes mid-read. False: it carries a
    # $skiptoken that pins the first page's instant, so a read is consistent.
    nextlink_drift: bool = True
    # Numbers come back as strings with 8 decimals ("250.00000000").
    numbers_as_strings: bool = True
    # Entities whose dates and timestamps come back as "/Date(ms)/".
    legacy_date_format_entities: list[str] = ["LaborDetails"]

    # --- What the ERP itself got wrong (written into the store) ---

    # Changes that do not touch LastModified: an operation's quantities
    # changing, and labor posting (its row keeps the time the scan closed,
    # not the time it posted). A client that loads by LastModified misses them.
    lastmodified_skips: list[Literal["JobOperations.qty", "LaborDetails.post"]] = [
        "JobOperations.qty",
        "LaborDetails.post",
    ]
    # Share of orders a clerk enters twice; the duplicate is cancelled the
    # next morning (its line soft-deleted, its unreleased job deleted).
    soft_delete_rate: float = Field(default=0.01, ge=0, le=1)
    # Share of jobs the planner splits after release into J and J-1, with
    # the quantity shared out. The floor still runs the lot under the parent.
    job_split_rate: float = Field(default=0.02, ge=0, le=1)

    @field_validator("legacy_date_format_entities")
    @classmethod
    def _known_entities(cls, names: list[str]) -> list[str]:
        unknown = sorted(set(names) - set(ERP_ENTITIES))
        if unknown:
            raise ValueError(f"not ERP entities: {', '.join(unknown)}")
        return names

    @field_validator("labor_post_time", mode="before")
    @classmethod
    def _time_written_as_text(cls, value: object) -> object:
        # Unquoted, YAML reads 22:00 as the base-60 number 1320, which
        # pydantic would quietly take as 1,320 seconds (00:22). Insist on text.
        if isinstance(value, int | float):
            # ValueError, not TypeError: pydantic reports only the former as a
            # validation error.
            raise ValueError('write the time in quotes, e.g. "22:00"')  # noqa: TRY004
        return value


class MachinesMess(BaseModel):
    model_config = ConfigDict(extra="forbid")

    # Width of the legacy polled counters behind gateway GW02 (PB-02): a
    # 16-bit counter rolls over from 65,535 to 0. None means it never wraps.
    counter_rollover_bits: int | None = Field(default=16, ge=1)


# Presets for the levels other than 2, whose values are the field defaults.
# Level 1 halves every rate; level 3 doubles them, throttles harder (2 per
# second) and expires tokens in half the time. The formats (string numbers,
# /Date()/, rounding, the nightly batch) are the same at 1, 2, and 3.
_FORMATS = {
    "numbers_as_strings": True,
    "legacy_date_format_entities": ["LaborDetails"],
    "lastmodified_skips": ["JobOperations.qty", "LaborDetails.post"],
    "labor_rounding_hours": 0.1,
    "labor_post_time": time(22, 0),
}
LEVELS: dict[int, dict[str, dict[str, Any]]] = {
    0: {
        "mes": {"forgot_scan_off_rate": 0},
        "erp": {
            "token_ttl_seconds": None,
            "throttle_rps": None,
            "random_500_rate": 0,
            "nextlink_drift": False,
            "numbers_as_strings": False,
            "legacy_date_format_entities": [],
            "lastmodified_skips": [],
            "soft_delete_rate": 0,
            "job_split_rate": 0,
            "labor_rounding_hours": 0,
            "labor_post_time": None,
        },
        "machines": {"counter_rollover_bits": None},
    },
    1: {
        "mes": {"forgot_scan_off_rate": 0.015},
        "erp": {
            **_FORMATS,
            "token_ttl_seconds": 3600,
            "throttle_rps": 10,
            "random_500_rate": 0.0025,
            "nextlink_drift": True,
            "soft_delete_rate": 0.005,
            "job_split_rate": 0.01,
        },
    },
    2: {},
    3: {
        "mes": {"forgot_scan_off_rate": 0.06},
        "erp": {
            **_FORMATS,
            "token_ttl_seconds": 1800,
            "throttle_rps": 2,
            "random_500_rate": 0.01,
            "nextlink_drift": True,
            "soft_delete_rate": 0.02,
            "job_split_rate": 0.04,
        },
    },
}


class MessConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    seed: int = 42
    mess_level: Literal[0, 1, 2, 3] = 2
    mes: MesMess = Field(default_factory=MesMess)
    erp: ErpMess = Field(default_factory=ErpMess)
    machines: MachinesMess = Field(default_factory=MachinesMess)

    @model_validator(mode="before")
    @classmethod
    def _start_from_the_level(cls, data: Any) -> Any:
        """Lay the level's preset under whatever keys were written."""
        if not isinstance(data, dict):
            return data
        level = data.get("mess_level", 2)
        if level not in LEVELS:
            raise ValueError(f"mess_level must be 0, 1, 2, or 3, not {level!r}")
        merged = dict(data)
        for section, preset in LEVELS[level].items():
            written = data.get(section)
            merged[section] = preset | written if isinstance(written, dict) else preset
        return merged


def load_mess(path: Path) -> MessConfig:
    with path.open(encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return MessConfig(**data)
