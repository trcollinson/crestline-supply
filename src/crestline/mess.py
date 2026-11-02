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

    # --- The downtime file (M3): what gets written down, and how ---

    # A stop shorter than this is never written down, so microstops never
    # are. 0 means every stop is a candidate.
    min_recorded_downtime_minutes: float = Field(default=5, ge=0)
    # A stop at least this long is always written down: nobody forgets a
    # two-hour maintenance stop.
    always_recorded_downtime_minutes: float = Field(default=30, ge=0)
    # Share of the stops between those two that nobody writes down, because
    # the operator was busy fixing it. 0 means every candidate is logged.
    downtime_unrecorded_rate: float = Field(default=0.4, ge=0, le=1)
    # How the reason is written. "none": the canonical phrase for the reason.
    # "high": the way people write it (jam, JAMMED, jam - see Ray, blank).
    free_text_noise: Literal["none", "high"] = "high"

    @model_validator(mode="after")
    def _always_is_not_below_minimum(self) -> "MesMess":
        if self.always_recorded_downtime_minutes < self.min_recorded_downtime_minutes:
            raise ValueError(
                "always_recorded_downtime_minutes must not be below "
                "min_recorded_downtime_minutes"
            )
        return self


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


class ClockDrift(BaseModel):
    """How far each device or gateway clock wanders from true time (RMS seconds).

    The keys are the clocks that stamp a payload: PB-01's own controller
    (`ts`), LSR-01's (the Sparkplug timestamps), and gateway GW02's
    (`gw_ts`, shared by PB-02 and PEM-01). GW03 keeps NTP time. 0 is exact.
    """

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    lsr_01: float = Field(default=5, ge=0, alias="LSR-01")
    pb_01: float = Field(default=140, ge=0, alias="PB-01")
    gw02: float = Field(default=20, ge=0, alias="GW02")

    def seconds(self, clock: str) -> float:
        return {"LSR-01": self.lsr_01, "PB-01": self.pb_01, "GW02": self.gw02}[clock]


class MachinesMess(BaseModel):
    model_config = ConfigDict(extra="forbid")

    # Width of the legacy polled counters behind gateway GW02 (PB-02, PEM-01):
    # a 16-bit counter rolls over from 65,535 to 0. None means it never wraps.
    counter_rollover_bits: int | None = Field(default=16, ge=1)
    # Share of a legacy machine's working days with a power cycle, which
    # resets its counter to 0: three in four at 1st-shift start, the rest at
    # a break or meal. 0 means never.
    counter_reset_rate: float = Field(default=0.1, ge=0, le=1)
    # Typical size of each clock's offset from true time, slowly wandering.
    clock_drift_seconds: ClockDrift = Field(default_factory=ClockDrift)
    # Share of messages a gateway delivers twice (a QoS 1 redelivery): the
    # same bytes again a moment later. 0 means never.
    duplicate_rate: float = Field(default=0.005, ge=0, le=1)
    # Network outages per gateway per week. What happens to the data depends
    # on the gateway: GW01 and EDGE-LASER buffer and replay, GW02 loses it,
    # GW03 replays it in order (spec §4.3). 0 means none.
    outages_per_week: float = Field(default=2, ge=0)
    # How long an outage lasts: uniform between the two, in minutes.
    outage_minutes: tuple[float, float] = (2, 45)
    # Share of GW01's messages that arrive as truncated JSON. 0 means never.
    malformed_json_rate: float = Field(default=0.001, ge=0, le=1)

    @field_validator("outage_minutes")
    @classmethod
    def _outage_range(cls, value: tuple[float, float]) -> tuple[float, float]:
        low, high = value
        if not 0 < low <= high:
            raise ValueError("outage_minutes must be [low, high] with 0 < low <= high")
        return value


# Presets for the levels other than 2, whose values are the field defaults.
# Level 1 halves every rate; level 3 doubles them, throttles harder (2 per
# second) and expires tokens in half the time. The formats (string numbers,
# /Date()/, rounding, the nightly batch, the downtime thresholds and free
# text, how far a clock wanders, how long an outage lasts) are the same at
# 1, 2, and 3.
_FORMATS = {
    "numbers_as_strings": True,
    "legacy_date_format_entities": ["LaborDetails"],
    "lastmodified_skips": ["JobOperations.qty", "LaborDetails.post"],
    "labor_rounding_hours": 0.1,
    "labor_post_time": time(22, 0),
}
_MES_FORMATS = {
    "min_recorded_downtime_minutes": 5,
    "always_recorded_downtime_minutes": 30,
    "free_text_noise": "high",
}
_MACHINE_FORMATS = {
    "counter_rollover_bits": 16,
    "clock_drift_seconds": {"LSR-01": 5, "PB-01": 140, "GW02": 20},
    "outage_minutes": (2, 45),
}
LEVELS: dict[int, dict[str, dict[str, Any]]] = {
    0: {
        "mes": {
            "forgot_scan_off_rate": 0,
            "min_recorded_downtime_minutes": 0,
            "always_recorded_downtime_minutes": 30,
            "downtime_unrecorded_rate": 0,
            "free_text_noise": "none",
        },
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
        "machines": {
            "counter_rollover_bits": None,
            "counter_reset_rate": 0,
            "clock_drift_seconds": {"LSR-01": 0, "PB-01": 0, "GW02": 0},
            "duplicate_rate": 0,
            "outages_per_week": 0,
            "outage_minutes": (2, 45),
            "malformed_json_rate": 0,
        },
    },
    1: {
        "mes": {
            **_MES_FORMATS,
            "forgot_scan_off_rate": 0.015,
            "downtime_unrecorded_rate": 0.2,
        },
        "erp": {
            **_FORMATS,
            "token_ttl_seconds": 3600,
            "throttle_rps": 10,
            "random_500_rate": 0.0025,
            "nextlink_drift": True,
            "soft_delete_rate": 0.005,
            "job_split_rate": 0.01,
        },
        "machines": {
            **_MACHINE_FORMATS,
            "counter_reset_rate": 0.05,
            "duplicate_rate": 0.0025,
            "outages_per_week": 1,
            "malformed_json_rate": 0.0005,
        },
    },
    2: {},
    3: {
        "mes": {
            **_MES_FORMATS,
            "forgot_scan_off_rate": 0.06,
            "downtime_unrecorded_rate": 0.8,
        },
        "erp": {
            **_FORMATS,
            "token_ttl_seconds": 1800,
            "throttle_rps": 2,
            "random_500_rate": 0.01,
            "nextlink_drift": True,
            "soft_delete_rate": 0.02,
            "job_split_rate": 0.04,
        },
        "machines": {
            **_MACHINE_FORMATS,
            "counter_reset_rate": 0.2,
            "duplicate_rate": 0.01,
            "outages_per_week": 4,
            "malformed_json_rate": 0.002,
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
