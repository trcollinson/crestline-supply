"""Settings for the ERP API, read from the environment.

The API is configured the way a deployed one would be: where the store is,
one client's credentials, and its clock. The defaults let it run on a laptop
against a fresh backfill (`out/erp/erp.db`), serving it as it stands;
anything deployed sets its own.
"""

import os
from collections.abc import Mapping
from pathlib import Path

from pydantic import AwareDatetime, BaseModel, Field

# Setting -> environment variable.
ENV_VARS = {
    "db_path": "CRESTLINE_ERP_DB",
    "client_id": "CRESTLINE_ERP_CLIENT_ID",
    "client_secret": "CRESTLINE_ERP_CLIENT_SECRET",
    "signing_key": "CRESTLINE_ERP_SIGNING_KEY",
    "mess_path": "CRESTLINE_ERP_MESS",
    "clock_start": "CRESTLINE_ERP_CLOCK_START",
    "clock_speed": "CRESTLINE_ERP_CLOCK_SPEED",
    "clock_anchor": "CRESTLINE_ERP_CLOCK_ANCHOR",
    "clock_stops": "CRESTLINE_ERP_CLOCK_STOPS",
}


class Settings(BaseModel):
    db_path: Path = Path("out/erp/erp.db")
    client_id: str = "crestline-local"
    client_secret: str = "local-secret"
    # Signs access tokens. Every copy of the API with the same key accepts
    # the others' tokens, so nothing about a token is stored anywhere.
    signing_key: str = "local-signing-key"
    # The imperfections to serve with (`mess.yaml`, spec §5): token lifetime,
    # throttling, random 500s, paging drift, and the formats. None serves a
    # clean API (mess level 0).
    mess_path: Path | None = None
    # The ERP's clock (see `clock.py`). With no start, the ERP's now is the
    # wall clock and every row is current. With one, the API replays the
    # ERP's history from `clock_start` at `clock_speed` times the wall clock,
    # counting from `clock_anchor`, the wall time at which the clock read
    # `clock_start`: the moment the app starts unless set, and set whenever
    # several copies of the API must agree on the time. Times carry an
    # offset (`2026-07-03T06:00:00Z`), as every timestamp in the API does.
    clock_start: AwareDatetime | None = None
    clock_speed: float = Field(default=1.0, ge=0)
    clock_anchor: AwareDatetime | None = None
    # A replay stops at the store's last change, the end of its history
    # (`clock.py`). False lets it run on past the end, the way a live
    # ERP's clock keeps going after its last write: the extraction eval
    # wants that, since tokens and slow pages count the ERP's clock.
    clock_stops: bool = True

    @classmethod
    def from_env(
        cls, environ: Mapping[str, str] = os.environ, **overrides: object
    ) -> "Settings":
        """Settings from the environment; `overrides` (command-line flags) win."""
        values = {
            name: environ[var] for name, var in ENV_VARS.items() if var in environ
        }
        return cls(**(values | overrides))
