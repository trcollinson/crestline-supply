"""Run the ERP API locally: `uv run python -m crestline_erp_api`.

Serves `out/erp/erp.db` as it stands unless told otherwise; `--clock-start`
replays its history instead (see `clock.py`). Every flag can also come from
the environment; see `settings.py`.
"""

import argparse
from pathlib import Path

import uvicorn

from .app import create_app
from .settings import Settings


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="python -m crestline_erp_api")
    parser.add_argument(
        "--db", type=Path, help="the ERP store (default: out/erp/erp.db)"
    )
    parser.add_argument(
        "--mess",
        type=Path,
        help="mess.yaml, to serve with its imperfections (default: a clean API)",
    )
    parser.add_argument(
        "--clock-start",
        metavar="TIME",
        help="replay the ERP's history from this time, e.g. 2026-07-03T06:00:00Z"
        " (default: serve it as it stands)",
    )
    parser.add_argument(
        "--clock-speed",
        type=float,
        help="ERP seconds per wall second while replaying (default: 1; 0 freezes it)",
    )
    parser.add_argument(
        "--clock-anchor",
        metavar="TIME",
        help="wall time at which the clock read --clock-start (default: now)",
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8100)
    args = parser.parse_args(argv)

    flags = {
        "db_path": args.db,
        "mess_path": args.mess,
        "clock_start": args.clock_start,
        "clock_speed": args.clock_speed,
        "clock_anchor": args.clock_anchor,
    }
    settings = Settings.from_env(**{k: v for k, v in flags.items() if v is not None})
    # One Date header, the ERP's (app.py), not uvicorn's.
    uvicorn.run(create_app(settings), host=args.host, port=args.port, date_header=False)


if __name__ == "__main__":
    main()
