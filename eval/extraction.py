"""Score an ERP extractor against the ERP's own final state (M2 step 4).

An extractor runs every night across the whole window against the messy,
changing API, and what it ends up holding is compared with what the ERP holds
at the end. For the naive extractor that is the copy it keeps; for the robust
one, the copy rebuilt from the raw pages it wrote (`erp_extract.merge`), so
the score is of what it actually delivered. The comparison is row for row, by entity:

- **missed**: the ERP has the row and the extractor never got it
- **wrongly deleted**: the extractor had the row and removed it
- **stale**: it has the row, but not what the ERP has now
- **kept deleted** ("ghosts"): the ERP deleted the row; the extractor still has it
- **pulled twice**: rows the API handed over twice within one read

The API runs in this process, on a clock the harness controls, so a 90-day
history takes seconds and the same inputs give the same numbers. A request
takes `seconds_per_request` of wall time on average (give or take half, drawn
from the seeded RNG, so bursts come and go as they do in life), and the ERP's
own clock runs fast enough that it also takes `erp_seconds_per_request` of
ERP time. That is what makes the throttle (a wall-clock limit) and the token
lifetime (an ERP-clock limit) both real. The defaults are a client just
under the 5-requests-a-second limit, against an ERP that takes 30 seconds
over a page.

Only this module reads the ERP's store directly (`erp.db`, the final state);
the extractor under test sees nothing but the API.

    uv run python eval/extraction.py [--out out] [--mess mess.yaml] [--extractor both]
"""

import argparse
import sqlite3
import tempfile
from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from fastapi.testclient import TestClient

from crestline.mess import MessConfig, load_mess
from crestline.rng import make_rng
from crestline_erp_api.app import create_app
from crestline_erp_api.settings import Settings
from erp_extract.client import ErpClient, Response
from erp_extract.entities import ENTITIES, key_of
from erp_extract.formats import LEGACY
from erp_extract.merge import MergeTally, rebuild
from erp_extract.naive import NaiveExtractor, Tally
from erp_extract.raw import RawWriter
from erp_extract.replica import Replica
from erp_extract.robust import SLACK, RobustExtractor

PLANT_TZ = ZoneInfo("America/Denver")
WALL_START = 1_800_000_000.0  # where the harness's wall clock starts, epoch seconds
SYSTEM_COLUMNS = ("SysValidFrom", "SysValidTo")


@dataclass(frozen=True)
class Scenario:
    """How the extractor is run, and how fast the ERP answers."""

    nightly_at: time = time(23, 0)  # plant time, after the 22:00 labor batch
    seconds_per_request: float = 0.25  # wall time, on average
    erp_seconds_per_request: float = 30.0  # ERP time, on average

    @property
    def speed(self) -> float:
        """ERP seconds per wall second."""
        return self.erp_seconds_per_request / self.seconds_per_request


@dataclass
class EntityScore:
    erp_rows: int = 0
    held: int = 0
    correct: int = 0
    missed: int = 0
    wrongly_deleted: int = 0
    stale: int = 0
    kept_deleted: int = 0
    pulled_twice: int = 0
    stale_fields: Counter[str] = field(default_factory=Counter)

    @property
    def wrong(self) -> int:
        return self.missed + self.wrongly_deleted + self.stale


@dataclass
class Scores:
    entities: dict[str, EntityScore]
    tally: object  # the extractor's own: requests, reads given up, rows pulled twice
    nights: int
    extractor: str = "naive"
    merge: MergeTally | None = None  # the robust extractor's: what its sweeps fixed

    @property
    def erp_rows(self) -> int:
        return sum(score.erp_rows for score in self.entities.values())

    @property
    def wrong(self) -> int:
        return sum(score.wrong for score in self.entities.values())

    @property
    def kept_deleted(self) -> int:
        return sum(score.kept_deleted for score in self.entities.values())


class World:
    """The harness's wall clock: it moves when a request is answered or a night comes."""

    def __init__(self) -> None:
        self.wall = WALL_START

    def now(self) -> float:
        return self.wall

    def sleep(self, seconds: float) -> None:
        self.wall += seconds


def window(db_path: Path) -> tuple[date, date]:
    """The first and last day the store covers, from its jobs."""
    with sqlite3.connect(db_path) as db:
        first, last = db.execute(
            "SELECT min(ReleaseDate), max(LastModified) FROM Jobs"
        ).fetchone()
    db.close()
    last_day = datetime.fromisoformat(last).astimezone(PLANT_TZ).date()
    return date.fromisoformat(first), last_day


def final_state(db_path: Path) -> tuple[dict, dict]:
    """The ERP's current rows per entity, keyed, and each column's declared type."""
    rows: dict[str, dict[tuple, dict]] = {}
    types: dict[str, dict[str, str]] = {}
    with sqlite3.connect(db_path) as db:
        for entity in ENTITIES:
            info = db.execute(f'PRAGMA table_info("{entity}")').fetchall()
            types[entity] = {
                name: declared.upper()
                for _, name, declared, *_ in info
                if name not in SYSTEM_COLUMNS
            }
            cursor = db.execute(f'SELECT {", ".join(types[entity])} FROM "{entity}"')
            rows[entity] = {}
            for values in cursor:
                row = {
                    column: bool(value)
                    if declared == "BOOLEAN" and value is not None
                    else value
                    for (column, declared), value in zip(
                        types[entity].items(), values, strict=True
                    )
                }
                rows[entity][key_of(entity, row)] = row
    db.close()
    return rows, types


def normalize(row: dict, types: dict[str, str]) -> dict:
    """A row as the API served it, put back the way the store holds it."""
    normal = {}
    for column, value in row.items():
        declared = types[column]
        if isinstance(value, str) and declared in ("INTEGER", "REAL"):
            value = float(value)
            value = int(value) if declared == "INTEGER" else value
        elif isinstance(value, str) and declared in ("DATE", "TIMESTAMP"):
            legacy = LEGACY.match(value)
            if legacy:
                moment = datetime.fromtimestamp(int(legacy.group(1)) / 1000, UTC)
                value = (
                    moment.date().isoformat()
                    if declared == "DATE"
                    else moment.strftime("%Y-%m-%dT%H:%M:%SZ")
                )
        normal[column] = value
    return normal


@dataclass
class _Harness:
    """The API in this process, a client for it, and the nightly schedule."""

    world: World
    client: ErpClient
    start: datetime
    days: tuple[date, date]
    scenario: Scenario

    def every_night(self, run) -> int:
        """Call `run` at `nightly_at` on every day of the window; returns how many."""
        nights = 0
        day, last_day = self.days
        while day <= last_day:
            night = datetime.combine(day, self.scenario.nightly_at, PLANT_TZ)
            due = (
                WALL_START
                + (night.astimezone(UTC) - self.start).total_seconds()
                / self.scenario.speed
            )
            self.world.wall = max(
                self.world.wall, due
            )  # an overrun starts the next late
            run()
            nights += 1
            day += timedelta(days=1)
        return nights


def _harness(db_path: Path, mess: MessConfig, scenario: Scenario) -> _Harness:
    first_day, last_day = window(db_path)
    start = datetime.combine(first_day, time(0), PLANT_TZ).astimezone(UTC)
    world = World()
    jitter = make_rng(mess.seed, "extraction_harness")
    settings = Settings(
        db_path=db_path,
        clock_start=start,
        clock_speed=scenario.speed,
        clock_anchor=datetime.fromtimestamp(WALL_START, UTC),
        # A live plant's ERP: its clock keeps going after the last change,
        # so a slow last night still outlives its tokens.
        clock_stops=False,
    )
    http = TestClient(create_app(settings, clock=world.now, mess=mess))

    def transport(method, path, headers, body) -> Response:
        reply = http.request(method, path, headers=dict(headers), content=body)
        world.wall += scenario.seconds_per_request * jitter.uniform(0.5, 1.5)
        lowered = {name.lower(): value for name, value in reply.headers.items()}
        return Response(reply.status_code, lowered, reply.json())

    client = ErpClient(transport, settings.client_id, settings.client_secret)
    return _Harness(world, client, start, (first_day, last_day), scenario)


def run_naive(
    db_path: Path, mess: MessConfig, scenario: Scenario | None = None
) -> tuple[Replica, Tally, int]:
    """Run the naive extractor every night over the store's window.

    Returns what it holds, what happened, and how many nights it ran.
    """
    harness = _harness(db_path, mess, scenario or Scenario())
    extractor = NaiveExtractor(harness.client)
    nights = harness.every_night(extractor.run)
    return extractor.replica, extractor.tally, nights


def run_robust(
    db_path: Path, mess: MessConfig, raw_root: Path, scenario: Scenario | None = None
):
    """Run the robust extractor every night, writing raw pages under `raw_root`.

    Returns the copy rebuilt from those pages, what happened, how many nights
    it ran, and what the merge found.
    """
    harness = _harness(db_path, mess, scenario or Scenario())
    extractor = RobustExtractor(
        harness.client,
        RawWriter(raw_root),
        rng=make_rng(mess.seed, "robust_backoff"),
        sleep=harness.world.sleep,
        clock=harness.world.now,
    )
    nights = harness.every_night(extractor.run)
    replica, merged = rebuild(raw_root)
    return replica, extractor.tally, nights, merged


def score(
    db_path: Path,
    replica: Replica,
    tally,
    nights: int,
    extractor: str = "naive",
    merge: MergeTally | None = None,
) -> Scores:
    """Compare what the extractor holds with the ERP's final state."""
    erp_rows, types = final_state(db_path)
    scores: dict[str, EntityScore] = {}
    for entity in ENTITIES:
        held = {
            key_of(entity, normal): normal
            for normal in (
                normalize(r, types[entity]) for r in replica.rows[entity].values()
            )
        }
        removed = {
            key_of(entity, normal)
            for normal in (
                normalize(r, types[entity]) for r in replica.removed[entity].values()
            )
        }
        result = EntityScore(
            erp_rows=len(erp_rows[entity]),
            held=len(held),
            pulled_twice=tally.pulled_twice[entity],
        )
        for key, row in erp_rows[entity].items():
            if key not in held:
                if key in removed:
                    result.wrongly_deleted += 1
                else:
                    result.missed += 1
            elif held[key] == row:
                result.correct += 1
            else:
                result.stale += 1
                result.stale_fields.update(c for c in row if held[key][c] != row[c])
        result.kept_deleted = len(set(held) - set(erp_rows[entity]))
        scores[entity] = result
    return Scores(scores, tally, nights, extractor, merge)


def report(scores: Scores) -> str:
    lines = [
        (
            f"{scores.extractor.title()} extractor, {scores.nights} nights, "
            "against the ERP's final state"
        ),
        "",
        (
            f"Of the ERP's {scores.erp_rows:,} rows, {scores.wrong:,} "
            f"({scores.wrong / scores.erp_rows:.1%}) are missing, lost, or stale in the "
            f"copy, which also holds {scores.kept_deleted:,} rows the ERP has deleted."
        ),
        "",
        (
            f"  {'entity':<17}{'ERP':>7}{'correct':>9}{'missed':>8}{'lost':>6}"
            f"{'stale':>7}{'kept deleted':>14}{'pulled twice':>14}"
        ),
    ]
    for entity, s in scores.entities.items():
        lines.append(
            f"  {entity:<17}{s.erp_rows:>7,}{s.correct:>9,}{s.missed:>8,}"
            f"{s.wrongly_deleted:>6,}{s.stale:>7,}{s.kept_deleted:>14,}{s.pulled_twice:>14,}"
        )
    stale = [(e, s.stale_fields) for e, s in scores.entities.items() if s.stale]
    if stale:
        lines += ["", "Stale rows differ in:"]
        for entity, fields in stale:
            changed = ", ".join(f"{name} ({n:,})" for name, n in fields.most_common())
            lines.append(f"  {entity:<17}{changed}")

    tally = scores.tally
    statuses = ", ".join(
        f"{status}: {n:,}" for status, n in sorted(tally.requests.items())
    )
    lines += ["", f"Requests by status: {statuses}"]
    if tally.abandoned:
        gave_up = ", ".join(f"{e} {n}" for e, n in tally.abandoned.most_common())
        lines.append(f"Reads given up on: {gave_up}")
    if scores.extractor == "robust":
        reads = ", ".join(f"{kind} {n:,}" for kind, n in sorted(tally.reads.items()))
        lines.append(
            f"Reads completed: {reads}; signed in {tally.logins:,} times; "
            f"waited {tally.waited_seconds:,.0f} s when told to slow down or a server failed"
        )
        short = ", ".join(f"{e} {n}" for e, n in tally.short.most_common()) or "none"
        gaps = ", ".join(f"{e} {n}" for e, n in tally.gaps.most_common() if n) or "0"
        lines.append(
            f"Complete reads that came up short: {short}; the biggest gap a good "
            f"one needed outside the ERP's counts: {gaps} (slack {SLACK})"
        )
    if scores.merge:
        lines.append("What the nightly sweeps changed, over all nights:")
        m = scores.merge
        for entity in ENTITIES:
            fixed = (
                m.sweep_corrected[entity],
                m.sweep_added[entity],
                m.sweep_removed[entity],
            )
            if any(fixed):
                lines.append(
                    f"  {entity:<17}{fixed[0]:,} stale rows put right, {fixed[1]:,} rows "
                    f"the increments never read, {fixed[2]:,} deleted rows removed"
                )
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--out", type=Path, default=Path("out"), help="backfill output")
    parser.add_argument("--mess", type=Path, default=Path("mess.yaml"))
    parser.add_argument("--seconds-per-request", type=float, default=0.25)
    parser.add_argument("--erp-seconds-per-request", type=float, default=30.0)
    parser.add_argument(
        "--extractor", choices=["naive", "robust", "both"], default="both"
    )
    parser.add_argument(
        "--raw",
        type=Path,
        help="where the robust extractor writes (default: a temporary folder)",
    )
    args = parser.parse_args(argv)

    db_path = args.out / "erp" / "erp.db"
    scenario = Scenario(
        seconds_per_request=args.seconds_per_request,
        erp_seconds_per_request=args.erp_seconds_per_request,
    )
    mess = load_mess(args.mess)
    reports = []
    if args.extractor in ("naive", "both"):
        replica, tally, nights = run_naive(db_path, mess, scenario)
        reports.append(report(score(db_path, replica, tally, nights)))
    if args.extractor in ("robust", "both"):
        with tempfile.TemporaryDirectory() as scratch:
            raw = args.raw or Path(scratch)
            replica, tally, nights, merged = run_robust(db_path, mess, raw, scenario)
            reports.append(
                report(score(db_path, replica, tally, nights, "robust", merged))
            )
    print("\n\n".join(reports))


if __name__ == "__main__":
    main()
