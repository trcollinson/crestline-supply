"""The robust extractor: what the naive one gets wrong, handled.

Each run signs in, then reads every entity, writing each read's pages raw
(`raw.py`). What it does differently, and why:

- **Every failure has its own answer.** A 401 means the token expired: sign
  in again. A 429 means slow down: wait as long as `Retry-After` says. A 5xx
  may pass: back off exponentially, with jitter so retries don't line up. A
  read that still fails is dropped whole: nothing from it is written and its
  watermark does not move, so the next run reads it all again.
- **Pages by key, never by position.** Each page asks for the rows after the
  last key it has (`JobNum gt 'J1' or (JobNum eq 'J1' and OprSeq gt 20)`),
  so rows that change or vanish while a read is going on can't shift a page
  and make it skip or repeat a row, the way `$skip` can.
- **The ERP's clock, not ours, and an overlap.** An incremental read asks
  for rows changed since the ERP's `Date` when the last complete read of that
  entity began, less an hour. Rows read twice merge into one.
- **Each entity read the way it changes.** Master data has no change stamp:
  read in full. `LaborDetails` is insert-only: read by its sequence number,
  which back-dated stamps can't hide. Orders and lines: incrementally.
- **A sweep where the stamp can't be trusted.** An operation's quantities
  change without its `LastModified` moving, and cancelled duplicates' jobs
  disappear without a trace, so the four job entities are also read in full
  (every night by default). A sweep is a complete picture: anything it
  doesn't contain is gone. `merge.py` turns the raw reads into a copy.

Soft-deleted order lines are kept with their `Deleted` flag, as the ERP keeps
them; split jobs (`J104221-1`) are simply new keys.
"""

import json
import random
import time
from collections import Counter
from collections.abc import Callable, Iterable
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
from pathlib import Path

from .client import ErpClient, Response, entity_path
from .entities import ENTITIES, key_of, literal
from .formats import iso
from .raw import Page, RawWriter

PAGE_SIZE = 500
OVERLAP = timedelta(hours=1)
MAX_ATTEMPTS = 8
BACKOFF_SECONDS = 1.0  # the first wait after a 5xx; doubles each time
BACKOFF_CAP_SECONDS = 60.0
# Insert-only entities, read by their sequence number.
SEQUENCES = {"LaborDetails": "LaborDtlSeq"}
# Entities whose LastModified misses changes or deletions: swept in full.
SWEPT = ("Jobs", "JobOperations", "JobMaterials", "JobCosts")


class ReadFailed(Exception):
    """A request kept failing; the read it was part of is dropped."""


@dataclass
class State:
    """What one run leaves for the next."""

    # entity -> how many runs have read it (the sweep's cadence counts these)
    runs: dict[str, int] = field(default_factory=dict)
    # entity -> the ERP's time when its last complete incremental read began
    watermarks: dict[str, str] = field(default_factory=dict)
    # entity -> the highest sequence number read so far
    sequences: dict[str, int] = field(default_factory=dict)
    # entity -> the last run that read it (the ERP's time at sign-in, its name)
    last_runs: dict[str, str] = field(default_factory=dict)

    def save(self, path: Path) -> None:
        path.write_text(json.dumps(asdict(self), indent=2) + "\n", encoding="utf-8")

    @classmethod
    def load(cls, path: Path) -> "State":
        return cls(**json.loads(path.read_text())) if path.exists() else cls()


@dataclass
class Tally:
    """What happened over all the runs."""

    runs: int = 0
    requests: Counter[int] = field(default_factory=Counter)  # by status
    reads: Counter[str] = field(default_factory=Counter)  # completed, by kind
    abandoned: Counter[str] = field(default_factory=Counter)  # reads dropped, by entity
    # runs that found the ERP's clock no later than the last run left it, by entity
    skipped: Counter[str] = field(default_factory=Counter)
    # rows handed over twice within one read, by entity
    pulled_twice: Counter[str] = field(default_factory=Counter)
    rows: Counter[str] = field(default_factory=Counter)  # rows read, by kind of read
    logins: int = 0
    waited_seconds: float = 0.0


class RobustExtractor:
    def __init__(
        self,
        client: ErpClient,
        writer: RawWriter,
        state: State | None = None,
        *,
        rng: random.Random,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.time,
        sweep_every: int = 1,
    ) -> None:
        self.client = client
        self.writer = writer
        self.state = state or State()
        self.rng = rng  # for backoff jitter; seeded by the caller
        self.sleep = sleep
        self.clock = clock
        self.sweep_every = sweep_every
        self.tally = Tally()

    def run(self, entities: Iterable[str] | None = None) -> None:
        """One scheduled run: every entity (or those given), read the way it changes.

        In AWS each entity is its own Lambda invocation, so a run can be one
        entity; each entity counts its own runs for the sweep's cadence.

        An entity is skipped if the ERP's clock hasn't moved past the last run
        that read it. Nothing can have changed, and reading again would reuse
        that run's name, and so its files. A live ERP's clock always moves;
        a replay's stops at the end of its history, and the poller goes quiet.
        """
        started = server_time(self._login())
        self._run = started.strftime("%Y%m%dT%H%M%SZ")
        self._ingest_date = started.date().isoformat()
        self._ordinal = 0
        for entity in entities or ENTITIES:
            last_run = self.state.last_runs.get(entity)
            if last_run is not None and self._run <= last_run:
                self.tally.skipped[entity] += 1
                continue
            self.state.last_runs[entity] = self._run
            runs = self.state.runs.get(entity, 0)
            for kind, condition in self._reads(entity, runs % self.sweep_every == 0):
                try:
                    self._read(entity, kind, condition)
                except ReadFailed:
                    self.tally.abandoned[entity] += 1
            self.state.runs[entity] = runs + 1
        self.tally.runs += 1

    def _reads(self, entity: str, sweep: bool) -> list[tuple[str, str | None]]:
        """The reads an entity needs this run: (kind, the filter it starts from)."""
        _, has_stamp = ENTITIES[entity]
        if entity in SEQUENCES:
            last = self.state.sequences.get(entity)
            column = SEQUENCES[entity]
            return [("sequence", None if last is None else f"{column} gt {last}")]
        if not has_stamp:
            return [("full", None)]
        since = self.state.watermarks.get(entity)
        condition = None
        if since is not None:
            from_ = datetime.fromisoformat(since) - OVERLAP
            condition = f"LastModified gt {iso(from_)}"
        reads = [("incremental", condition)]
        if entity in SWEPT and sweep:
            reads.append(("sweep", None))
        return reads

    def _read(self, entity: str, kind: str, condition: str | None) -> None:
        """Page through one read by key, then write it and only then move on."""
        columns = ENTITIES[entity][0]
        pages: list[Page] = []
        after: dict | None = None
        while True:
            conditions = [c for c in (condition, keyset(columns, after)) if c]
            options = {"orderby": ",".join(columns), "top": str(PAGE_SIZE)}
            if conditions:
                options["filter"] = " and ".join(f"({c})" for c in conditions)
            path = entity_path(entity, **options)
            requested_at = datetime.fromtimestamp(self.clock(), UTC)
            response = self._get(path)
            rows = response.body["value"]
            pages.append(
                Page(
                    entity=entity,
                    run=self._run,
                    read=kind,
                    page=len(pages) + 1,
                    url=path,
                    requested_at=iso(requested_at),
                    server_time=iso(server_time(response)),
                    body=response.body,
                )
            )
            if not rows or "@odata.nextLink" not in response.body:
                break
            after = rows[-1]

        self.writer.write(pages, self._ordinal, self._ingest_date)
        self._ordinal += 1
        self.tally.reads[kind] += 1
        keys = keys_of(entity, pages)
        self.tally.pulled_twice[entity] += len(keys) - len(set(keys))
        self.tally.rows[kind] += len(keys)
        # Bookkeeping moves only now, with the read complete and written.
        if kind == "incremental":
            self.state.watermarks[entity] = pages[0].server_time
        elif kind == "sequence":
            column = SEQUENCES[entity]
            seen = [int(float(r[column])) for p in pages for r in p.body["value"]]
            if seen:
                self.state.sequences[entity] = max(
                    seen + [self.state.sequences.get(entity, 0)]
                )

    def _get(self, path: str) -> Response:
        """A page, answering each kind of failure the way it asks to be answered."""
        for attempt in range(MAX_ATTEMPTS):
            response = self.client.get(path)
            self.tally.requests[response.status] += 1
            if response.ok:
                return response
            if response.status not in (401, 429) and response.status < 500:
                # A 400 or 404 is this code's mistake; trying again won't help.
                raise ReadFailed(f"{response.status} {path}: {response.body}")
            if attempt + 1 == MAX_ATTEMPTS:
                break  # no point waiting before giving up
            if response.status == 401:  # the token expired: get another
                self._login()
            elif response.status == 429:  # too fast: wait as long as asked
                self._wait(float(response.headers.get("retry-after", "1")))
            else:  # a 5xx may pass: back off, with jitter
                backoff = min(BACKOFF_CAP_SECONDS, BACKOFF_SECONDS * 2**attempt)
                self._wait(backoff * self.rng.uniform(0.5, 1.0))
        raise ReadFailed(f"gave up after {MAX_ATTEMPTS} tries: {path}")

    def _login(self) -> Response:
        self.tally.logins += 1
        return self.client.login()

    def _wait(self, seconds: float) -> None:
        self.tally.waited_seconds += seconds
        self.sleep(seconds)


def keyset(columns: tuple[str, ...], after: dict | None) -> str | None:
    """The rows after `after` in key order, as a `$filter`; None from the start.

    For (JobNum, OprSeq) after J1/20: `JobNum gt 'J1' or (JobNum eq 'J1' and OprSeq gt 20)`.
    """
    if after is None:
        return None
    alternatives = []
    for i, column in enumerate(columns):
        same = [f"{c} eq {literal(c, after[c])}" for c in columns[:i]]
        clause = " and ".join([*same, f"{column} gt {literal(column, after[column])}"])
        alternatives.append(clause if not same else f"({clause})")
    return " or ".join(alternatives)


def server_time(response: Response) -> datetime:
    """When the ERP says it answered.

    `X-Server-Time` if it sent one, because a front door can replace `Date`
    with its own clock (an AWS Lambda function URL does); else `Date`.
    """
    if "x-server-time" in response.headers:
        return datetime.fromisoformat(response.headers["x-server-time"]).astimezone(UTC)
    return parsedate_to_datetime(response.headers["date"]).astimezone(UTC)


def keys_of(entity: str, pages: list[Page]) -> list[tuple]:
    return [key_of(entity, row) for page in pages for row in page.body["value"]]
