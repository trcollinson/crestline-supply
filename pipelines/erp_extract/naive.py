"""The extractor most teams write first.

It does what the API's documentation suggests and nothing more:

1. Sign in once at the start of a run.
2. For each entity, ask for everything that changed since the newest
   `LastModified` already held (all of it, the first time) and follow
   `@odata.nextLink` to the end.
3. Put each row into a copy of the ERP, replacing the row with the same key.
4. If a request fails, try it again, straight away, up to three times. If it
   still fails, give up on that entity until tomorrow.

Master data (work centers, customers, parts) has no `LastModified`, so
nothing says what changed: each night it is deleted and loaded again.

Every one of those is a reasonable thing to do, and each is wrong in a way
the API will find sooner or later. This module is here to be measured
(`eval/erp_extract.py`), not used: what it gets wrong is what the robust
extractor next to it is for.
"""

from collections import Counter
from dataclasses import dataclass, field

from .client import ErpClient, entity_path
from .entities import ENTITIES, key_of
from .formats import iso
from .replica import Replica

PAGE_SIZE = 500  # the most the API gives
TRIES = 3


class ReadFailed(Exception):
    """A request failed every time it was tried."""


@dataclass
class Tally:
    """What happened over all the runs."""

    runs: int = 0
    requests: Counter[int] = field(default_factory=Counter)  # by status
    abandoned: Counter[str] = field(
        default_factory=Counter
    )  # reads given up, by entity
    received: Counter[str] = field(default_factory=Counter)  # rows, by entity
    pulled_twice: Counter[str] = field(
        default_factory=Counter
    )  # rows seen twice in one read


class NaiveExtractor:
    def __init__(
        self,
        client: ErpClient,
        replica: Replica | None = None,
        tally: Tally | None = None,
        tries: int = TRIES,
    ) -> None:
        self.client = client
        self.replica = replica or Replica()
        self.tally = tally or Tally()
        self.tries = tries

    def run(self) -> None:
        """One scheduled run: every entity, in turn."""
        self.client.login()  # the token is good "for a while"; nobody checks how long
        self.tally.runs += 1
        for entity in ENTITIES:
            self._load(entity)

    def _load(self, entity: str) -> None:
        _, has_stamp = ENTITIES[entity]
        path = entity_path(entity, top=str(PAGE_SIZE))
        if not has_stamp:
            self.replica.clear(entity)  # nothing says what changed, so start again
        elif entity in self.replica.newest:
            since = f"LastModified gt {iso(self.replica.newest[entity])}"
            path = entity_path(entity, top=str(PAGE_SIZE), filter=since)

        seen: set[tuple] = set()
        try:
            while path:
                page = self._get(path)
                for row in page["value"]:
                    key = key_of(entity, row)
                    if key in seen:
                        self.tally.pulled_twice[entity] += 1
                    seen.add(key)
                    self.tally.received[entity] += 1
                    self.replica.put(entity, row)
                path = page.get("@odata.nextLink")
        except ReadFailed:
            # Whatever was loaded stays loaded, and so does the newest
            # LastModified among it, which is where the next run starts.
            self.tally.abandoned[entity] += 1

    def _get(self, path: str) -> dict:
        """The page at `path`, trying again at once if it fails."""
        for _ in range(self.tries):
            response = self.client.get(path)
            self.tally.requests[response.status] += 1
            if response.ok:
                return response.body
        raise ReadFailed(path)
