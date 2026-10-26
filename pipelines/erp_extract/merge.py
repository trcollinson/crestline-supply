"""Turn the raw reads into a copy of the ERP, by replaying them in order.

For each entity, oldest read first:

- an incremental or sequence read: each row replaces the row with its key
- a full read or a sweep: a complete picture, so the entity becomes exactly
  what it holds, and any key it doesn't hold is gone from the ERP

This is the same merge the curated layer does in SQL; here it lets the
extraction eval score what the extractor actually wrote.
"""

from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path

from .entities import ENTITIES, key_of
from .raw import pages_in, read_files
from .replica import Replica

COMPLETE = ("full", "sweep")


@dataclass
class MergeTally:
    pulled_twice: Counter[str] = field(default_factory=Counter)  # within one read
    # What sweeps changed in the copy the increments had built:
    sweep_corrected: Counter[str] = field(default_factory=Counter)  # stale rows
    sweep_added: Counter[str] = field(default_factory=Counter)  # rows never read
    sweep_removed: Counter[str] = field(default_factory=Counter)  # rows the ERP deleted


def rebuild(root: Path) -> tuple[Replica, MergeTally]:
    replica = Replica()
    tally = MergeTally()
    for entity in ENTITIES:
        for path in read_files(root, entity):
            pages = pages_in(path)
            kind = pages[0]["read"]
            read: dict[tuple, dict] = {}
            for page in pages:
                for row in page["body"]["value"]:
                    key = key_of(entity, row)
                    if key in read:
                        tally.pulled_twice[entity] += 1
                    read[key] = row
            current = replica.rows[entity]
            if kind not in COMPLETE:
                current.update(read)
                continue
            gone = {key: current[key] for key in current.keys() - read.keys()}
            replica.removed[entity] |= gone
            if kind == "sweep":
                tally.sweep_removed[entity] += len(gone)
                tally.sweep_added[entity] += len(read.keys() - current.keys())
                tally.sweep_corrected[entity] += sum(
                    1
                    for key in read.keys() & current.keys()
                    if read[key] != current[key]
                )
            replica.rows[entity] = read
    return replica, tally
