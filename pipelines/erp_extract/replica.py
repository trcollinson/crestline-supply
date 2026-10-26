"""A copy of the ERP: one row per key per entity, exactly as the API served it."""

from dataclasses import dataclass, field
from datetime import datetime

from .entities import ENTITIES, key_of
from .formats import parse_timestamp


@dataclass
class Replica:
    """The extractor's copy of the ERP: one row per key, exactly as the API served it."""

    rows: dict[str, dict[tuple, dict]] = field(
        default_factory=lambda: {entity: {} for entity in ENTITIES}
    )
    # Rows the extractor itself removed from the copy, to tell a row lost from
    # one never got.
    removed: dict[str, dict[tuple, dict]] = field(
        default_factory=lambda: {entity: {} for entity in ENTITIES}
    )
    # The newest LastModified seen per entity: where the next load starts.
    newest: dict[str, datetime] = field(default_factory=dict)

    def put(self, entity: str, row: dict) -> None:
        self.rows[entity][key_of(entity, row)] = row
        if "LastModified" in row:
            stamp = parse_timestamp(row["LastModified"])
            newest = self.newest.get(entity)
            if newest is None or stamp > newest:
                self.newest[entity] = stamp

    def clear(self, entity: str) -> None:
        self.removed[entity] |= self.rows[entity]
        self.rows[entity] = {}
