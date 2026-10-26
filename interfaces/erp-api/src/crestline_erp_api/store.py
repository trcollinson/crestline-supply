"""Read-only access to the ERP store (`erp.db`).

The store describes itself: each table's columns, declared types, and
primary key come from SQLite, so this service never needs the simulator's
schema. True/false columns are declared BOOLEAN; SQLite stores them as 0
and 1, and the API serves `true` and `false`.

The store keeps every version of every row. `Jobs` holds the current rows,
each with `SysValidFrom`, the moment that version began; `Jobs_History`
holds every superseded version with `SysValidFrom` and `SysValidTo`, the
moment the next version replaced it. Neither the history tables nor the
`Sys` columns are part of the API: they are how it answers "as of".
"""

import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

SYSTEM_COLUMNS = ("SysValidFrom", "SysValidTo")
HISTORY_SUFFIX = "_History"


@dataclass(frozen=True)
class Entity:
    name: str
    columns: dict[str, str]  # column -> declared type: TEXT, INTEGER, REAL, BOOLEAN
    key: tuple[str, ...]
    history: str | None = None  # the history table, when the store keeps one

    def record(
        self,
        row: tuple,
        *,
        legacy_dates: bool = False,
        numbers_as_strings: bool = False,
    ) -> dict[str, object]:
        """A row as the API serves it.

        `legacy_dates` writes dates and timestamps as `/Date(ms)/`, and
        `numbers_as_strings` writes numbers as strings with 8 decimals, the
        way older ERP APIs do.
        """
        record: dict[str, object] = {}
        for (column, declared), value in zip(self.columns.items(), row, strict=True):
            if value is None:
                pass
            elif declared == "BOOLEAN":
                value = bool(value)
            elif legacy_dates and declared in ("DATE", "TIMESTAMP"):
                value = _legacy_date(value)
            elif numbers_as_strings and declared in ("INTEGER", "REAL"):
                value = f"{float(value):.8f}"
            record[column] = value
        return record

    def source(self, as_of: str | None) -> tuple[str, list[object]]:
        """What to select from: the table as it stands, or as of a moment.

        As of a moment, each row's version is the one whose validity covers
        it: the current row if that began by then, otherwise the superseded
        version that began by then and was replaced after it. Master data,
        in the ERP since go-live, has no `SysValidFrom`.
        """
        if as_of is None or self.history is None:
            return f'"{self.name}"', []
        columns = ", ".join(f'"{c}"' for c in self.columns)
        source = (
            f'(SELECT {columns} FROM "{self.name}"'
            f' WHERE "SysValidFrom" IS NULL OR "SysValidFrom" <= ?'
            f' UNION ALL SELECT {columns} FROM "{self.history}"'
            f' WHERE "SysValidFrom" <= ? AND "SysValidTo" > ?)'
        )
        return source, [as_of, as_of, as_of]


def _legacy_date(value: str) -> str:
    """`2026-07-06T14:00:00Z` (or a date, at midnight UTC) as `/Date(1783346400000)/`."""
    moment = datetime.fromisoformat(value)
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=UTC)
    return f"/Date({int(moment.timestamp() * 1000)})/"


class Store:
    def __init__(self, path: Path) -> None:
        if not path.is_file():
            raise FileNotFoundError(f"no ERP store at {path}; run a backfill first")
        # Read-only: the API never writes to the ERP.
        self._uri = f"{path.resolve().as_uri()}?mode=ro"
        db = self._connect()
        try:
            self.entities = _read_schema(db)
        finally:
            db.close()

    @property
    def versioned(self) -> bool:
        """Whether the store keeps history, so it can be read as of a moment."""
        return any(entity.history for entity in self.entities.values())

    def last_change(self) -> datetime | None:
        """When the history ends: the newest moment any row was written or deleted.

        A version's `SysValidTo` is when it was replaced or deleted, so a row
        deleted outright still counts. None if the store keeps no history.
        """
        moments = []
        for entity in self.entities.values():
            if entity.history is None:
                continue
            moments += [
                self.fetch(f'SELECT MAX("SysValidFrom") FROM "{entity.name}"', [])[0][
                    0
                ],
                self.fetch(
                    f'SELECT MAX(MAX("SysValidFrom"), MAX("SysValidTo"))'
                    f' FROM "{entity.history}"',
                    [],
                )[0][0],
            ]
        moments = [moment for moment in moments if moment is not None]
        if not moments:
            return None
        return datetime.fromisoformat(max(moments)).astimezone(UTC)

    def fetch(self, sql: str, params: list[object]) -> list[tuple]:
        db = self._connect()
        try:
            return db.execute(sql, params).fetchall()
        finally:
            db.close()

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self._uri, uri=True)


def _read_schema(db: sqlite3.Connection) -> dict[str, Entity]:
    entities = {}
    tables = [
        name
        for (name,) in db.execute(
            "SELECT name FROM sqlite_master"
            " WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )
    ]
    for table in tables:
        if table.endswith(HISTORY_SUFFIX):
            continue
        # (cid, name, type, notnull, default, pk): pk is the column's
        # position in the primary key, 0 if it isn't part of it.
        info = db.execute(f'PRAGMA table_info("{table}")').fetchall()
        history = f"{table}{HISTORY_SUFFIX}"
        entities[table] = Entity(
            name=table,
            columns={
                column[1]: column[2].upper()
                for column in info
                if column[1] not in SYSTEM_COLUMNS
            },
            key=tuple(
                column[1] for column in sorted(info, key=lambda c: c[5]) if column[5]
            ),
            history=history if history in tables else None,
        )
    return entities
