"""The ERP API's query options: `$filter`, `$orderby`, `$top`, `$skip`, `$count`.

A small, strict subset of OData, turned into one parameterized SQL query:

- `$filter`: `<Field> <op> <value>` clauses joined by `and` and `or`, with
  parentheses; `and` binds tighter, as in SQL. `op` is `eq ne gt ge lt le`.
  Values are `'text'` (a quote inside is written twice), numbers,
  `true`/`false`, `null` (with `eq`/`ne` only), dates (`2026-07-06`), and
  timestamps (`2026-07-06T14:30:00Z`, or with an offset). No functions. A
  client paging by a key of two columns needs the `or`:
  `JobNum gt 'J1' or (JobNum eq 'J1' and OprSeq gt 20)`.
- `$orderby`: `Field [asc|desc]`, comma-separated. The primary key is always
  added after them, so rows that tie keep the same order on every page.
- `$top`: the page size. 100 if omitted; anything over 500 is quietly cut to
  500, so a client must always follow `@odata.nextLink`.
- `$skip`: rows to skip, which is all `@odata.nextLink` changes.
- `$skiptoken`: instead of `$skip`, an opaque cursor that `@odata.nextLink`
  carries when paging is consistent: the rows to skip and the instant the
  first page was read at, so later pages see the ERP as it was then.
- `$count`: `true` adds `@odata.count`, how many rows match the filter as of
  this page's instant, whatever `$top` and `$skip` say. A client checks a
  full read against it: did it collect as many rows as the ERP holds?

Field names are checked against the entity, and values are always passed as
parameters, so nothing from the URL is pasted into the SQL.
"""

import base64
import binascii
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime

from .store import Entity

DEFAULT_TOP = 100
MAX_TOP = 500
OPTIONS = {"$filter", "$orderby", "$top", "$skip", "$skiptoken", "$count"}
TEXTUAL = ("TEXT", "DATE", "TIMESTAMP")  # ISO 8601 text in the store
OPERATORS = {"eq": "=", "ne": "<>", "gt": ">", "ge": ">=", "lt": "<", "le": "<="}

TOKEN = re.compile(
    r"""\s*(?:
        (?P<text>'(?:[^']|'')*')
      | (?P<timestamp>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?(?:Z|[+-]\d{2}:\d{2}))
      | (?P<date>\d{4}-\d{2}-\d{2})
      | (?P<number>-?\d+(?:\.\d+)?)
      | (?P<word>[A-Za-z_][A-Za-z0-9_]*)
      | (?P<paren>[()])
    )""",
    re.VERBOSE,
)


class QueryError(ValueError):
    """A query the API can't serve; the client gets a 400 with this message."""


@dataclass(frozen=True)
class Query:
    entity: Entity
    where: list[str]  # SQL conditions, each with ? placeholders
    params: list[object]
    order_by: list[str]  # SQL sort terms
    top: int
    skip: int
    count: bool = False  # `$count=true`: also say how many rows match

    def sql(self, as_of: str | None = None) -> tuple[str, list[object]]:
        """One page plus one row, which tells whether another page follows.

        `as_of` (the store's timestamp form) reads the entity as it stood at
        that moment; None reads it as it stands.
        """
        columns = ", ".join(f'"{c}"' for c in self.entity.columns)
        source, source_params = self.entity.source(as_of)
        where = f" WHERE {' AND '.join(self.where)}" if self.where else ""
        sql = (
            f"SELECT {columns} FROM {source}{where}"
            f" ORDER BY {', '.join(self.order_by)} LIMIT ? OFFSET ?"
        )
        return sql, [*source_params, *self.params, self.top + 1, self.skip]

    def count_sql(self, as_of: str | None = None) -> tuple[str, list[object]]:
        """How many rows match the filter, as of `as_of`: every page, not one."""
        source, source_params = self.entity.source(as_of)
        where = f" WHERE {' AND '.join(self.where)}" if self.where else ""
        return f"SELECT count(*) FROM {source}{where}", [*source_params, *self.params]


def parse_query(entity: Entity, options: Mapping[str, str]) -> Query:
    unknown = sorted(
        name for name in options if name.startswith("$") and name not in OPTIONS
    )
    if unknown:
        raise QueryError(f"unsupported query option {unknown[0]}")
    where, params = _parse_filter(entity, options.get("$filter", ""))
    return Query(
        entity=entity,
        where=where,
        params=params,
        order_by=_parse_orderby(entity, options.get("$orderby", "")),
        top=min(_parse_count("$top", options.get("$top"), DEFAULT_TOP, 1), MAX_TOP),
        skip=_parse_count("$skip", options.get("$skip"), 0, 0),
        count=_parse_boolean("$count", options.get("$count", "false")),
    )


def _parse_filter(entity: Entity, text: str) -> tuple[list[str], list[object]]:
    """The filter as SQL conditions to AND together, and their parameters."""
    tokens = _tokenize(text)
    if not tokens:
        return [], []
    parser = _FilterParser(entity, tokens)
    node = parser.disjunction()
    if parser.position < len(tokens):
        _, extra = tokens[parser.position]
        if extra == ")":
            raise QueryError("unbalanced parentheses in $filter")
        raise QueryError(f"$filter clauses are joined with 'and' or 'or', not {extra}")
    # A plain run of `and`s stays a list of conditions, as the query was before
    # `or` existed; anything else is one parenthesized condition.
    parts = node[1] if node[0] == "and" else [node]
    where, params = [], []
    for part in parts:
        sql, part_params = _render(part)
        where.append(sql)
        params += part_params
    return where, params


# A parsed filter: ("cmp", sql, params) | ("and", [nodes]) | ("or", [nodes])
_Node = tuple


class _FilterParser:
    """`a or b and (c or d)`: or of ands of comparisons or parenthesized filters."""

    def __init__(self, entity: Entity, tokens: list[tuple[str, str]]) -> None:
        self.entity = entity
        self.tokens = tokens
        self.position = 0

    def disjunction(self) -> _Node:
        nodes = [self.conjunction()]
        while self._next_is("or"):
            nodes.append(self.conjunction())
        return nodes[0] if len(nodes) == 1 else ("or", nodes)

    def conjunction(self) -> _Node:
        nodes = [self.factor()]
        while self._next_is("and"):
            nodes.append(self.factor())
        return nodes[0] if len(nodes) == 1 else ("and", nodes)

    def factor(self) -> _Node:
        if self.position >= len(self.tokens):
            raise QueryError(f"$filter ends with '{self.tokens[-1][1]}'")
        if self.tokens[self.position] == ("paren", "("):
            self.position += 1
            node = self.disjunction()
            if self.tokens[self.position : self.position + 1] != [("paren", ")")]:
                raise QueryError("unbalanced parentheses in $filter")
            self.position += 1
            return node
        return self.comparison()

    def comparison(self) -> _Node:
        clause = self.tokens[self.position : self.position + 3]
        if len(clause) < 3 or any(kind == "paren" for kind, _ in clause):
            shown = " ".join(t for _, t in self.tokens[self.position :])
            raise QueryError(f"incomplete $filter clause: {shown}")
        self.position += 3
        (_, field), (op_kind, op), (kind, literal) = clause
        _check_field(self.entity, field)
        if op_kind != "word" or op.lower() not in OPERATORS:
            raise QueryError(
                f"unknown $filter operator {op}; use one of {', '.join(OPERATORS)}"
            )
        op = op.lower()
        if kind == "word" and literal.lower() == "null":
            if op not in ("eq", "ne"):
                raise QueryError("null can only be compared with eq or ne")
            return ("cmp", f'"{field}" IS {"NOT " if op == "ne" else ""}NULL', [])
        value = _value(self.entity, field, kind, literal)
        return ("cmp", f'"{field}" {OPERATORS[op]} ?', [value])

    def _next_is(self, word: str) -> bool:
        if self.position < len(self.tokens):
            kind, text = self.tokens[self.position]
            if kind == "word" and text.lower() == word:
                self.position += 1
                return True
        return False


def _render(node: _Node) -> tuple[str, list[object]]:
    if node[0] == "cmp":
        return node[1], node[2]
    rendered = [_render(child) for child in node[1]]
    joiner = " AND " if node[0] == "and" else " OR "
    sql = "(" + joiner.join(part for part, _ in rendered) + ")"
    return sql, [param for _, params in rendered for param in params]


def _tokenize(text: str) -> list[tuple[str, str]]:
    tokens = []
    position = 0
    text = text.rstrip()
    while position < len(text):
        match = TOKEN.match(text, position)
        if not match:
            raise QueryError(f"can't read $filter at: {text[position:]}")
        tokens.append((match.lastgroup or "", match.group(match.lastgroup or 0)))
        position = match.end()
    return tokens


def _value(entity: Entity, field: str, kind: str, literal: str) -> object:
    """The literal as a value of the field's type, or a QueryError."""
    declared = entity.columns[field]
    if declared in TEXTUAL:
        if kind == "text":
            return literal[1:-1].replace("''", "'")
        if kind == "date":
            try:
                return date.fromisoformat(literal).isoformat()
            except ValueError as error:
                raise QueryError(f"not a date: {literal}") from error
        if kind == "timestamp":
            return _utc_timestamp(literal)
    elif declared == "INTEGER" and kind == "number" and "." not in literal:
        return int(literal)
    elif declared == "REAL" and kind == "number":
        return float(literal)
    elif (
        declared == "BOOLEAN"
        and kind == "word"
        and literal.lower() in ("true", "false")
    ):
        return int(literal.lower() == "true")
    raise QueryError(f"{field} is {declared.lower()}; {literal} is not")


def _utc_timestamp(literal: str) -> str:
    """A timestamp literal in the store's own form, `2026-07-06T20:30:00Z`.

    The store's timestamps are whole seconds in UTC, so they compare
    correctly as text once the literal is written the same way.
    """
    try:
        moment = datetime.fromisoformat(literal)
    except ValueError as error:
        raise QueryError(f"not a timestamp: {literal}") from error
    if moment.microsecond:
        raise QueryError(f"timestamps are whole seconds: {literal}")
    return moment.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def encode_skiptoken(skip: int, as_of: str) -> str:
    """A cursor for the page after `skip` rows, read as of `as_of`."""
    raw = json.dumps({"skip": skip, "as_of": as_of}, separators=(",", ":"))
    return base64.urlsafe_b64encode(raw.encode()).rstrip(b"=").decode()


def decode_skiptoken(token: str) -> tuple[int, str]:
    """The rows to skip and the instant a `$skiptoken` pins, or a QueryError."""
    try:
        padded = token + "=" * (-len(token) % 4)
        claims = json.loads(base64.urlsafe_b64decode(padded))
        skip, as_of = claims["skip"], claims["as_of"]
        if isinstance(skip, int) and skip >= 0 and isinstance(as_of, str):
            return skip, as_of
    except (binascii.Error, ValueError, KeyError, TypeError):
        pass
    raise QueryError("$skiptoken is not one this API issued")


def _parse_orderby(entity: Entity, text: str) -> list[str]:
    terms: list[str] = []
    seen: set[str] = set()
    for part in filter(None, (p.strip() for p in text.split(","))):
        words = part.split()
        field, direction = words[0], (words[1].lower() if len(words) > 1 else "asc")
        _check_field(entity, field)
        if len(words) > 2 or direction not in ("asc", "desc"):
            raise QueryError(f"can't read $orderby term: {part}")
        terms.append(f'"{field}" {direction.upper()}')
        seen.add(field)
    terms += [f'"{field}" ASC' for field in entity.key if field not in seen]
    return terms


def _parse_count(name: str, text: str | None, default: int, minimum: int) -> int:
    if text is None:
        return default
    if not text.isdigit() or int(text) < minimum:
        raise QueryError(f"{name} must be a whole number of at least {minimum}")
    return int(text)


def _parse_boolean(name: str, text: str) -> bool:
    if text not in ("true", "false"):
        raise QueryError(f"{name} must be true or false")
    return text == "true"


def _check_field(entity: Entity, field: str) -> None:
    if field not in entity.columns:
        raise QueryError(f"{entity.name} has no field {field}")
