"""Score the curated tables against the ERP itself (M2 step 8).

The curated tables in Athena (`crestline_curated`) are what the platform
built from the poller's raw pages. Each one is compared, row for row by key,
with the ERP's store (`erp.db`) as it stood when the newest raw read merged
into that table was taken: the server time of that read's last page. Once
the ERP's replay has passed the end of its history, that is the ERP's final
state, and the report says so. By entity:

- **missing**: the ERP has the row; the curated table doesn't
- **extra**: the curated table has a row the ERP doesn't (never had, or deleted)
- **different**: both have the row, but some value differs

With `--margin`, it also runs the margin query (`queries/margin_gap.sql`)
twice, in DuckDB over the backfill's export (`out/`) and in Athena, where the
ERP comes from the curated tables, and compares the answer table cell for cell and the per-job
margins job by job. That only means something once the curated tables hold
the final state; run `crestline athena setup` first, so the Athena views
point at them.

Like the extraction eval, this reads the ERP's store directly; it reads
nothing from the answer key. AWS account and region from AWS_PROFILE and
AWS_REGION.

    uv run python eval/curated.py [--out out] [--margin]
"""

import argparse
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from crestline import query
from crestline.athena import Athena
from crestline.stack import curate_setup
from crestline_erp_api.store import Store
from erp_extract.curated import COLUMNS, TABLES, column_name
from erp_extract.entities import ENTITIES
from erp_extract.raw import SOURCE, ingest_date_of


@dataclass(frozen=True)
class TableScore:
    entity: str
    as_of: str | None  # the ERP moment compared against; None: nothing merged yet
    final: bool  # as_of is past the ERP's last change
    erp_rows: int
    curated_rows: int
    missing: int
    extra: int
    different: int
    examples: list[str]

    @property
    def wrong(self) -> int:
        return self.missing + self.extra + self.different


def as_of(setup: dict, entity: str) -> str | None:
    """When the ERP served the last page of the newest read merged into the table."""
    item = setup["table"].get_item(Key={"entity": entity}).get("Item", {})
    read = item.get("merged_through")
    if not read:
        return None
    key = f"{SOURCE}/entity={entity}/ingest_date={ingest_date_of(read)}/{read}.jsonl"
    body = setup["s3"].get_object(Bucket=setup["bucket"], Key=key)["Body"].read()
    last_page = json.loads(body.decode("utf-8").splitlines()[-1])
    return last_page["server_time"]


def erp_rows(store: Store, entity: str, moment: str) -> dict[tuple, dict]:
    """The ERP's rows as of a moment, keyed, in the curated table's column names."""
    table = store.entities[entity]
    source, params = table.source(moment)
    columns = ", ".join(f'"{c}"' for c in table.columns)
    rows = {}
    for values in store.fetch(f"SELECT {columns} FROM {source}", params):
        row = {
            column_name(column): _comparable(value, declared)
            for (column, declared), value in zip(
                table.columns.items(), values, strict=True
            )
        }
        rows[_key(entity, row)] = row
    return rows


def curated_rows(athena: Athena, entity: str, database: str) -> dict[tuple, dict]:
    """A curated table's rows, keyed, as Athena returns them."""
    query_id, _ = athena.execute(f'SELECT * FROM "{database}"."{TABLES[entity]}"')
    # Athena returns timestamps as text, in UTC: `2026-07-03 13:26:48.000000`.
    timestamps = {column_name(f) for f, kind in COLUMNS[entity] if kind == "timestamp"}
    rows = {}
    for row in athena.fetch(query_id).iter_rows(named=True):
        for column in timestamps:
            if row[column] is not None:
                row[column] = datetime.fromisoformat(row[column]).replace(tzinfo=UTC)
        rows[_key(entity, row)] = row
    return rows


def last_change(store: Store) -> str:
    """The moment of the ERP's last change: after it, any as-of is the final state."""
    return max(
        store.fetch(f'SELECT max("SysValidFrom") FROM "{entity}"', [])[0][0] or ""
        for entity in ENTITIES
    )


def score_tables(out: Path) -> list[TableScore]:
    setup = curate_setup()
    athena = Athena.from_stack()
    store = Store(out / "erp" / "erp.db")
    end = last_change(store)
    scores = []
    for entity in ENTITIES:
        moment = as_of(setup, entity)
        if moment is None:
            scores.append(TableScore(entity, None, False, 0, 0, 0, 0, 0, []))
            continue
        erp = erp_rows(store, entity, moment)
        curated = curated_rows(athena, entity, setup["curated_database"])
        missing = erp.keys() - curated.keys()
        extra = curated.keys() - erp.keys()
        different = [
            key for key in erp.keys() & curated.keys() if erp[key] != curated[key]
        ]
        examples = [f"missing {key}" for key in sorted(missing)[:2]]
        examples += [f"extra {key}" for key in sorted(extra)[:2]]
        examples += [
            f"{key}: ERP {_diff(erp[key], curated[key])}"
            for key in sorted(different)[:2]
        ]
        scores.append(
            TableScore(
                entity,
                moment,
                moment >= end,
                len(erp),
                len(curated),
                len(missing),
                len(extra),
                len(different),
                examples,
            )
        )
    return scores


def compare_margin(out: Path) -> list[str]:
    """The margin query: DuckDB over the export against Athena over the curated tables."""
    con = query.connect(out)
    local_answer = query.run(con, "margin_gap").pl()
    local_jobs = con.sql("SELECT * FROM job_margins").pl()

    athena = Athena.from_stack()
    result = athena.run_query("margin_gap")
    query_id, _ = athena.execute("SELECT * FROM job_margins")
    athena_jobs = athena.fetch(query_id)

    lines = [
        "Margin query, DuckDB (export) vs Athena (curated tables):",
        str(local_answer),
        str(result.frame),
    ]
    answer_equal = _frames_equal(local_answer, result.frame, sort=["brake", "shift"])
    lines.append(f"answer table identical in every cell: {answer_equal}")

    joined = local_jobs.join(athena_jobs, on="job_num", how="full", suffix="_athena")
    only_one = joined.filter(
        pl.col("job_num").is_null() | pl.col("job_num_athena").is_null()
    ).height
    both = joined.filter(
        pl.col("job_num").is_not_null() & pl.col("job_num_athena").is_not_null()
    )
    differ = {}
    for column in local_jobs.columns:
        if column == "job_num":
            continue
        left, right = pl.col(column), pl.col(f"{column}_athena")
        if local_jobs.schema[column].is_numeric():
            mismatch = (left - right).abs() > 1e-9
        else:
            mismatch = left.ne_missing(right)
        differ[column] = both.filter(
            mismatch.fill_null(True) & ~(left.is_null() & right.is_null())
        ).height
    lines.append(
        f"jobs: {local_jobs.height} in DuckDB, {athena_jobs.height} in Athena, "
        f"{only_one} in only one; per column, jobs that differ: {differ}"
    )
    lines.append(f"Athena scanned {result.bytes_scanned / 1e6:.1f} MB")
    return lines


def report(scores: list[TableScore]) -> str:
    lines = [
        "Curated tables against the ERP as of the newest read merged into each:",
        (
            f"{'entity':<16} {'as of (ERP time)':<22} {'ERP':>6} {'curated':>8} "
            f"{'missing':>8} {'extra':>6} {'different':>10}"
        ),
    ]
    for s in scores:
        moment = (s.as_of or "nothing merged") + (" final" if s.final else "")
        lines.append(
            f"{s.entity:<16} {moment:<22} {s.erp_rows:>6} {s.curated_rows:>8} "
            f"{s.missing:>8} {s.extra:>6} {s.different:>10}"
        )
        lines += [f"    {example}" for example in s.examples]
    total = sum(s.erp_rows for s in scores)
    wrong = sum(s.wrong for s in scores)
    final = all(s.final for s in scores)
    lines.append(
        f"{total:,} ERP rows, {wrong:,} wrong; "
        + (
            "every table as of the ERP's final state"
            if final
            else "not yet the final state"
        )
    )
    return "\n".join(lines)


def _key(entity: str, row: dict) -> tuple:
    return tuple(row[column_name(field)] for field in ENTITIES[entity][0])


def _comparable(value: object, declared: str) -> object:
    """A store value in the form Athena hands back the curated one."""
    if value is None:
        return None
    if declared == "BOOLEAN":
        return bool(value)
    if declared == "TIMESTAMP":
        return datetime.fromisoformat(value).astimezone(UTC)
    return value


def _diff(erp: dict, curated: dict) -> str:
    return ", ".join(
        f"{column} {erp[column]!r} vs {curated.get(column)!r}"
        for column in erp
        if erp[column] != curated.get(column)
    )


def _frames_equal(left: pl.DataFrame, right: pl.DataFrame, sort: list[str]) -> bool:
    if left.columns != right.columns or left.height != right.height:
        return False
    left, right = left.sort(sort), right.sort(sort)
    for column in left.columns:
        a, b = left[column].to_list(), right[column].to_list()
        for x, y in zip(a, b, strict=True):
            if isinstance(x, float) and isinstance(y, float):
                if abs(x - y) > 1e-9:
                    return False
            elif x != y and not (x in (None, "") and y in (None, "")):
                return False
    return True


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--out", type=Path, default=Path("out"), help="backfill output")
    parser.add_argument(
        "--margin",
        action="store_true",
        help="also compare the margin query on both engines",
    )
    args = parser.parse_args(argv)
    print(report(score_tables(args.out)))
    if args.margin:
        print()
        print("\n".join(compare_margin(args.out)))


if __name__ == "__main__":
    main()
