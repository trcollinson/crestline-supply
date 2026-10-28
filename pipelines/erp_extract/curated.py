"""The curated ERP tables: one typed Iceberg table per entity (spec §9, M2 step 8).

The raw pages keep what the API served: `"ProdQty": "250.00000000"`, and
`LaborDetails` dates as `/Date(1783036800000)/`. The curated tables hold the
same rows, one per key, with real types: whole numbers, decimals, true/false,
dates, and UTC timestamps. Their columns are the ERP's own names, which
Athena shows in lower case (`jobnum`), so a query written against the ERP's
fields runs on them unchanged.

The tables are plain Iceberg tables in the Glue database `crestline_curated`
(`erp_jobs`, `erp_job_operations`, ...), so Athena can put views over them.

This module is the one place the curated columns are written down:
CuratedStack creates the tables from `COLUMNS`, and `merge_sql()` fills in
the MERGE in `curated.sql` (next to this file) for one entity and one window
of raw reads; `check_sql()` fills in `curated_check.sql`, the check the
curate step runs before it. Around a MERGE that could delete, the curate step
writes down which keys went: `deleted_sql()` (`curated_deleted.sql`) after it,
or `gone_sql()` (`curated_gone.sql`) in its place if the check refused it.
"""

import re
from pathlib import Path

from .entities import ENTITIES
from .raw import ingest_date_of

CURATED_DATABASE = "crestline_curated"
RAW_PAGES_TABLE = "raw_erp_api"  # in the raw database: the poller's pages, a line each
# The ingest dates RAW_PAGES_TABLE's partition projection covers
# (ErpIngestStack). Athena sees no page filed outside them.
RAW_INGEST_DATES = ("2020-01-01", "2099-12-31")
TEMPLATE = Path(__file__).with_name("curated.sql")
CHECK_TEMPLATE = Path(__file__).with_name("curated_check.sql")
GONE_TEMPLATE = Path(__file__).with_name("curated_gone.sql")
DELETED_TEMPLATE = Path(__file__).with_name("curated_deleted.sql")
PLACEHOLDER = re.compile(r"\$\{(\w+)\}")

# entity -> its curated table
TABLES: dict[str, str] = {
    "WorkCenters": "erp_work_centers",
    "Customers": "erp_customers",
    "Parts": "erp_parts",
    "PartRevisions": "erp_part_revisions",
    "SalesOrders": "erp_sales_orders",
    "SalesOrderLines": "erp_sales_order_lines",
    "Jobs": "erp_jobs",
    "JobOperations": "erp_job_operations",
    "JobMaterials": "erp_job_materials",
    "LaborDetails": "erp_labor_details",
    "JobCosts": "erp_job_costs",
}

# entity -> (field, Iceberg type), in the order the API serves them
COLUMNS: dict[str, tuple[tuple[str, str], ...]] = {
    "WorkCenters": (
        ("Company", "string"),
        ("WorkCenter", "string"),
        ("LaborRate", "double"),
        ("BurdenRate", "double"),
    ),
    "Customers": (
        ("Company", "string"),
        ("CustomerCode", "string"),
        ("Name", "string"),
    ),
    "Parts": (
        ("Company", "string"),
        ("PartNum", "string"),
        ("Description", "string"),
        ("PartType", "string"),
        ("Family", "string"),
        ("UOM", "string"),
        ("UnitPrice", "double"),
        ("UnitCost", "double"),
    ),
    "PartRevisions": (
        ("Company", "string"),
        ("PartNum", "string"),
        ("RevisionNum", "string"),
        ("Approved", "boolean"),
        ("MtlPartNum", "string"),
        ("MtlQtyPerPiece", "double"),
    ),
    "SalesOrders": (
        ("Company", "string"),
        ("OrderNum", "string"),
        ("CustomerCode", "string"),
        ("OrderDate", "date"),
        ("LastModified", "timestamp"),
    ),
    "SalesOrderLines": (
        ("Company", "string"),
        ("OrderNum", "string"),
        ("OrderLine", "int"),
        ("PartNum", "string"),
        ("RevisionNum", "string"),
        ("OrderQty", "int"),
        ("UnitPrice", "double"),
        ("Deleted", "boolean"),
        ("LastModified", "timestamp"),
    ),
    "Jobs": (
        ("Company", "string"),
        ("JobNum", "string"),
        ("OrderNum", "string"),
        ("OrderLine", "int"),
        ("PartNum", "string"),
        ("RevisionNum", "string"),
        ("ProdQty", "int"),
        ("ReleaseDate", "date"),
        ("ReqDueDate", "date"),
        ("JobReleased", "boolean"),
        ("JobComplete", "boolean"),
        ("CompletedQty", "int"),
        ("LastModified", "timestamp"),
    ),
    "JobOperations": (
        ("Company", "string"),
        ("JobNum", "string"),
        ("OprSeq", "int"),
        ("WorkCenter", "string"),
        ("StdSetupHours", "double"),
        ("StdRunHoursPerPiece", "double"),
        ("QtyCompleted", "int"),
        ("ScrapQty", "int"),
        ("OpComplete", "boolean"),
        ("LastModified", "timestamp"),
    ),
    "JobMaterials": (
        ("Company", "string"),
        ("JobNum", "string"),
        ("MtlSeq", "int"),
        ("MtlPartNum", "string"),
        ("RequiredQty", "double"),
        ("IssuedQty", "double"),
        ("UnitCost", "double"),
        ("IssuedComplete", "boolean"),
        ("LastModified", "timestamp"),
    ),
    "LaborDetails": (
        ("Company", "string"),
        ("LaborDtlSeq", "int"),
        ("EmployeeNum", "string"),
        ("JobNum", "string"),
        ("OprSeq", "int"),
        ("WorkCenter", "string"),
        ("PayrollDate", "date"),
        ("LaborHrs", "double"),
        ("LaborQty", "int"),
        ("ScrapQty", "int"),
        ("LastModified", "timestamp"),
    ),
    "JobCosts": (
        ("Company", "string"),
        ("JobNum", "string"),
        ("StdLaborCost", "double"),
        ("StdBurdenCost", "double"),
        ("StdMaterialCost", "double"),
        ("ActLaborCost", "double"),
        ("ActBurdenCost", "double"),
        ("ActMaterialCost", "double"),
        ("LastModified", "timestamp"),
    ),
}

# How a value served as text becomes its type, in Athena SQL. `{v}` is the
# text: a JSON string or number as written, `true`/`false`, or NULL.
_LEGACY = "{v} like '/Date(%'"
_EPOCH = "from_unixtime(cast(regexp_extract({v}, '-?\\d+') as bigint) / 1000e0, 'UTC')"
TYPE_SQL: dict[str, str] = {
    "string": "{v}",
    "boolean": "cast({v} as boolean)",
    # "250.00000000" won't cast straight to a whole number; through double it does.
    "int": "cast(cast({v} as double) as integer)",
    "double": "cast({v} as double)",
    # Dates and timestamps: ISO 8601, or /Date(ms)/ for the legacy entities.
    "date": f"case when {_LEGACY} then cast({_EPOCH} as date) else from_iso8601_date({{v}}) end",
    "timestamp": (
        f"cast(case when {_LEGACY} then {_EPOCH} else from_iso8601_timestamp({{v}}) end"
        " at time zone 'UTC' as timestamp)"
    ),
}


def column_name(field: str) -> str:
    """The curated column for an ERP field: its name in lower case."""
    return field.lower()


def merge_sql(
    entity: str,
    *,
    curated_database: str,
    raw_database: str,
    first_read: str,
    last_read: str,
    starts_complete: bool,
    template: str | None = None,
) -> str:
    """The MERGE that folds one window of an entity's raw reads into its table.

    `first_read` and `last_read` are raw file names without `.jsonl`
    (`20260806T061351Z-001-sweep`); the window is every read between them.
    `starts_complete` says the first is a full read or a sweep, so a key it
    and the reads after it lack is deleted.
    """
    sql = template if template is not None else TEMPLATE.read_text(encoding="utf-8")
    return _fill(
        sql,
        entity,
        curated_database,
        raw_database,
        first_read,
        last_read,
        starts_complete,
    )


def check_sql(
    entity: str,
    *,
    curated_database: str,
    raw_database: str,
    first_read: str,
    last_read: str,
    starts_complete: bool,
) -> str:
    """The check run before that MERGE: rows Athena sees, rows held, rows it would delete."""
    sql = CHECK_TEMPLATE.read_text(encoding="utf-8")
    return _fill(
        sql,
        entity,
        curated_database,
        raw_database,
        first_read,
        last_read,
        starts_complete,
    )


def gone_sql(
    entity: str,
    *,
    curated_database: str,
    raw_database: str,
    first_read: str,
    last_read: str,
    starts_complete: bool,
) -> str:
    """The keys the MERGE for this window would delete, one row each."""
    sql = GONE_TEMPLATE.read_text(encoding="utf-8")
    return _fill(
        sql,
        entity,
        curated_database,
        raw_database,
        first_read,
        last_read,
        starts_complete,
    )


def deleted_sql(entity: str, *, curated_database: str, snapshot: int) -> str:
    """The keys the table held at `snapshot` and doesn't hold now, one row each."""
    sql = DELETED_TEMPLATE.read_text(encoding="utf-8")
    values = {
        "curated_table": f'"{curated_database}"."{TABLES[entity]}"',
        "keys": ", ".join(column_name(field) for field in ENTITIES[entity][0]),
        "snapshot": str(int(snapshot)),
    }
    return PLACEHOLDER.sub(lambda match: values[match.group(1)], sql)


def _fill(
    sql: str,
    entity: str,
    curated_database: str,
    raw_database: str,
    first_read: str,
    last_read: str,
    starts_complete: bool,
) -> str:
    """Fill in a template's ${...} names for one entity and one window."""
    fields = COLUMNS[entity]
    kinds = dict(fields)
    keys = [column_name(field) for field in ENTITIES[entity][0]]
    columns = [column_name(field) for field, _ in fields]
    others = [column for column in columns if column not in keys]
    values = {
        "entity": entity,
        "curated_table": f'"{curated_database}"."{TABLES[entity]}"',
        "raw_table": f'"{raw_database}"."{RAW_PAGES_TABLE}"',
        "first_read": first_read,
        "last_read": last_read,
        "first_date": ingest_date_of(first_read),
        "last_date": ingest_date_of(last_read),
        "starts_complete": "true" if starts_complete else "false",
        "as_served": _lines(
            f"json_extract_scalar(record, '$.{field}') as {column_name(field)}"
            for field, _ in fields
        ),
        "typed": _lines(_typed(column_name(field), kind) for field, kind in fields),
        "keys": ", ".join(keys),
        "columns": ", ".join(columns),
        "key_match": " and ".join(f"t.{key} = s.{key}" for key in keys),
        "copy_match": " and ".join(f"n.{key} = c.{key}" for key in keys),
        "changed": "\n                  or ".join(
            f"t.{column} is distinct from s.{column}" for column in others
        ),
        "assignments": ", ".join(f"{column} = s.{column}" for column in others),
        "source_columns": ", ".join(f"s.{column}" for column in columns),
        "keys_as_served": ", ".join(
            f"json_extract_scalar(record, '$.{field}') as {column_name(field)}"
            for field in ENTITIES[entity][0]
        ),
        "typed_keys": ", ".join(
            _typed(column_name(field), kinds[field]) for field in ENTITIES[entity][0]
        ),
    }
    missing = set(PLACEHOLDER.findall(sql)) - values.keys()
    if missing:
        raise KeyError(f"no value for {sorted(missing)}")
    return PLACEHOLDER.sub(lambda match: values[match.group(1)], sql)


def upkeep_sql(entity: str, *, curated_database: str) -> list[str]:
    """A table's upkeep: compact its small files, then drop what nothing needs.

    Every MERGE adds a few small files (new rows, and delete files marking
    the rows it replaced). OPTIMIZE rewrites them into fewer, larger ones;
    VACUUM expires old snapshots (older than Iceberg's default of five days)
    and deletes the files only they used.
    """
    # Unquoted: Athena hands these two to a parser that won't take "quotes".
    table = f"{curated_database}.{TABLES[entity]}"
    return [
        f"optimize {table} rewrite data using bin_pack",
        f"vacuum {table}",
    ]


def _typed(column: str, kind: str) -> str:
    """One column of the `typed` step: `cast(prodqty ...) as prodqty`, or just `company`."""
    expression = TYPE_SQL[kind].format(v=column)
    return column if expression == column else f"{expression} as {column}"


def _lines(items) -> str:
    return ",\n               ".join(items)
