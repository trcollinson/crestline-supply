"""The ERP's entities as a client has to know them: primary key, and whether it can be loaded by change.

A client learns this from the API's documentation, not from the API: the
service returns rows and nothing about their keys. A test checks this table
against the store the simulator writes.
"""

# entity -> (primary key, has a LastModified to load by)
ENTITIES: dict[str, tuple[tuple[str, ...], bool]] = {
    # Master data: set up before anyone started extracting, with no change stamp.
    "WorkCenters": (("WorkCenter",), False),
    "Customers": (("CustomerCode",), False),
    "Parts": (("PartNum",), False),
    "PartRevisions": (("PartNum", "RevisionNum"), False),
    "SalesOrders": (("OrderNum",), True),
    "SalesOrderLines": (("OrderNum", "OrderLine"), True),
    "Jobs": (("JobNum",), True),
    "JobOperations": (("JobNum", "OprSeq"), True),
    "JobMaterials": (("JobNum", "MtlSeq"), True),
    "LaborDetails": (("LaborDtlSeq",), True),
    "JobCosts": (("JobNum",), True),
}


# Key columns that hold whole numbers. The API may serve them as strings
# ("20.00000000"); a filter on them must say 20.
INTEGER_KEYS = {"OrderLine", "OprSeq", "MtlSeq", "LaborDtlSeq"}


def key_of(entity: str, row: dict) -> tuple:
    """A row's primary key, as the API served it."""
    return tuple(row[column] for column in ENTITIES[entity][0])


def literal(column: str, value: object) -> str:
    """A key value written as a `$filter` literal: 20, or 'J100123' (quotes doubled)."""
    if column in INTEGER_KEYS:
        return str(int(float(value)))
    return "'" + str(value).replace("'", "''") + "'"
