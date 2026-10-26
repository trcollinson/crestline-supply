"""The ERP API's query options, parsed into SQL without a store."""

import pytest

from crestline_erp_api.odata import QueryError, parse_query
from crestline_erp_api.store import Entity

JOBS = Entity(
    name="Jobs",
    columns={
        "JobNum": "TEXT",
        "ProdQty": "INTEGER",
        "UnitPrice": "REAL",
        "JobComplete": "BOOLEAN",
        "ReleaseDate": "TEXT",
        "LastModified": "TEXT",
    },
    key=("JobNum",),
)


def test_no_options_is_the_first_100_rows_in_key_order():
    sql, params = parse_query(JOBS, {}).sql()
    assert sql.endswith('FROM "Jobs" ORDER BY "JobNum" ASC LIMIT ? OFFSET ?')
    assert params == [101, 0]  # one extra row says whether another page follows


def test_filter_values_become_parameters_of_the_field_type():
    query = parse_query(
        JOBS,
        {
            "$filter": "JobNum gt 'J100500' and ProdQty le 40 and UnitPrice lt 9.5"
            " and JobComplete eq false and ReleaseDate ge 2026-07-06"
        },
    )
    assert query.where == [
        '"JobNum" > ?',
        '"ProdQty" <= ?',
        '"UnitPrice" < ?',
        '"JobComplete" = ?',
        '"ReleaseDate" >= ?',
    ]
    assert query.params == ["J100500", 40, 9.5, 0, "2026-07-06"]


def test_a_timestamp_is_compared_in_the_stores_utc_form():
    query = parse_query(JOBS, {"$filter": "LastModified gt 2026-11-01T01:30:00-06:00"})
    assert query.params == ["2026-11-01T07:30:00Z"]


def test_a_quote_inside_text_is_written_twice():
    query = parse_query(JOBS, {"$filter": "JobNum eq 'O''Brien'"})
    assert query.params == ["O'Brien"]


def test_null_needs_eq_or_ne():
    assert parse_query(JOBS, {"$filter": "UnitPrice ne null"}).where == [
        '"UnitPrice" IS NOT NULL'
    ]
    with pytest.raises(QueryError, match="eq or ne"):
        parse_query(JOBS, {"$filter": "UnitPrice gt null"})


def test_orderby_ends_with_the_key_so_ties_page_the_same_way():
    query = parse_query(JOBS, {"$orderby": "ProdQty desc, ReleaseDate"})
    assert query.order_by == ['"ProdQty" DESC', '"ReleaseDate" ASC', '"JobNum" ASC']
    assert parse_query(JOBS, {"$orderby": "JobNum desc"}).order_by == ['"JobNum" DESC']


def test_top_is_the_page_size_cut_to_500():
    assert parse_query(JOBS, {"$top": "37", "$skip": "74"}).sql()[1] == [38, 74]
    assert parse_query(JOBS, {"$top": "5000"}).top == 500


def test_as_of_reads_the_versions_valid_then_before_filtering():
    versioned = Entity(JOBS.name, JOBS.columns, JOBS.key, history="Jobs_History")
    query = parse_query(versioned, {"$filter": "ProdQty ge 40"})

    sql, params = query.sql("2026-07-10T04:00:00Z")
    assert "UNION ALL" in sql
    assert 'FROM "Jobs_History" WHERE "SysValidFrom" <= ? AND "SysValidTo" > ?' in sql
    assert sql.endswith('WHERE "ProdQty" >= ? ORDER BY "JobNum" ASC LIMIT ? OFFSET ?')
    assert params == ["2026-07-10T04:00:00Z"] * 3 + [40, 101, 0]
    # Without a moment, the table as it stands, as before.
    assert query.sql() == parse_query(JOBS, {"$filter": "ProdQty ge 40"}).sql()


@pytest.mark.parametrize(
    ("options", "message"),
    [
        ({"$expand": "Lines"}, "unsupported query option"),
        ({"$filter": "Nope eq 1"}, "Jobs has no field Nope"),
        ({"$filter": "ProdQty eq 1.5"}, "ProdQty is integer"),
        ({"$filter": "JobComplete eq 1"}, "JobComplete is boolean"),
        ({"$filter": "ProdQty like 1"}, "unknown \\$filter operator"),
        ({"$filter": "ProdQty eq 1 xor ProdQty eq 2"}, "joined with 'and' or 'or'"),
        ({"$filter": "(ProdQty eq 1"}, "unbalanced"),
        ({"$filter": "ProdQty eq 1)"}, "unbalanced"),
        ({"$filter": "ProdQty eq 1 or"}, "ends with 'or'"),
        ({"$filter": "ProdQty eq 1 and"}, "ends with 'and'"),
        ({"$filter": "ProdQty eq"}, "incomplete"),
        ({"$filter": "ProdQty eq 1;"}, "can't read"),
        ({"$filter": "LastModified gt 2026-07-06T14:30:00.5Z"}, "whole seconds"),
        ({"$filter": "ReleaseDate eq 2026-02-30"}, "not a date"),
        ({"$orderby": "ProdQty sideways"}, "can't read \\$orderby"),
        ({"$top": "0"}, "at least 1"),
        ({"$skip": "ten"}, "whole number"),
    ],
)
def test_what_the_api_cannot_serve_is_named(options, message):
    with pytest.raises(QueryError, match=message):
        parse_query(JOBS, options)


def test_or_and_parentheses_for_paging_by_a_key_of_two_columns():
    ops = Entity(
        "JobOperations", {"JobNum": "TEXT", "OprSeq": "INTEGER"}, ("JobNum", "OprSeq")
    )
    query = parse_query(
        ops, {"$filter": "JobNum gt 'J1' or (JobNum eq 'J1' and OprSeq gt 20)"}
    )
    assert query.where == ['("JobNum" > ? OR ("JobNum" = ? AND "OprSeq" > ?))']
    assert query.params == ["J1", "J1", 20]


def test_and_binds_tighter_than_or_and_parentheses_override_it():
    loose = parse_query(
        JOBS, {"$filter": "ProdQty eq 1 or ProdQty eq 2 and JobComplete eq true"}
    )
    assert loose.where == ['("ProdQty" = ? OR ("ProdQty" = ? AND "JobComplete" = ?))']
    grouped = parse_query(
        JOBS, {"$filter": "(ProdQty eq 1 or ProdQty eq 2) and JobComplete eq true"}
    )
    assert grouped.where == ['("ProdQty" = ? OR "ProdQty" = ?)', '"JobComplete" = ?']
    assert grouped.params == [1, 2, 1]
