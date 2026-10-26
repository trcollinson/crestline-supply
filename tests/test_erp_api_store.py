"""Reading a versioned store as of a moment, on a hand-built `erp.db`."""

import sqlite3
from datetime import UTC, datetime

import pytest

from crestline_erp_api.odata import parse_query
from crestline_erp_api.store import Store

T1, T2, T3 = "2026-07-06T14:00:00Z", "2026-07-08T04:00:00Z", "2026-07-09T04:00:00Z"
JOB_COLUMNS = "Company TEXT, JobNum TEXT, CompletedQty INTEGER, JobComplete BOOLEAN, LastModified TEXT"


@pytest.fixture
def store(tmp_path) -> Store:
    """One job with three versions, one with one, and a master-data part."""
    db = sqlite3.connect(tmp_path / "erp.db")
    db.executescript(
        f"""
        CREATE TABLE Jobs ({JOB_COLUMNS}, SysValidFrom TEXT, PRIMARY KEY (JobNum));
        CREATE TABLE Jobs_History ({JOB_COLUMNS}, SysValidFrom TEXT, SysValidTo TEXT);
        CREATE TABLE Parts (Company TEXT, PartNum TEXT, SysValidFrom TEXT, PRIMARY KEY (PartNum));
        CREATE TABLE Parts_History (Company TEXT, PartNum TEXT, SysValidFrom TEXT, SysValidTo TEXT);
        INSERT INTO Jobs VALUES ('CRE', 'J100001', 40, 1, '{T3}', '{T3}');
        INSERT INTO Jobs_History VALUES ('CRE', 'J100001', 0, 0, '{T1}', '{T1}', '{T2}');
        INSERT INTO Jobs_History VALUES ('CRE', 'J100001', 25, 0, '{T2}', '{T2}', '{T3}');
        INSERT INTO Jobs VALUES ('CRE', 'J100002', 0, 0, '{T2}', '{T2}');
        INSERT INTO Parts VALUES ('CRE', 'BRK-1042', NULL);
        """
    )
    db.commit()
    db.close()
    return Store(tmp_path / "erp.db")


def read(store: Store, entity: str, as_of: str | None, **options: str) -> list[dict]:
    e = store.entities[entity]
    return [e.record(row) for row in store.fetch(*parse_query(e, options).sql(as_of))]


def test_the_store_hides_its_own_columns_and_history_tables(store):
    assert set(store.entities) == {"Jobs", "Parts"}
    assert list(store.entities["Jobs"].columns) == [
        "Company",
        "JobNum",
        "CompletedQty",
        "JobComplete",
        "LastModified",
    ]
    assert store.entities["Jobs"].key == ("JobNum",)
    assert store.entities["Jobs"].history == "Jobs_History"
    assert store.versioned


def test_exactly_one_version_of_a_row_is_visible_at_any_moment(store):
    def jobs(as_of: str | None) -> list[tuple]:
        return [
            (r["JobNum"], r["CompletedQty"], r["JobComplete"])
            for r in read(store, "Jobs", as_of)
        ]

    assert jobs("2026-07-06T13:59:59Z") == []
    assert jobs(T1) == [("J100001", 0, False)]  # a change at the moment itself counts
    assert jobs("2026-07-07T00:00:00Z") == [("J100001", 0, False)]
    assert jobs(T2) == [("J100001", 25, False), ("J100002", 0, False)]
    assert jobs("2026-07-09T03:59:59Z") == [
        ("J100001", 25, False),
        ("J100002", 0, False),
    ]
    assert jobs(T3) == [("J100001", 40, True), ("J100002", 0, False)]
    assert jobs("2027-01-01T00:00:00Z") == [
        ("J100001", 40, True),
        ("J100002", 0, False),
    ]
    assert jobs(None) == jobs(T3)  # as it stands


def test_filters_apply_to_the_version_valid_then(store):
    complete = {"$filter": "JobComplete eq true"}
    assert read(store, "Jobs", T2, **complete) == []
    assert [r["JobNum"] for r in read(store, "Jobs", T3, **complete)] == ["J100001"]
    changed = {"$filter": f"LastModified gt {T1}"}
    assert [
        r["JobNum"] for r in read(store, "Jobs", "2026-07-07T00:00:00Z", **changed)
    ] == []
    assert [r["JobNum"] for r in read(store, "Jobs", T2, **changed)] == [
        "J100001",
        "J100002",
    ]


def test_master_data_has_always_been_there(store):
    part = {"Company": "CRE", "PartNum": "BRK-1042"}
    assert read(store, "Parts", "2000-01-01T00:00:00Z") == [part]
    assert read(store, "Parts", None) == [part]


def test_a_store_without_history_is_served_as_it_stands(tmp_path):
    db = sqlite3.connect(tmp_path / "erp.db")
    db.execute("CREATE TABLE Jobs (Company TEXT, JobNum TEXT, PRIMARY KEY (JobNum))")
    db.execute("INSERT INTO Jobs VALUES ('CRE', 'J100001')")
    db.commit()
    db.close()
    store = Store(tmp_path / "erp.db")

    assert not store.versioned
    assert store.entities["Jobs"].history is None
    assert read(store, "Jobs", T1) == [{"Company": "CRE", "JobNum": "J100001"}]


def test_the_history_ends_at_its_last_change(store, tmp_path):
    assert store.last_change() == datetime.fromisoformat(T3)
    # A row deleted outright counts: its last version ends when it was deleted.
    db = sqlite3.connect(tmp_path / "erp.db")
    db.execute(
        "INSERT INTO Jobs_History VALUES"
        " ('CRE', 'J900001', 0, 0, ?, ?, '2026-07-10T15:00:00Z')",
        (T1, T1),
    )
    db.commit()
    db.close()
    assert store.last_change() == datetime(2026, 7, 10, 15, tzinfo=UTC)


def test_a_store_without_history_has_no_end(tmp_path):
    db = sqlite3.connect(tmp_path / "flat.db")
    db.execute("CREATE TABLE Parts (Company TEXT, PartNum TEXT, PRIMARY KEY (PartNum))")
    db.commit()
    db.close()
    assert Store(tmp_path / "flat.db").last_change() is None
