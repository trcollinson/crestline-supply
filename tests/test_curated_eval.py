"""eval/curated.py's side of the comparison: the ERP as of a moment, in curated form."""

import sqlite3
from datetime import UTC, datetime

import curated as curated_eval
from crestline_erp_api.store import Store

T1, T2 = "2026-07-06T14:00:00Z", "2026-07-08T04:00:00Z"
COLUMNS = "Company TEXT, JobNum TEXT, ProdQty INTEGER, JobComplete BOOLEAN, LastModified TIMESTAMP"


def make_store(tmp_path) -> Store:
    db = sqlite3.connect(tmp_path / "erp.db")
    db.executescript(
        f"""
        CREATE TABLE Jobs ({COLUMNS}, SysValidFrom TEXT, PRIMARY KEY (JobNum));
        CREATE TABLE Jobs_History ({COLUMNS}, SysValidFrom TEXT, SysValidTo TEXT);
        INSERT INTO Jobs VALUES ('CRE', 'J100001', 40, 1, '{T2}', '{T2}');
        INSERT INTO Jobs_History VALUES ('CRE', 'J100001', 40, 0, '{T1}', '{T1}', '{T2}');
        """
    )
    db.commit()
    db.close()
    return Store(tmp_path / "erp.db")


def test_erp_rows_are_the_version_valid_then_in_curated_names_and_types(tmp_path):
    store = make_store(tmp_path)
    before = curated_eval.erp_rows(store, "Jobs", "2026-07-07T00:00:00Z")
    assert before == {
        ("J100001",): {
            "company": "CRE",
            "jobnum": "J100001",
            "prodqty": 40,
            "jobcomplete": False,
            "lastmodified": datetime(2026, 7, 6, 14, 0, tzinfo=UTC),
        }
    }
    after = curated_eval.erp_rows(store, "Jobs", T2)
    assert after[("J100001",)]["jobcomplete"] is True


def test_report_says_when_every_table_is_at_the_final_state():
    def score(final: bool, missing: int = 0) -> curated_eval.TableScore:
        return curated_eval.TableScore(
            "Jobs", T2, final, 5, 5 - missing, missing, 0, 0, []
        )

    assert "every table as of the ERP's final state" in curated_eval.report(
        [score(True)]
    )
    text = curated_eval.report([score(True), score(False, missing=1)])
    assert "10 ERP rows, 1 wrong; not yet the final state" in text
