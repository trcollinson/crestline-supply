"""The ERP's change log: what changed, when, and the store that keeps every version.

On the plant run fixture: Mon Jul 6 to Fri Jul 17 2026, extracted Sat Jul 18
06:00 local, so nine nightly labor batches (Friday's labor would post Monday).
"""

import json
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime, time, timedelta
from itertools import pairwise

import pytest

from crestline.mess import ErpMess
from crestline.sim.clock import PLANT_TZ
from crestline.sim.systems import erp

MASTER_DATA = ("WorkCenters", "Customers", "Parts", "PartRevisions")
ONE_SECOND = timedelta(seconds=1)


def key(entity: str, row: dict) -> tuple:
    return tuple(row[k] for k in erp.PRIMARY_KEYS[entity])


def keyed(tables: dict[str, list[dict]]) -> dict[str, dict[tuple, dict]]:
    return {
        entity: {key(entity, row): row for row in rows}
        for entity, rows in tables.items()
    }


def timed(changes: erp.ErpChanges) -> list[erp.Change]:
    return [c for c in changes.changes if c.at is not None]


def postings(changes: erp.ErpChanges) -> list[datetime]:
    return sorted({c.at for c in timed(changes) if c.entity == "LaborDetails"})


@pytest.fixture(scope="module")
def batches(erp_changes) -> list[datetime]:
    return postings(erp_changes)


def test_the_log_is_master_data_then_time_order_up_to_the_extract(
    plant_run, erp_changes
):
    changes = erp_changes.changes
    untimed = [c for c in changes if c.at is None]
    assert changes[: len(untimed)] == untimed  # master data first
    assert {c.entity for c in untimed} == set(MASTER_DATA)

    stamps = [c.at for c in changes[len(untimed) :]]
    assert stamps == sorted(stamps)
    assert stamps[-1] <= plant_run.extracted_at
    # The store compares stamps as text, so every instant is a whole second.
    assert all(at.microsecond == 0 for at in stamps)
    # Clean mode: a row is stamped with the moment it changed.
    for change in changes[len(untimed) :]:
        assert change.row["LastModified"] == erp._timestamp(change.at)


def test_building_twice_gives_the_same_log(build_erp_changes, erp_changes):
    assert build_erp_changes() == erp_changes


# The M1 ERP, and the ERP with every mess on (duplicate orders, splits, stale
# stamps), each posting labor in the nightly batch and at scan-off.
ERP_CASES = {
    "nightly": lambda erp_mess: erp_mess(),
    "at scan-off": lambda erp_mess: erp_mess(labor_post_time=None),
    "every mess on, nightly": lambda erp_mess: ErpMess(),
    "every mess on, at scan-off": lambda erp_mess: ErpMess(labor_post_time=None),
}


@pytest.mark.parametrize("make_mess", ERP_CASES.values(), ids=ERP_CASES)
def test_as_of_equals_an_extract_taken_then(
    plant_run, build_erp_changes, erp_mess, make_mess
):
    """The log replayed up to T is the ERP a backfill extracted at T would write."""
    mess = make_mess(erp_mess)
    changes = build_erp_changes(mess)
    posted = postings(changes)
    entries = {
        c.row["OrderNum"]: c.at for c in timed(changes) if c.entity == "SalesOrders"
    }
    releases: dict[str, datetime] = {}
    for c in timed(changes):
        if c.entity == "Jobs" and c.row["JobReleased"]:
            releases.setdefault(
                c.row["OrderNum"], c.at
            )  # a split child shares the order
    releases = {order: at for order, at in releases.items() if order < "S900000"}
    some_order = next(iter(releases))
    between = entries[some_order] + (releases[some_order] - entries[some_order]) / 2

    moments = [
        posted[3],
        posted[3] - ONE_SECOND,
        releases[some_order],
        min(entries.values()),
        between.replace(microsecond=0),
        plant_run.extracted_at,
    ]
    for at in moments:
        replayed = keyed(changes.as_of(at))
        extracted = keyed(build_erp_changes(mess, extracted_at=at).as_of())
        assert replayed == extracted, at

    # Before the first order was entered there was only master data.
    tables = changes.as_of(min(entries.values()) - ONE_SECOND)
    assert all(tables[entity] for entity in MASTER_DATA)
    assert not any(rows for entity, rows in tables.items() if entity not in MASTER_DATA)


def test_a_job_is_created_at_order_entry_and_released_later(erp_changes):
    versions: dict[tuple, list[erp.Change]] = defaultdict(list)
    for c in timed(erp_changes):
        versions[(c.entity, key(c.entity, c.row))].append(c)
    entries = {
        c.row["OrderNum"]: c.at for c in timed(erp_changes) if c.entity == "SalesOrders"
    }
    jobs = [v for (entity, _), v in versions.items() if entity == "Jobs"]
    assert jobs

    for job_versions in jobs:
        created, released = job_versions[:2]
        job_num = created.row["JobNum"]
        # The order line created the job, unreleased, with nothing issued...
        assert created.at == entries[created.row["OrderNum"]]
        assert created.row["JobReleased"] is False
        material = versions[("JobMaterials", (job_num, erp.MATERIAL_SEQ))]
        costs = versions[("JobCosts", (job_num,))]
        assert material[0].at == costs[0].at == created.at
        assert (material[0].row["IssuedQty"], material[0].row["IssuedComplete"]) == (
            0.0,
            False,
        )
        assert costs[0].row["ActMaterialCost"] == 0.0
        assert costs[0].row["StdMaterialCost"] > 0
        # ...and the planner released it during 1st shift, issuing the sheet stock.
        assert created.at < released.at
        assert time(6, 0) <= released.at.astimezone(PLANT_TZ).time() < time(14, 30)
        assert released.row["JobReleased"] is True
        assert material[1].at == costs[1].at == released.at
        assert material[1].row["IssuedQty"] == material[1].row["RequiredQty"] > 0
        assert material[1].row["IssuedComplete"] is True
        assert costs[1].row["ActMaterialCost"] == costs[1].row["StdMaterialCost"]

    # A job nobody has worked on yet has exactly those two versions.
    untouched = [v for v in jobs if len(v) == 2]
    assert 0 < len(untouched) < len(jobs)
    assert all(not v[-1].row["JobComplete"] for v in untouched)


def test_a_batch_changes_only_what_it_posted_labor_to(erp_changes, batches):
    by_at: dict[datetime, list[erp.Change]] = defaultdict(list)
    for c in timed(erp_changes):
        by_at[c.at].append(c)

    assert len(batches) == 9
    for at in batches:
        local = at.astimezone(PLANT_TZ)
        assert (local.time(), local.weekday() < 5) == (time(22, 0), True)
        changes = by_at[at]
        labor = [c.row for c in changes if c.entity == "LaborDetails"]
        touched_jobs = {row["JobNum"] for row in labor}
        touched_ops = {(row["JobNum"], row["OprSeq"]) for row in labor}
        assert {c.entity for c in changes} == {
            "LaborDetails",
            "JobOperations",
            "Jobs",
            "JobCosts",
        }
        assert {c.row["JobNum"] for c in changes if c.entity == "Jobs"} == touched_jobs
        assert {
            c.row["JobNum"] for c in changes if c.entity == "JobCosts"
        } == touched_jobs
        assert {
            (c.row["JobNum"], c.row["OprSeq"])
            for c in changes
            if c.entity == "JobOperations"
        } == touched_ops

    seqs = [
        c.row["LaborDtlSeq"] for c in timed(erp_changes) if c.entity == "LaborDetails"
    ]
    assert seqs == list(
        range(erp.FIRST_LABOR_DTL_SEQ, erp.FIRST_LABOR_DTL_SEQ + len(seqs))
    )


def test_every_version_differs_from_the_one_before(erp_changes):
    last: dict[tuple, dict] = {}
    for c in erp_changes.changes:
        k = (c.entity, key(c.entity, c.row))
        assert c.row != last.get(k), k
        last[k] = c.row


def test_replay_reproduces_the_store_and_the_export(erp_changes, tmp_path):
    written = erp.write_erp(erp_changes, tmp_path)
    tables = erp_changes.as_of()
    assert written[tmp_path / "erp.db"] == len(erp_changes.changes)

    with sqlite3.connect(tmp_path / "erp.db") as db:
        for entity, columns in erp.ENTITIES.items():
            select = ", ".join(columns)
            current = db.execute(
                f"SELECT {select}, SysValidFrom FROM {entity} ORDER BY rowid"
            ).fetchall()
            lines = (tmp_path / "export" / f"{entity}.jsonl").read_text().splitlines()

            assert [row[:-1] for row in current] == [
                tuple(r[c] for c in columns) for r in tables[entity]
            ]
            assert [json.loads(line) for line in lines] == tables[entity]
            for row in current:
                began = row[-1]
                if entity in MASTER_DATA:
                    assert began is None  # in the ERP since go-live
                else:
                    assert began == row[list(columns).index("LastModified")]
    db.close()


def test_history_holds_exactly_the_superseded_versions(erp_changes, tmp_path):
    erp.write_erp(erp_changes, tmp_path)
    versions = Counter(c.entity for c in erp_changes.changes)

    with sqlite3.connect(tmp_path / "erp.db") as db:
        for entity in erp.ENTITIES:
            history = f"{entity}_History"
            (current,) = db.execute(f"SELECT count(*) FROM {entity}").fetchone()
            (superseded,) = db.execute(f"SELECT count(*) FROM {history}").fetchone()
            assert current + superseded == versions[entity]
            if entity in MASTER_DATA:
                assert superseded == 0
            elif entity in ("Jobs", "JobOperations", "JobMaterials", "JobCosts"):
                assert superseded > 0

            # Each key's versions chain: one ends the moment the next begins,
            # and the last one is the current row.
            key_columns = ", ".join(erp.PRIMARY_KEYS[entity])
            began = {
                row[:-1]: row[-1]
                for row in db.execute(
                    f"SELECT {key_columns}, SysValidFrom FROM {entity}"
                )
            }
            chains: dict[tuple, list[tuple]] = defaultdict(list)
            for row in db.execute(
                f"SELECT {key_columns}, SysValidFrom, SysValidTo FROM {history}"
                f" ORDER BY {key_columns}, SysValidFrom"
            ):
                chains[row[:-2]].append((row[-2], row[-1]))
            for k, chain in chains.items():
                assert all(frm < to for frm, to in chain)
                for (_, to), (next_from, _) in pairwise(chain):
                    assert to == next_from
                assert chain[-1][1] == began[k]
            indexes = [row[1] for row in db.execute(f"PRAGMA index_list({history})")]
            assert f"{history}_Key" in indexes
    db.close()
