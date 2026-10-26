"""The ERP's own mistakes, each on and off: stale stamps, duplicate orders, splits.

The plant run fixture's ERP is built with every one of them off (`M1_ERP` in
conftest); each test turns one on and shows what it changes, and that nothing
else moves.
"""

import sqlite3
from datetime import time, timedelta

import pytest

from crestline.sim.clock import PLANT_TZ
from crestline.sim.systems import erp

ONE_SECOND = timedelta(seconds=1)
STAMP = erp._timestamp


def changes_of(changes: erp.ErpChanges, entity: str) -> list[erp.Change]:
    return [c for c in changes.changes if c.at is not None and c.entity == entity]


def is_duplicate(number: str) -> bool:
    """An order or job number from the block duplicate entries are numbered in."""
    return int(number[1:7]) >= erp.DUPLICATE_FIRST_SEQ


def without(rows: list[dict], *columns: str) -> list[dict]:
    return [{k: v for k, v in row.items() if k not in columns} for row in rows]


def batch_times(changes: erp.ErpChanges):
    return sorted({c.at for c in changes_of(changes, "LaborDetails")})


# --- Stale LastModified ----------------------------------------------------


def test_an_operations_quantities_can_change_without_its_stamp_moving(
    build_erp_changes, erp_mess
):
    honest = build_erp_changes(erp_mess())
    stale = build_erp_changes(erp_mess(lastmodified_skips=["JobOperations.qty"]))

    # Honest: each time labor posts to an operation, the stamp moves with it.
    posted = [
        c for c in changes_of(honest, "JobOperations") if c.at in batch_times(honest)
    ]
    assert posted
    assert all(c.row["LastModified"] == STAMP(c.at) for c in posted)
    created: dict[tuple, object] = {}
    updates = 0
    for c in changes_of(stale, "JobOperations"):
        key = (c.row["JobNum"], c.row["OprSeq"])
        if key not in created:
            created[key] = c.at
            continue
        updates += 1
        # Stale: a later version, with new quantities, still carries the stamp
        # of the day the operation was created.
        assert c.at > created[key]
        assert c.row["LastModified"] == STAMP(created[key])
    assert updates > 0

    # The quantities changed all the same; only the stamps differ.
    assert without(stale.as_of()["JobOperations"], "LastModified") == without(
        honest.as_of()["JobOperations"], "LastModified"
    )
    # Everything else is stamped when it changed, as before.
    for c in stale.changes:
        if c.at and c.entity != "JobOperations":
            assert c.row["LastModified"] == STAMP(c.at), c.entity


def test_posted_labor_keeps_the_time_its_scan_closed(
    build_erp_changes, erp_mess, tmp_path
):
    honest = build_erp_changes(erp_mess())
    stale = build_erp_changes(erp_mess(lastmodified_skips=["LaborDetails.post"]))

    assert all(
        c.row["LastModified"] == STAMP(c.at) for c in changes_of(honest, "LaborDetails")
    )
    assert all(
        c.row["LastModified"] < STAMP(c.at) for c in changes_of(stale, "LaborDetails")
    )
    # Same rows posted at the same moments; only the stamp is back-dated.
    assert without(stale.as_of()["LaborDetails"], "LastModified") == without(
        honest.as_of()["LaborDetails"], "LastModified"
    )

    # What a load by LastModified sees: everything stamped after the previous
    # night's batch. Honest, that is the whole of tonight's batch. Stale, the
    # rows whose scans closed before the previous batch are never seen again.
    batches = batch_times(honest)
    watermark, tonight = batches[4], batches[5]

    def missed(changes: erp.ErpChanges) -> int:
        return sum(
            c.at == tonight and c.row["LastModified"] <= STAMP(watermark)
            for c in changes_of(changes, "LaborDetails")
        )

    assert missed(honest) == 0
    assert missed(stale) > 0

    # The store still knows when each row really arrived.
    erp.write_erp(stale, tmp_path)
    with sqlite3.connect(tmp_path / "erp.db") as db:
        (liars,) = db.execute(
            "SELECT count(*) FROM LaborDetails WHERE SysValidFrom <> LastModified"
        ).fetchone()
        (count,) = db.execute("SELECT count(*) FROM LaborDetails").fetchone()
    db.close()
    assert liars == count > 0


# --- Duplicate orders, soft-deleted ------------------------------------------


def test_duplicate_orders_are_cancelled_the_next_morning(
    build_erp_changes, erp_mess, tmp_path
):
    clean = build_erp_changes(erp_mess())
    messy = build_erp_changes(erp_mess(soft_delete_rate=0.3))
    clean_tables, tables = clean.as_of(), messy.as_of()

    # Off: nothing is deleted and no duplicate number exists.
    assert not any(
        is_duplicate(c.row["OrderNum"]) for c in changes_of(clean, "SalesOrders")
    )
    assert not any(row["Deleted"] for row in clean_tables["SalesOrderLines"])

    # On: real work is untouched, row for row, in every entity.
    for entity, rows in tables.items():
        real = [
            row
            for row in rows
            if not is_duplicate(row.get("OrderNum") or row.get("JobNum", "J000000"))
        ]
        assert real == clean_tables[entity], entity

    duplicates = [row for row in tables["SalesOrders"] if is_duplicate(row["OrderNum"])]
    assert duplicates
    lines = {row["OrderNum"]: row for row in tables["SalesOrderLines"]}
    for order in duplicates:
        # The order stays; its line is soft-deleted; its job is gone.
        assert lines[order["OrderNum"]]["Deleted"] is True
    for entity in ("Jobs", "JobOperations", "JobMaterials", "JobCosts"):
        assert not any(is_duplicate(row["JobNum"]) for row in tables[entity]), entity

    # The life of one duplicate: entered, live and unreleased overnight,
    # cancelled the next working morning between 08:00 and 10:00.
    order = duplicates[0]["OrderNum"]
    entered = next(
        c.at for c in changes_of(messy, "SalesOrders") if c.row["OrderNum"] == order
    )
    cancelled = next(
        c.at
        for c in changes_of(messy, "SalesOrderLines")
        if c.row["OrderNum"] == order and c.row["Deleted"]
    )
    local = cancelled.astimezone(PLANT_TZ)
    assert local.date() > entered.astimezone(PLANT_TZ).date()
    assert local.weekday() < 5
    assert time(8, 0) <= local.time() <= time(10, 0)

    job_num = "J" + order[1:]

    def live(at):
        t = messy.as_of(at)
        job = [r for r in t["Jobs"] if r["JobNum"] == job_num]
        line = next(r for r in t["SalesOrderLines"] if r["OrderNum"] == order)
        return job, line["Deleted"]

    (job,), deleted = live(entered)
    assert (job["JobReleased"], deleted) == (False, False)
    (job,), deleted = live(cancelled - ONE_SECOND)
    assert deleted is False
    assert live(cancelled) == ([], True)

    # The store keeps what was deleted: the job's last version, closed at the
    # moment of the cancellation. Deletes are not versions.
    written = erp.write_erp(messy, tmp_path)
    assert written[tmp_path / "erp.db"] == sum(not c.deleted for c in messy.changes)
    with sqlite3.connect(tmp_path / "erp.db") as db:
        assert db.execute(
            "SELECT count(*) FROM Jobs WHERE JobNum = ?", (job_num,)
        ).fetchone() == (0,)
        assert db.execute(
            "SELECT SysValidTo FROM Jobs_History WHERE JobNum = ?", (job_num,)
        ).fetchall() == [(STAMP(cancelled),)]
    db.close()


def test_more_duplicates_leave_the_splits_where_they_were(build_erp_changes, erp_mess):
    def duplicates(changes):
        return {
            c.row["OrderNum"]
            for c in changes_of(changes, "SalesOrders")
            if is_duplicate(c.row["OrderNum"])
        }

    def splits(changes):
        return {
            c.row["JobNum"]
            for c in changes_of(changes, "Jobs")
            if c.row["JobNum"].endswith("-1")
        }

    few = build_erp_changes(erp_mess(soft_delete_rate=0.1, job_split_rate=0.2))
    many = build_erp_changes(erp_mess(soft_delete_rate=0.5, job_split_rate=0.2))
    assert 0 < len(duplicates(few)) < len(duplicates(many))
    assert len(splits(few)) == len(splits(many)) > 0


# --- Job splits ------------------------------------------------------------------


def test_a_split_job_gives_part_of_its_quantity_to_a_child_nobody_works_on(
    build_erp_changes, erp_mess
):
    clean = build_erp_changes(erp_mess())
    messy = build_erp_changes(erp_mess(job_split_rate=0.3))
    clean_tables, tables = clean.as_of(), messy.as_of()

    # Off: no child jobs.
    assert not any(row["JobNum"].endswith("-1") for row in clean_tables["Jobs"])

    jobs = {row["JobNum"]: row for row in tables["Jobs"]}
    originals = {row["JobNum"]: row for row in clean_tables["Jobs"]}
    children = {num: row for num, row in jobs.items() if num.endswith("-1")}
    assert children
    materials = {row["JobNum"]: row for row in tables["JobMaterials"]}
    costs = {row["JobNum"]: row for row in tables["JobCosts"]}
    original_costs = {row["JobNum"]: row for row in clean_tables["JobCosts"]}
    ops = [row for row in tables["JobOperations"] if row["JobNum"] in children]
    labor_jobs = {row["JobNum"] for row in tables["LaborDetails"]}

    for num, child in children.items():
        parent = jobs[num.removesuffix("-1")]
        original = originals[parent["JobNum"]]
        # The quantity is shared out, both halves non-empty...
        assert parent["ProdQty"] + child["ProdQty"] == original["ProdQty"]
        assert 1 <= child["ProdQty"] < original["ProdQty"]
        assert 1 <= parent["ProdQty"] < original["ProdQty"]
        # ...for the same order line and part...
        for field in ("OrderNum", "OrderLine", "PartNum", "RevisionNum", "ReqDueDate"):
            assert child[field] == parent[field]
        # ...but the floor kept reporting against the parent: the child is
        # released, never worked, never complete.
        assert child["JobReleased"] is True
        assert (child["JobComplete"], child["CompletedQty"]) == (False, 0)
        assert num not in labor_jobs
        assert costs[num]["ActLaborCost"] == costs[num]["ActBurdenCost"] == 0
        # Material follows the quantities, and adds back up.
        for job_num in (parent["JobNum"], num):
            assert materials[job_num]["RequiredQty"] == pytest.approx(
                jobs[job_num]["ProdQty"]
                * original_costs[parent["JobNum"]]["StdMaterialCost"]
                / original["ProdQty"]
                / materials[job_num]["UnitCost"],
                rel=1e-3,
            )
        assert costs[parent["JobNum"]]["StdMaterialCost"] + costs[num][
            "StdMaterialCost"
        ] == pytest.approx(
            original_costs[parent["JobNum"]]["StdMaterialCost"], abs=0.011
        )
        assert (
            costs[parent["JobNum"]]["ActMaterialCost"]
            == costs[parent["JobNum"]]["StdMaterialCost"]
        )
    assert not any(row["QtyCompleted"] or row["OpComplete"] for row in ops)

    # Labor, orders, and every job that was not split are exactly as before.
    assert tables["LaborDetails"] == clean_tables["LaborDetails"]
    for entity in ("SalesOrders", "SalesOrderLines"):
        assert tables[entity] == clean_tables[entity]
    split = set(children) | {num.removesuffix("-1") for num in children}
    for entity in ("Jobs", "JobOperations", "JobMaterials", "JobCosts"):
        kept = [row for row in tables[entity] if row["JobNum"] not in split]
        assert kept == [
            row for row in clean_tables[entity] if row["JobNum"] not in split
        ]
    # A parent still completes when the floor finishes the lot.
    assert [jobs[n.removesuffix("-1")]["JobComplete"] for n in children] == [
        originals[n.removesuffix("-1")]["JobComplete"] for n in children
    ]


def test_a_job_is_split_an_hour_or_more_after_it_is_released(
    build_erp_changes, erp_mess
):
    messy = build_erp_changes(erp_mess(job_split_rate=0.3))
    released = {}
    for c in changes_of(messy, "Jobs"):
        if c.row["JobReleased"]:
            released.setdefault(c.row["JobNum"], c.at)
    children = [c for c in changes_of(messy, "Jobs") if c.row["JobNum"].endswith("-1")]
    assert children
    for child in children:
        gap = child.at - released[child.row["JobNum"].removesuffix("-1")]
        assert timedelta(hours=1) <= gap <= timedelta(hours=4)
        assert child.row["LastModified"] == STAMP(child.at)


def test_a_higher_split_rate_only_adds_splits(build_erp_changes, erp_mess):
    def splits(rate):
        changes = build_erp_changes(erp_mess(job_split_rate=rate))
        return {
            c.row["JobNum"]
            for c in changes_of(changes, "Jobs")
            if c.row["JobNum"].endswith("-1")
        }

    assert splits(0.1) < splits(0.3)  # a proper subset


@pytest.mark.parametrize("post_time", ["nightly", "at scan-off"])
def test_the_store_holds_every_version_with_every_mess_on(
    build_erp_changes, erp_mess, post_time, tmp_path
):
    mess = erp_mess(
        soft_delete_rate=0.2,
        job_split_rate=0.2,
        lastmodified_skips=["JobOperations.qty", "LaborDetails.post"],
        **({"labor_post_time": None} if post_time == "at scan-off" else {}),
    )
    changes = build_erp_changes(mess)
    erp.write_erp(changes, tmp_path)
    tables = changes.as_of()

    with sqlite3.connect(tmp_path / "erp.db") as db:
        for entity in erp.ENTITIES:
            (current,) = db.execute(f"SELECT count(*) FROM {entity}").fetchone()
            (history,) = db.execute(f"SELECT count(*) FROM {entity}_History").fetchone()
            versions = sum(
                c.entity == entity and not c.deleted for c in changes.changes
            )
            assert current == len(tables[entity])
            assert current + history == versions, entity
    db.close()
