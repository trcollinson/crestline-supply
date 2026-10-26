"""The ERP's tables at the extract: the costing rules, on the whole log replayed."""

import json
import sqlite3
from datetime import date, datetime, time, timedelta

import polars as pl
import pytest

from crestline.mess import ErpMess
from crestline.sim.clock import PLANT_TZ
from crestline.sim.systems import erp

CENT = 0.01


@pytest.fixture(scope="module")
def tables(erp_changes):
    return erp_changes.as_of()


def by_key(rows, *keys):
    return {tuple(row[k] for k in keys): row for row in rows}


def test_standard_cost_is_the_routing_at_the_rates(reference_data, tables):
    parts = {(p.part_id, p.rev): p for p in reference_data.parts}
    rates = {r.work_center: r for r in reference_data.rates}
    cost_per_lb = {m.material_id: m.cost_per_lb for m in reference_data.materials}
    costs = by_key(tables["JobCosts"], "JobNum")

    for job in tables["Jobs"]:
        part = parts[(job["PartNum"], job["RevisionNum"])]
        hours = {
            op.work_center: (op.setup_min + op.run_min_per_piece * job["ProdQty"]) / 60
            for op in part.routing
        }
        cost = costs[(job["JobNum"],)]
        assert cost["StdLaborCost"] == pytest.approx(
            sum(h * rates[wc].labor_per_hr for wc, h in hours.items()), abs=CENT
        )
        assert cost["StdBurdenCost"] == pytest.approx(
            sum(h * rates[wc].machine_burden_per_hr for wc, h in hours.items()),
            abs=CENT,
        )
        assert cost["StdMaterialCost"] == pytest.approx(
            job["ProdQty"] * part.weight_lb * cost_per_lb[part.material_id], abs=CENT
        )
        assert cost["ActMaterialCost"] == cost["StdMaterialCost"]


def test_actual_labor_is_the_posted_mes_hours(reference_data, tables):
    rates = {r.work_center: r.labor_per_hr for r in reference_data.rates}
    expected: dict[str, float] = {}
    for labor in tables["LaborDetails"]:
        expected[labor["JobNum"]] = (
            expected.get(labor["JobNum"], 0)
            + labor["LaborHrs"] * rates[labor["WorkCenter"]]
        )
    for cost in tables["JobCosts"]:
        assert cost["ActLaborCost"] == pytest.approx(
            expected.get(cost["JobNum"], 0), abs=CENT
        )


def test_burden_is_applied_at_standard_hours(reference_data, tables):
    rates = {r.work_center: r.machine_burden_per_hr for r in reference_data.rates}
    started = {(row["JobNum"], row["OprSeq"]) for row in tables["LaborDetails"]}
    earned: dict[str, float] = {}
    for op in tables["JobOperations"]:
        key = (op["JobNum"], op["OprSeq"])
        hours = (op["StdSetupHours"] if key in started else 0) + op[
            "StdRunHoursPerPiece"
        ] * (op["QtyCompleted"] + op["ScrapQty"])
        earned[op["JobNum"]] = (
            earned.get(op["JobNum"], 0) + hours * rates[op["WorkCenter"]]
        )

    for cost in tables["JobCosts"]:
        assert cost["ActBurdenCost"] == pytest.approx(earned[cost["JobNum"]], abs=CENT)
        # However long the machines really ran, burden never exceeds standard.
        assert cost["ActBurdenCost"] <= cost["StdBurdenCost"] + CENT


def test_completed_qty_posts_only_when_the_job_completes(tables):
    final_ops = {}
    for op in tables["JobOperations"]:
        final_ops[op["JobNum"]] = op  # routing order: the last one wins

    assert any(job["JobComplete"] for job in tables["Jobs"])
    assert not all(job["JobComplete"] for job in tables["Jobs"])
    for job in tables["Jobs"]:
        final = final_ops[job["JobNum"]]
        assert job["JobComplete"] == final["OpComplete"]
        if job["JobComplete"]:
            assert job["CompletedQty"] == final["QtyCompleted"]
        else:
            assert job["CompletedQty"] == 0


def test_jobs_are_released_over_the_order_to_cover_scrap(tables):
    lines = by_key(tables["SalesOrderLines"], "OrderNum", "OrderLine")

    for job in tables["Jobs"]:
        line = lines[(job["OrderNum"], job["OrderLine"])]
        assert job["ProdQty"] > line["OrderQty"]
        assert job["PartNum"] == line["PartNum"]


def test_orders_are_entered_before_their_jobs_are_released(plant_run, tables):
    entered = {o.order_num: o.order_date for o in plant_run.jobs.sales_orders}
    jobs = by_key(tables["Jobs"], "OrderNum")

    for order in tables["SalesOrders"]:
        assert order["OrderDate"] == entered[order["OrderNum"]].isoformat()
        job = jobs[(order["OrderNum"],)]
        assert order["OrderDate"] < job["ReleaseDate"]
        assert order["LastModified"] < job["LastModified"]


def test_labor_rounding_on_and_off(build_erp_changes, tables):
    rounded = [row["LaborHrs"] for row in tables["LaborDetails"]]
    assert all(round(h * 10, 6) == round(h * 10) for h in rounded)
    assert 0.0 in rounded  # a two-minute scan rounds away to nothing

    exact = build_erp_changes(ErpMess(labor_rounding_hours=0)).as_of()
    exact_hours = [row["LaborHrs"] for row in exact["LaborDetails"]]
    assert len(exact_hours) == len(rounded)
    assert not all(round(h * 10, 6) == round(h * 10) for h in exact_hours)
    for h_rounded, h_exact in zip(rounded, exact_hours, strict=True):
        assert abs(h_rounded - h_exact) <= 0.05 + 1e-9


def test_labor_posts_in_the_nightly_batch_on_and_off(
    plant_run, scans, build_erp_changes, tables
):
    batched = tables["LaborDetails"]
    for row in batched:
        posted = datetime.fromisoformat(row["LastModified"]).astimezone(PLANT_TZ)
        payroll = date.fromisoformat(row["PayrollDate"])
        # 22:00 the next working day; Friday's labor posts Monday night.
        next_working_day = payroll + timedelta(days=3 if payroll.weekday() == 4 else 1)
        assert (posted.date(), posted.time()) == (next_working_day, time(22, 0))
    # The window ends on a Friday, so its labor waits for Monday's batch.
    assert plant_run.last_day.isoformat() not in {r["PayrollDate"] for r in batched}

    immediate = build_erp_changes(ErpMess(labor_post_time=None)).as_of()["LaborDetails"]
    closed = scans.filter(pl.col("scan_off") <= plant_run.extracted_at)
    assert len(immediate) == closed.height > len(batched)
    assert sorted(row["LastModified"] for row in immediate) == sorted(
        erp._timestamp(scan_off) for scan_off in closed["scan_off"]
    )


def test_the_exception_report_cuts_forgotten_scans_to_the_shift_end(
    scans, build_erp_changes
):
    exact = build_erp_changes(ErpMess(labor_rounding_hours=0)).as_of()
    hours_by_record = {
        (r["EmployeeNum"], r["JobNum"], r["OprSeq"], r["PayrollDate"]): r["LaborHrs"]
        for r in exact["LaborDetails"]
    }
    forgotten = 0
    for scan in scans.iter_rows(named=True):
        on = scan["scan_on"].astimezone(PLANT_TZ)
        key = (
            scan["employee_id"],
            scan["job_num"],
            scan["op_seq"],
            on.date().isoformat(),
        )
        if key not in hours_by_record:
            continue  # not posted yet
        shift_end = datetime.combine(
            on.date(),
            time(14, 30) if on.time() < time(14, 30) else time(23, 0),
            PLANT_TZ,
        )
        if scan["scan_off"] > shift_end:  # left open, closed the next morning
            forgotten += 1
            assert hours_by_record[key] == pytest.approx(
                (shift_end - on).total_seconds() / 3600
            )
        else:
            assert hours_by_record[key] == pytest.approx(
                (scan["scan_off"] - scan["scan_on"]).total_seconds() / 3600
            )
    assert forgotten > 0


def test_the_erp_never_knows_which_machine(reference_data, tables):
    work_centers = {r.work_center for r in reference_data.rates}
    assert {op["WorkCenter"] for op in tables["JobOperations"]} <= work_centers
    for columns in erp.ENTITIES.values():
        assert not any("Machine" in column for column in columns)


def test_export_matches_the_database(erp_changes, tables, tmp_path):
    written = erp.write_erp(erp_changes, tmp_path)

    assert tmp_path / "erp.db" in written
    with sqlite3.connect(tmp_path / "erp.db") as db:
        for entity, columns in erp.ENTITIES.items():
            from_db = db.execute(
                f"SELECT {', '.join(columns)} FROM {entity} ORDER BY rowid"
            ).fetchall()
            lines = (tmp_path / "export" / f"{entity}.jsonl").read_text().splitlines()
            from_export = [
                tuple(json.loads(line)[c] for c in columns) for line in lines
            ]
            assert from_db == from_export, entity
            assert len(from_db) == len(tables[entity])


def test_job_materials_are_the_standard_material_cost(reference_data, tables):
    costs = by_key(tables["JobCosts"], "JobNum")
    revisions = by_key(tables["PartRevisions"], "PartNum", "RevisionNum")
    materials = by_key(tables["JobMaterials"], "JobNum", "MtlSeq")

    assert len(materials) == len(tables["Jobs"])  # one sheet stock per job
    for job in tables["Jobs"]:
        material = materials[(job["JobNum"], 10)]
        revision = revisions[(job["PartNum"], job["RevisionNum"])]
        assert material["MtlPartNum"] == revision["MtlPartNum"]
        assert material["RequiredQty"] == pytest.approx(
            job["ProdQty"] * revision["MtlQtyPerPiece"]
        )
        assert material["IssuedQty"] == material["RequiredQty"]
        assert material["IssuedComplete"] is True
        assert material["RequiredQty"] * material["UnitCost"] == pytest.approx(
            costs[(job["JobNum"],)]["StdMaterialCost"], abs=CENT / 2 + 1e-9
        )  # the cost is rounded to the cent, and a half cent can go either way


def test_master_data_is_the_reference_data(reference_data, tables):
    parts = by_key(tables["Parts"], "PartNum")
    made = {p.part_id for p in reference_data.parts}
    bought = {m.material_id for m in reference_data.materials}

    assert {c["CustomerCode"] for c in tables["Customers"]} == {
        c.code for c in reference_data.customers
    }
    assert set(parts) == {(n,) for n in made | bought}
    for part in reference_data.parts:
        row = parts[(part.part_id,)]
        assert (row["PartType"], row["UOM"], row["UnitPrice"]) == (
            "M",
            "EA",
            part.price,
        )
    for material in reference_data.materials:
        row = parts[(material.material_id,)]
        assert (row["PartType"], row["UOM"], row["UnitCost"]) == (
            "P",
            "LB",
            material.cost_per_lb,
        )
    for revision in tables["PartRevisions"]:
        assert (revision["MtlPartNum"],) in parts
    # Master data was set up before the window and has no change stamp.
    for entity in ("WorkCenters", "Customers", "Parts", "PartRevisions"):
        assert "LastModified" not in erp.ENTITIES[entity]
