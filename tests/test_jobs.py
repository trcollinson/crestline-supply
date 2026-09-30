from datetime import date, timedelta

import pytest

from crestline.sim import generate_jobs
from crestline.sim.jobs import release_quantity

START = date(2026, 7, 6)  # a Monday
END = date(2026, 7, 17)  # second Friday: 10 weekdays, 4 weekend days
SEED = 42


def test_same_seed_produces_identical_jobs(reference_data):
    first = generate_jobs(reference_data, START, END, SEED)
    second = generate_jobs(reference_data, START, END, SEED)

    assert first == second


def test_different_seed_produces_different_jobs(reference_data):
    first = generate_jobs(reference_data, START, END, SEED)
    other = generate_jobs(reference_data, START, END, SEED + 1)

    assert first.jobs != other.jobs


def test_only_weekdays_get_releases_within_expected_volume(reference_data):
    result = generate_jobs(reference_data, START, END, SEED)

    jobs_by_date: dict[date, int] = {}
    for job in result.jobs:
        assert job.release_date.weekday() < 5
        jobs_by_date[job.release_date] = jobs_by_date.get(job.release_date, 0) + 1

    num_days = (END - START).days + 1
    weekdays = {
        d for i in range(num_days) if (d := START + timedelta(days=i)).weekday() < 5
    }
    assert set(jobs_by_date) == weekdays
    for count in jobs_by_date.values():
        assert 15 <= count <= 25


def test_job_fields_are_in_range_and_reference_real_data(reference_data):
    result = generate_jobs(reference_data, START, END, SEED)

    known_parts = {(p.part_id, p.rev) for p in reference_data.parts}
    known_customers = {c.code for c in reference_data.customers}

    assert len(result.jobs) == len(result.sales_orders) == len(result.sales_order_lines)

    job_nums = {j.job_num for j in result.jobs}
    assert len(job_nums) == len(result.jobs)

    for order, line, job in zip(
        result.sales_orders, result.sales_order_lines, result.jobs, strict=True
    ):
        assert order.order_num == job.order_num
        assert line.order_num == job.order_num
        assert order.customer_code in known_customers
        assert (job.part_id, job.rev) in known_parts
        assert (line.part_id, line.rev) in known_parts
        assert 10 <= job.qty_ordered <= 500
        assert job.qty_ordered == line.qty
        assert START <= job.release_date <= END
        assert job.due_date > job.release_date


def test_generate_jobs_rejects_end_before_start(reference_data):
    with pytest.raises(ValueError, match="end_date"):
        generate_jobs(reference_data, END, START, SEED)


def test_quantities_are_skewed_toward_small_lots(reference_data):
    result = generate_jobs(reference_data, START, END, SEED)

    quantities = sorted(job.qty_ordered for job in result.jobs)
    median = quantities[len(quantities) // 2]
    assert 15 <= median <= 45  # most jobs are a few dozen pieces...
    assert quantities[-1] > 100  # ...with a long tail of big runs


def test_jobs_are_released_with_a_scrap_allowance(reference_data):
    result = generate_jobs(reference_data, START, END, SEED)

    for job in result.jobs:
        assert job.qty_released == release_quantity(job.qty_ordered)
        assert job.qty_ordered < job.qty_released
    # 5% rounded up, in integers: 20 x 1.05 must be 21, not the float ceiling 22.
    assert [release_quantity(q) for q in (10, 20, 25, 100, 500)] == [
        11,
        21,
        27,
        105,
        525,
    ]
