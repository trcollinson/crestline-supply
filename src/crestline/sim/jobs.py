"""Sales order and job generation (spec §3.1).

On every weekday in a date range, release a random number of jobs, each a
random part and quantity for a random customer. One sales order line per job.

A job is released for a little more than the customer ordered: the planner
adds a scrap allowance so that the usual few scrapped pieces still leave
enough good ones to ship. The order line keeps the ordered quantity, which
is what gets invoiced; the job carries the released quantity, which is what
the plant makes and what the ERP costs.
"""

import math
import random
from datetime import date, timedelta

from pydantic import BaseModel

from crestline.reference import ReferenceData
from crestline.rng import make_rng

JOBS_PER_WEEKDAY = (15, 25)
QTY_RANGE = (10, 500)
QTY_LOGNORMAL_MEDIAN = 25
QTY_LOGNORMAL_SIGMA = 0.75
DUE_LEAD_DAYS_RANGE = (5, 21)
FIRST_JOB_SEQ = 100001
# The planner's standing rule: release 5% over the order, rounded up. Overs
# go to stock; shorts are shipped short.
SCRAP_ALLOWANCE_PCT = 5


class SalesOrder(BaseModel):
    order_num: str
    customer_code: str


class SalesOrderLine(BaseModel):
    order_num: str
    line_num: int
    part_id: str
    rev: str
    qty: int


class Job(BaseModel):
    job_num: str
    order_num: str
    line_num: int
    part_id: str
    rev: str
    qty_ordered: int  # the order line: what is invoiced
    qty_released: int  # ordered plus the scrap allowance: what the plant makes
    release_date: date
    due_date: date


class GeneratedJobs(BaseModel):
    sales_orders: list[SalesOrder]
    sales_order_lines: list[SalesOrderLine]
    jobs: list[Job]


def release_quantity(qty_ordered: int) -> int:
    """The quantity the planner releases: the order plus the scrap allowance, rounded up.

    Integer arithmetic on purpose: `math.ceil(20 * 1.05)` is 22, because 1.05
    has no exact binary representation.
    """
    return (qty_ordered * (100 + SCRAP_ALLOWANCE_PCT) + 99) // 100


def _draw_quantity(rng: random.Random) -> int:
    """Draw a lot size: mostly a few dozen pieces, occasionally hundreds.

    Lot sizes in a high-mix fab shop are heavily skewed, so a log-normal draw
    (median 25, mean ~33) clamped to the spec's 10-500 range reads more like
    a real order book than a uniform draw would. It is also what keeps the
    release volume inside the plant's two-shift capacity: with uniform
    quantities (mean ~255) demand is several times what one laser and one
    assembly station can do, and jobs would never finish.
    """
    qty = round(rng.lognormvariate(math.log(QTY_LOGNORMAL_MEDIAN), QTY_LOGNORMAL_SIGMA))
    return max(QTY_RANGE[0], min(QTY_RANGE[1], qty))


def generate_jobs(
    reference_data: ReferenceData,
    start_date: date,
    end_date: date,
    seed: int,
) -> GeneratedJobs:
    """Release ~15-25 jobs on every weekday in `[start_date, end_date]`.

    No releases on weekends: the plant's occasional Saturday OT (spec §2) is
    production overtime on already-released jobs, not a day new orders land.
    """
    if end_date < start_date:
        raise ValueError("end_date must not be before start_date")

    rng = make_rng(seed)

    sales_orders: list[SalesOrder] = []
    sales_order_lines: list[SalesOrderLine] = []
    jobs: list[Job] = []

    job_seq = FIRST_JOB_SEQ
    current = start_date
    while current <= end_date:
        if current.weekday() < 5:  # Monday=0 ... Friday=4
            for _ in range(rng.randint(*JOBS_PER_WEEKDAY)):
                customer = rng.choice(reference_data.customers)
                part = rng.choice(reference_data.parts)
                qty = _draw_quantity(rng)
                due_date = current + timedelta(days=rng.randint(*DUE_LEAD_DAYS_RANGE))

                order_num = f"S{job_seq:06d}"
                job_num = f"J{job_seq:06d}"
                job_seq += 1

                sales_orders.append(
                    SalesOrder(order_num=order_num, customer_code=customer.code)
                )
                sales_order_lines.append(
                    SalesOrderLine(
                        order_num=order_num,
                        line_num=10,
                        part_id=part.part_id,
                        rev=part.rev,
                        qty=qty,
                    )
                )
                jobs.append(
                    Job(
                        job_num=job_num,
                        order_num=order_num,
                        line_num=10,
                        part_id=part.part_id,
                        rev=part.rev,
                        qty_ordered=qty,
                        qty_released=release_quantity(qty),
                        release_date=current,
                        due_date=due_date,
                    )
                )
        current += timedelta(days=1)

    return GeneratedJobs(
        sales_orders=sales_orders, sales_order_lines=sales_order_lines, jobs=jobs
    )
