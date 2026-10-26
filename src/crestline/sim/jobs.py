"""Sales order and job generation (spec §3.1).

On every weekday in a date range, release a random number of jobs, each a
random part and quantity for a random customer. One sales order line per job.

A job is released for a little more than the customer ordered: the planner
adds a scrap allowance so that the usual few scrapped pieces still leave
enough good ones to ship. The order line keeps the ordered quantity, which
is what gets invoiced; the job carries the released quantity, which is what
the plant makes and what the ERP costs.

Orders arrive before they are released: sales enters an order, and the
planner releases its job 1-5 working days later, once it has been checked
and scheduled. That lead time is drawn from its own random stream, so adding
it left every job, quantity, and due date exactly where it was. The moment
of entry, some second of that day's office hours, comes from a stream of its
own for the same reason.
"""

import math
import random
from datetime import UTC, date, datetime, time, timedelta

from pydantic import BaseModel

from crestline.reference import ReferenceData
from crestline.rng import make_rng

from .clock import PLANT_TZ

JOBS_PER_WEEKDAY = (15, 25)
QTY_RANGE = (10, 500)
QTY_LOGNORMAL_MEDIAN = 25
QTY_LOGNORMAL_SIGMA = 0.75
DUE_LEAD_DAYS_RANGE = (5, 21)
# Working days from order entry to job release.
ORDER_LEAD_WORKING_DAYS = (1, 5)
# When the office enters orders, plant local time.
OFFICE_HOURS = (time(8, 0), time(17, 0))
FIRST_JOB_SEQ = 100001
# The planner's standing rule: release 5% over the order, rounded up. Overs
# go to stock; shorts are shipped short.
SCRAP_ALLOWANCE_PCT = 5


class SalesOrder(BaseModel):
    order_num: str
    customer_code: str
    order_date: date  # entered by sales, a few working days before release
    entered_at: datetime  # the moment it was entered, UTC


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


def _working_days_before(day: date, working_days: int) -> date:
    """The weekday `working_days` weekdays before `day`."""
    while working_days:
        day -= timedelta(days=1)
        if day.weekday() < 5:
            working_days -= 1
    return day


def _office_moment(day: date, rng: random.Random) -> datetime:
    """A whole second during office hours on `day`, in UTC."""
    opens = datetime.combine(day, OFFICE_HOURS[0], tzinfo=PLANT_TZ)
    closes = datetime.combine(day, OFFICE_HOURS[1], tzinfo=PLANT_TZ)
    seconds = rng.randint(0, int((closes - opens).total_seconds()))
    return (opens + timedelta(seconds=seconds)).astimezone(UTC)


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
    order_entry_rng = make_rng(seed, "order_entry")
    order_entry_time_rng = make_rng(seed, "order_entry_time")

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
                order_date = _working_days_before(
                    current, order_entry_rng.randint(*ORDER_LEAD_WORKING_DAYS)
                )
                entered_at = _office_moment(order_date, order_entry_time_rng)

                order_num = f"S{job_seq:06d}"
                job_num = f"J{job_seq:06d}"
                job_seq += 1

                sales_orders.append(
                    SalesOrder(
                        order_num=order_num,
                        customer_code=customer.code,
                        order_date=order_date,
                        entered_at=entered_at,
                    )
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
