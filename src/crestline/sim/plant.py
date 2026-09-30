"""The SimPy plant model (spec §3).

How it fits together:

- Each **job** is a SimPy process. It waits for its release time, then walks
  its part's routing in order. Whole-job transfer: an operation starts only
  after the previous one has finished every piece, and its input quantity is
  the previous operation's good pieces.
- Each **work center** is a queue of machines (`WorkCenterPool`). An
  operation takes whichever machine frees up first, in arrival order.
- **Time only passes inside shift windows** (`ShiftCalendar`). Setup and
  downtime are "durable work" that pauses at a break or shift end and resumes
  after it; pieces are atomic and must fit before the window ends.
- Every stretch of SETUP, RUNNING, or DOWN is recorded on the machine with the
  job, the shift, and the operator on duty. After the run, the gaps are filled
  in from the calendar as IDLE (inside a window) or PLANNED_STOP (outside).
- Every piece produced becomes a `CycleRecord`, good or scrap with its cause.
"""

import math
from collections.abc import Generator, Iterable
from datetime import date

import simpy

from crestline.reference import ReferenceData, RoutingOp
from crestline.rng import make_rng

from .clock import SimClock
from .config import PlantConfig
from .jobs import GeneratedJobs, Job
from .machines import Machine, WorkCenterPool
from .operators import OperatorRoster
from .records import (
    CycleRecord,
    DowntimeReason,
    JobRecord,
    JobStatus,
    MachineState,
    MachineStateRecord,
    OperationRecord,
    ScrapCause,
    SimulationResult,
)
from .scenarios import Scenarios, SetupMultiplier
from .shifts import ShiftCalendar, Window

SimSteps = Generator[simpy.Event, object, object]


class Plant:
    def __init__(
        self,
        reference: ReferenceData,
        jobs: Iterable[Job],
        first_day: date,
        last_day: date,
        seed: int,
        config: PlantConfig | None = None,
        setup_multiplier: SetupMultiplier | None = None,
    ) -> None:
        self.reference = reference
        self.jobs = list(jobs)
        self.config = config or PlantConfig()
        self.setup_multiplier = setup_multiplier

        for job in self.jobs:
            if not first_day <= job.release_date <= last_day:
                raise ValueError(
                    f"{job.job_num} is released on {job.release_date}, "
                    f"outside the simulation window {first_day}..{last_day}"
                )

        self.clock = SimClock.starting_on(first_day)
        self.calendar = ShiftCalendar(
            self.clock, self.config.calendar, first_day, last_day
        )
        self.env = simpy.Environment()
        self.rng = make_rng(seed, stream="plant")

        self.machines = {
            wc.machine_id: Machine(
                wc.machine_id, wc.work_center, is_manual=wc.type == "manual"
            )
            for wc in reference.work_centers
        }
        machines_by_work_center: dict[str, list[Machine]] = {}
        for machine in self.machines.values():
            machines_by_work_center.setdefault(machine.work_center, []).append(machine)
        self.pools = {
            work_center: WorkCenterPool(self.env, machines)
            for work_center, machines in machines_by_work_center.items()
        }
        self.roster = OperatorRoster(
            reference.employees,
            list(self.machines.values()),
            self.calendar.working_days,
            [shift.name for shift in self.config.calendar.shifts],
            make_rng(seed, stream="operators"),
        )

        self._parts = {(part.part_id, part.rev): part for part in reference.parts}
        self._downtime_weights = [kind.weight for kind in self.config.downtime.kinds]

        self.job_records: list[JobRecord] = []
        self.operation_records: list[OperationRecord] = []
        self.cycles: list[CycleRecord] = []

    def run(self) -> SimulationResult:
        for job in self.jobs:
            self.env.process(self._job_process(job))
        self.env.run(until=self.calendar.end)
        return SimulationResult(
            jobs=self.job_records,
            operations=self.operation_records,
            machine_states=self._machine_states(),
            cycles=self.cycles,
        )

    # -- jobs and operations -------------------------------------------------

    def _job_process(self, job: Job) -> SimSteps:
        yield self.env.timeout(self._release_second(job))
        record = JobRecord(
            job_num=job.job_num,
            order_num=job.order_num,
            part_id=job.part_id,
            rev=job.rev,
            qty_ordered=job.qty_ordered,
            qty_released=job.qty_released,
            release_date=job.release_date,
            due_date=job.due_date,
            released_at=self.clock.to_utc(self.env.now),
        )
        self.job_records.append(record)

        qty = job.qty_released
        for op in self._parts[(job.part_id, job.rev)].routing:
            if qty == 0:
                break
            qty = yield from self._run_operation(record, op, qty)

        record.completed_at = self.clock.to_utc(self.env.now)
        record.qty_completed = qty
        record.status = JobStatus.COMPLETE

    def _release_second(self, job: Job) -> int:
        """Jobs are released by the office at some point during 1st shift."""
        first_shift = next(s for s in self.config.calendar.shifts if s.name == "1st")
        start = self.clock.seconds_at(job.release_date, first_shift.start)
        end = self.clock.seconds_at(job.release_date, first_shift.end)
        return int(self.rng.uniform(start, end))

    def _run_operation(
        self, job_record: JobRecord, op: RoutingOp, qty_in: int
    ) -> Generator[simpy.Event, object, int]:
        pool = self.pools[op.work_center]
        queued_at = self.env.now
        machine: Machine = yield pool.acquire()

        record = OperationRecord(
            job_num=job_record.job_num,
            op_seq=op.op_seq,
            work_center=op.work_center,
            machine_id=machine.machine_id,
            std_setup_min=op.setup_min,
            std_run_min_per_piece=op.run_min_per_piece,
            qty_in=qty_in,
            queued_at=self.clock.to_utc(queued_at),
            setup_started_at=self.clock.to_utc(self.env.now),
        )
        self.operation_records.append(record)
        if job_record.status == JobStatus.OPEN:
            job_record.status = JobStatus.IN_PROGRESS
            job_record.started_at = record.setup_started_at

        setup_seconds = self._draw_setup_seconds(op)
        yield from self._work(
            machine,
            setup_seconds,
            MachineState.SETUP,
            None,
            record,
            self.setup_multiplier,
        )
        record.run_started_at = self.clock.to_utc(self.env.now)

        after_setup = setup_seconds > 0 and not machine.is_manual
        good, scrap = yield from self._run_pieces(
            machine, op, record, qty_in, after_setup
        )
        record.qty_good = good
        record.qty_scrap = scrap
        record.completed_at = self.clock.to_utc(self.env.now)
        job_record.qty_scrapped += scrap

        pool.release(machine)
        return good

    # -- time on a machine ---------------------------------------------------

    def _wait_for_window(self) -> Generator[simpy.Event, object, Window]:
        """Advance to working time if the plant is currently stopped."""
        window = self.calendar.next_window(self.env.now)
        if window is None:
            # Past the last shift of the window; the run ends at `until`.
            yield self.env.event()
        assert window is not None
        if window.start > self.env.now:
            yield self.env.timeout(window.start - self.env.now)
        return window

    def _work(
        self,
        machine: Machine,
        work_seconds: int,
        state: MachineState,
        reason: str | None,
        op_record: OperationRecord,
        multiplier: SetupMultiplier | None = None,
    ) -> SimSteps:
        """Spend `work_seconds` of working time on `machine`, pausing for stops.

        A 25-minute setup that starts five minutes before the meal break
        finishes twenty minutes after it. Each chunk is recorded as its own
        busy interval, tagged with the shift it fell in and the operator on
        duty. `multiplier` scales the clock time of each chunk by (machine,
        shift, date), which is how a scenario can slow setups on one machine
        on one shift only; it defaults to 1.
        """
        # Float, not int: a chunk cut short by a break does `chunk / factor`
        # seconds of work, and flooring that would cost an extra second later.
        remaining: float = work_seconds
        while remaining > 0:
            window = yield from self._wait_for_window()
            factor = (
                multiplier(machine.machine_id, window.shift, window.local_date)
                if multiplier
                else 1.0
            )
            needed = math.ceil(remaining * factor)
            chunk = min(needed, window.end - self.env.now)
            start = self.env.now
            yield self.env.timeout(chunk)
            # NO_OPERATOR downtime means nobody is at the machine, so nobody
            # is recorded on it; that keeps the stretch out of truth labor.
            employee_id = (
                None
                if reason == DowntimeReason.NO_OPERATOR
                else self.roster.employee_for(
                    machine.machine_id, window.shift, window.local_date
                )
            )
            machine.record(
                start,
                self.env.now,
                state,
                reason,
                window.shift,
                op_record.job_num,
                op_record.op_seq,
                employee_id,
            )
            remaining = 0 if chunk == needed else remaining - chunk / factor

    def _run_pieces(
        self,
        machine: Machine,
        op: RoutingOp,
        op_record: OperationRecord,
        qty: int,
        after_setup: bool,
    ) -> Generator[simpy.Event, object, tuple[int, int]]:
        """Produce `qty` pieces on `machine`, one cycle at a time, in chunks.

        Cycles are short (seconds to a couple of minutes), so one SimPy event
        per piece would dominate the run time. Instead the loop lays out as
        many cycles as fit before the next stop or downtime event, yields a
        single timeout for the whole chunk, and records every piece with its
        own timestamp. Same truth, hundreds of times fewer events.
        """
        sigma = (
            self.config.variation.manual_cycle_sigma
            if machine.is_manual
            else self.config.variation.machine_cycle_sigma
        )
        cycle_std = op.run_min_per_piece * 60
        produced = good = scrap = 0
        run_since_downtime = 0
        run_until_downtime = self._draw_run_until_downtime(machine)

        while produced < qty:
            window = yield from self._wait_for_window()
            employee_id = self.roster.employee_for(
                machine.machine_id, window.shift, window.local_date
            )
            start = t = self.env.now
            pieces: list[tuple[int, ScrapCause | None]] = []
            downtime_due = False
            while produced + len(pieces) < qty:
                cycle = max(1, round(cycle_std * self.rng.lognormvariate(0.0, sigma)))
                if t + cycle > window.end:
                    break
                if (
                    run_until_downtime is not None
                    and run_since_downtime + cycle > run_until_downtime
                ):
                    downtime_due = True
                    break
                t += cycle
                run_since_downtime += cycle
                first_piece = after_setup and produced + len(pieces) == 0
                pieces.append((t, self._judge_piece(first_piece)))

            if pieces:
                yield self.env.timeout(t - start)
                machine.record(
                    start, t, MachineState.RUNNING, None, window.shift,
                    op_record.job_num, op_record.op_seq, employee_id,
                )  # fmt: skip
                for at, cause in pieces:
                    self.cycles.append(
                        CycleRecord(
                            machine.machine_id,
                            op_record.job_num,
                            op_record.op_seq,
                            self.clock.to_utc(at),
                            cause is None,
                            cause,
                        )
                    )
                    if cause is None:
                        good += 1
                    else:
                        scrap += 1
                produced += len(pieces)

            if downtime_due:
                reason, seconds = self._draw_downtime()
                yield from self._work(
                    machine, seconds, MachineState.DOWN, reason, op_record
                )
                run_since_downtime = 0
                run_until_downtime = self._draw_run_until_downtime(machine)
            elif produced < qty:
                # The next piece does not fit before the stop: wait it out.
                yield self.env.timeout(window.end - self.env.now)

        return good, scrap

    # -- random draws --------------------------------------------------------

    def _draw_setup_seconds(self, op: RoutingOp) -> int:
        if op.setup_min == 0:
            return 0
        factor = self.rng.lognormvariate(0.0, self.config.variation.setup_sigma)
        return max(1, round(op.setup_min * 60 * factor))

    def _draw_run_until_downtime(self, machine: Machine) -> float | None:
        mean_minutes = self.config.downtime.mean_run_minutes_between_events
        if machine.is_manual or mean_minutes is None:
            return None
        return self.rng.expovariate(1.0 / (mean_minutes * 60))

    def _draw_downtime(self) -> tuple[str, int]:
        kind = self.rng.choices(
            self.config.downtime.kinds, weights=self._downtime_weights
        )[0]
        minutes = self.rng.uniform(kind.min_minutes, kind.max_minutes)
        return kind.reason, max(1, round(minutes * 60))

    def _judge_piece(self, first_after_setup: bool) -> ScrapCause | None:
        if (
            first_after_setup
            and self.rng.random() < self.config.scrap.setup_scrap_probability
        ):
            return ScrapCause.SETUP_SCRAP
        if self.rng.random() < self.config.scrap.random_rate:
            return ScrapCause.RANDOM
        return None

    # -- machine state table -------------------------------------------------

    def _machine_states(self) -> list[MachineStateRecord]:
        records: list[MachineStateRecord] = []
        for machine in self.machines.values():
            records.extend(self._states_for(machine))
        return records

    def _states_for(self, machine: Machine) -> list[MachineStateRecord]:
        """Busy intervals were recorded as they happened; fill in the rest.

        Gaps inside a shift window are IDLE; planned stops come straight from
        the calendar. Busy intervals never cross a window boundary, so this is
        a single pass over the two sorted lists.
        """
        busy = sorted(machine.busy, key=lambda b: b.start)
        out = [
            MachineStateRecord(
                machine_id=machine.machine_id,
                state=b.state,
                reason=b.reason,
                start=self.clock.to_utc(b.start),
                end=self.clock.to_utc(b.end),
                shift=b.shift,
                job_num=b.job_num,
                op_seq=b.op_seq,
                employee_id=b.employee_id,
            )
            for b in busy
        ]

        def idle(start: int, end: int, window: Window) -> MachineStateRecord:
            return MachineStateRecord(
                machine_id=machine.machine_id,
                state=MachineState.IDLE,
                reason=None,
                start=self.clock.to_utc(start),
                end=self.clock.to_utc(end),
                shift=window.shift,
                job_num=None,
                op_seq=None,
                employee_id=None,
            )

        i = 0
        for window in self.calendar.windows:
            cursor = window.start
            while i < len(busy) and busy[i].start < window.end:
                if busy[i].start > cursor:
                    out.append(idle(cursor, busy[i].start, window))
                cursor = busy[i].end
                i += 1
            if cursor < window.end:
                out.append(idle(cursor, window.end, window))

        for stop in self.calendar.stops:
            out.append(
                MachineStateRecord(
                    machine_id=machine.machine_id,
                    state=MachineState.PLANNED_STOP,
                    reason=stop.reason,
                    start=self.clock.to_utc(stop.start),
                    end=self.clock.to_utc(stop.end),
                    shift=None,
                    job_num=None,
                    op_seq=None,
                    employee_id=None,
                )
            )
        out.sort(key=lambda r: r.start)
        return out


def simulate(
    reference: ReferenceData,
    jobs: GeneratedJobs | Iterable[Job],
    first_day: date,
    last_day: date,
    seed: int,
    config: PlantConfig | None = None,
    scenarios: Scenarios | None = None,
) -> SimulationResult:
    """Run the plant over `first_day..last_day` (inclusive) and return the answer key.

    `scenarios` is what bends the plant away from standard; leave it out for
    a plant with no hidden stories in it.
    """
    job_list = jobs.jobs if isinstance(jobs, GeneratedJobs) else list(jobs)
    setup_multiplier = scenarios.setup_multiplier() if scenarios is not None else None
    return Plant(
        reference, job_list, first_day, last_day, seed, config, setup_multiplier
    ).run()
