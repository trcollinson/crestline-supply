"""Machines, and the work-center queues in front of them."""

from dataclasses import dataclass
from typing import Any

import simpy

from .records import MachineState


@dataclass(frozen=True, slots=True)
class BusyInterval:
    """A stretch of SETUP, RUNNING, or DOWN on one machine, in sim seconds."""

    start: int
    end: int
    state: MachineState
    reason: str | None
    shift: str
    job_num: str
    op_seq: int
    employee_id: str | None


class Machine:
    def __init__(self, machine_id: str, work_center: str, is_manual: bool) -> None:
        self.machine_id = machine_id
        self.work_center = work_center
        self.is_manual = is_manual
        self.busy: list[BusyInterval] = []

    def record(
        self,
        start: int,
        end: int,
        state: MachineState,
        reason: str | None,
        shift: str,
        job_num: str,
        op_seq: int,
        employee_id: str | None,
    ) -> None:
        if end > start:
            self.busy.append(
                BusyInterval(
                    start, end, state, reason, shift, job_num, op_seq, employee_id
                )
            )


class WorkCenterPool:
    """The queue in front of a work center.

    A SimPy Store holds the work center's machines as items. An operation
    takes whichever machine is put back first, and waiting operations are
    served in arrival order. BRAKE holds PB-01 and PB-02; every other work
    center holds one machine, so the same code covers both cases.
    """

    def __init__(self, env: simpy.Environment, machines: list[Machine]) -> None:
        self.machines = list(machines)
        self._store: simpy.Store = simpy.Store(env)
        for machine in machines:
            self._store.put(machine)

    def acquire(self) -> Any:
        return self._store.get()

    def release(self, machine: Machine) -> None:
        self._store.put(machine)
