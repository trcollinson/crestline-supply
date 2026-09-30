"""Who is running each machine.

Operators are labels on the work, not a constraint on it: every work center
has one operator per shift, BRAKE has two, and the two brake operators swap
between PB-01 and PB-02 from day to day (the spec's "some float between the
two brakes"). Modelling operators as a scarce resource, so that absences
cause real NO_OPERATOR downtime, is deferred; in M1 NO_OPERATOR is just one
of the random downtime reasons.
"""

import random
from datetime import date

from crestline.reference import Employee

from .machines import Machine


class OperatorRoster:
    def __init__(
        self,
        employees: list[Employee],
        machines: list[Machine],
        working_days: list[date],
        shifts: list[str],
        rng: random.Random,
    ) -> None:
        crews: dict[tuple[str, str], list[str]] = {}
        for employee in employees:
            crews.setdefault((employee.work_center, employee.shift), []).append(
                employee.employee_id
            )
        machines_by_work_center: dict[str, list[Machine]] = {}
        for machine in machines:
            machines_by_work_center.setdefault(machine.work_center, []).append(machine)

        # Assignments are drawn up front, in calendar order, from their own RNG
        # stream, so the roster never depends on how the simulation unfolds.
        self._assignment: dict[tuple[date, str, str], str] = {}
        for day in working_days:
            for shift in shifts:
                for work_center, wc_machines in machines_by_work_center.items():
                    crew = list(crews.get((work_center, shift), []))
                    if not crew:
                        continue
                    if len(wc_machines) > 1:
                        rng.shuffle(crew)
                    for i, machine in enumerate(wc_machines):
                        self._assignment[(day, shift, machine.machine_id)] = crew[
                            i % len(crew)
                        ]

    def employee_for(self, machine_id: str, shift: str, local_date: date) -> str | None:
        return self._assignment.get((local_date, shift, machine_id))
