-- cycles_by_day: one machine's cycles per plant day (queries/machines.sql).
-- Parameters: machine_id, then the window's first and last plant day.
select machine_id,
       work_date,
       cycles
from machine_cycles_by_day
where machine_id = ?
  and work_date between cast(? as date) and cast(? as date)
order by work_date
limit 201
