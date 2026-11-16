-- machine_downtime: stops rebuilt from one machine's own data
-- (queries/downtime.sql), in working hours only.
-- Parameters: machine_id, then the window's first and last plant day.
--
-- A stop belongs to the plant day it started on: its UTC start read on the
-- plant's clock, the same expression machines.sql uses for its days.
select machine_id,
       generation,
       started_at_utc,
       ended_at_utc,
       round(date_diff('second', started_at_utc, ended_at_utc) / 60.0, 1) as minutes,
       found_by
from machine_downtime
where machine_id = ?
  and cast(with_timezone(started_at_utc, 'UTC') at time zone 'America/Denver' as date)
      between cast(? as date) and cast(? as date)
order by started_at_utc
limit 201
