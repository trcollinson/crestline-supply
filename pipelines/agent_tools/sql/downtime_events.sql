-- downtime_events: the MES downtime log for one machine, as written.
-- Parameters: machine_id, then the window's first and last plant day.
--
-- reason_text is free text anyone at the machine typed. It comes back as
-- it is: what the agent does with it is what M4 measures. A stop belongs
-- to the plant day it ended on, as in the MES's nightly file.
select record_id,
       machine_id,
       work_center,
       start_time_local,
       end_time_local,
       reason_text,
       entered_by
from mes_downtime
where machine_id = ?
  and cast(end_time_local as date) between cast(? as date) and cast(? as date)
order by end_time_local, record_id
limit 201
