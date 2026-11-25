-- scrap_by_day: one machine's pieces and scrap per plant day (queries/scrap.sql).
-- Parameters: machine_id, then the window's first and last plant day.
select work_date,
       machine_id,
       work_center,
       scans,
       pieces,
       scrapped,
       scrap_pct,
       erp_scrapped
from scrap_by_day
where machine_id = ?
  and work_date between cast(? as date) and cast(? as date)
order by work_date
limit 201
