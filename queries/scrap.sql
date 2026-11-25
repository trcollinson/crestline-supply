-- Scrap: pieces made and pieces scrapped, per machine per plant day.
--
-- The MES is the source: at scan-off an operator enters the pieces they
-- made and the ones they scrapped, as a lump count. The ERP gets the same
-- counts a night later (its labor batch, spec §4.1), so it is the
-- cross-check, not the source: on any morning, yesterday's scrap is in the
-- MES and not yet in the ERP.
--
-- Runs after margin_gap.sql, whose brake attribution it reuses: the MES
-- and the ERP both just say BRAKE, and only the brakes' stroke counts tell
-- PB-01's pieces from PB-02's. The same file runs in DuckDB and Athena.


-- Each work center that has one machine or station (spec §2.3). BRAKE has
-- two, so it isn't here.
create or replace view station_machines as
select * from (values
    ('LASER',  'LSR-01'),
    ('DEBURR', 'DB-01'),
    ('PEM',    'PEM-01'),
    ('POWDER', 'PC-LINE'),
    ('ASSY',   'AS-01')
) as s (work_center, machine_id);


-- Pieces and scrap per machine per plant day, from the MES scans.
--
-- A scan's plant day is the day it was opened, in plant time, which is how
-- the ERP dates labor too (PayrollDate). A scan someone forgot to close is
-- closed the next morning, but it still counts on the day it was worked.
-- Pieces are good plus scrapped: what the machine made, before anyone
-- looked. A brake scan goes to the brake margin_gap.sql's attribution placed its
-- operation on; one it couldn't place keeps machine_id null.
create or replace view scrap_by_day as
with mes as (
    select cast(l.scan_on_local as date)                     as work_date,
           l.work_center,
           coalesce(m.machine_id, b.brake)                   as machine_id,
           count(*)                                          as scans,
           cast(sum(l.qty_good + l.qty_scrap) as bigint)     as pieces,
           cast(sum(l.qty_scrap) as bigint)                  as scrapped
    from mes_labor as l
    left join station_machines as m
      on m.work_center = l.work_center
    left join brake_operations as b
      on l.work_center = 'BRAKE'
     and b.job_num = l.job_num
     and b.op_seq  = l.op_seq
    group by 1, 2, 3
),

-- The same counts as the ERP posted them, one row per scan, by the same
-- machine rule. A day the nightly batch hasn't posted yet has no rows.
erp as (
    select d.PayrollDate                                     as work_date,
           d.WorkCenter                                      as work_center,
           coalesce(m.machine_id, b.brake)                   as machine_id,
           cast(sum(d.ScrapQty) as bigint)                   as scrapped
    from erp_labor_details as d
    left join station_machines as m
      on m.work_center = d.WorkCenter
    left join brake_operations as b
      on d.WorkCenter = 'BRAKE'
     and b.job_num = d.JobNum
     and b.op_seq  = d.OprSeq
    group by 1, 2, 3
)

select mes.work_date,
       mes.work_center,
       mes.machine_id,
       mes.scans,
       mes.pieces,
       mes.scrapped,
       round(100 * cast(mes.scrapped as double) / nullif(mes.pieces, 0), 2) as scrap_pct,
       erp.scrapped                                                         as erp_scrapped
from mes
left join erp
  on erp.work_date   = mes.work_date
 and erp.work_center = mes.work_center
 -- (an unplaced brake scan has no machine on either side; null = null is
 -- not true, so say it)
 and erp.machine_id is not distinct from mes.machine_id;


-- The answer: each machine's scrap by week, the report the plant reads on
-- Monday mornings.
select machine_id,
       cast(date_trunc('week', work_date) as date)                      as week_of,
       sum(pieces)                                                       as pieces,
       sum(scrapped)                                                     as scrapped,
       round(100 * cast(sum(scrapped) as double) / nullif(sum(pieces), 0), 1) as scrap_pct
from scrap_by_day
where machine_id is not null
group by 1, 2
order by 1, 2;
