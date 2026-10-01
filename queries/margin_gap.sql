-- Margin gap: what the ERP says each job made, against what it really cost,
-- by the press brake and shift that set the job up.
--
-- The ERP costs jobs at standard: machine burden is applied at standard
-- hours, so a setup that runs long never shows up in its job cost. This
-- query rebuilds each job's actual cost from what the shop floor recorded
-- (MES scans and brake stroke counters) and compares the two.
--
-- Reads only the sources, never the answer key. The same file runs in
-- DuckDB (sources.sql) and Athena (athena/sources.sql), so it sticks to SQL
-- both engines read the same way. Every timestamp a view hands on is UTC
-- with no time zone attached: Athena views can't store a zoned timestamp.


-- The plant's working windows (spec §2): each shift minus its two breaks
-- and meal, in plant local time. Operators stay scanned on through breaks,
-- so only time inside these windows counts as worked.
create or replace view plant_work_windows as
select * from (values
    ('1st', '06:00:00', '08:00:00'),
    ('1st', '08:10:00', '10:00:00'),
    ('1st', '10:30:00', '12:30:00'),
    ('1st', '12:40:00', '14:30:00'),
    ('2nd', '14:30:00', '16:30:00'),
    ('2nd', '16:40:00', '18:30:00'),
    ('2nd', '19:00:00', '21:00:00'),
    ('2nd', '21:10:00', '23:00:00')
) as w (shift, start_time, end_time);


-- The same windows on every day the MES recorded work, in plant local time
-- (to compare with MES scans) and in UTC (to compare with the machines).
--
-- The MES writes local time with no offset, so the conversion has to say
-- which zone it is in: with_timezone(). Not `at time zone`, which the two
-- engines read differently: DuckDB takes a bare 14:30 as plant time
-- (20:30 UTC), but Athena takes it as UTC and converts, six hours early.
create or replace view work_windows as
with work_days as (
    select distinct cast(scan_on as date) as work_date
    from mes_labor
),

local_windows as (
    select d.work_date,
           w.shift,
           cast(cast(d.work_date as varchar) || ' ' || w.start_time as timestamp) as starts_local,
           cast(cast(d.work_date as varchar) || ' ' || w.end_time   as timestamp) as ends_local
    from work_days as d
    cross join plant_work_windows as w
)

select work_date,
       shift,
       starts_local,
       ends_local,
       cast(with_timezone(starts_local, 'America/Denver') at time zone 'UTC' as timestamp) as starts_at,
       cast(with_timezone(ends_local,   'America/Denver') at time zone 'UTC' as timestamp) as ends_at
from local_windows;


-- 1. MES scans, with forgotten scan-offs fixed.
--
-- Operators sometimes forget to scan off, and a supervisor closes the
-- scan at 06:00 the next working morning. The plant's labor
-- exception report cuts those back to the end of the shift. We can do
-- better: nobody works two jobs at once, so a scan also ends when the same
-- operator scans onto their next job. The fixed scans leave in UTC.
create or replace view scans as
with shifts as (
    select work_date, shift, min(starts_local) as starts_local, max(ends_local) as ends_local
    from work_windows
    group by work_date, shift
),

fixed as (
    select
        l.record_id,
        l.employee_id,
        l.job_num,
        l.op_seq,
        l.work_center,
        s.work_date,
        s.shift,
        l.qty_good + l.qty_scrap as pieces,
        l.scan_on,
        -- (least() must not see a null: Athena would return null, DuckDB
        -- would skip it. The last scan of the window has no next scan-on.)
        least(
            l.scan_off,
            s.ends_local,
            coalesce(lead(l.scan_on) over (partition by l.employee_id order by l.scan_on),
                     l.scan_off)
        ) as scan_off,
        l.scan_off > s.ends_local as was_left_open
    from mes_labor as l
    join shifts as s
      on s.work_date = cast(l.scan_on as date)
     and l.scan_on >= s.starts_local
     and l.scan_on <  s.ends_local
)

select record_id,
       employee_id,
       job_num,
       op_seq,
       work_center,
       work_date,
       shift,
       pieces,
       cast(with_timezone(scan_on,  'America/Denver') at time zone 'UTC' as timestamp) as scan_on,
       cast(with_timezone(scan_off, 'America/Denver') at time zone 'UTC' as timestamp) as scan_off,
       was_left_open
from fixed;


-- 2. The time actually worked: each scan cut into the pieces that fall
--    inside a working window.
create or replace view work_segments as
select
    s.record_id,
    s.job_num,
    s.op_seq,
    s.work_center,
    greatest(s.scan_on, w.starts_at) as starts_at,
    least(s.scan_off,   w.ends_at)   as ends_at
from scans as s
join work_windows as w
  on w.work_date = s.work_date
 and w.shift     = s.shift
 and s.scan_on   < w.ends_at
 and s.scan_off  > w.starts_at;


-- 3. Which brake ran each brake operation?
--
-- Neither the ERP nor the MES knows: both just say BRAKE. The brakes don't
-- know the job either. But each counts its strokes, one per piece. So for
-- every brake operation, count each brake's strokes while it was scanned
-- on and pick the brake whose count matches the pieces the operator
-- reported. The first stroke marks the end of setup.
create or replace view brake_operations as
with brake_strokes as (
    -- PB-01 reports its stroke counter whenever it changes.
    select 'PB-01' as brake,
           ts as counted_at,
           cyc_cnt - lag(cyc_cnt) over (order by ts) as strokes
    from pb01_data
    union all
    -- PB-02's counter is 16 bits wide and rolls over at 65,535, so the
    -- strokes since the last poll are the difference mod 65,536.
    select 'PB-02',
           gw_ts,
           (cnt - lag(cnt) over (order by gw_ts) + 65536) % 65536
    from pb02_counts
),

operations as (
    select job_num,
           op_seq,
           min(scan_on)           as started_at,
           max(scan_off)          as ended_at,
           sum(pieces)            as pieces,
           min_by(shift, scan_on) as setup_shift
    from scans
    where work_center = 'BRAKE'
    group by job_num, op_seq
),

strokes_per_brake as (
    -- PB-02 is polled every 30 s, so it reports a stroke up to 30 s after
    -- it happens: look 30 s late at both ends. (Nothing is lost at the
    -- start: every brake operation begins with a setup.)
    select o.job_num,
           o.op_seq,
           b.brake,
           sum(b.strokes) as strokes,
           min(b.counted_at) filter (where b.strokes > 0) as first_stroke_at,
           max(b.counted_at) filter (where b.strokes > 0) as last_stroke_at
    from operations as o
    join brake_strokes as b
      on b.counted_at >  o.started_at + interval '30' second
     and b.counted_at <= o.ended_at   + interval '30' second
    group by o.job_num, o.op_seq, b.brake
),

ranked as (
    select o.*,
           s.brake,
           s.strokes,
           s.first_stroke_at,
           row_number() over (
               partition by o.job_num, o.op_seq
               -- The closest count wins. On a tie, the brake whose last
               -- stroke came nearest the operator's final scan-off.
               order by abs(s.strokes - o.pieces),
                        abs(date_diff('second', s.last_stroke_at, o.ended_at))
           ) as rank
    from operations as o
    join strokes_per_brake as s
      on s.job_num = o.job_num
     and s.op_seq  = o.op_seq
    where s.strokes > 0
)

select job_num, op_seq, brake, setup_shift, pieces, strokes, started_at, first_stroke_at
from ranked
where rank = 1;


-- 4. Each finished job, three ways. Revenue is what shipped: the pieces
--    the customer ordered, or fewer if the job came up short. The planner
--    releases a few extra to cover scrap, and overs go to stock, not the
--    invoice.
--    Standard: the routing at standard hours, what the job was quoted on.
--    ERP:      the ERP's own "actual" job cost, burden still at standard.
--    Rebuilt:  hours actually worked x the ERP's own work center rates.
create or replace view job_margins as
with rebuilt_conversion as (
    select g.job_num,
           sum(date_diff('second', g.starts_at, g.ends_at) / 3600.0
               * (w.LaborRate + w.BurdenRate)) as cost
    from work_segments as g
    join erp_work_centers as w on w.WorkCenter = g.work_center
    group by g.job_num
),

brake_setups as (
    select b.job_num,
           b.brake,
           b.setup_shift,
           sum(greatest(0, date_diff('second', g.starts_at, least(g.ends_at, b.first_stroke_at))))
               / 3600.0 as setup_hours
    from brake_operations as b
    join work_segments as g
      on g.job_num = b.job_num
     and g.op_seq  = b.op_seq
    group by b.job_num, b.brake, b.setup_shift
)

select
    j.JobNum                                                   as job_num,
    -- A job with no brake operation, or one whose brake couldn't be told
    -- from the stroke counts, gets its own group rather than a guess.
    coalesce(s.brake, case when o.JobNum is null then 'no brake op'
                           else 'unplaced' end)                as brake,
    s.setup_shift,
    least(j.CompletedQty, l.OrderQty) * l.UnitPrice            as revenue,
    c.StdLaborCost + c.StdBurdenCost + c.StdMaterialCost       as standard_cost,
    c.ActLaborCost + c.ActBurdenCost + c.ActMaterialCost       as erp_cost,
    r.cost + c.ActMaterialCost                                 as rebuilt_cost,
    s.setup_hours,
    o.StdSetupHours                                            as std_setup_hours
from erp_jobs as j
join erp_sales_order_lines as l
  on l.OrderNum = j.OrderNum
 and l.OrderLine = j.OrderLine
join erp_job_costs as c on c.JobNum = j.JobNum
join rebuilt_conversion as r on r.job_num = j.JobNum
left join brake_setups as s on s.job_num = j.JobNum
left join erp_job_operations as o
  on o.JobNum = j.JobNum
 and o.WorkCenter = 'BRAKE'
where j.JobComplete;


-- 5. The answer: ERP margin against rebuilt margin, by brake and shift.
--    gap_pts is how far the ERP margin sits above the rebuilt one: positive
--    means the ERP makes those jobs look better than they were.
select
    brake,
    coalesce(setup_shift, '')                                        as shift,
    count(*)                                                         as jobs,
    cast(round(sum(revenue)) as bigint)                              as revenue,
    round(100 * (1 - sum(standard_cost) / sum(revenue)), 1)          as standard_margin_pct,
    round(100 * (1 - sum(erp_cost)      / sum(revenue)), 1)          as erp_margin_pct,
    round(100 * (1 - sum(rebuilt_cost)  / sum(revenue)), 1)          as rebuilt_margin_pct,
    round(100 * (sum(rebuilt_cost) - sum(erp_cost)) / sum(revenue), 1) as gap_pts,
    round(sum(setup_hours) / sum(std_setup_hours), 2)                as setup_vs_standard
from job_margins
group by 1, 2
order by 1, 2;
