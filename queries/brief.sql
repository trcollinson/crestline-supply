-- The morning brief's facts (M5): what a plant manager reads before first
-- shift, as numbers, one row per fact.
--
-- The brief is about the plant day: the last day before the morning that
-- anyone worked (on a Monday, Friday). A model may write the facts up in
-- words, but every number in the brief comes from here, and a check refuses
-- a brief that states any other.
--
-- Runs on top of as_of.sql, so every source is what had arrived by 05:30 on
-- the morning, and after machines.sql, margin_gap.sql, downtime.sql, and
-- scrap.sql, whose views it reads. The same file runs in DuckDB and Athena.
--
-- Five families:
--   output    pieces and scans by work center, and jobs finished (MES)
--   margin    margin_gap.sql's margin by brake and setup shift, over the jobs the
--             ERP closed in the last 20 working days
--   stops     stops over 5 minutes the machines show, against the ones in
--             the operators' log (no reason text: it is free text, and the
--             brief's prompt never sees free text, M4)
--   scrap     each machine's scrap rate against its own last 20 working days
--   anomaly   a row for each rule that fires on the facts above
--
-- Each fact: a stable fact_id (family:subject:metric, the same on every
-- morning), the value, its unit, a baseline where it has one, and
-- source_rows: where to find the rows behind it, in this layer's views as
-- of the same morning.
--
-- Athena expands every view and WITH clause wherever it is named, and
-- computes it again each time. Each family's view below cleans the machine
-- data on its own, so each is read once: a baseline comes from a window
-- over the same rows, not a second pass, and each family's anomaly rule
-- runs in the same select as its facts. (Read twice, the facts query was
-- too big for Athena's engine.)


-- The rules' thresholds, in one place. M5 step 5 picked them from a backtest
-- over every morning of the window (eval/brief_backtest.py): keep every
-- planted problem the rule can find, as early as it can, then raise the
-- fewest false alerts, then change the starting point (3, 2 and 60, 0.5, 3)
-- the least. Tuned on the same 90 days they are scored on; a second seed is
-- the check.
create or replace view brief_thresholds as
select cast(3.5 as double) as scrap_standard_errors,  -- scrap: the rate this many standard errors above its baseline
       cast(2   as double) as stop_minutes_factor,    -- stops: this many times the machine's usual daily stop minutes...
       cast(90  as double) as stop_minutes_floor,     -- ...and at least this many minutes
       cast(0.3 as double) as output_low_share,       -- output: under this share of the work center's usual pieces
       cast(3   as double) as margin_gap_points,      -- margin: the ERP this many points above the rebuilt margin...
       cast(5   as double) as margin_min_jobs;        -- ...on at least this many jobs


-- The plant's working days, newest first, as of the morning: every day the
-- MES has a scan from. Day 0 is the plant day the brief is about; days 1 to
-- 20 are the baseline it is measured against.
create or replace view brief_days as
with working_days as (
    select distinct cast(scan_on_local as date) as work_date
    from mes_labor
)

select work_date,
       row_number() over (order by work_date desc) - 1 as days_back
from working_days;


-- 1. Output: what each work center made, by the day its scans were opened
--    (how the ERP dates labor too). A scan left open last night isn't here:
--    it is closed at 06:00 this morning, and arrives tomorrow.
create or replace view brief_output as
with made as (
    select cast(scan_on_local as date)  as work_date,
           work_center,
           count(*)                     as scans,
           sum(qty_good)                as good,
           sum(qty_scrap)               as scrapped,
           sum(qty_good + qty_scrap)    as pieces
    from mes_labor
    group by 1, 2
),

-- Every work center on every working day, so a day it made nothing counts
-- as nothing in its baseline, not as missing.
every_day as (
    select d.work_date,
           d.days_back,
           w.WorkCenter                 as work_center,
           coalesce(m.scans, 0)         as scans,
           coalesce(m.good, 0)          as good,
           coalesce(m.scrapped, 0)      as scrapped,
           coalesce(m.pieces, 0)        as pieces
    from brief_days as d
    cross join erp_work_centers as w
    left join made as m
      on m.work_date   = d.work_date
     and m.work_center = w.WorkCenter
    where d.days_back <= 20
),

with_usual as (
    select e.*,
           avg(case when days_back between 1 and 20 then cast(pieces as double) end)
               over (partition by work_center) as usual_pieces
    from every_day as e
)

select work_date, work_center, scans, good, scrapped, pieces, usual_pieces
from with_usual
where days_back = 0;


-- Jobs finished: the MES has no "job complete", but a job's operations run
-- one after another, and its last one gets exactly the good pieces the one
-- before it made. A job is finished on the day its last operation has made
-- that many. (The ERP's own "complete" comes a night later, in its labor
-- batch; the margin below uses that.)
create or replace view brief_jobs_finished as
with last_operations as (
    select JobNum as job_num, max(OprSeq) as last_op
    from erp_routings
    group by JobNum
),

feeding_operations as (
    select r.JobNum as job_num, max(r.OprSeq) as feeding_op
    from erp_routings as r
    join last_operations as l
      on l.job_num = r.JobNum
     and r.OprSeq  < l.last_op
    group by r.JobNum
),

made as (
    select job_num,
           op_seq,
           sum(qty_good)                        as good,
           sum(qty_good + qty_scrap)            as pieces,
           max(cast(scan_on_local as date))     as last_worked_on
    from mes_labor
    group by job_num, op_seq
)

select l.job_num,
       m.last_worked_on as finished_on,
       m.pieces
from last_operations as l
join feeding_operations as f on f.job_num = l.job_num
join made as m
  on m.job_num = l.job_num
 and m.op_seq  = l.last_op
join made as fed
  on fed.job_num = f.job_num
 and fed.op_seq  = f.feeding_op
where m.pieces >= fed.good;


-- 2. Margin: the job margins of margin_gap.sql for the jobs the ERP
--    closed in the last 20 working days, the plant day included. The ERP
--    closes a job in the nightly labor batch (22:00), when the last of its
--    labor posts, so this runs a day behind the floor.
create or replace view brief_margin_jobs as
with closed as (
    select JobNum as job_num,
           cast(with_timezone(lastmodified_utc, 'UTC') at time zone 'America/Denver' as date) as closed_on
    from erp_jobs
    where JobComplete
),

margin_window as (
    select min(work_date) as first_day, max(work_date) as last_day
    from brief_days
    where days_back < 20
)

select m.*, c.closed_on
from job_margins as m
join closed as c on c.job_num = m.job_num
cross join margin_window as w
where c.closed_on between w.first_day and w.last_day;

-- By brake and setup shift, and for every job closed (the grouping set
-- with no columns).
create or replace view brief_margin as
with grouped as (
    select case when grouping(brake, setup_shift) = 3 then 'all jobs'
                else brake || ' ' || setup_shift end                       as subject,
           brake,
           count(*)                                                        as jobs,
           round(100 * (1 - sum(erp_cost)     / sum(revenue)), 1)          as erp_margin_pct,
           round(100 * (1 - sum(rebuilt_cost) / sum(revenue)), 1)          as rebuilt_margin_pct,
           round(100 * (sum(rebuilt_cost) - sum(erp_cost)) / sum(revenue), 1) as gap_points,
           round(sum(setup_hours) / sum(std_setup_hours), 2)               as setup_vs_standard
    from brief_margin_jobs
    group by grouping sets ((brake, setup_shift), ())
)

select subject, jobs, erp_margin_pct, rebuilt_margin_pct, gap_points, setup_vs_standard
from grouped
where subject = 'all jobs' or brake in ('PB-01', 'PB-02');


-- 3. Stops: what the machines show (downtime.sql) and what the operators
--    wrote down (the MES downtime file), on the plant day. Only stops over
--    5 minutes: the log never has shorter ones.
--
--    A machine's stop is in the log if an operator wrote down a stop on the
--    same machine that overlaps it. One that ran into the night is in
--    tomorrow's file, so as of this morning it isn't logged yet.
create or replace view brief_machine_stops as
select d.machine_id,
       cast(with_timezone(d.started_at_utc, 'UTC') at time zone 'America/Denver' as date) as work_date,
       d.started_at_utc,
       d.ended_at_utc,
       cast(date_diff('second', d.started_at_utc, d.ended_at_utc) as double) / 60 as minutes,
       count(l.record_id) > 0 as logged
from machine_downtime as d
left join mes_downtime as l
  on l.machine_id     = d.machine_id
 and l.start_time_utc < d.ended_at_utc
 and l.end_time_utc   > d.started_at_utc
where date_diff('second', d.started_at_utc, d.ended_at_utc) > 300
group by d.machine_id, d.started_at_utc, d.ended_at_utc;

create or replace view brief_stops as
with machine_days as (
    select d.work_date,
           d.days_back,
           m.machine_id,
           count(s.machine_id)                                            as stops,
           coalesce(sum(s.minutes), 0)                                    as stop_minutes,
           count(s.machine_id) - count(case when s.logged then 1 end)    as stops_not_logged
    from brief_days as d
    cross join machines as m
    left join brief_machine_stops as s
      on s.machine_id = m.machine_id
     and s.work_date  = d.work_date
    where d.days_back <= 20
    group by d.work_date, d.days_back, m.machine_id
),

with_usual as (
    select m.*,
           avg(case when days_back between 1 and 20 then stop_minutes end)
               over (partition by machine_id) as usual_stop_minutes
    from machine_days as m
),

-- The log's stops on the plant day: in the file of the day they ended, with
-- minutes counted in working time (an operator writes a stop from when the
-- machine quit to when it ran again, nights included).
logged as (
    select l.machine_id,
           count(distinct l.record_id)                                     as logged_stops,
           sum(cast(date_diff('second', greatest(l.start_time_utc, w.starts_at_utc),
                                        least(l.end_time_utc, w.ends_at_utc)) as double)) / 60 as logged_minutes
    from mes_downtime as l
    join brief_days as d
      on d.work_date = cast(l.end_time_local as date)
     and d.days_back = 0
    join work_windows as w
      on l.start_time_utc < w.ends_at_utc
     and l.end_time_utc   > w.starts_at_utc
    group by l.machine_id
)

select y.work_date,
       y.machine_id,
       y.stops,
       y.stop_minutes,
       y.stops_not_logged,
       coalesce(l.logged_stops, 0)      as logged_stops,
       coalesce(l.logged_minutes, 0)    as logged_minutes,
       y.usual_stop_minutes
from with_usual as y
left join logged as l on l.machine_id = y.machine_id
where y.days_back = 0;


-- 4. Scrap: each machine's scrap rate on the plant day (scrap.sql, from the
--    MES) against its own rate over the 20 working days before, pooled
--    (all their scrap over all their pieces). How far apart the two are is
--    measured in standard errors of a rate on that many pieces: 1% over
--    200 pieces can't be told from 2%, over 2,000 it can.
create or replace view brief_scrap as
with by_day as (
    select s.machine_id,
           s.pieces,
           s.scrapped,
           d.days_back,
           sum(case when d.days_back between 1 and 20 then s.scrapped end) over (partition by s.machine_id) as baseline_scrapped,
           sum(case when d.days_back between 1 and 20 then s.pieces end)   over (partition by s.machine_id) as baseline_pieces
    from scrap_by_day as s
    join brief_days as d on d.work_date = s.work_date
    where s.machine_id is not null
      and d.days_back <= 20
),

rates as (
    select machine_id,
           pieces,
           scrapped,
           cast(scrapped as double) / nullif(pieces, 0)                    as rate,
           cast(baseline_scrapped as double) / nullif(baseline_pieces, 0)  as baseline_rate
    from by_day
    where days_back = 0
)

select machine_id,
       pieces,
       scrapped,
       rate,
       baseline_rate,
       (rate - baseline_rate) / nullif(sqrt(baseline_rate * (1 - baseline_rate) / pieces), 0) as standard_errors
from rates;


-- 5. Every fact in one shape, each family's anomaly rule beside its facts.
--    A family's numbers become rows by crossing them with its list of
--    metrics; the rule is one more item on the list, whose value is null
--    (no row) unless it fires. Its value is what tripped it, and its
--    baseline the threshold it beat.
--
--    One view per family, and brief_facts their union. Athena plans a query
--    before it runs it, and the four families in one query took it 30 s
--    just to plan (M5 step 3), so it asks for the four separately, at the
--    same time (crestline.brief.facts); DuckDB reads brief_facts.

-- The morning and its plant day, which every fact carries.
create or replace view brief_plant_day as
select c.morning, d.work_date as plant_day, cast(d.work_date as varchar) as day_text
from brief_cut as c
cross join brief_days as d
where d.days_back = 0;

-- The margin's window: the 20 working days its jobs closed in.
create or replace view brief_margin_window as
select cast(min(work_date) as varchar) as first_text, cast(max(work_date) as varchar) as last_text
from brief_days
where days_back < 20;

create or replace view brief_output_facts as
with family_facts as (
    select m.family,
           o.work_center as subject,
           m.metric,
           case m.metric when 'pieces'   then cast(o.pieces as double)
                         when 'good'     then cast(o.good as double)
                         when 'scrapped' then cast(o.scrapped as double)
                         when 'scans'    then cast(o.scans as double)
                         -- Rule: a work center made under output_low_share
                         -- of its usual pieces.
                         when 'output_below_usual' then
                             case when o.pieces < t.output_low_share * o.usual_pieces
                                  then cast(o.pieces as double) end
           end as value,
           m.unit,
           case m.metric when 'pieces' then round(o.usual_pieces, 0)
                         when 'output_below_usual' then round(t.output_low_share * o.usual_pieces, 0)
           end as baseline,
           case when m.family = 'anomaly' then 'output:' || o.work_center || ':pieces'
                else 'mes_labor where work_center = ''' || o.work_center
                     || ''' and cast(scan_on_local as date) = date ''' || p.day_text || ''''
           end as source_rows
    from brief_output as o
    cross join brief_thresholds as t
    cross join brief_plant_day as p
    cross join (values
        ('output',  'pieces',             'pieces'),
        ('output',  'good',               'pieces'),
        ('output',  'scrapped',           'pieces'),
        ('output',  'scans',              'scans'),
        ('anomaly', 'output_below_usual', 'pieces')
    ) as m (family, metric, unit)

    union all

    select 'output', 'plant', 'jobs_finished',
           cast(count(f.job_num) as double), 'jobs', cast(null as double),
           'brief_jobs_finished where finished_on = date ''' || p.day_text || ''''
    from brief_plant_day as p
    left join brief_jobs_finished as f on f.finished_on = p.plant_day
    group by p.day_text
)

select f.family || ':' || f.subject || ':' || f.metric as fact_id,
       p.morning,
       p.plant_day,
       f.family,
       f.subject,
       f.metric,
       f.value,
       f.unit,
       f.baseline,
       f.source_rows
from family_facts as f
cross join brief_plant_day as p
-- No value, no fact: a rule that didn't fire, or a rate with nothing to
-- measure it on (no pieces, no baseline yet).
where f.value is not null;

create or replace view brief_margin_facts as
with family_facts as (
    select m.family,
           g.subject,
           m.metric,
           case m.metric when 'jobs'               then cast(g.jobs as double)
                         when 'erp_margin_pct'     then g.erp_margin_pct
                         when 'rebuilt_margin_pct' then g.rebuilt_margin_pct
                         when 'gap_points'         then g.gap_points
                         when 'setup_vs_standard'  then g.setup_vs_standard
                         -- Rule: on one brake and shift's closed jobs, the
                         -- ERP's margin more than margin_gap_points above
                         -- the rebuilt one, on margin_min_jobs jobs or more.
                         when 'erp_margin_above_rebuilt' then
                             case when g.subject <> 'all jobs'
                                   and g.gap_points > t.margin_gap_points
                                   and g.jobs >= t.margin_min_jobs
                                  then g.gap_points end
           end as value,
           m.unit,
           case when m.family = 'anomaly' then t.margin_gap_points end as baseline,
           case when m.family = 'anomaly' then 'margin:' || g.subject || ':gap_points'
                else 'brief_margin_jobs where '
                     || case when g.subject = 'all jobs' then 'true'
                             else 'brake || '' '' || setup_shift = ''' || g.subject || '''' end
                     || ' (closed ' || w.first_text || ' to ' || w.last_text || ')'
           end as source_rows
    from brief_margin as g
    cross join brief_thresholds as t
    cross join brief_margin_window as w
    cross join (values
        ('margin',  'jobs',                     'jobs'),
        ('margin',  'erp_margin_pct',           'percent'),
        ('margin',  'rebuilt_margin_pct',       'percent'),
        ('margin',  'gap_points',               'points'),
        ('margin',  'setup_vs_standard',        'ratio'),
        ('anomaly', 'erp_margin_above_rebuilt', 'points')
    ) as m (family, metric, unit)
)

select f.family || ':' || f.subject || ':' || f.metric as fact_id,
       p.morning,
       p.plant_day,
       f.family,
       f.subject,
       f.metric,
       f.value,
       f.unit,
       f.baseline,
       f.source_rows
from family_facts as f
cross join brief_plant_day as p
-- No value, no fact: a rule that didn't fire, or a rate with nothing to
-- measure it on (no pieces, no baseline yet).
where f.value is not null;

create or replace view brief_stop_facts as
with family_facts as (
    select m.family,
           s.machine_id as subject,
           m.metric,
           case m.metric when 'stops'            then cast(s.stops as double)
                         when 'stop_minutes'     then round(s.stop_minutes, 0)
                         when 'stops_not_logged' then cast(s.stops_not_logged as double)
                         when 'logged_stops'     then cast(s.logged_stops as double)
                         when 'logged_minutes'   then round(s.logged_minutes, 0)
                         -- Rule: more stop minutes than stop_minutes_factor
                         -- times the machine's usual day, and at least
                         -- stop_minutes_floor of them.
                         when 'stop_minutes_above_usual' then
                             case when s.stop_minutes > t.stop_minutes_factor * s.usual_stop_minutes
                                   and s.stop_minutes >= t.stop_minutes_floor
                                  then round(s.stop_minutes, 0) end
           end as value,
           m.unit,
           case m.metric when 'stop_minutes' then round(s.usual_stop_minutes, 0)
                         when 'stop_minutes_above_usual' then round(t.stop_minutes_factor * s.usual_stop_minutes, 0)
           end as baseline,
           case when m.family = 'anomaly' then 'stops:' || s.machine_id || ':stop_minutes'
                when m.metric like 'logged%'
                then 'mes_downtime where machine_id = ''' || s.machine_id
                     || ''' and cast(end_time_local as date) = date ''' || p.day_text || ''''
                else 'brief_machine_stops where machine_id = ''' || s.machine_id
                     || ''' and work_date = date ''' || p.day_text || ''''
                     || case when m.metric = 'stops_not_logged' then ' and not logged' else '' end
           end as source_rows
    from brief_stops as s
    cross join brief_thresholds as t
    cross join brief_plant_day as p
    cross join (values
        ('stops',   'stops',                    'stops'),
        ('stops',   'stop_minutes',             'minutes'),
        ('stops',   'stops_not_logged',         'stops'),
        ('stops',   'logged_stops',             'stops'),
        ('stops',   'logged_minutes',           'minutes'),
        ('anomaly', 'stop_minutes_above_usual', 'minutes')
    ) as m (family, metric, unit)
)

select f.family || ':' || f.subject || ':' || f.metric as fact_id,
       p.morning,
       p.plant_day,
       f.family,
       f.subject,
       f.metric,
       f.value,
       f.unit,
       f.baseline,
       f.source_rows
from family_facts as f
cross join brief_plant_day as p
-- No value, no fact: a rule that didn't fire, or a rate with nothing to
-- measure it on (no pieces, no baseline yet).
where f.value is not null;

create or replace view brief_scrap_facts as
with family_facts as (
    select m.family,
           s.machine_id as subject,
           m.metric,
           case m.metric when 'pieces'          then cast(s.pieces as double)
                         when 'scrapped'        then cast(s.scrapped as double)
                         when 'scrap_pct'       then round(100 * s.rate, 1)
                         when 'standard_errors' then round(s.standard_errors, 1)
                         -- Rule: the rate more than scrap_standard_errors
                         -- standard errors above the machine's baseline.
                         when 'scrap_above_baseline' then
                             case when s.standard_errors > t.scrap_standard_errors
                                  then round(s.standard_errors, 1) end
           end as value,
           m.unit,
           case m.metric when 'scrap_pct' then round(100 * s.baseline_rate, 1)
                         when 'scrap_above_baseline' then t.scrap_standard_errors
           end as baseline,
           case when m.family = 'anomaly' then 'scrap:' || s.machine_id || ':standard_errors'
                else 'scrap_by_day where machine_id = ''' || s.machine_id || ''' and work_date = date '''
                     || p.day_text || ''' (baseline: the 20 working days before)'
           end as source_rows
    from brief_scrap as s
    cross join brief_thresholds as t
    cross join brief_plant_day as p
    cross join (values
        ('scrap',   'pieces',               'pieces'),
        ('scrap',   'scrapped',             'pieces'),
        ('scrap',   'scrap_pct',            'percent'),
        ('scrap',   'standard_errors',      'standard errors'),
        ('anomaly', 'scrap_above_baseline', 'standard errors')
    ) as m (family, metric, unit)
)

select f.family || ':' || f.subject || ':' || f.metric as fact_id,
       p.morning,
       p.plant_day,
       f.family,
       f.subject,
       f.metric,
       f.value,
       f.unit,
       f.baseline,
       f.source_rows
from family_facts as f
cross join brief_plant_day as p
-- No value, no fact: a rule that didn't fire, or a rate with nothing to
-- measure it on (no pieces, no baseline yet).
where f.value is not null;

create or replace view brief_facts as
select * from brief_output_facts
union all select * from brief_margin_facts
union all select * from brief_stop_facts
union all select * from brief_scrap_facts;


-- The brief's facts for the morning, in the order a reader takes them.
select *
from brief_facts
order by case family when 'anomaly' then 0 when 'output' then 1 when 'margin' then 2
                     when 'stops' then 3 when 'scrap' then 4 end,
         subject,
         metric;
