-- As of a morning: the sources as the plant's systems held them at 05:30 on
-- it, when the morning brief runs (M5).
--
-- A backtest of the brief is only honest if each morning sees what had
-- arrived by then, and nothing later. The views over the full data can't
-- tell: a scan someone forgot to close yesterday is closed at 06:00 today,
-- and is in the full data already; an operation that ran on into today has
-- today's strokes; the ERP's tables hold each row as it stands now. So this
-- file redefines every source under its own name, cut at the moment the
-- brief runs, and every query file on top of it runs unchanged:
-- machines.sql, margin_gap.sql, downtime.sql, scrap.sql, and brief.sql.
--
-- The sources themselves live next door, in crestline_raw (a Glue database
-- in Athena, a schema in DuckDB), so each cut view reads its source by the
-- full name. Run in production at 05:30, the cut keeps everything; it is
-- the backtest's mornings that it changes.
--
-- What "arrived" means, source by source:
--   MES       a nightly file for each day, dropped at 02:15 the next
--             morning, holding the scans that closed (the stops that ended)
--             that day. By the morning, every file up to yesterday's.
--   machines  each message as the gateway published it.
--   ERP       the curated tables keep each row's newest version only. A
--             row changed after the cut shows values the morning never
--             saw, so the cut keeps the rows nothing has touched since
--             (LastModified before it). That is right for what the brief
--             reads from the ERP: jobs it had closed, whose rows never
--             change again, and rates and routings, which never change.


-- The morning, and the moment the brief runs on it: 05:30 plant time, in
-- UTC. One row; the brief replaces this view to move to another morning.
--
-- And how far back the machine data is read: six weeks. The brief looks 20
-- working days back (about a month), and at jobs closed in that time,
-- which take at most four days from first piece to last. Cleaning the
-- machine data is the slow part, and over the whole history it slowed
-- every morning a little more than the last (M5 step 3: over 60 s in
-- Athena by the window's end). A counter's first reading after the cutoff
-- counts no strokes, so nothing the brief reads is changed by it, which a
-- test checks against the whole history.
create or replace view brief_cut as
select morning,
       cut_utc,
       cut_utc - interval '42' day as machines_from_utc
from (
    select morning,
           cast(with_timezone(cast(morning as timestamp) + interval '330' minute, 'America/Denver')
                at time zone 'UTC' as timestamp) as cut_utc
    from (values (date '${morning}')) as m (morning)
) as c;


-- MES: the files dated before the morning. A scan is in the file of the
-- day it closed, so it had arrived if it closed before midnight; a stop is
-- in the file of the day it ended.
create or replace view mes_labor as
select * from crestline_raw.mes_labor
where scan_off_local < (select cast(morning as timestamp) from brief_cut);

create or replace view mes_downtime as
select * from crestline_raw.mes_downtime
where end_time_local < (select cast(morning as timestamp) from brief_cut);


-- Machines: the messages published before the brief runs, from six weeks
-- before it.
create or replace view pb01_data as
select * from crestline_raw.pb01_data
where published_at_utc >= (select machines_from_utc from brief_cut)
  and published_at_utc <  (select cut_utc from brief_cut);

create or replace view pb02_counts as
select * from crestline_raw.pb02_counts
where published_at_utc >= (select machines_from_utc from brief_cut)
  and published_at_utc <  (select cut_utc from brief_cut);

create or replace view pem01_counts as
select * from crestline_raw.pem01_counts
where published_at_utc >= (select machines_from_utc from brief_cut)
  and published_at_utc <  (select cut_utc from brief_cut);

create or replace view pcline_process as
select * from crestline_raw.pcline_process
where published_at_utc >= (select machines_from_utc from brief_cut)
  and published_at_utc <  (select cut_utc from brief_cut);

create or replace view lsr01_messages as
select * from crestline_raw.lsr01_messages
where published_at_utc >= (select machines_from_utc from brief_cut)
  and published_at_utc <  (select cut_utc from brief_cut);

create or replace view lsr01_metrics as
select * from crestline_raw.lsr01_metrics
where published_at_utc >= (select machines_from_utc from brief_cut)
  and published_at_utc <  (select cut_utc from brief_cut);


-- ERP: rates are master data, set up before the window and never changed.
create or replace view erp_work_centers as
select * from crestline_raw.erp_work_centers;

-- Rows nothing has changed since the cut.
create or replace view erp_jobs as
select * from crestline_raw.erp_jobs
where lastmodified_utc < (select cut_utc from brief_cut);

create or replace view erp_sales_order_lines as
select * from crestline_raw.erp_sales_order_lines
where lastmodified_utc < (select cut_utc from brief_cut);

create or replace view erp_job_operations as
select * from crestline_raw.erp_job_operations
where lastmodified_utc < (select cut_utc from brief_cut);

create or replace view erp_job_costs as
select * from crestline_raw.erp_job_costs
where lastmodified_utc < (select cut_utc from brief_cut);

-- Labor details can't be dated by arrival at all: the ERP can stamp a row
-- with the time its scan closed instead of the time it posted (the stale
-- LastModified mess, spec §4.1), so a stamp before the cut doesn't mean the
-- row was there. As of a morning, none of it is read; scrap comes from the
-- MES, which is where it is first recorded anyway.
create or replace view erp_labor_details as
select * from crestline_raw.erp_labor_details
where false;

-- Routings: each job's operations, in order. The ERP routes a job when the
-- order is entered and never changes the routing, but the operation rows
-- move on (quantities post every night), so the cut above would drop a job
-- still on the floor. Only the routing's own columns, then, and only for
-- jobs the floor had scanned by the morning, which the ERP had entered.
create or replace view erp_routings as
select JobNum, OprSeq, WorkCenter
from crestline_raw.erp_job_operations
where JobNum in (select job_num from mes_labor);
