-- Sources for the queries in queries/, in Athena: views with the same names
-- and columns sources.sql gives DuckDB, so margin_gap.sql runs unchanged on
-- either engine.
--
-- The ERP comes from the curated tables (crestline_curated): the poller's
-- raw pages merged into one typed Iceberg table per entity, kept current
-- after every poll (M2). Everything else comes from the raw tables
-- (tables.sql), where each upload is a full extract under its own
-- ingest_date; those views read only the newest one, so a later upload
-- replaces an earlier one instead of doubling every row.
--
-- Only what the plant's systems recorded is here, and no query may read
-- anything else.


-- ERP: the curated tables, one row per key, as the ERP holds it now.
create or replace view erp_work_centers as
select * from crestline_curated.erp_work_centers;

create or replace view erp_jobs as
select * from crestline_curated.erp_jobs;

create or replace view erp_sales_order_lines as
select * from crestline_curated.erp_sales_order_lines;

create or replace view erp_job_operations as
select * from crestline_curated.erp_job_operations;

create or replace view erp_job_costs as
select * from crestline_curated.erp_job_costs;


-- MES: the nightly labor files. Timestamps are plant local time with no
-- UTC offset, exactly as the MES writes them. An empty note is no note.
create or replace view mes_labor as
select record_id,
       record_type,
       employee_id,
       employee_name,
       job_num,
       op_seq,
       work_center,
       scan_on,
       scan_off,
       qty_good,
       qty_scrap,
       nullif(notes, '') as notes
from raw_mes_labor
where ingest_date = (select max(ingest_date) from raw_mes_labor);


-- Brake gateways: one MQTT message per line. Timestamps are UTC, and come
-- out as plain timestamps (the ISO 8601 `Z` is converted, then dropped).

-- PB-01 (GW01) publishes its controller registers whenever one changes.
-- Each message is one line of JSON text (see tables.sql).
create or replace view pb01_data as
select
    cast(from_iso8601_timestamp(json_extract_scalar(message, '$.payload.ts'))
         at time zone 'UTC' as timestamp)                                 as ts,
    cast(json_extract_scalar(message, '$.payload["N7:12"]') as integer)  as mode,        -- 0 idle, 1 setup, 2 run, 3 fault
    cast(json_extract_scalar(message, '$.payload.CYC_CNT_2') as bigint)  as cyc_cnt,     -- stroke counter
    cast(json_extract_scalar(message, '$.payload["B3:0/4"]') as boolean) as guard_open,
    cast(json_extract_scalar(message, '$.payload.PRG') as integer)       as program
from raw_pb01
where ingest_date = (select max(ingest_date) from raw_pb01);

-- PB-02 (GW02) is a 16-bit stroke counter, polled every 30 seconds.
create or replace view pb02_counts as
select
    cast(from_iso8601_timestamp(payload.gw_ts) at time zone 'UTC' as timestamp) as gw_ts,
    payload.cnt                                                                  as cnt
from raw_pb02
where ingest_date = (select max(ingest_date) from raw_pb02);
