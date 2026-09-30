-- Sources for the queries in this folder: DuckDB views over one backfill's
-- output. Paths are relative to the output folder, so run from inside it,
-- or point DuckDB's file_search_path at it (`crestline query` does that).
-- athena/sources.sql defines the same names over the raw bucket, so the
-- queries themselves run unchanged on either engine.
--
-- Only what the plant's systems recorded is here. The answer key (truth/)
-- is not, and no query may read it.


-- Reading a plant-local timestamp as a moment in time: Athena calls it
-- with_timezone(ts, zone), DuckDB timezone(zone, ts). One name for both.
create or replace macro with_timezone(ts, zone) as timezone(zone, ts);


-- ERP: the JSONL export, one file per entity.
create or replace view erp_work_centers      as select * from read_json('erp/export/WorkCenters.jsonl');
create or replace view erp_jobs              as select * from read_json('erp/export/Jobs.jsonl');
create or replace view erp_sales_order_lines as select * from read_json('erp/export/SalesOrderLines.jsonl');
create or replace view erp_job_operations    as select * from read_json('erp/export/JobOperations.jsonl');
create or replace view erp_job_costs         as select * from read_json('erp/export/JobCosts.jsonl');


-- MES: the nightly labor files. Timestamps are plant local time with no
-- UTC offset, exactly as the MES writes them.
create or replace view mes_labor as
select *
from read_csv(
    'mes/outbound/labor_*.csv',
    header = true,
    columns = {
        'record_id': 'BIGINT',
        'record_type': 'VARCHAR',
        'employee_id': 'VARCHAR',
        'employee_name': 'VARCHAR',
        'job_num': 'VARCHAR',
        'op_seq': 'INTEGER',
        'work_center': 'VARCHAR',
        'scan_on': 'TIMESTAMP',
        'scan_off': 'TIMESTAMP',
        'qty_good': 'INTEGER',
        'qty_scrap': 'INTEGER',
        'notes': 'VARCHAR'
    }
);


-- Brake gateways: one MQTT message per line. Timestamps are UTC, and come
-- out as plain timestamps (the ISO 8601 `Z` is converted, then dropped).

-- PB-01 (GW01) publishes its controller registers whenever one changes.
create or replace view pb01_data as
select
    cast(payload.ts as timestamp) as ts,
    payload."N7:12"               as mode,        -- 0 idle, 1 setup, 2 run, 3 fault
    payload."CYC_CNT_2"           as cyc_cnt,     -- stroke counter
    payload."B3:0/4"              as guard_open,
    payload."PRG"                 as program
from read_json('machines/pb01.jsonl');

-- PB-02 (GW02) is a 16-bit stroke counter, polled every 30 seconds.
create or replace view pb02_counts as
select
    cast(payload.gw_ts as timestamp) as gw_ts,
    payload.cnt                      as cnt
from read_json('machines/pb02.jsonl');
