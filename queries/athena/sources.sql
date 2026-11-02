-- Sources for the queries in queries/, in Athena: views with the same names
-- and columns sources.sql gives DuckDB, so margin_gap.sql runs unchanged on
-- either engine.
--
-- The ERP comes from the curated tables (crestline_curated): the poller's
-- raw pages merged into one typed Iceberg table per entity, kept current
-- after every poll (M2). So do the machines: what AWS IoT Core landed,
-- merged hourly into a table per device (M3). The MES comes from the raw
-- tables (tables.sql), where each upload is a full extract under its own
-- ingest_date; those views read only the newest one, so a later upload
-- replaces an earlier one instead of doubling every row.
--
-- Only what the plant's systems recorded is here, and no query may read
-- anything else.


-- ERP: the curated tables, one row per key, as the ERP holds it now. Their
-- timestamp column is already named for its clock (lastmodified_utc).
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
-- UTC offset, exactly as the MES writes them, and named _local to say so.
-- Each scan is also given in UTC, where subtracting gives the real duration
-- (a scan left open over the weekend the clocks fall back ran an hour longer
-- than its local times say). An empty note is no note.
create or replace view mes_labor as
select record_id,
       record_type,
       employee_id,
       employee_name,
       job_num,
       op_seq,
       work_center,
       scan_on  as scan_on_local,
       scan_off as scan_off_local,
       qty_good,
       qty_scrap,
       nullif(notes, '') as notes,
       cast(with_timezone(scan_on,  'America/Denver') at time zone 'UTC' as timestamp) as scan_on_utc,
       cast(with_timezone(scan_off, 'America/Denver') at time zone 'UTC' as timestamp) as scan_off_utc
from raw_mes_labor
where ingest_date = (select max(ingest_date) from raw_mes_labor);


-- Machines: the curated tables (crestline_curated.machine_<device>), one
-- typed row per message AWS IoT Core landed (M3). Their columns are already
-- named as these views name them, clock included. Each table holds every
-- replay of the backfill's machine files through IoT Core; like the raw
-- extracts above, a view reads only the newest (replays are named for when
-- they started, so the newest sorts last).
--
-- A message GW01 corrupted in transit mostly never reaches a table: IoT
-- Core's rules engine drops JSON it can't parse (M3 step 7). Cut between
-- two tokens, it can get through as a row with none of the payload's
-- fields (8 of 29 in the 90-day replay, M3 step 8); pb01_data skips those,
-- just as DuckDB's view skips every corrupted line.

-- PB-01 (GW01) publishes its controller registers whenever one changes.
create or replace view pb01_data as
select device_ts_utc,
       mode,           -- 0 idle, 1 setup, 2 run, 3 fault
       cyc_cnt,        -- stroke counter
       guard_open,
       program,
       published_at_utc
from crestline_curated.machine_pb01
where replay = (select max(replay) from crestline_curated.machine_pb01)
  and device_ts_utc is not null;

-- PB-02 and PEM-01 (GW02) are 16-bit stroke counters, polled every 30 seconds.
create or replace view pb02_counts as
select gateway_ts_utc, cnt, published_at_utc
from crestline_curated.machine_pb02
where replay = (select max(replay) from crestline_curated.machine_pb02);

create or replace view pem01_counts as
select gateway_ts_utc, cnt, published_at_utc
from crestline_curated.machine_pem01
where replay = (select max(replay) from crestline_curated.machine_pem01);

-- PC-LINE (GW03) samples the powder line every 5 seconds.
create or replace view pcline_process as
select gateway_ts_utc, zone1_f, zone2_f, zone3_f, zone4_f,
       line_speed_ft_min, hook_cnt, published_at_utc
from crestline_curated.machine_pcline
where replay = (select max(replay) from crestline_curated.machine_pcline);

-- LSR-01 (EDGE-LASER) speaks Sparkplug B. The IoT rule decoded each payload
-- into protobuf's JSON mapping (`decoded`): 64-bit integers as strings,
-- timestamps as epoch milliseconds on the laser's clock. The message type
-- is the topic's third part: spBv1.0/Crestline/DDATA/EDGE-LASER/LSR-01.
create or replace view lsr01_messages as
select split_part(topic, '/', 3)                                    as message_type,
       cast(json_extract_scalar(decoded, '$.seq') as integer)       as seq,
       cast(from_unixtime(cast(json_extract_scalar(decoded, '$.timestamp') as bigint) / 1000.0)
            at time zone 'UTC' as timestamp)                         as device_ts_utc,
       published_at_utc
from crestline_curated.machine_lsr01
where replay = (select max(replay) from crestline_curated.machine_lsr01);

-- ...and its metrics, one row each. Births name every metric with its
-- alias; data messages carry the alias alone. A metric's value travels in
-- the field its datatype says: whole numbers in intValue or longValue.
create or replace view lsr01_metrics as
select split_part(m.topic, '/', 3)                                       as message_type,
       cast(json_extract_scalar(m.decoded, '$.seq') as integer)          as seq,
       cast(from_unixtime(cast(json_extract_scalar(m.decoded, '$.timestamp') as bigint) / 1000.0)
            at time zone 'UTC' as timestamp)                              as device_ts_utc,
       m.published_at_utc,
       json_extract_scalar(metric, '$.name')                             as metric_name,
       cast(json_extract_scalar(metric, '$.alias') as integer)           as metric_alias,
       cast(from_unixtime(cast(json_extract_scalar(metric, '$.timestamp') as bigint) / 1000.0)
            at time zone 'UTC' as timestamp)                              as metric_ts_utc,
       coalesce(cast(json_extract_scalar(metric, '$.isHistorical') as boolean), false) as is_historical,
       coalesce(cast(json_extract_scalar(metric, '$.intValue') as bigint),
                cast(json_extract_scalar(metric, '$.longValue') as bigint)) as int_value,
       cast(json_extract_scalar(metric, '$.floatValue') as double)       as float_value,
       json_extract_scalar(metric, '$.stringValue')                      as string_value,
       cast(json_extract_scalar(metric, '$.booleanValue') as boolean)    as boolean_value
from crestline_curated.machine_lsr01 as m
cross join unnest(cast(json_extract(m.decoded, '$.metrics') as array(json))) as t (metric)
where m.replay = (select max(replay) from crestline_curated.machine_lsr01);
