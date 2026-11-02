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
--
-- They differ in one hour a year. When the clocks fall back, 01:00 to 02:00
-- happens twice, and a local time in it names two moments. Athena takes the
-- earlier one (still daylight time), DuckDB the later. Crestline's rule is
-- the earlier one (spec §4.2), so here DuckDB is told: if the moment an
-- hour before its answer shows the same wall-clock time, that's the one.
create or replace macro with_timezone(ts, zone) as
    case when timezone(zone, timezone(zone, ts) - interval 1 hour) = ts
         then timezone(zone, ts) - interval 1 hour
         else timezone(zone, ts)
    end;


-- ERP: the JSONL export, one file per entity. LastModified is UTC, and is
-- named for it, as in the curated tables Athena reads.
create or replace view erp_work_centers      as select * from read_json('erp/export/WorkCenters.jsonl');
create or replace view erp_jobs              as select * rename (LastModified as lastmodified_utc) from read_json('erp/export/Jobs.jsonl');
create or replace view erp_sales_order_lines as select * rename (LastModified as lastmodified_utc) from read_json('erp/export/SalesOrderLines.jsonl');
create or replace view erp_job_operations    as select * rename (LastModified as lastmodified_utc) from read_json('erp/export/JobOperations.jsonl');
create or replace view erp_job_costs         as select * rename (LastModified as lastmodified_utc) from read_json('erp/export/JobCosts.jsonl');


-- MES: the nightly labor files. Timestamps are plant local time with no
-- UTC offset, exactly as the MES writes them, and named _local to say so.
-- (Naming the columns here skips the file's own header row.) Each scan is
-- also given in UTC, where subtracting gives the real duration: a scan
-- left open over the weekend the clocks fall back ran an hour longer than
-- its local times say.
create or replace view mes_labor as
select *,
       cast(with_timezone(scan_on_local,  'America/Denver') at time zone 'UTC' as timestamp) as scan_on_utc,
       cast(with_timezone(scan_off_local, 'America/Denver') at time zone 'UTC' as timestamp) as scan_off_utc
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
        'scan_on_local': 'TIMESTAMP',
        'scan_off_local': 'TIMESTAMP',
        'qty_good': 'INTEGER',
        'qty_scrap': 'INTEGER',
        'notes': 'VARCHAR'
    }
);


-- Machine gateways: one MQTT message per line. Timestamps are UTC, and come
-- out as plain timestamps (the ISO 8601 `Z` is converted, then dropped).
-- Each is named for the clock that stamped it: a device's own (device_ts_utc,
-- PB-01's controller and LSR-01's, which drift), a gateway's (gateway_ts_utc:
-- GW02's drifts, GW03's keeps NTP time), or the broker's, when the gateway
-- published it (published_at_utc, which is true time). machines.sql works
-- out which to believe.
--
-- The columns are declared rather than guessed, because GW01 can corrupt a
-- message in transit (M3): the payload is then the text that arrived, not
-- an object, and a guessed schema would fail on it. Such a line has no
-- fields to read and is skipped here.

-- PB-01 (GW01) publishes its controller registers whenever one changes.
create or replace view pb01_data as
select
    cast(payload->>'$.ts' as timestamp)         as device_ts_utc,
    cast(payload->>'$."N7:12"' as integer)      as mode,        -- 0 idle, 1 setup, 2 run, 3 fault
    cast(payload->>'$.CYC_CNT_2' as bigint)     as cyc_cnt,     -- stroke counter
    cast(payload->>'$."B3:0/4"' as boolean)     as guard_open,
    cast(payload->>'$.PRG' as integer)          as program,
    published_at                                as published_at_utc
from read_json('machines/pb01.jsonl', columns = {topic: 'VARCHAR', published_at: 'TIMESTAMP', payload: 'JSON'})
where json_type(payload) = 'OBJECT';

-- PB-02 and PEM-01 (GW02) are 16-bit stroke counters, polled every 30 seconds.
create or replace view pb02_counts as
select
    cast(payload->>'$.gw_ts' as timestamp) as gateway_ts_utc,
    cast(payload->>'$.cnt' as integer)     as cnt,
    published_at                           as published_at_utc
from read_json('machines/pb02.jsonl', columns = {topic: 'VARCHAR', published_at: 'TIMESTAMP', payload: 'JSON'})
where json_type(payload) = 'OBJECT';

create or replace view pem01_counts as
select
    cast(payload->>'$.gw_ts' as timestamp) as gateway_ts_utc,
    cast(payload->>'$.cnt' as integer)     as cnt,
    published_at                           as published_at_utc
from read_json('machines/pem01.jsonl', columns = {topic: 'VARCHAR', published_at: 'TIMESTAMP', payload: 'JSON'})
where json_type(payload) = 'OBJECT';

-- PC-LINE (GW03) samples the powder line every 5 seconds: four oven zones
-- (°F), conveyor speed (ft/min), and the hook counter. `t` is GW03's clock
-- in epoch milliseconds.
create or replace view pcline_process as
select
    epoch_ms(cast(payload->>'$.t' as bigint))   as gateway_ts_utc,
    cast(payload->>'$.z1' as double)            as zone1_f,
    cast(payload->>'$.z2' as double)            as zone2_f,
    cast(payload->>'$.z3' as double)            as zone3_f,
    cast(payload->>'$.z4' as double)            as zone4_f,
    cast(payload->>'$.spd' as double)           as line_speed_ft_min,
    cast(payload->>'$.hook_cnt' as bigint)      as hook_cnt,
    published_at                                as published_at_utc
from read_json('machines/pcline.jsonl', columns = {topic: 'VARCHAR', published_at: 'TIMESTAMP', payload: 'JSON'})
where json_type(payload) = 'OBJECT';

-- LSR-01 (EDGE-LASER) speaks Sparkplug B: protobuf, decoded before SQL sees
-- it, the way the IoT rule decodes it in AWS (decoded/lsr01.jsonl, written
-- by `crestline query`). The decoded payload is protobuf's JSON mapping:
-- 64-bit integers arrive as strings, and timestamps as epoch milliseconds
-- on the laser's clock. The message type is the topic's third part:
-- spBv1.0/Crestline/DDATA/EDGE-LASER/LSR-01.
create or replace view lsr01_messages as
select
    split_part(topic, '/', 3)                           as message_type,
    cast(decoded->>'$.seq' as integer)                  as seq,
    epoch_ms(cast(decoded->>'$.timestamp' as bigint))   as device_ts_utc,
    published_at                                        as published_at_utc
from read_json('decoded/lsr01.jsonl', columns = {topic: 'VARCHAR', published_at: 'TIMESTAMP', payload_b64: 'VARCHAR', decoded: 'JSON'});

-- ...and its metrics, one row each. Births name every metric with its
-- alias; data messages carry the alias alone. A metric's value travels in
-- the field its datatype says: whole numbers in intValue or longValue.
create or replace view lsr01_metrics as
with metrics as (
    select topic,
           decoded,
           published_at,
           unnest(cast(decoded->'$.metrics' as json[])) as metric
    from read_json('decoded/lsr01.jsonl', columns = {topic: 'VARCHAR', published_at: 'TIMESTAMP', payload_b64: 'VARCHAR', decoded: 'JSON'})
)
select
    split_part(topic, '/', 3)                                       as message_type,
    cast(decoded->>'$.seq' as integer)                              as seq,
    epoch_ms(cast(decoded->>'$.timestamp' as bigint))               as device_ts_utc,
    published_at                                                    as published_at_utc,
    metric->>'$.name'                                               as metric_name,
    cast(metric->>'$.alias' as integer)                             as metric_alias,
    epoch_ms(cast(metric->>'$.timestamp' as bigint))                as metric_ts_utc,
    coalesce(cast(metric->>'$.isHistorical' as boolean), false)     as is_historical,
    coalesce(cast(metric->>'$.intValue' as bigint),
             cast(metric->>'$.longValue' as bigint))                as int_value,
    cast(metric->>'$.floatValue' as double)                         as float_value,
    metric->>'$.stringValue'                                        as string_value,
    cast(metric->>'$.booleanValue' as boolean)                      as boolean_value
from metrics;
