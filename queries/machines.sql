-- Machine data, cleaned: what each machine counted, and when, rebuilt from
-- what its gateway published (M3).
--
-- Four generations of equipment reach the platform four ways (spec §4.3),
-- and each way has its own imperfections: device clocks that drift, QoS 1
-- redeliveries, gateways that go offline and replay their buffers late (or
-- lose the data), and legacy 16-bit counters that roll over and reset to 0
-- when the machine is power cycled. The views below undo them in order:
--
--   machine_messages    every message as one row in one shape, redeliveries dropped
--   machine_readings    each put at the moment it really happened
--   machine_counts      counter readings turned into cycles (strokes, parts, hooks)
--   machine_cycles_by_minute, machine_cycles_by_day
--   machine_states      the state each machine reported, as intervals
--
-- downtime.sql builds the stops on top of these and the MES scans.
--
-- Reads only the sources (sources.sql), never the answer key. Runs unchanged
-- on DuckDB and Athena, so it sticks to SQL both engines read the same way.


-- The machines that send data, and what each one is.
--   clock:          whose clock stamps its readings
--   counter_modulus: where its counter rolls over (null: it doesn't)
--   poll_seconds:   how often the gateway polls it (null: it publishes on change)
create or replace view machines as
select * from (values
    ('LSR-01',  'modern',  'Sparkplug B',    'LSR-01', cast(null as bigint), cast(null as integer)),
    ('PB-01',   'mid-age', 'flat JSON',      'PB-01',  cast(null as bigint), cast(null as integer)),
    ('PB-02',   'legacy',  'polled counter', 'GW02',   cast(65536 as bigint), cast(30 as integer)),
    ('PEM-01',  'legacy',  'polled counter', 'GW02',   cast(65536 as bigint), cast(30 as integer)),
    ('PC-LINE', 'process', 'process stream', 'GW03',   cast(null as bigint),  cast(5 as integer))
) as m (machine_id, generation, protocol, clock, counter_modulus, poll_seconds);


-- 1. Every message from every machine, in one shape, each one once.
--
--   stamped_at_utc  the time in the payload, on whichever clock wrote it
--   counter         the machine's cycle counter (strokes, parts, or hooks)
--   state_code      the state it reports, in its own codes (null if none)
--
-- A QoS 1 redelivery is the same message again a moment later: the same
-- bytes, so the same payload. Grouping on everything the payload says keeps
-- one of each, at its first arrival. For Sparkplug that is the message
-- type, its seq, and its timestamp: seq alone repeats every 256 messages,
-- and bdSeq only names the session, so neither identifies a message alone.
create or replace view machine_messages as
with laser_aliases as (
    -- DDATA names its metrics by alias only (alias 2, not PartCount), the
    -- way most edge nodes are set up; the births say which alias is which.
    select distinct metric_alias, metric_name
    from lsr01_metrics
    where message_type = 'DBIRTH'
),

laser as (
    -- One row per DBIRTH and DDATA, the two metrics we need pivoted out.
    select 'LSR-01'                                                       as machine_id,
           m.message_type,
           m.seq,
           m.device_ts_utc                                                as stamped_at_utc,
           m.published_at_utc,
           max(case when coalesce(m.metric_name, a.metric_name) = 'PartCount'
                    then m.int_value end)                                 as counter,
           max(case when coalesce(m.metric_name, a.metric_name) = 'State'
                    then m.int_value end)                                 as state_code,  -- 0 idle, 1 setup, 2 run, 3 alarm, 4 hold
           -- Replayed after an outage: the node says so on every metric.
           max(case when m.is_historical then 1 else 0 end) = 1           as is_historical
    from lsr01_metrics as m
    left join laser_aliases as a on a.metric_alias = m.metric_alias
    where m.message_type in ('DBIRTH', 'DDATA')
    group by m.message_type, m.seq, m.device_ts_utc, m.published_at_utc
),

every_message as (
    select machine_id, message_type, seq, stamped_at_utc, published_at_utc,
           counter, state_code, is_historical
    from laser
    union all
    -- The laser switched off for the night: no state until it's back.
    select 'LSR-01', message_type, seq, device_ts_utc, published_at_utc,
           cast(null as bigint), cast(null as bigint), false
    from lsr01_messages
    where message_type = 'DDEATH'
    union all
    select 'PB-01', 'data', cast(null as integer), device_ts_utc, published_at_utc,
           cyc_cnt, mode, false                         -- mode: 0 idle, 1 setup, 2 run, 3 fault
    from pb01_data
    union all
    select 'PB-02', 'data', cast(null as integer), gateway_ts_utc, published_at_utc,
           cnt, cast(null as bigint), false
    from pb02_counts
    union all
    select 'PEM-01', 'data', cast(null as integer), gateway_ts_utc, published_at_utc,
           cnt, cast(null as bigint), false
    from pem01_counts
    union all
    select 'PC-LINE', 'data', cast(null as integer), gateway_ts_utc, published_at_utc,
           hook_cnt,
           case when line_speed_ft_min > 0 then 1 else 0 end,   -- 1 conveyor moving, 0 stopped
           false
    from pcline_process
)

select machine_id,
       message_type,
       seq,
       stamped_at_utc,
       min(published_at_utc) as published_at_utc,
       counter,
       state_code,
       is_historical
from every_message
group by machine_id, message_type, seq, stamped_at_utc, counter, state_code, is_historical;


-- 2. Each message at the moment it really happened.
--
-- Two clocks say when a message happened, and neither is right on its own.
-- The device's clock (stamped_at_utc) drifts: PB-01's controller wanders by
-- minutes. The broker's clock (published_at_utc) is right, but only says
-- when the message arrived, and a message a gateway held through an outage
-- arrives up to 45 minutes late.
--
-- So: the gap between the two (the skew) is the device clock's error plus
-- the time the message spent in transit. A message that came straight
-- through spent next to none, so its skew is the clock's error, and the
-- fastest message in each hour shows it. A message that waited in a buffer
-- has a skew far below that (or, for Sparkplug, is flagged historical).
-- That is the trick NTP uses: trust the quickest round trip.
--
-- A message that came straight through happened when it arrived. One that
-- waited is put at its own timestamp, corrected by the error the clock
-- showed on the last message before it that came straight through.
create or replace view machine_readings as
with skews as (
    select g.*,
           m.clock,
           date_diff('millisecond', g.published_at_utc, g.stamped_at_utc) as skew_ms
    from machine_messages as g
    join machines as m on m.machine_id = g.machine_id
),

fastest as (
    select clock,
           date_trunc('hour', published_at_utc) as published_hour,
           max(skew_ms)                         as fastest_skew_ms
    from skews
    group by clock, date_trunc('hour', published_at_utc)
),

classified as (
    -- Within 30 s of the hour's fastest counts as straight through: a clock
    -- drifts far less than that in an hour, and a buffered message is late
    -- by however long the outage had left to run when it was made.
    select s.*,
           f.fastest_skew_ms,
           not s.is_historical and s.skew_ms >= f.fastest_skew_ms - 30000 as came_straight_through
    from skews as s
    join fastest as f
      on f.clock = s.clock
     and f.published_hour = date_trunc('hour', s.published_at_utc)
),

since_straight as (
    -- Number the runs of messages, in the device's own time order, that
    -- start with one that came straight through...
    select c.*,
           sum(case when came_straight_through then 1 else 0 end) over (
               partition by clock
               order by stamped_at_utc, published_at_utc
               rows between unbounded preceding and current row
           ) as run_number
    from classified as c
),

offsets as (
    -- ...so every message in a run takes the skew of the one that starts it.
    select s.*,
           coalesce(
               max(case when came_straight_through then skew_ms end) over (partition by clock, run_number),
               fastest_skew_ms
           ) as clock_offset_ms
    from since_straight as s
)

select machine_id,
       message_type,
       seq,
       stamped_at_utc,
       published_at_utc,
       -- A millisecond is written as 0.001 s: Athena has no millisecond interval.
       stamped_at_utc - clock_offset_ms * interval '0.001' second as occurred_at_utc,
       clock_offset_ms,
       came_straight_through,
       counter,
       state_code
from offsets;


-- 3. Cycles counted: how far each machine's counter moved since its last
--    reading, in the order the readings really happened.
--
-- PB-01's counter, the laser's PartCount, and the powder line's hooks only
-- ever go up. PB-02's and PEM-01's are 16 bits wide, and go down two ways:
--   - a rollover: past 65,535 it starts again at 0, so 65,530 then 4 is 10
--     strokes;
--   - a reset: the machine was switched off and on, and counts again from
--     0, so 31,207 then 4 is 4 strokes.
-- The gateway can't tell them apart; the arithmetic can. A rollover can
-- only be read from a counter that was near the top: no machine here makes
-- more than a stroke a second, so if counting on through 65,535 adds more
-- strokes than seconds have passed, it was a reset.
--
-- Two readings can share an instant: the laser's once-a-minute heartbeat
-- repeats every metric, and can land on the same millisecond as the piece
-- that moved PartCount on. Of two counts at one instant, the lower came first.
create or replace view machine_counts as
with ordered as (
    select r.machine_id,
           r.occurred_at_utc,
           r.counter,
           m.counter_modulus,
           lag(r.counter) over (partition by r.machine_id order by r.occurred_at_utc, r.counter) as previous_counter,
           lag(r.occurred_at_utc) over (partition by r.machine_id order by r.occurred_at_utc, r.counter) as previous_at_utc
    from machine_readings as r
    join machines as m on m.machine_id = r.machine_id
    where r.counter is not null
)

select machine_id,
       occurred_at_utc as counted_at_utc,
       previous_at_utc,
       case
           when previous_counter is null       then 0
           when counter >= previous_counter    then counter - previous_counter
           when counter_modulus - previous_counter + counter
                <= date_diff('second', previous_at_utc, occurred_at_utc)
                                               then counter_modulus - previous_counter + counter
           else counter
       end as cycles,
       case
           when previous_counter is null or counter >= previous_counter then 'count'
           when counter_modulus - previous_counter + counter
                <= date_diff('second', previous_at_utc, occurred_at_utc) then 'rollover'
           else 'reset'
       end as counted_as
from ordered;


-- Cycles per machine per minute (UTC) and per day. A day is the plant's
-- calendar day, in its own time zone: 2nd shift ends at 23:00 in Denver,
-- which is the next morning in UTC.
create or replace view machine_cycles_by_minute as
select machine_id,
       date_trunc('minute', counted_at_utc) as minute_utc,
       sum(cycles)                          as cycles
from machine_counts
group by machine_id, date_trunc('minute', counted_at_utc);

-- The plant day of a UTC instant: with_timezone says it is UTC, `at time
-- zone` reads it on the plant's clock, and the date is taken straight from
-- that. Cast it to a timestamp on the way and Athena turns it back into UTC
-- (DuckDB keeps the plant's wall clock), so every day is a UTC day, six or
-- seven hours off (found in M3 step 8).
create or replace view machine_cycles_by_day as
select machine_id,
       cast(with_timezone(counted_at_utc, 'UTC') at time zone 'America/Denver' as date) as work_date,
       sum(cycles) as cycles
from machine_counts
group by 1, 2;


-- 4. The state each machine reported, as intervals: a state lasts from the
--    reading that changed to it until the next change. Machines that only
--    count (PB-02, PEM-01) have none. The laser has none while switched off
--    (state_code -1, from its DDEATH until its next DBIRTH); the powder
--    line's is whether its conveyor is moving.
--
--    When the laser's heartbeat (the old state, repeated) lands on the same
--    instant as a change, its seq says which was sent first.
create or replace view machine_states as
with readings as (
    select machine_id,
           occurred_at_utc,
           published_at_utc,
           seq,
           coalesce(state_code, -1) as state_code
    from machine_readings
    where state_code is not null or message_type = 'DDEATH'
),

changes as (
    select r.*,
           lag(state_code) over (
               partition by machine_id order by occurred_at_utc, published_at_utc, seq
           ) as previous_state
    from readings as r
),

starts as (
    select machine_id, state_code, occurred_at_utc as started_at_utc
    from changes
    where previous_state is null or previous_state <> state_code
)

select machine_id,
       state_code,
       started_at_utc,
       lead(started_at_utc) over (partition by machine_id order by started_at_utc) as ended_at_utc
from starts;
