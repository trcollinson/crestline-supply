-- Downtime, rebuilt from machine data (M3): every stop each machine had
-- while the plant was working, and how we know.
--
-- The plant's own record of its downtime is the MES downtime file: the
-- stops operators wrote down. Most stops over five minutes are there, and
-- no stop under it. The machines can do better, each in the way its
-- generation allows:
--
--   LSR-01 (modern)    says what it is doing: in alarm, or on hold
--   PB-01 (mid-age)    says when it faults, but not when it just stops;
--                      its stroke counter says that
--   PB-02, PEM-01      say nothing but a stroke count, every 30 seconds
--   (legacy)
--   PC-LINE (process)  its conveyor stops
--
-- What no machine can say: the laser shows a nozzle change as setup and
-- maintenance as idle; PB-01 leaves its mode wherever the operator left it.
-- Those stops only the operator's note can name.
--
-- Builds on machines.sql (the cleaned machine data) and margin_gap.sql
-- (the MES scans, the plant's working windows, and which brake ran each
-- brake operation), so it runs after both.


-- 1. Stops a machine reports itself, from the state it publishes.
create or replace view reported_stops as
select machine_id,
       started_at_utc,
       ended_at_utc,
       'state' as found_by
from machine_states
where (machine_id = 'LSR-01'  and state_code in (3, 4))  -- in alarm, or on hold
   or (machine_id = 'PB-01'   and state_code = 3)        -- faulted
   or (machine_id = 'PC-LINE' and state_code = 0);       -- conveyor stopped


-- 2. Stops found in the gaps between strokes.
--
-- While an operation runs, a press makes a stroke every minute or so (a
-- standard brake cycle is at most 1.6 minutes). Three minutes with no
-- stroke in working time, between the operation's first stroke and its
-- last, is a stop.
-- Before the first stroke the press is being set up, and after the last
-- it is done: neither is downtime.
--
-- Which press ran the operation: PEM-01 is the only PEM machine, and the
-- margin query's brake attribution (margin_gap.sql) says which brake ran
-- each brake operation.
--
-- A gap is only a stop if the machine was heard from throughout. GW02 has
-- no buffer, so an outage loses polls, and a gap in strokes across lost
-- polls says nothing about the press.

-- Each operation's run on its press: from the first stroke to the last,
-- while the operation was scanned on (and, as in the margin query, 30 s
-- late for a polled counter).
create or replace view operation_runs as
with operations as (
    select b.job_num, b.op_seq, b.brake as machine_id
    from brake_operations as b
    union all
    select distinct job_num, op_seq, 'PEM-01'
    from scans
    where work_center = 'PEM'
)

select o.machine_id,
       o.job_num,
       o.op_seq,
       min(c.counted_at_utc) as first_stroke_at_utc,
       max(c.counted_at_utc) as last_stroke_at_utc
from operations as o
join scans as s
  on s.job_num = o.job_num
 and s.op_seq  = o.op_seq
join machine_counts as c
  on c.machine_id     = o.machine_id
 and c.counted_at_utc >  s.scan_on_utc  + interval '30' second
 and c.counted_at_utc <= s.scan_off_utc + interval '30' second
 and c.cycles > 0
group by o.machine_id, o.job_num, o.op_seq;

create or replace view stroke_gaps as
with readings as (
    -- Every counter reading, and how many polls have gone missing so far.
    select c.machine_id,
           c.counted_at_utc,
           c.previous_at_utc,
           c.cycles,
           m.poll_seconds,
           sum(case when m.poll_seconds is not null
                     and date_diff('second', c.previous_at_utc, c.counted_at_utc) > 1.5 * m.poll_seconds
                    then 1 else 0 end) over (
               partition by c.machine_id
               order by c.counted_at_utc
               rows between unbounded preceding and current row
           ) as polls_lost
    from machine_counts as c
    join machines as m on m.machine_id = c.machine_id
    where c.machine_id in ('PB-01', 'PB-02', 'PEM-01')
),

strokes as (
    select r.*,
           lag(counted_at_utc) over (partition by machine_id order by counted_at_utc) as stroke_before_at_utc,
           lag(polls_lost)     over (partition by machine_id order by counted_at_utc) as polls_lost_before
    from readings as r
    where cycles > 0
),

gaps as (
    -- The press stopped after the stroke before, and was still stopped at
    -- the last reading before this one: the poll before, for a polled
    -- counter. PB-01 publishes on change, so its next stroke ends the gap.
    select machine_id,
           stroke_before_at_utc as started_at_utc,
           case when poll_seconds is not null then previous_at_utc
                else counted_at_utc end as ended_at_utc
    from strokes
    where stroke_before_at_utc is not null
      and polls_lost = polls_lost_before
),

working_gaps as (
    -- Three minutes of *working* time: a gap that spans a break is mostly
    -- the break, and what's left either side is the cycle the operator
    -- stopped in, not a stop. So each gap is cut at the working windows
    -- first, and each piece has to make three minutes on its own.
    select g.machine_id,
           greatest(g.started_at_utc, w.starts_at_utc) as started_at_utc,
           least(g.ended_at_utc, w.ends_at_utc)        as ended_at_utc
    from gaps as g
    join operation_runs as o
      on o.machine_id = g.machine_id
     and g.started_at_utc >= o.first_stroke_at_utc
     and g.ended_at_utc   <= o.last_stroke_at_utc
    join work_windows as w
      on g.started_at_utc < w.ends_at_utc
     and g.ended_at_utc   > w.starts_at_utc
)

select machine_id,
       started_at_utc,
       ended_at_utc,
       'stroke gap' as found_by
from working_gaps
where date_diff('second', started_at_utc, ended_at_utc) >= 180;


-- 3. Every stop, inside the plant's working windows. Breaks, meals, and
--    nights are planned stops, never downtime; a stop that runs into a
--    break is cut at it. Where the state and the stroke gaps both found a
--    stop, the two overlap; they are merged into one.
create or replace view machine_downtime as
with found as (
    select * from reported_stops
    union all
    select * from stroke_gaps
),

working as (
    select f.machine_id,
           greatest(f.started_at_utc, w.starts_at_utc) as started_at_utc,
           least(f.ended_at_utc, w.ends_at_utc)        as ended_at_utc,
           f.found_by
    from found as f
    join work_windows as w
      on f.started_at_utc < w.ends_at_utc
     and f.ended_at_utc   > w.starts_at_utc
),

ends_before as (
    -- A stop starts a new one unless it begins before an earlier one ends.
    select s.*,
           max(ended_at_utc) over (
               partition by machine_id
               order by started_at_utc, ended_at_utc
               rows between unbounded preceding and 1 preceding
           ) as latest_end_before_utc
    from working as s
),

numbered as (
    select o.*,
           sum(case when latest_end_before_utc >= started_at_utc then 0 else 1 end) over (
               partition by machine_id
               order by started_at_utc, ended_at_utc
               rows between unbounded preceding and current row
           ) as stop_number
    from ends_before as o
)

select n.machine_id,
       m.generation,
       min(n.started_at_utc) as started_at_utc,
       max(n.ended_at_utc)   as ended_at_utc,
       case when count(distinct n.found_by) > 1 then 'both' else max(n.found_by) end as found_by
from numbered as n
join machines as m on m.machine_id = n.machine_id
group by n.machine_id, m.generation, n.stop_number;


-- How much downtime each machine had, and how it was found.
select machine_id,
       generation,
       found_by,
       count(*)                                                                   as stops,
       round(sum(date_diff('second', started_at_utc, ended_at_utc)) / 3600.0, 1)  as hours
from machine_downtime
group by machine_id, generation, found_by
order by machine_id, found_by;
