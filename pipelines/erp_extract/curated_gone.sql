-- The keys a refused MERGE would have deleted.
--
-- When curated_check.sql finds a MERGE would delete more of a table than the
-- curate step allows, the step refuses it and runs this instead, for the same
-- entity and window (`gone_sql()` in curated.py fills in the ${...} names):
-- the same anti-join the check counts, as a list, so whoever gets the alarm
-- can see which rows would have gone before saying yes.

with

-- The window's raw pages, as the MERGE reads them.
pages as (
    select regexp_extract("$path", '([^/]+)\.jsonl$', 1)           as read_name,
           cast(json_extract(line, '$.body.value') as array(json)) as records
    from ${raw_table}
    where entity = '${entity}'
      and ingest_date between '${first_date}' and '${last_date}'
),

-- The keys the window's reads served, typed as the curated table types them.
window_keys as (
    select distinct ${typed_keys}
    from (
        select ${keys_as_served}
        from pages as p
        cross join unnest(p.records) as r (record)
        where p.read_name between '${first_read}' and '${last_read}'
    )
)

-- The table's keys that no read in the window holds.
select ${keys}
from ${curated_table} as c
where ${starts_complete}
  and not exists (select 1 from window_keys as n where ${copy_match})
order by ${keys}
