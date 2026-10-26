-- Before a curated table's MERGE: does Athena see every row the extractor
-- wrote, and how much would the MERGE delete?
--
-- The curate step (curate.py) runs this first, for the same entity and the
-- same window of raw reads as the MERGE (`check_sql()` in curated.py fills in
-- the ${...} names), and fails the run, before anything changes, if
--
--   * rows_seen differs from the rows in the window's raw files, which the
--     curate step counts itself, straight from S3. A read Athena can't see
--     (filed on a day the table doesn't project, say) looks empty, and an
--     empty sweep would say every row was deleted.
--   * rows_gone is more than a small share of rows_held. The ERP deletes a
--     few cancelled duplicates a day; a merge that would delete a large part
--     of a table is a fault until someone says otherwise.

with

-- The window's raw pages, as the MERGE reads them.
pages as (
    select regexp_extract("$path", '([^/]+)\.jsonl$', 1)           as read_name,
           cast(json_extract(line, '$.body.value') as array(json)) as records
    from ${raw_table}
    where entity = '${entity}'
      and ingest_date between '${first_date}' and '${last_date}'
),

-- Every row the window's reads served.
served as (
    select r.record
    from pages as p
    cross join unnest(p.records) as r (record)
    where p.read_name between '${first_read}' and '${last_read}'
),

-- The keys among them, typed as the curated table types them.
window_keys as (
    select distinct ${typed_keys}
    from (
        select ${keys_as_served}
        from served
    )
)

select
    (select count(*) from served)           as rows_seen,
    (select count(*) from ${curated_table}) as rows_held,
    -- What the MERGE's `gone` step would delete.
    (select count(*)
     from ${curated_table} as c
     where ${starts_complete}
       and not exists (select 1 from window_keys as n where ${copy_match})
    )                                       as rows_gone
