-- Curated ERP tables: one entity's raw reads merged into its typed Iceberg
-- table, in Athena.
--
-- The curate step (curate.py) runs this once per entity after every poll.
-- `merge_sql()` in curated.py fills in the ${...} names for one entity and
-- one window of raw reads; `crestline curate --show Jobs` prints the result.
--
-- It does what merge.py does in Python: replay the raw reads in order. An
-- incremental or sequence read replaces the rows it holds. A full read or a
-- sweep is a complete picture of the ERP, so a key it lacks is gone.
--
-- The curate step picks the window before this runs, from the file names in
-- the bucket: the reads not merged yet, starting at the newest full read or
-- sweep among them if there is one (it supersedes everything before it). So
-- the MERGE reads a few files, not the whole history, and its partition
-- filter is a constant Athena can prune with.
--
-- One MERGE is one commit: the table moves from one consistent state to the
-- next, with no moment where the updates are in and the deletes are not.

merge into ${curated_table} as t
using (
    with

    -- The window's raw pages, one JSON line each. A read's file name is its
    -- place in the order: 20260806T061351Z-001-sweep sorts before
    -- 20260807T061402Z-000-incremental.
    pages as (
        select regexp_extract("$path", '([^/]+)\.jsonl$', 1)           as read_name,
               cast(json_extract_scalar(line, '$.page') as integer)    as page,
               cast(json_extract(line, '$.body.value') as array(json)) as records
        from ${raw_table}
        where entity = '${entity}'
          and ingest_date between '${first_date}' and '${last_date}'
    ),

    -- Every row each page served, still as text: numbers may be strings
    -- ("250.00000000") and dates /Date(ms)/. Each row keeps its place: the
    -- read, the page, and its position on the page.
    served as (
        select p.read_name, p.page, r.position_on_page,
               ${as_served}
        from pages as p
        cross join unnest(p.records) with ordinality as r (record, position_on_page)
        where p.read_name between '${first_read}' and '${last_read}'
    ),

    -- The newest version of each key in the window. A key can turn up twice
    -- in one read if the ERP changed between pages; the later page wins.
    newest as (
        select *
        from (
            select *,
                   row_number() over (
                       partition by ${keys}
                       order by read_name desc, page desc, position_on_page desc
                   ) as version
            from served
        )
        where version = 1
    ),

    -- Each column as the curated table types it.
    typed as (
        select ${typed}
        from newest
    ),

    -- If the window starts with a complete read, a key the table holds but
    -- no read in the window does was deleted from the ERP. (The curate step
    -- runs curated_check.sql first, so this never acts on a read Athena
    -- can't see, and never deletes much of the table at once.)
    gone as (
        select c.*
        from ${curated_table} as c
        where ${starts_complete}
          and not exists (select 1 from typed as n where ${copy_match})
    )

    select *, false as gone_from_erp from typed
    union all
    select *, true from gone
) as s
on ${key_match}

when matched and s.gone_from_erp then
    delete

-- Rows the window read again unchanged (a sweep reads every row) are left
-- alone, so the table's history shows only real changes.
when matched and (${changed}) then
    update set ${assignments}

when not matched and not s.gone_from_erp then
    insert (${columns})
    values (${source_columns})
