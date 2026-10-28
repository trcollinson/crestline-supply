-- After a MERGE that could delete: exactly which keys it deleted.
--
-- Iceberg keeps every commit as a snapshot, so the table as it stood before
-- the MERGE can still be read. The curate step (curate.py) notes the table's
-- snapshot ID before it merges, then runs this (`deleted_sql()` in curated.py
-- fills in the ${...} names): every key the table held then and doesn't now.
-- The MERGE never deletes a key and inserts it again, so that is what it
-- deleted, and no more.

select ${keys}
from ${curated_table} for version as of ${snapshot}

except

select ${keys}
from ${curated_table}

order by ${keys}
