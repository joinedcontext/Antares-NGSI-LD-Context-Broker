-- Each stored view's digest, kept with the row. A periodic reload lists the
-- id and digest of every stored view; computed in the query, Postgres
-- re-serialised and hashed each body per tick (one deployment: 354 rows,
-- 18.8 ms mean, 29.5 ms under load). The stored column is computed once per
-- write, so the listing reads 16 bytes per row.
-- jsonb's text form and md5 are immutable, which a stored generated column
-- needs; the column inherits the table's RLS policies. Adding it rewrites
-- `views` once, a few hundred rows.
ALTER TABLE views ADD COLUMN digest text GENERATED ALWAYS AS (left(md5(doc::text), 16)) STORED;
