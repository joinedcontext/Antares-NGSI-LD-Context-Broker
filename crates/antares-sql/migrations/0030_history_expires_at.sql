-- 4.22 for history rows: the instance's `expiresAt` as a column, so the
-- maintenance reap finds the expired rows by an index instead of reading
-- every row of every partition (try_timestamptz(data->>'expiresAt') on each,
-- every tick). Written by the instance writes (store/pg/temporal.rs), NULL
-- on rows whose instance carries no expiry, which is nearly all of them.
--
-- Nullable without a default: a catalog change on every partition and chunk,
-- no rewrite, no scan. No index here: building one would read the whole
-- history under a lock that blocks INSERT (0009's note). The maintenance job
-- builds a partial index on each partition or chunk with CREATE INDEX
-- CONCURRENTLY (store/pg/maintenance.rs `history_expiry_index`). Rows written
-- before this column keep NULL; the job's daily scan of `data` still reaps
-- them.
ALTER TABLE attr_instances ADD COLUMN IF NOT EXISTS expires_at timestamptz;
ALTER TABLE attr_rows ADD COLUMN IF NOT EXISTS expires_at timestamptz;
ALTER TABLE attr_versions ADD COLUMN IF NOT EXISTS expires_at timestamptz;

-- attr_rows in the row type of attr_instances (0020), which just gained the
-- column: a SETOF function returning fewer columns than its row type fails
-- at the call.
CREATE OR REPLACE FUNCTION attr_rows_of(p_tenant text, p_entity text)
  RETURNS SETOF public.attr_instances LANGUAGE sql STABLE PARALLEL SAFE
  AS $$
  SELECT r.tenant_id, p_entity, a.iri,
         COALESCE(r.data->>'instanceId', 'urn:ngsi-ld:Instance:' || r.instance_no::text),
         r.dataset_id, r.observed_at, r.created_at, r.modified_at, r.deleted_at,
         r.data, r.geo_value, r.version, r.provenance, r.expires_at
    FROM public.attr_rows r
    JOIN public.iri_ids a ON a.tenant_id = r.tenant_id AND a.id = r.attr_no
   WHERE r.tenant_id = p_tenant AND r.entity_no = public.iri_no(p_tenant, p_entity)
  $$;

CREATE OR REPLACE FUNCTION attr_rows_at(p_tenant text, p_entity text, p_attr text)
  RETURNS SETOF public.attr_instances LANGUAGE sql STABLE PARALLEL SAFE
  AS $$
  SELECT r.tenant_id, p_entity, p_attr,
         COALESCE(r.data->>'instanceId', 'urn:ngsi-ld:Instance:' || r.instance_no::text),
         r.dataset_id, r.observed_at, r.created_at, r.modified_at, r.deleted_at,
         r.data, r.geo_value, r.version, r.provenance, r.expires_at
    FROM public.attr_rows r
   WHERE r.tenant_id = p_tenant AND r.entity_no = public.iri_no(p_tenant, p_entity)
     AND r.attr_no = public.iri_no(p_tenant, p_attr)
  $$;

-- The daily scan of `data` for the rows the column misses (written before
-- it, or by an older release after a rollback): its own claim row, last_run
-- NULL so the first pass runs it.
INSERT INTO maintenance_jobs (name) VALUES ('history_expiry_scan') ON CONFLICT DO NOTHING;
