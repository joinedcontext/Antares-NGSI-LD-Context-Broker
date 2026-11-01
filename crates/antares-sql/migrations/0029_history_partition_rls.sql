-- History partitions wear their parent's belt.
--
-- PostgreSQL applies a partitioned parent's row security to the statements
-- that name the parent; a statement that names a partition is judged by the
-- partition's own, and attr_instances_default (0001), attr_rows_default
-- (0020) and the weekly partitions the maintenance job creates had none: a
-- statement naming one, mistaken or injected, read and wrote every tenant's
-- history. attr_rows_long and attr_rows_short already carry 0005's policies
-- (0020, 0023).
--
-- history_partition_rls gives one partition 0005's per-command policies,
-- ENABLE and FORCE, once; this migration runs it over every partition of
-- the policed history parents, and the maintenance job over each week it
-- creates or adopts (store/pg/maintenance.rs). Statements that name the
-- parent are unchanged: PostgreSQL applies the parent's policies there and
-- never a partition's. Under timescale the history tables carry no RLS
-- (ADR-0006) and their chunks are no partitions, so nothing is found.
CREATE FUNCTION history_partition_rls(part regclass) RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
  IF (SELECT relrowsecurity FROM pg_class WHERE oid = part) THEN
    RETURN;
  END IF;
  EXECUTE format('ALTER TABLE %s ENABLE ROW LEVEL SECURITY', part);
  EXECUTE format('ALTER TABLE %s FORCE ROW LEVEL SECURITY', part);
  EXECUTE format('CREATE POLICY tenant_read ON %s FOR SELECT
             USING (tenant_id = current_setting(''antares.tenant'', true)
                    OR current_setting(''antares.service'', true) = ''on'')', part);
  EXECUTE format('CREATE POLICY tenant_reap ON %s FOR DELETE
             USING (tenant_id = current_setting(''antares.tenant'', true)
                    OR current_setting(''antares.service'', true) = ''on'')', part);
  EXECUTE format('CREATE POLICY tenant_write ON %s FOR INSERT
             WITH CHECK (tenant_id = current_setting(''antares.tenant'', true))', part);
  EXECUTE format('CREATE POLICY tenant_update ON %s FOR UPDATE
             USING (tenant_id = current_setting(''antares.tenant'', true))
             WITH CHECK (tenant_id = current_setting(''antares.tenant'', true))', part);
END $$;

DO $$
DECLARE part regclass;
BEGIN
  FOR part IN
    SELECT i.inhrelid::regclass FROM pg_inherits i
      JOIN pg_class c ON c.oid = i.inhrelid
      JOIN pg_class p ON p.oid = i.inhparent
      JOIN pg_namespace n ON n.oid = p.relnamespace
     WHERE n.nspname = 'public' AND c.relispartition AND p.relrowsecurity
       AND p.relname IN ('attr_instances', 'attr_rows', 'attr_rows_long')
  LOOP
    PERFORM history_partition_rls(part);
  END LOOP;
END $$;
