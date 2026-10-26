-- Records the exact selected canonical memory's update time on each current
-- head so the startup-context reader can order current memories by recency
-- without a lexical or vector search.
--
-- This is a forward, additive, transactional change. Apply it inside a declared
-- memory-write maintenance window with finite lock and statement limits, for
-- example:
--
--   psql --single-transaction \
--     -c lock_timeout=5s -c statement_timeout=60s \
--     -f crates/memory-store/migrations/0002_memory_search_head_recency.sql
--
-- The migration refuses to run without finite limits. Any lock or statement
-- timeout aborts the surrounding transaction, so the baseline schema and data
-- are left unchanged and memory writers can resume safely.

DO $recency_window$
BEGIN
  IF pg_catalog.current_setting('lock_timeout')::interval <= interval '0' THEN
    RAISE EXCEPTION 'memory head recency migration requires a finite lock_timeout'
      USING ERRCODE = '22023';
  END IF;
  IF pg_catalog.current_setting('statement_timeout')::interval <= interval '0' THEN
    RAISE EXCEPTION 'memory head recency migration requires a finite statement_timeout'
      USING ERRCODE = '22023';
  END IF;
END;
$recency_window$;

-- Add the recency column as nullable so the backfill can run before the
-- non-null guarantee is enforced.
ALTER TABLE public.memory_search_heads
  ADD COLUMN updated_at TIMESTAMPTZ;

-- Replace the secured head-maintenance trigger so the greatest inserted
-- version carries that canonical version's update time. Lower versions never
-- change the sole current head.
CREATE OR REPLACE FUNCTION public.memory_search_heads_after_insert()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
BEGIN
  INSERT INTO public.memory_search_heads AS memory_search_head (
    id,
    version,
    search_document,
    updated_at
  )
  VALUES (
    NEW.id,
    NEW.version,
    ARRAY[NEW.title, NEW.content] || NEW.concepts,
    NEW.updated_at
  )
  ON CONFLICT (id) DO UPDATE
  SET
    version = EXCLUDED.version,
    search_document = EXCLUDED.search_document,
    updated_at = EXCLUDED.updated_at
  WHERE memory_search_head.version < EXCLUDED.version;

  RETURN NULL;
END;
$$;

-- CREATE OR REPLACE preserves ownership and the existing ACL, but re-issue the
-- denials so the secured contract is explicit and drift-proof.
REVOKE ALL ON FUNCTION public.memory_search_heads_after_insert() FROM PUBLIC;
REVOKE ALL ON FUNCTION public.memory_search_heads_after_insert() FROM memory_application;

-- Backfill the exact canonical row selected by each head's (id, version).
UPDATE public.memory_search_heads AS memory_search_head
SET updated_at = memory.updated_at
FROM public.memories AS memory
WHERE memory.id = memory_search_head.id
  AND memory.version = memory_search_head.version;

-- Validate non-null without a separate full-table ACCESS EXCLUSIVE scan, then
-- promote the column to NOT NULL. The transient check is dropped afterwards
-- because the catalog column flag is now authoritative.
ALTER TABLE public.memory_search_heads
  ADD CONSTRAINT memory_search_heads_updated_at_not_null
  CHECK (updated_at IS NOT NULL) NOT VALID;

ALTER TABLE public.memory_search_heads
  VALIDATE CONSTRAINT memory_search_heads_updated_at_not_null;

ALTER TABLE public.memory_search_heads
  ALTER COLUMN updated_at SET NOT NULL;

ALTER TABLE public.memory_search_heads
  DROP CONSTRAINT memory_search_heads_updated_at_not_null;
