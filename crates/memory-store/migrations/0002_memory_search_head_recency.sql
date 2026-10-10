SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE public.memory_search_heads
  ADD COLUMN updated_at TIMESTAMPTZ;

ALTER TABLE public.memory_search_heads
  ADD CONSTRAINT memory_search_heads_updated_at_present
  CHECK (updated_at IS NOT NULL) NOT VALID;

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

REVOKE ALL ON FUNCTION public.memory_search_heads_after_insert() FROM PUBLIC;
REVOKE ALL ON FUNCTION public.memory_search_heads_after_insert() FROM memory_application;

UPDATE public.memory_search_heads AS memory_search_head
SET updated_at = memory.updated_at
FROM public.memories AS memory
WHERE memory.id = memory_search_head.id
  AND memory.version = memory_search_head.version;

ALTER TABLE public.memory_search_heads
  VALIDATE CONSTRAINT memory_search_heads_updated_at_present;

ALTER TABLE public.memory_search_heads
  ALTER COLUMN updated_at SET NOT NULL;

ALTER TABLE public.memory_search_heads
  DROP CONSTRAINT memory_search_heads_updated_at_present;
