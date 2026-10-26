-- Build the ordered access path used by the startup-context reader.
--
-- This is a standalone, non-transactional migration. Apply it after 0002 has
-- committed and memory writers have resumed:
--
--   psql --no-psqlrc --no-password --set=ON_ERROR_STOP=1 \
--     -f crates/memory-store/migrations/0003_memory_context_recency_index.sql
--
-- Do not add --single-transaction or wrap this file in BEGIN/COMMIT:
-- PostgreSQL refuses CREATE INDEX CONCURRENTLY inside a transaction block.
--
-- A cancelled or failed concurrent build leaves an invalid index behind.
-- Recover explicitly by dropping only that invalid index concurrently, then
-- rerunning this same file:
--
--   DROP INDEX CONCURRENTLY IF EXISTS
--     public.memory_search_heads_context_recency_idx;
--
-- The migration intentionally does not drop an existing index itself: a valid
-- index must stay available to readers, and silently replacing one would hide
-- the operator-visible recovery step. The transactional 0002 path remains
-- unchanged and independent of this build.

CREATE INDEX CONCURRENTLY memory_search_heads_context_recency_idx
ON public.memory_search_heads
USING btree (
  updated_at DESC,
  id COLLATE "C" ASC,
  version DESC
);
