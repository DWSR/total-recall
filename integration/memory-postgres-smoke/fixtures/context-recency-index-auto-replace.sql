-- Negative fixture: a pre-task implementation that repairs a failed concurrent
-- build by silently replacing the invalid index.
--
-- The verifier simulates an invalid index and then reruns the migration. A
-- migration that drops the invalid index first succeeds, so supplying this file
-- as MEMORY_CONTEXT_INDEX_MIGRATION must make the verifier fail with:
--
--   expected invalid-index collision rejection
--
-- The real migration must leave the invalid index in place and require the
-- documented explicit DROP INDEX CONCURRENTLY recovery step instead.
DROP INDEX CONCURRENTLY IF EXISTS memory_search_heads_context_recency_idx;
CREATE INDEX CONCURRENTLY memory_search_heads_context_recency_idx
ON public.memory_search_heads
USING btree (
  updated_at DESC,
  id COLLATE "C" ASC,
  version DESC
);
