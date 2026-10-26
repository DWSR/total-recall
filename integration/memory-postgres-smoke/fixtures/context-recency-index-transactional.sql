-- Negative fixture: the pre-task implementation of the context recency index.
--
-- This plain build is transaction-compatible, so it silently succeeds inside
-- the transactional wrapper that the verifier applies. Supplying this file as
-- MEMORY_CONTEXT_INDEX_MIGRATION must make the verifier fail with:
--
--   expected transaction-wrapped concurrent index rejection
--
-- The real migration must reject that mode instead.
CREATE INDEX memory_search_heads_context_recency_idx
ON public.memory_search_heads
USING btree (
  updated_at DESC,
  id COLLATE "C" ASC,
  version DESC
);
