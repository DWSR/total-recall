CREATE INDEX CONCURRENTLY memory_search_heads_context_recency_idx
  ON public.memory_search_heads (
    updated_at DESC,
    id COLLATE "C" ASC,
    version DESC
  );
