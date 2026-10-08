CREATE TABLE public.memories (
  id TEXT NOT NULL,
  version BIGINT NOT NULL,
  type TEXT NOT NULL,
  title TEXT NOT NULL,
  content TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL,
  concepts TEXT[] NOT NULL,
  files TEXT[] NOT NULL,
  session_ids TEXT[] NOT NULL,
  source_observation_ids TEXT[] NOT NULL,
  CONSTRAINT memories_pkey PRIMARY KEY (id, version),
  CONSTRAINT memories_id_not_empty CHECK (id <> ''),
  CONSTRAINT memories_version_positive CHECK (version > 0),
  CONSTRAINT memories_type_not_empty CHECK (type <> ''),
  CONSTRAINT memories_title_not_empty CHECK (title <> ''),
  CONSTRAINT memories_content_not_empty CHECK (content <> ''),
  CONSTRAINT memories_created_at_finite CHECK (isfinite(created_at)),
  CONSTRAINT memories_updated_at_finite CHECK (isfinite(updated_at)),
  CONSTRAINT memories_updated_at_not_before_created_at CHECK (updated_at >= created_at)
);

CREATE TABLE public.memory_embeddings (
  id TEXT NOT NULL,
  version BIGINT NOT NULL,
  embedding vector NOT NULL,
  CONSTRAINT memory_embeddings_pkey PRIMARY KEY (id, version),
  CONSTRAINT memory_embeddings_memory_fk
    FOREIGN KEY (id, version)
    REFERENCES public.memories (id, version)
    ON UPDATE RESTRICT
    ON DELETE RESTRICT
    NOT DEFERRABLE,
  CONSTRAINT memory_embeddings_embedding_dimensions_positive
    CHECK (vector_dims(embedding) > 0),
  CONSTRAINT memory_embeddings_embedding_norm_positive
    CHECK (vector_norm(embedding) > 0)
);

CREATE TABLE public.memory_search_heads (
  id TEXT NOT NULL,
  version BIGINT NOT NULL,
  search_document TEXT[] NOT NULL,
  CONSTRAINT memory_search_heads_pkey PRIMARY KEY (id),
  CONSTRAINT memory_search_heads_memory_fk
    FOREIGN KEY (id, version)
    REFERENCES public.memories (id, version)
    ON UPDATE RESTRICT
    ON DELETE RESTRICT
    NOT DEFERRABLE
);

CREATE FUNCTION public.memory_search_heads_after_insert()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
BEGIN
  INSERT INTO public.memory_search_heads AS memory_search_head (
    id,
    version,
    search_document
  )
  VALUES (
    NEW.id,
    NEW.version,
    ARRAY[NEW.title, NEW.content] || NEW.concepts
  )
  ON CONFLICT (id) DO UPDATE
  SET
    version = EXCLUDED.version,
    search_document = EXCLUDED.search_document
  WHERE memory_search_head.version < EXCLUDED.version;

  RETURN NULL;
END;
$$;

REVOKE ALL ON FUNCTION public.memory_search_heads_after_insert() FROM PUBLIC;
REVOKE ALL ON FUNCTION public.memory_search_heads_after_insert() FROM memory_application;

CREATE TRIGGER memory_search_heads_after_insert
AFTER INSERT ON public.memories
FOR EACH ROW
EXECUTE FUNCTION public.memory_search_heads_after_insert();

CREATE INDEX memory_search_heads_search_document_bm25_idx
ON public.memory_search_heads
USING bm25 (search_document) WITH (text_config = 'english');

REVOKE ALL ON TABLE public.memories, public.memory_embeddings, public.memory_search_heads FROM PUBLIC;
REVOKE ALL ON TABLE public.memories, public.memory_embeddings, public.memory_search_heads FROM memory_application;
GRANT SELECT, INSERT ON TABLE public.memories, public.memory_embeddings TO memory_application;
GRANT SELECT ON TABLE public.memory_search_heads TO memory_application;
