CREATE TABLE session_embeddings (
    receipt_id UUID PRIMARY KEY DEFAULT uuid_generate_v7(),
    session_id TEXT NOT NULL,
    model_id TEXT NOT NULL,
    dimensions INTEGER NOT NULL CHECK (dimensions > 0),
    embedding vector NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT statement_timestamp(),
    CHECK (vector_dims(embedding) = dimensions)
);
