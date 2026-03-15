CREATE TABLE IF NOT EXISTS public.ingestion_runs (
    ingestion_id UUID PRIMARY KEY,
    source TEXT NOT NULL,
    original_filename TEXT NOT NULL,
    bucket TEXT NOT NULL,
    object_key TEXT NOT NULL,
    size_bytes BIGINT NOT NULL CHECK (size_bytes >= 0),
    content_type TEXT,
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT ingestion_runs_bucket_object_key_key UNIQUE (bucket, object_key)
);

CREATE INDEX IF NOT EXISTS idx_ingestion_runs_created_at
    ON public.ingestion_runs (created_at DESC);

CREATE INDEX IF NOT EXISTS idx_ingestion_runs_status
    ON public.ingestion_runs (status);
