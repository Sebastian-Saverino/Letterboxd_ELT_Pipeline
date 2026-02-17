# api/app/services/meta.py

import os
from typing import Optional
from uuid import UUID

import psycopg2


def _must(name: str) -> str:
    val = os.getenv(name)
    if val is None or val == "":
        raise RuntimeError(f"Missing required environment variable: {name}")
    return val


def meta_conn():
    """
    Returns a live Postgres connection to the metadata DB.
    Inside Docker: POSTGRES_META_HOST should be 'postgres-meta' (service name).
    """
    host = os.getenv("POSTGRES_META_HOST", "postgres-meta")
    port = os.getenv("POSTGRES_META_PORT", "5432")
    user = _must("POSTGRES_META_USER")
    pwd = _must("POSTGRES_META_PASSWORD")
    db = _must("POSTGRES_META_DB")

    dsn = f"postgresql://{user}:{pwd}@{host}:{port}/{db}"
    return psycopg2.connect(dsn)


def upsert_ingestion_run(
    *,
    ingestion_id: UUID,
    source: str,
    original_filename: str,
    bucket: str,
    object_key: str,
    size_bytes: int,
    content_type: Optional[str],
    status: str = "uploaded",
) -> UUID:
    """
    Inserts one row into public.ingestion_runs.
    If the ingestion_id somehow collides (unlikely), it no-ops and returns the id.
    """
    sql = """
    INSERT INTO public.ingestion_runs (
      ingestion_id,
      source,
      original_filename,
      bucket,
      object_key,
      size_bytes,
      content_type,
      status
    )
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (ingestion_id) DO NOTHING
    RETURNING ingestion_id;
    """

    with meta_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                (
                    str(ingestion_id),
                    source,
                    original_filename,
                    bucket,
                    object_key,
                    size_bytes,
                    content_type,
                    status,
                ),
            )
            row = cur.fetchone()
        conn.commit()

    # If conflict occurred, RETURNING returns nothing; we still return the original ID.
    return UUID(row[0]) if row else ingestion_id












# import os
# from uuid import UUID
# from typing import Optional, Dict, Any

# import psycopg2

# def meta_conn():
#     host = os.getenv("POSTGRES_META_HOST", "postgres-meta")
#     port = os.getenv("POSTGRES_META_PORT", "5432")
#     user = os.getenv("POSTGRES_META_USER")
#     pwd  = os.getenv("POSTGRES_META_PASSWORD")
#     db   = os.getenv("POSTGRES_META_DB")

#     dsn = f"postgresql://{user}:{pwd}@{host}:{port}/{db}"
#     return psycopg2.connect(dsn)

# UPSERT_INGESTION_RUN = """
# INSERT INTO ingestion_runs (
#   ingestion_id, source, original_filename,
#   bucket, object_key, size_bytes, content_type, status
# )
# VALUES (
#   %(ingestion_id)s, %(source)s, %(original_filename)s,
#   %(bucket)s, %(object_key)s, %(size_bytes)s, %(content_type)s, %(status)s
# )
# ON CONFLICT (bucket, object_key)
# DO UPDATE SET
#   source = EXCLUDED.source,
#   original_filename = EXCLUDED.original_filename,
#   size_bytes = EXCLUDED.size_bytes,
#   content_type = EXCLUDED.content_type,
#   status = EXCLUDED.status
# RETURNING ingestion_id, created_at, updated_at;
# """

# def upsert_ingestion_run(
#     *,
#     ingestion_id: UUID,
#     source: str,
#     original_filename: str,
#     bucket: str,
#     object_key: str,
#     size_bytes: int,
#     content_type: Optional[str],
#     status: str = "uploaded",
# ) -> Dict[str, Any]:
#     payload = {
#         "ingestion_id": str(ingestion_id),
#         "source": source,
#         "original_filename": original_filename,
#         "bucket": bucket,
#         "object_key": object_key,
#         "size_bytes": size_bytes,
#         "content_type": content_type,
#         "status": status,
#     }

#     with psycopg2.connect(_meta_dsn()) as conn:
#         with conn.cursor() as cur:
#             cur.execute(UPSERT_INGESTION_RUN, payload)
#             ingestion_id, created_at, updated_at = cur.fetchone()
#         conn.commit()

#     return {
#         "ingestion_id": str(ingestion_id),
#         "created_at": created_at.isoformat(),
#         "updated_at": updated_at.isoformat(),
#     }
