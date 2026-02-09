import hashlib
import os
from datetime import datetime, timezone

from fastapi import APIRouter, File, UploadFile, HTTPException

from app.services.minio_client import s3_client
from app.services.meta import meta_conn

router = APIRouter(prefix="/ingest", tags=["ingest"])

RAW_BUCKET = os.getenv("MINIO_BUCKET_RAW", "raw")

@router.post("/letterboxd/upload")
async def upload_letterboxd_csv(file: UploadFile = File(...)):
    # Basic validation
    if not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only .csv files are supported")

    # Read bytes (v1: in-memory; later we’ll stream)
    data = await file.read()
    size_bytes = len(data)
    if size_bytes == 0:
        raise HTTPException(status_code=400, detail="Empty file")

    sha256 = hashlib.sha256(data).hexdigest()

    # Build an object key
    ts = datetime.now(timezone.utc).strftime("%Y/%m/%d/%H%M%S")
    safe_name = file.filename.replace(" ", "_")
    object_key = f"letterboxd/{ts}_{sha256[:12]}_{safe_name}"

    # Upload to MinIO
    s3 = s3_client()
    s3.put_object(
        Bucket=RAW_BUCKET,
        Key=object_key,
        Body=data,
        ContentType=file.content_type or "text/csv",
    )

    # Write metadata row to Postgres meta
    with meta_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ingest_runs (source, bucket, object_key, original_name, content_type, size_bytes, sha256)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING id;
                """,
                (
                    "letterboxd_csv",
                    RAW_BUCKET,
                    object_key,
                    file.filename,
                    file.content_type,
                    size_bytes,
                    sha256,
                ),
            )
            ingest_id = cur.fetchone()[0]
        conn.commit()

    return {
        "status": "ok",
        "ingest_id": ingest_id,
        "bucket": RAW_BUCKET,
        "object_key": object_key,
        "size_bytes": size_bytes,
        "sha256": sha256,
    }
