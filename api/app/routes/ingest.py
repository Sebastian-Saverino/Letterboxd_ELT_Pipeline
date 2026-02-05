from fastapi import APIRouter, UploadFile, File, HTTPException, Query
from uuid import uuid4
from datetime import datetime, timezone

import boto3
from botocore.client import Config as BotoConfig
from sqlalchemy import text

from app.core import config
from app.db.session import SessionLocal

router = APIRouter(prefix="/ingestions", tags=["ingestions"])


def s3_client():
    scheme = "https" if config.MINIO_SECURE else "http"
    endpoint_url = f"{scheme}://{config.MINIO_ENDPOINT}"
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=config.MINIO_ACCESS_KEY,
        aws_secret_access_key=config.MINIO_SECRET_KEY,
        config=BotoConfig(signature_version="s3v4"),
        region_name="us-east-1",
    )


@router.post("/letterboxd")
async def ingest_letterboxd(file: UploadFile = File(...)):
    if not file.filename:
        raise HTTPException(status_code=400, detail="Missing filename")

    ingestion_id = uuid4()
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    object_key = f"letterboxd/{ts}_{ingestion_id}_{file.filename}"

    data = await file.read()
    size_bytes = len(data)
    content_type = file.content_type or "application/octet-stream"

    # Upload to MinIO
    try:
        s3 = s3_client()
        s3.put_object(
            Bucket=config.MINIO_BUCKET_RAW,
            Key=object_key,
            Body=data,
            ContentType=content_type,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"MinIO upload failed: {e}")

    # Insert metadata
    db = SessionLocal()
    try:
        db.execute(
            text("""
                INSERT INTO ingestion_runs
                (ingestion_id, source, original_filename, bucket, object_key,
                 size_bytes, content_type, status)
                VALUES
                (:ingestion_id, 'letterboxd', :filename, :bucket, :object_key,
                 :size_bytes, :content_type, 'uploaded')
            """),
            {
                "ingestion_id": str(ingestion_id),
                "filename": file.filename,
                "bucket": config.MINIO_BUCKET_RAW,
                "object_key": object_key,
                "size_bytes": size_bytes,
                "content_type": content_type,
            },
        )
        db.commit()
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Metadata insert failed: {e}")
    finally:
        db.close()

    return {
        "ingestion_id": str(ingestion_id),
        "status": "uploaded",
        "bucket": config.MINIO_BUCKET_RAW,
        "object_key": object_key,
        "size_bytes": size_bytes,
    }


@router.get("")
def list_ingestions(
    status: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
):
    db = SessionLocal()
    try:
        if status:
            rows = db.execute(
                text("""
                    SELECT *
                    FROM ingestion_runs
                    WHERE status = :status
                    ORDER BY created_at DESC
                    LIMIT :limit
                """),
                {"status": status, "limit": limit},
            ).mappings().all()
        else:
            rows = db.execute(
                text("""
                    SELECT *
                    FROM ingestion_runs
                    ORDER BY created_at DESC
                    LIMIT :limit
                """),
                {"limit": limit},
            ).mappings().all()

        return {"count": len(rows), "items": rows}
    finally:
        db.close()
