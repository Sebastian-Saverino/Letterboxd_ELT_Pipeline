from __future__ import annotations

from typing import Optional
from uuid import UUID

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from app.db.models import IngestionRun


def upsert_ingestion_run(
    session: Session,
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
    stmt = (
        insert(IngestionRun)
        .values(
            ingestion_id=ingestion_id,
            source=source,
            original_filename=original_filename,
            bucket=bucket,
            object_key=object_key,
            size_bytes=size_bytes,
            content_type=content_type,
            status=status,
        )
        .on_conflict_do_nothing(index_elements=[IngestionRun.ingestion_id])
        .returning(IngestionRun.ingestion_id)
    )

    inserted_id = session.execute(stmt).scalar_one_or_none()
    return inserted_id or ingestion_id
