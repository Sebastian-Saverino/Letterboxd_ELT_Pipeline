from __future__ import annotations

import csv
import io
import os
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Tuple

import psycopg2
from psycopg2.extras import execute_values

from app.services.minio_client import s3_client


RAW_BUCKET = os.getenv("MINIO_BUCKET_RAW", "raw")


# -------------------------
# Dataset routing + mapping
# -------------------------

@dataclass(frozen=True)
class BronzeSpec:
    table: str
    # Map from *CSV header* -> *bronze column name*
    column_map: Dict[str, str]


SPECS: Dict[str, BronzeSpec] = {
    "watchlist.csv": BronzeSpec(
        table="bronze.watchlist",
        column_map={
            "Date": "list_date",
            "Name": "name",
            "Year": "year",
            "Letterboxd URI": "letterboxd_uri",
        },
    ),
    "watched.csv": BronzeSpec(
        table="bronze.watched",
        column_map={
            "Date": "list_date",
            "Name": "name",
            "Year": "year",
            "Letterboxd URI": "letterboxd_uri",
        },
    ),
    "ratings.csv": BronzeSpec(
        table="bronze.ratings",
        column_map={
            "Date": "list_date",
            "Name": "name",
            "Year": "year",
            "Letterboxd URI": "letterboxd_uri",
            "Rating": "rating",
        },
    ),
    "diary.csv": BronzeSpec(
        table="bronze.diary",
        column_map={
            "Date": "list_date",
            "Name": "name",
            "Year": "year",
            "Letterboxd URI": "letterboxd_uri",
            "Rating": "rating",
            "Rewatch": "rewatch",
            "Tags": "tags",
            "Watched Date": "watched_date",
        },
    ),
}


def _infer_spec(object_key: str) -> BronzeSpec:
    """
    Uses the filename at the end of the key to pick the correct Bronze table.
    Example keys:
      raw/2026/02/15/235959/watchlist.csv
      letterboxd/2026/02/15/.../diary.csv
    """
    filename = object_key.split("/")[-1].lower()
    spec = SPECS.get(filename)
    if not spec:
        raise ValueError(f"Unsupported file for bronze load: {filename} (key={object_key})")
    return spec


# -------------------------
# Idempotency (load_history)
# -------------------------

def _already_loaded(cur: psycopg2.extensions.cursor, source_sha256: str) -> bool:
    cur.execute(
        "SELECT 1 FROM bronze.load_history WHERE source_sha256 = %s LIMIT 1",
        (source_sha256,),
    )
    return cur.fetchone() is not None


def _mark_loaded(
    cur: psycopg2.extensions.cursor,
    *,
    ingestion_run_id: int,
    source_object_key: str,
    source_sha256: str,
) -> None:
    cur.execute(
        """
        INSERT INTO bronze.load_history (source_sha256, source_object_key, ingestion_run_id)
        VALUES (%s, %s, %s)
        ON CONFLICT (source_sha256) DO NOTHING
        """,
        (source_sha256, source_object_key, ingestion_run_id),
    )


# -------------------------
# CSV parsing
# -------------------------

def _download_minio_object(bucket: str, object_key: str) -> bytes:
    resp = s3_client.get_object(Bucket=bucket, Key=object_key)
    body = resp["Body"].read()
    return body


def _read_csv_rows(data: bytes) -> Tuple[List[str], List[Dict[str, str]]]:
    """
    Returns (fieldnames, rows) with original CSV headers preserved.
    """
    text = data.decode("utf-8-sig")  # handles BOM if present
    f = io.StringIO(text)
    reader = csv.DictReader(f)
    if reader.fieldnames is None:
        raise ValueError("CSV missing headers")
    rows = [r for r in reader]
    return list(reader.fieldnames), rows


# -------------------------
# Main load function
# -------------------------

def load_object_to_bronze(
    *,
    warehouse_conn: psycopg2.extensions.connection,
    ingestion_run_id: int,
    source_object_key: str,
    source_sha256: str,
    bucket: str = RAW_BUCKET,
    batch_size: int = 5_000,
) -> Dict[str, Any]:
    """
    Loads ONE MinIO CSV object into the appropriate bronze table.
    Adds lineage columns and is safe to rerun via bronze.load_history.

    Returns a small status dict for logging/observability.
    """
    spec = _infer_spec(source_object_key)

    with warehouse_conn:
        with warehouse_conn.cursor() as cur:
            if _already_loaded(cur, source_sha256):
                return {
                    "status": "skipped",
                    "reason": "already_loaded",
                    "table": spec.table,
                    "object_key": source_object_key,
                    "sha256": source_sha256,
                    "rows_inserted": 0,
                }

            data = _download_minio_object(bucket, source_object_key)
            headers, raw_rows = _read_csv_rows(data)

            # Validate headers contain what we expect
            missing = [h for h in spec.column_map.keys() if h not in headers]
            if missing:
                raise ValueError(
                    f"CSV headers missing expected columns: {missing}. "
                    f"Found headers: {headers}"
                )

            # Build rows for insertion in correct column order
            bronze_cols = list(spec.column_map.values()) + [
                "ingestion_run_id",
                "source_object_key",
                "source_sha256",
                "row_num",
            ]

            values: List[Tuple[Any, ...]] = []
            for idx, r in enumerate(raw_rows, start=1):
                mapped = []
                for csv_col, bronze_col in spec.column_map.items():
                    v = r.get(csv_col)
                    # Normalize empty strings -> None (keeps Postgres cleaner)
                    if v is not None:
                        v = v.strip()
                        if v == "":
                            v = None
                    mapped.append(v)

                mapped.extend([ingestion_run_id, source_object_key, source_sha256, idx])
                values.append(tuple(mapped))

            if not values:
                # empty file (besides header) — still mark loaded so we don’t keep retrying
                _mark_loaded(
                    cur,
                    ingestion_run_id=ingestion_run_id,
                    source_object_key=source_object_key,
                    source_sha256=source_sha256,
                )
                return {
                    "status": "ok",
                    "table": spec.table,
                    "object_key": source_object_key,
                    "sha256": source_sha256,
                    "rows_inserted": 0,
                    "note": "no_data_rows",
                }

            insert_sql = f"INSERT INTO {spec.table} ({', '.join(bronze_cols)}) VALUES %s"

            # Insert in chunks (safer on memory + faster than row-by-row)
            total = 0
            for start in range(0, len(values), batch_size):
                chunk = values[start : start + batch_size]
                execute_values(cur, insert_sql, chunk, page_size=len(chunk))
                total += len(chunk)

            _mark_loaded(
                cur,
                ingestion_run_id=ingestion_run_id,
                source_object_key=source_object_key,
                source_sha256=source_sha256,
            )

            return {
                "status": "ok",
                "table": spec.table,
                "object_key": source_object_key,
                "sha256": source_sha256,
                "rows_inserted": total,
            }
