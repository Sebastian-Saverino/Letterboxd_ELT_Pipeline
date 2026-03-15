# app/main.py
from __future__ import annotations

import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from botocore.exceptions import BotoCoreError, ClientError
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy import text

from app.db.session import engine
from app.routes.ingest import router as ingest_router
from app.services.minio_client import s3_client


def must(name: str) -> str:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def check_minio_ready() -> Dict[str, str]:
    try:
        bucket = must("MINIO_BUCKET_RAW")
        s3_client().head_bucket(Bucket=bucket)
    except (BotoCoreError, ClientError, RuntimeError) as exc:
        return {"status": "error", "detail": str(exc)}
    return {"status": "ok", "detail": f"bucket '{bucket}' reachable"}


def check_postgres_meta_ready() -> Dict[str, str]:
    try:
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
    except Exception as exc:
        return {"status": "error", "detail": str(exc)}
    return {"status": "ok", "detail": "metadata database reachable"}


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup/shutdown hook."""
    app.state.started_at = now_utc_iso()
    yield


app = FastAPI(
    title="Letterboxd Data Pipeline API",
    version="0.1.0",
    lifespan=lifespan,
)

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"

app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost",
        "http://localhost:3000",
        "http://127.0.0.1",
        "http://127.0.0.1:3000",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(ingest_router)


@app.get("/", tags=["frontend"])
async def serve_home():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/health", tags=["health"])
def health() -> Dict[str, Any]:
    """Liveness probe: the process is running."""
    return {"status": "ok", "time_utc": now_utc_iso()}


@app.get("/health/ready", tags=["health"], response_model=None)
def readiness():
    dependencies = {
        "minio": check_minio_ready(),
        "postgres_meta": check_postgres_meta_ready(),
    }
    ready = all(dependency["status"] == "ok" for dependency in dependencies.values())
    payload = {
        "status": "ok" if ready else "not_ready",
        "dependencies": dependencies,
        "time_utc": now_utc_iso(),
    }
    if not ready:
        return JSONResponse(status_code=503, content=payload)
    return payload


@app.get("/debug/env", tags=["debug"])
def debug_env() -> Dict[str, Optional[str]]:
    """
    WARNING: do not expose in prod.
    This intentionally does NOT return secrets. It just helps you confirm wiring.
    """

    def safe(name: str) -> Optional[str]:
        value = os.getenv(name)
        return None if value is None or value.strip() == "" else value

    return {
        "API_ENV": safe("API_ENV"),
        "MINIO_BUCKET_RAW": safe("MINIO_BUCKET_RAW"),
        "MINIO_HOST": safe("MINIO_HOST"),
        "MINIO_PORT": safe("MINIO_PORT"),
        "POSTGRES_META_HOST": safe("POSTGRES_META_HOST"),
        "POSTGRES_WAREHOUSE_HOST": safe("POSTGRES_WAREHOUSE_HOST"),
        "time_utc": now_utc_iso(),
    }
