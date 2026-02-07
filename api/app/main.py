# app/main.py
from __future__ import annotations

import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware


def must(name: str) -> str:
    v = os.getenv(name)
    if v is None or v.strip() == "":
        raise RuntimeError(f"Missing required environment variable: {name}")
    return v


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Startup/Shutdown hook.
    Keep this lean in v1. We'll add:
      - MinIO connectivity check
      - Bucket ensure
      - DB connectivity check / migrations
    when we wire dependencies.
    """
    app.state.started_at = now_utc_iso()
    yield
    # shutdown hooks go here


app = FastAPI(
    title="Letterboxd Data Pipeline API",
    version="0.1.0",
    lifespan=lifespan,
)

# ---- CORS (safe defaults for local dev; tighten later) ----
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


# ---- Basic routes ----
@app.get("/", tags=["root"])
def root() -> Dict[str, Any]:
    return {
        "service": "letterboxd-data-pipeline-api",
        "status": "ok",
        "started_at": getattr(app.state, "started_at", None),
        "time_utc": now_utc_iso(),
    }


@app.get("/health", tags=["health"])
def health() -> Dict[str, Any]:
    """
    Liveness probe: tells Docker / a load balancer 'the process is alive'.
    We'll add readiness checks (MinIO/DB) later.
    """
    return {"status": "ok", "time_utc": now_utc_iso()}


@app.get("/health/ready", tags=["health"])
def readiness() -> Dict[str, Any]:
    """
    Readiness probe: tells you whether dependencies are ready.
    For now it's a placeholder; we’ll wire MinIO + Postgres checks next.
    """
    return {
        "status": "ok",
        "dependencies": {
            "minio": "unchecked",
            "postgres_meta": "unchecked",
            "postgres_warehouse": "unchecked",
        },
        "time_utc": now_utc_iso(),
    }


# ---- Example: config sanity endpoint (optional but useful) ----
@app.get("/debug/env", tags=["debug"])
def debug_env() -> Dict[str, Optional[str]]:
    """
    WARNING: do not expose in prod.
    This intentionally does NOT return secrets. It just helps you confirm wiring.
    """
    def safe(name: str) -> Optional[str]:
        v = os.getenv(name)
        return None if v is None or v.strip() == "" else v

    return {
        "API_ENV": safe("API_ENV"),
        "MINIO_BUCKET_RAW": safe("MINIO_BUCKET_RAW"),
        "MINIO_HOST": safe("MINIO_HOST"),
        "MINIO_PORT": safe("MINIO_PORT"),
        "POSTGRES_META_HOST": safe("POSTGRES_META_HOST"),
        "POSTGRES_WAREHOUSE_HOST": safe("POSTGRES_WAREHOUSE_HOST"),
        "time_utc": now_utc_iso(),
    }


# ---- Optional: a placeholder route for your ingestion step ----
@app.post("/ingest/letterboxd", tags=["ingest"])
def ingest_letterboxd(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Placeholder ingestion endpoint.
    Next, we’ll accept:
      - a CSV upload (multipart/form-data)
      - or a URL to fetch
      - then write to MinIO RAW bucket
      - then write metadata row to Postgres Meta DB
    """
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Payload must be a JSON object")
    return {
        "status": "received",
        "keys": list(payload.keys()),
        "time_utc": now_utc_iso(),
    }
