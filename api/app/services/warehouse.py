import os
import psycopg2
from psycopg2.extensions import connection as PGConnection

def must(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return v

def get_warehouse_conn() -> PGConnection:
    host = os.getenv("POSTGRES_WAREHOUSE_HOST", "postgres_warehouse")
    port = int(os.getenv("POSTGRES_WAREHOUSE_PORT", "5432"))
    user = must("POSTGRES_WAREHOUSE_USER")
    password = must("POSTGRES_WAREHOUSE_PASSWORD")
    db = must("POSTGRES_WAREHOUSE_DB")

    return psycopg2.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        dbname=db,
    )
