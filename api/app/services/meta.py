import os
import psycopg

def meta_conn():
    return psycopg.connect(
        host=os.getenv("POSTGRES_META_HOST", "postgres_meta"),
        port=int(os.getenv("POSTGRES_META_PORT", "5432")),
        dbname=os.getenv("POSTGRES_META_DB"),
        user=os.getenv("POSTGRES_META_USER"),
        password=os.getenv("POSTGRES_META_PASSWORD"),
    )
