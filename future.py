# from __future__ import annotations

# import os
# from datetime import datetime, timezone

# import duckdb
# import psycopg2


# RAW_BUCKET = os.getenv("MINIO_BUCKET_RAW", "raw")


# def _must(name: str) -> str:
#     v = os.getenv(name)
#     if not v:
#         raise RuntimeError(f"Missing required env var: {name}")
#     return v


# def _duckdb_connect_minio() -> duckdb.DuckDBPyConnection:
#     con = duckdb.connect(database=":memory:")

#     # Enable reading from S3/MinIO
#     con.execute("INSTALL httpfs;")
#     con.execute("LOAD httpfs;")

#     # MinIO settings (S3-compatible)
#     con.execute(f"SET s3_endpoint='{_must('MINIO_ENDPOINT').replace('http://','').replace('https://','')}';")
#     con.execute(f"SET s3_access_key_id='{_must('MINIO_ROOT_USER')}';")
#     con.execute(f"SET s3_secret_access_key='{_must('MINIO_ROOT_PASSWORD')}';")
#     con.execute("SET s3_url_style='path';")
#     con.execute("SET s3_use_ssl=false;")
#     # region isn't always required for MinIO, but harmless if set
#     con.execute("SET s3_region='us-east-1';")

#     return con


# def _pg_conn_warehouse():
#     host = os.getenv("POSTGRES_WAREHOUSE_HOST", "postgres_warehouse")
#     port = int(os.getenv("POSTGRES_WAREHOUSE_PORT", "5432"))
#     user = _must("POSTGRES_WAREHOUSE_USER")
#     pw = _must("POSTGRES_WAREHOUSE_PASSWORD")
#     db = _must("POSTGRES_WAREHOUSE_DB")

#     return psycopg2.connect(host=host, port=port, user=user, password=pw, dbname=db)


# def load_bronze_via_duckdb(
#     *,
#     ingestion_run_id: int,
#     object_key: str,
#     source_sha256: str,
# ) -> dict:
#     """
#     Reads one Letterboxd CSV from MinIO using DuckDB, shapes it, then COPY loads into Postgres bronze.
#     Idempotency should be handled by your bronze.load_history check (recommended).
#     """

#     filename = object_key.split("/")[-1].lower()

#     # Route to table + select projection
#     if filename == "watchlist.csv":
#         table = "bronze.watchlist"
#         select_sql = """
#             SELECT
#               "Date" AS list_date,
#               "Name" AS name,
#               "Year" AS year,
#               "Letterboxd URI" AS letterboxd_uri
#             FROM src
#         """
#     elif filename == "watched.csv":
#         table = "bronze.watched"
#         select_sql = """
#             SELECT
#               "Date" AS list_date,
#               "Name" AS name,
#               "Year" AS year,
#               "Letterboxd URI" AS letterboxd_uri
#             FROM src
#         """
#     elif filename == "ratings.csv":
#         table = "bronze.ratings"
#         select_sql = """
#             SELECT
#               "Date" AS list_date,
#               "Name" AS name,
#               "Year" AS year,
#               "Letterboxd URI" AS letterboxd_uri,
#               "Rating" AS rating
#             FROM src
#         """
#     elif filename == "diary.csv":
#         table = "bronze.diary"
#         select_sql = """
#             SELECT
#               "Date" AS list_date,
#               "Name" AS name,
#               "Year" AS year,
#               "Letterboxd URI" AS letterboxd_uri,
#               "Rating" AS rating,
#               "Rewatch" AS rewatch,
#               "Tags" AS tags,
#               "Watched Date" AS watched_date
#             FROM src
#         """
#     else:
#         raise ValueError(f"Unsupported file type: {filename}")

#     # 1) DuckDB reads from MinIO
#     con = _duckdb_connect_minio()
#     s3_path = f"s3://{RAW_BUCKET}/{object_key}"

#     # Use read_csv_auto for resilience; DuckDB handles headers/spaces well.
#     con.execute(f"CREATE OR REPLACE TEMP VIEW src AS SELECT * FROM read_csv_auto('{s3_path}', HEADER=true);")

#     # Add lineage + row_num (window function)
#     loaded_at = datetime.now(timezone.utc).isoformat()

#     con.execute(f"""
#         CREATE OR REPLACE TEMP TABLE shaped AS
#         WITH base AS (
#           {select_sql}
#         )
#         SELECT
#           *,
#           {ingestion_run_id}::BIGINT AS ingestion_run_id,
#           '{object_key}'::VARCHAR AS source_object_key,
#           '{source_sha256}'::VARCHAR AS source_sha256,
#           ROW_NUMBER() OVER ()::INTEGER AS row_num,
#           '{loaded_at}'::TIMESTAMPTZ AS loaded_at
#         FROM base
#     """)

#     # 2) Export shaped rows to a temp CSV file inside the container
#     tmp_path = f"/tmp/bronze_{filename}_{ingestion_run_id}.csv"
#     con.execute(f"COPY shaped TO '{tmp_path}' (HEADER, DELIMITER ',');")

#     # 3) COPY into Postgres (fast bulk load)
#     pg = _pg_conn_warehouse()
#     try:
#         with pg:
#             with pg.cursor() as cur:
#                 # Use COPY FROM STDIN for speed
#                 with open(tmp_path, "r", encoding="utf-8") as f:
#                     cur.copy_expert(f"COPY {table} FROM STDIN WITH (FORMAT csv, HEADER true)", f)

#         return {"status": "ok", "table": table, "object_key": object_key}
#     finally:
#         pg.close()
#         con.close()
