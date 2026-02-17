# import os


# def must(name: str) -> str:
#     value = os.getenv(name)
#     if value is None or value == "":
#         raise RuntimeError(f"Missing required environment variable: {name}")
#     return value


# # =========================
# # FastAPI App (.env keys: API_HOST, API_PORT, API_ENV)
# # =========================

# API_HOST = must("API_HOST")
# API_PORT = int(must("API_PORT"))
# API_ENV = must("API_ENV")


# # =========================
# # MinIO (Object Storage) (.env keys: MINIO_ROOT_USER, MINIO_ROOT_PASSWORD, MINIO_BUCKET_RAW)
# # =========================
# # NOTE: Your .env does NOT define MINIO_ENDPOINT or MINIO_SECURE, so we set sane defaults here.

# MINIO_ENDPOINT = "minio:9000"  # docker-compose service name + port
# MINIO_SECURE = False

# MINIO_ACCESS_KEY = must("MINIO_ROOT_USER")
# MINIO_SECRET_KEY = must("MINIO_ROOT_PASSWORD")
# MINIO_BUCKET_RAW = must("MINIO_BUCKET_RAW")


# # =========================
# # Postgres - Metadata DB (.env keys: POSTGRES_META_USER, POSTGRES_META_PASSWORD, POSTGRES_META_DB, POSTGRES_META_PORT)
# # =========================
# # NOTE: Your .env does NOT define POSTGRES_META_HOST, so we default to the compose service name.

# POSTGRES_META_HOST = "postgres_meta"
# POSTGRES_META_PORT = int(must("POSTGRES_META_PORT"))
# POSTGRES_META_DB = must("POSTGRES_META_DB")
# POSTGRES_META_USER = must("POSTGRES_META_USER")
# POSTGRES_META_PASSWORD = must("POSTGRES_META_PASSWORD")


# # =========================
# # Postgres - Warehouse DB (.env keys: POSTGRES_WAREHOUSE_USER, POSTGRES_WAREHOUSE_PASSWORD, POSTGRES_WAREHOUSE_DB, POSTGRES_WAREHOUSE_PORT)
# # =========================
# # NOTE: Your .env does NOT define POSTGRES_WAREHOUSE_HOST, so we default to the compose service name.

# POSTGRES_WAREHOUSE_HOST = "postgres_warehouse"
# POSTGRES_WAREHOUSE_PORT = int(must("POSTGRES_WAREHOUSE_PORT"))
# POSTGRES_WAREHOUSE_DB = must("POSTGRES_WAREHOUSE_DB")
# POSTGRES_WAREHOUSE_USER = must("POSTGRES_WAREHOUSE_USER")
# POSTGRES_WAREHOUSE_PASSWORD = must("POSTGRES_WAREHOUSE_PASSWORD")


# # =========================
# # dbt (.env keys: DBT_TARGET, DBT_PROFILES_DIR)
# # =========================

# DBT_TARGET = must("DBT_TARGET")
# DBT_PROFILES_DIR = must("DBT_PROFILES_DIR")
