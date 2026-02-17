import io
import os
import pandas as pd
from sqlalchemy import create_engine

from app.services.minio_client import s3_client

RAW_BUCKET = os.getenv("MINIO_BUCKET_RAW", "raw")

def load_one_csv_to_bronze(object_key: str, target_table: str):

    # 1️⃣ Create MinIO client
    client = s3_client()

    # 2️⃣ Download object
    response = client.get_object(Bucket=RAW_BUCKET, Key=object_key)
    data = response["Body"].read()

    # 3️⃣ Load into pandas
    df = pd.read_csv(io.BytesIO(data), encoding="utf-8-sig")

    print("Columns:", df.columns.tolist())
    print(df.head())

    # 4️⃣ Rename columns (ratings example)
    rename_map = {
        "Date": "list_date",
        "Name": "name",
        "Year": "year",
        "Letterboxd URI": "letterboxd_uri",
        "Rating": "rating",
    }

    df = df.rename(columns=rename_map)

    # 5️⃣ Connect to Postgres
    user = os.getenv("POSTGRES_WAREHOUSE_USER")
    password = os.getenv("POSTGRES_WAREHOUSE_PASSWORD")
    host = os.getenv("POSTGRES_WAREHOUSE_HOST", "postgres_warehouse")
    port = os.getenv("POSTGRES_WAREHOUSE_PORT", "5432")
    db = os.getenv("POSTGRES_WAREHOUSE_DB")

    engine = create_engine(
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    )

    # 6️⃣ Load into bronze schema
    df.to_sql(
        name=target_table,
        con=engine,
        schema="bronze",
        if_exists="append",
        index=False
    )

    print(f"Loaded {len(df)} rows into bronze.{target_table}")
