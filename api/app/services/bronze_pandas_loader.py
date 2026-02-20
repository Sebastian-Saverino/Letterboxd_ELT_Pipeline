import io
import os
from typing import Optional

import pandas as pd
from sqlalchemy import create_engine

from app.services.minio_client import s3_client

RAW_BUCKET = os.getenv("MINIO_BUCKET_RAW", "raw")

RENAME_MAPS = {
    "ratings": {
        "Date": "list_date",
        "Name": "name",
        "Year": "year",
        "Letterboxd URI": "letterboxd_uri",
        "Rating": "rating",
    },

    "watched": {
        "Date": "list_date",
        "Name": "name",
        "Year": "year",
        "Letterboxd URI": "letterboxd_uri",
    },

    "watchlist": {
        "Date": "list_date",
        "Name": "name",
        "Year": "year",
        "Letterboxd URI": "letterboxd_uri",
    },

    "reviews": {
        "Date": "list_date",
        "Name": "name",
        "Year": "year",
        "Letterboxd URI": "letterboxd_uri",
        "Rating": "rating",
        "Rewatch": "rewatch",
        "Review": "review",
        "Tags": "tags",
        "Watched Date": "watched_date",
    },

    "diary": {
        "Date": "list_date",
        "Name": "name",
        "Year": "year",
        "Letterboxd URI": "letterboxd_uri",
        "Rating": "rating",
        "Rewatch": "rewatch",
        "Tags": "tags",
        "Watched Date": "watched_date",
    },

    "profile": {
        "Date Joined": "date_joined",
        "Username": "username",
        "Given Name": "given_name",
        "Family Name": "family_name",
        "Email Address": "email_address",
        "Location": "location",
        "Website": "website",
        "Bio": "bio",
        "Pronoun": "pronoun",
        "Favorite Films": "favorite_films",
    },
}


def _warehouse_engine():
    user = os.getenv("POSTGRES_WAREHOUSE_USER")
    password = os.getenv("POSTGRES_WAREHOUSE_PASSWORD")
    host = os.getenv("POSTGRES_WAREHOUSE_HOST", "postgres-warehouse")
    port = os.getenv("POSTGRES_WAREHOUSE_PORT", "5432")
    db = os.getenv("POSTGRES_WAREHOUSE_DB")
    return create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}")


def find_latest_key(prefix: str, suffix: str, bucket: str = RAW_BUCKET) -> str:
    """
    Lists objects under `prefix` and returns the key with the newest LastModified
    where Key endswith `suffix`.

    Example:
      find_latest_key(prefix="letterboxd/", suffix="_ratings.csv")
    """
    client = s3_client()
    token: Optional[str] = None
    best_key: Optional[str] = None
    best_ts = None  # datetime

    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token

        resp = client.list_objects_v2(**kwargs)

        for obj in resp.get("Contents", []):
            key = obj["Key"]
            if key.lower().endswith(suffix.lower()):
                ts = obj["LastModified"]
                if best_ts is None or ts > best_ts:
                    best_ts = ts
                    best_key = key

        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break

    if not best_key:
        raise FileNotFoundError(f"No object found in bucket='{bucket}' under prefix='{prefix}' ending with '{suffix}'")

    return best_key


def load_one_csv_to_bronze(object_key: str, target_table: str):
    target_table = target_table.lower()
    if target_table not in RENAME_MAPS:
        raise ValueError(f"Unsupported target_table: {target_table}. Supported: {list(RENAME_MAPS.keys())}")

    client = s3_client()
    response = client.get_object(Bucket=RAW_BUCKET, Key=object_key)
    data = response["Body"].read()

    df = pd.read_csv(io.BytesIO(data), encoding="utf-8-sig")

    print("Object key:", object_key)
    print("Columns:", df.columns.tolist())
    print(df.head())

    rename_map = RENAME_MAPS[target_table]
    missing = [c for c in rename_map.keys() if c not in df.columns]
    if missing:
        raise ValueError(
            f"CSV missing expected columns for {target_table}: {missing}. Found: {df.columns.tolist()}"
        )

    df = df.rename(columns=rename_map)

    df.to_sql(
        name=target_table,
        con=_warehouse_engine(),
        schema="bronze",
        if_exists="append",
        index=False,
    )

    print(f"Loaded {len(df)} rows into bronze.{target_table}")


def load_latest_to_bronze(target_table: str, prefix: str = "letterboxd/") -> str:
    """
    Finds the latest file for a given target_table and loads it into bronze.<target_table>.

    Assumes your MinIO naming convention ends with:
      *_ratings.csv, *_watched.csv, *_watchlist.csv, *_diary.csv
    """
    target_table = target_table.lower()
    suffix = f"_{target_table}.csv"
    key = find_latest_key(prefix=prefix, suffix=suffix, bucket=RAW_BUCKET)
    load_one_csv_to_bronze(object_key=key, target_table=target_table)
    return key


def fetch_from_warehouse(query: str) -> pd.DataFrame:
    return pd.read_sql(query, _warehouse_engine())


