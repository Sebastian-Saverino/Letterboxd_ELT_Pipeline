from __future__ import annotations

import io
import logging
import os
from typing import Optional

import boto3
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

logger = logging.getLogger("bronze_loader.loader")

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


def supported_datasets() -> list[str]:
    return sorted(RENAME_MAPS.keys())


def _must(name: str) -> str:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def s3_client():
    endpoint = f"http://{os.getenv('MINIO_HOST', 'minio')}:{os.getenv('MINIO_PORT', '9000')}"
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=_must("MINIO_ROOT_USER"),
        aws_secret_access_key=_must("MINIO_ROOT_PASSWORD"),
        region_name=os.getenv("MINIO_REGION", "us-east-1"),
    )


def warehouse_engine() -> Engine:
    user = _must("POSTGRES_WAREHOUSE_USER")
    password = _must("POSTGRES_WAREHOUSE_PASSWORD")
    host = os.getenv("POSTGRES_WAREHOUSE_HOST", "postgres-warehouse")
    port = os.getenv("POSTGRES_WAREHOUSE_PORT", "5432")
    database = _must("POSTGRES_WAREHOUSE_DB")
    return create_engine(
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}",
        pool_pre_ping=True,
    )


def find_latest_key(prefix: str, suffix: str, bucket: str = RAW_BUCKET) -> str:
    client = s3_client()
    token: Optional[str] = None
    best_key: Optional[str] = None
    best_ts = None

    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token

        response = client.list_objects_v2(**kwargs)

        for obj in response.get("Contents", []):
            key = obj["Key"]
            if key.lower().endswith(suffix.lower()):
                last_modified = obj["LastModified"]
                if best_ts is None or last_modified > best_ts:
                    best_ts = last_modified
                    best_key = key

        if response.get("IsTruncated"):
            token = response.get("NextContinuationToken")
        else:
            break

    if not best_key:
        raise FileNotFoundError(
            f"No object found in bucket='{bucket}' under prefix='{prefix}' ending with '{suffix}'"
        )

    return best_key


def load_one_csv_to_bronze(object_key: str, target_table: str) -> None:
    dataset = target_table.lower()
    if dataset not in RENAME_MAPS:
        raise ValueError(
            f"Unsupported target_table: {dataset}. Supported: {supported_datasets()}"
        )

    client = s3_client()
    response = client.get_object(Bucket=RAW_BUCKET, Key=object_key)
    data = response["Body"].read()

    dataframe = pd.read_csv(io.BytesIO(data), encoding="utf-8-sig")
    rename_map = RENAME_MAPS[dataset]
    missing = [column for column in rename_map if column not in dataframe.columns]
    if missing:
        raise ValueError(
            f"CSV missing expected columns for {dataset}: {missing}. Found: {dataframe.columns.tolist()}"
        )

    dataframe = dataframe.rename(columns=rename_map)
    dataframe.to_sql(
        name=dataset,
        con=warehouse_engine(),
        schema="bronze",
        if_exists="append",
        index=False,
    )

    logger.info(
        "Loaded %s rows into bronze.%s from object_key=%s",
        len(dataframe),
        dataset,
        object_key,
    )


def load_latest_to_bronze(target_table: str, prefix: str = "letterboxd/") -> str:
    dataset = target_table.lower()
    suffix = f"_{dataset}.csv"
    object_key = find_latest_key(prefix=prefix, suffix=suffix, bucket=RAW_BUCKET)
    load_one_csv_to_bronze(object_key=object_key, target_table=dataset)
    return object_key
