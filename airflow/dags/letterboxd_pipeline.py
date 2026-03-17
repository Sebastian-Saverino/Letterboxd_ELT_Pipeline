from __future__ import annotations

import logging
import os
import time
from pathlib import Path

import pendulum
import requests
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

LOGGER = logging.getLogger(__name__)
SUPPORTED_DATASETS = ("diary", "profile", "ratings", "reviews", "watched", "watchlist")
RAW_PREFIX = os.getenv("LETTERBOXD_RAW_PREFIX", "letterboxd/")
DBT_LOG_PATH = "/opt/airflow/logs/dbt"
DBT_TARGET_PATH = "/opt/airflow/logs/dbt/target"


def _api_base_url() -> str:
    return os.getenv("LETTERBOXD_API_BASE_URL", "http://api:8000").rstrip("/")


def _source_dir() -> Path:
    return Path(
        os.getenv(
            "LETTERBOXD_INGESTION_SOURCE_DIR",
            "/opt/airflow/config/ingestion",
        )
    )


def _file_glob() -> str:
    return os.getenv("LETTERBOXD_INGESTION_FILE_GLOB", "*.csv")


def _build_session() -> requests.Session:
    retry_strategy = Retry(
        total=4,
        connect=4,
        read=4,
        backoff_factor=2,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET", "POST"}),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session = requests.Session()
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def _infer_dataset(csv_path: Path) -> str:
    lowered = csv_path.name.lower()
    for dataset in SUPPORTED_DATASETS:
        stem = lowered[:-4] if lowered.endswith(".csv") else lowered
        if stem == dataset:
            return dataset
        if any(stem.endswith(f"{delimiter}{dataset}") for delimiter in ("_", "-", " ")):
            return dataset
    raise AirflowFailException(
        f"Unsupported export filename '{csv_path.name}'. Expected a suffix matching one of {SUPPORTED_DATASETS}."
    )


def _wait_for_api(session: requests.Session, timeout_seconds: int = 180) -> None:
    deadline = time.monotonic() + timeout_seconds
    readiness_url = f"{_api_base_url()}/health/ready"

    while time.monotonic() < deadline:
        try:
            response = session.get(readiness_url, timeout=10)
            if response.ok:
                LOGGER.info("FastAPI readiness probe returned healthy status.")
                return
            LOGGER.warning(
                "FastAPI readiness probe returned status=%s body=%s",
                response.status_code,
                response.text,
            )
        except requests.RequestException as exc:
            LOGGER.warning("FastAPI readiness probe failed: %s", exc)

        time.sleep(5)

    raise AirflowFailException(
        f"FastAPI service was not ready within {timeout_seconds} seconds at {readiness_url}."
    )


def _discover_local_exports(source_dir: Path, pattern: str) -> list[Path]:
    if not source_dir.exists():
        return []
    return sorted(path for path in source_dir.glob(pattern) if path.is_file())


def _discover_raw_datasets() -> list[str]:
    from bronze_loader.loader import find_latest_key_for_dataset

    available_datasets: list[str] = []
    for dataset in SUPPORTED_DATASETS:
        try:
            object_key = find_latest_key_for_dataset(dataset=dataset, prefix=RAW_PREFIX)
            LOGGER.info(
                "Found existing raw object for dataset=%s object_key=%s",
                dataset,
                object_key,
            )
            available_datasets.append(dataset)
        except FileNotFoundError:
            continue

    if not available_datasets:
        raise AirflowFailException(
            "No CSV exports were found in the mounted ingestion directory and no matching raw objects were found in MinIO."
        )

    return available_datasets


default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 2,
}


@dag(
    dag_id="letterboxd_pipeline",
    description="Upload Letterboxd exports, refresh bronze tables, and build silver/gold marts.",
    schedule=os.getenv("LETTERBOXD_PIPELINE_SCHEDULE", "0 2 * * *"),
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=pendulum.duration(hours=2),
    default_args=default_args,
    tags=["letterboxd", "elt", "dbt"],
)
def letterboxd_pipeline():
    @task(retries=2, retry_delay=pendulum.duration(minutes=5))
    def upload_exports() -> list[str]:
        source_dir = _source_dir()
        pattern = _file_glob()
        session = _build_session()

        _wait_for_api(session=session)

        local_exports = _discover_local_exports(source_dir=source_dir, pattern=pattern)
        if not local_exports:
            LOGGER.info(
                "No local CSV exports found in %s. Falling back to previously uploaded raw objects in MinIO.",
                source_dir,
            )
            return _discover_raw_datasets()

        datasets: set[str] = set()
        upload_url = f"{_api_base_url()}/ingest/letterboxd/upload"

        for csv_path in local_exports:
            dataset = _infer_dataset(csv_path)
            LOGGER.info("Uploading %s for dataset=%s", csv_path.name, dataset)

            with csv_path.open("rb") as handle:
                response = session.post(
                    upload_url,
                    files={"file": (csv_path.name, handle, "text/csv")},
                    timeout=(10, 120),
                )

            if not response.ok:
                raise AirflowFailException(
                    f"Upload failed for {csv_path.name} with status={response.status_code} body={response.text}"
                )

            payload = response.json()
            LOGGER.info(
                "Uploaded dataset=%s object_key=%s ingestion_id=%s",
                dataset,
                payload.get("object_key"),
                payload.get("ingestion_id"),
            )
            datasets.add(dataset)

        ordered_datasets = sorted(datasets)
        LOGGER.info("Uploaded datasets for this run: %s", ordered_datasets)
        return ordered_datasets

    @task(retries=0)
    def load_bronze_dataset(dataset: str) -> str:
        from bronze_loader.loader import load_latest_to_bronze

        LOGGER.info("Loading latest raw object into bronze.%s", dataset)
        object_key = load_latest_to_bronze(target_table=dataset)
        LOGGER.info("Bronze load complete for dataset=%s object_key=%s", dataset, object_key)
        return object_key

    uploaded_datasets = upload_exports()
    bronze_loads = load_bronze_dataset.expand(dataset=uploaded_datasets)

    bronze_load_complete = EmptyOperator(task_id="bronze_load_complete")

    dbt_run_silver = BashOperator(
        task_id="dbt_run_silver",
        bash_command=(
            "set -euo pipefail && "
            f"mkdir -p {DBT_LOG_PATH} {DBT_TARGET_PATH} && "
            "cd /opt/airflow/project/dbt && "
            "dbt run --project-dir /opt/airflow/project/dbt "
            "--profiles-dir /opt/airflow/project/dbt/profiles "
            f"--log-path {DBT_LOG_PATH} "
            f"--target-path {DBT_TARGET_PATH} "
            "--select silver --target \"$DBT_TARGET\""
        ),
        execution_timeout=pendulum.duration(minutes=30),
        retries=2,
        retry_delay=pendulum.duration(minutes=10),
        append_env=True,
    )

    dbt_run_gold = BashOperator(
        task_id="dbt_run_gold",
        bash_command=(
            "set -euo pipefail && "
            f"mkdir -p {DBT_LOG_PATH} {DBT_TARGET_PATH} && "
            "cd /opt/airflow/project/dbt && "
            "dbt run --project-dir /opt/airflow/project/dbt "
            "--profiles-dir /opt/airflow/project/dbt/profiles "
            f"--log-path {DBT_LOG_PATH} "
            f"--target-path {DBT_TARGET_PATH} "
            "--select gold --target \"$DBT_TARGET\""
        ),
        execution_timeout=pendulum.duration(minutes=30),
        retries=2,
        retry_delay=pendulum.duration(minutes=10),
        append_env=True,
    )

    bronze_loads >> bronze_load_complete >> dbt_run_silver >> dbt_run_gold


letterboxd_pipeline()
