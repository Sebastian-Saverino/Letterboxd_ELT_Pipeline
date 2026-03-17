from __future__ import annotations

import logging
import os
import time
from pathlib import Path
from typing import Iterable

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


def _api_base_url() -> str:
    return os.getenv("LETTERBOXD_API_BASE_URL", "http://api:8000").rstrip("/")


def _source_dir() -> Path:
    return Path(
        os.getenv(
            "LETTERBOXD_INGESTION_SOURCE_DIR",
            "/opt/airflow/project/airflow/config/ingestion",
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
        if lowered.endswith(f"{dataset}.csv"):
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


def _discover_exports(source_dir: Path, pattern: str) -> Iterable[Path]:
    if not source_dir.exists():
        raise AirflowFailException(
            f"Ingestion source directory does not exist: {source_dir}. Place Letterboxd CSV exports there."
        )

    files = sorted(path for path in source_dir.glob(pattern) if path.is_file())
    if not files:
        raise AirflowFailException(
            f"No CSV exports found in {source_dir} matching pattern '{pattern}'."
        )
    return files


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

        datasets: set[str] = set()
        upload_url = f"{_api_base_url()}/ingest/letterboxd/upload"

        for csv_path in _discover_exports(source_dir=source_dir, pattern=pattern):
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
            "cd /opt/airflow/project/dbt && "
            "dbt run --project-dir /opt/airflow/project/dbt "
            "--profiles-dir /opt/airflow/project/dbt/profiles "
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
            "cd /opt/airflow/project/dbt && "
            "dbt run --project-dir /opt/airflow/project/dbt "
            "--profiles-dir /opt/airflow/project/dbt/profiles "
            "--select gold --target \"$DBT_TARGET\""
        ),
        execution_timeout=pendulum.duration(minutes=30),
        retries=2,
        retry_delay=pendulum.duration(minutes=10),
        append_env=True,
    )

    bronze_loads >> bronze_load_complete >> dbt_run_silver >> dbt_run_gold


letterboxd_pipeline()
