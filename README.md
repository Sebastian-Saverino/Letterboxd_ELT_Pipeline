# Letterboxd Data Pipeline

A containerized data engineering pipeline for ingesting personal Letterboxd exports, preserving raw files in object storage, loading structured warehouse tables, transforming data with dbt, and serving analytics in Metabase.

This project is designed as a portfolio-grade ELT workflow with clear layer boundaries:

- `FastAPI` handles ingestion only
- `MinIO` stores immutable raw CSV files
- `bronze_loader` loads the latest raw objects into warehouse bronze tables
- `Postgres` stores ingestion metadata and warehouse data
- `dbt` transforms bronze data into silver and gold models
- `Metabase` provides downstream analytics and dashboards

## Project Overview

The pipeline accepts Letterboxd CSV exports through a FastAPI ingestion service, stores the raw files unchanged in MinIO, records ingestion metadata in Postgres, and loads curated bronze tables into a warehouse Postgres instance. dbt then applies transformation logic to produce analytics-ready silver and gold models for dashboarding in Metabase.

The project is intentionally modular and Docker Compose driven so it can be run locally end to end while still reflecting production-minded data engineering patterns.

## Architecture

```text
                     +----------------------+
                     | Letterboxd CSV Files |
                     +----------+-----------+
                                |
                                v
                     +----------------------+
                     | FastAPI Ingestion API|
                     +-----+-----------+----+
                           |           |
                           |           |
                           v           v
                +----------------+   +----------------------+
                | MinIO Raw Zone |   | Metadata Postgres    |
                | immutable CSVs |   | ingestion_runs       |
                +--------+-------+   +----------------------+
                         |
                         v
                +----------------------+
                | bronze_loader        |
                | latest raw CSV ->    |
                | bronze.* tables      |
                +----------+-----------+
                           |
                           v
                +----------------------+
                | Warehouse Postgres   |
                | bronze / silver /    |
                | gold schemas         |
                +----------+-----------+
                           |
                           v
                +----------------------+
                | dbt                  |
                | bronze -> silver ->  |
                | gold transformations |
                +----------+-----------+
                           |
                           v
                +----------------------+
                | Metabase             |
                | dashboards / BI      |
                +----------------------+
```

## Data Flow

1. A user uploads a Letterboxd CSV export to the FastAPI ingestion endpoint.
2. The API writes the raw file to MinIO without modifying its contents.
3. The API records an ingestion metadata row in the metadata Postgres database.
4. The standalone `bronze_loader` reads the latest raw object for a dataset and loads it into `bronze.*` warehouse tables.
5. dbt transforms bronze tables into cleaned `silver.*` models and analytics-focused `gold.*` models.
6. Metabase connects to the warehouse and exposes dashboards over the gold layer.

## Tech Stack

- `FastAPI` for ingestion endpoints and lightweight frontend hosting
- `MinIO` for S3-compatible raw object storage
- `Postgres 16` for metadata and warehouse storage
- `Python` and `pandas` for bronze loading
- `SQLAlchemy` for API-side database access
- `dbt` for silver and gold transformations
- `Metabase` for BI and dashboarding
- `Docker Compose` for local orchestration

## Repository Structure

```text
.
|-- api/                  # FastAPI ingestion service
|   |-- app/
|   |   |-- core/         # settings and configuration
|   |   |-- db/           # SQLAlchemy session and models
|   |   |-- repositories/ # persistence layer
|   |   |-- routes/       # API routes
|   |   |-- services/     # MinIO and ingestion services
|   |   `-- static/       # lightweight upload UI
|-- bronze_loader/        # standalone bronze loading package and image
|-- dbt/                  # dbt project, profiles, models, Dockerfile
|   `-- letterboxd/models/
|       |-- silver/       # cleaned, typed intermediate models
|       `-- gold/         # analytics-ready marts
|-- infra/bootstrap/      # database bootstrap SQL and bootstrap script
|-- docker-compose.yml    # local stack definition
|-- makefile              # canonical operator interface
`-- cheat_sheet.md        # operator runbook
```

## Setup Instructions (Docker Compose)

### Prerequisites

- Docker Desktop
- Docker Compose
- Git

### Environment

1. Copy the example environment file:

```bash
cp .env.example .env
```

2. Review and adjust credentials, database names, and MinIO settings in `.env`.

### Start the stack

Use the Makefile operator interface:

```bash
make up
```

Useful checks:

```bash
make ps
make health-check
make minio-check
```

Raw Compose equivalents:

```bash
docker compose up -d
docker compose ps
```

## Running the Pipeline

### 1. Upload raw Letterboxd data

Upload through the API:

```bash
curl.exe -X POST "http://localhost:8000/ingest/letterboxd/upload" ^
  -F "file=@C:\path\to\letterboxd_ratings.csv;type=text/csv"
```

Expected behavior:

- the raw CSV is stored in MinIO
- an ingestion row is written to the metadata database

### 2. Load bronze tables

Run the standalone bronze loader for a dataset:

```bash
make bronze-loader DATASET=ratings
```

Examples:

```bash
make bronze-loader DATASET=watched
make bronze-loader DATASET=watchlist
make bronze-loader DATASET=diary
make bronze-loader DATASET=reviews
make bronze-loader DATASET=profile
```

### 3. Run dbt transformations

Build silver models:

```bash
make dbt-silver
```

Build gold models:

```bash
make dbt-gold
```

Run tests:

```bash
make dbt-test
```

## Example Workflow

```bash
make up
make ps
make health-check

curl.exe -X POST "http://localhost:8000/ingest/letterboxd/upload" -F "file=@C:\path\to\letterboxd_ratings.csv;type=text/csv"
curl.exe -X POST "http://localhost:8000/ingest/letterboxd/upload" -F "file=@C:\path\to\letterboxd_watched.csv;type=text/csv"

make bronze-loader DATASET=ratings
make bronze-loader DATASET=watched

make dbt-silver
make dbt-gold
make dbt-test
```

Inspect the warehouse:

```bash
make warehouse-psql
```

Example verification queries:

```sql
select count(*) from bronze.ratings;
select count(*) from silver.silver_ratings;
select * from gold.gold_top_rated_movies limit 10;
select * from gold.gold_recent_watch;
```

## Letterboxd Dashboard


![Letterboxd Visualization](images/Letterboxd_Visualization.png)


## Future Improvements

- Add Airflow orchestration for scheduled ingestion, bronze loads, and dbt runs
- Track bronze load runs and idempotency more explicitly
- Add automated API and loader tests
- Add warehouse model contracts and broader dbt test coverage
- Add CI checks for container builds, dbt parse, and linting

