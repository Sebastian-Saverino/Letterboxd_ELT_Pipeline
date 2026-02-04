# Letterboxd_ELT_Pipeline

## Goal
Ingest Letterboxd CSV data via FastAPI (Extract), store raw files in MinIO (Load) , track metadata in Postgres , _transform_ with dbt, and serve analytics-ready data with metabase.

## Architecture (High Level)
- FastAPI: ingestion + metadata capture
- MinIO: raw data lake
- Postgres (metadata): ingestion tracking
- Postgres (warehouse): transformed data
- dbt: transformations (silver → gold)

## Data Flow
Client → FastAPI → MinIO (raw)  
Client → FastAPI → Postgres (metadata)  
MinIO + Metadata → dbt → Postgres (gold)

## Tech Stack
FastAPI (Extract), Postgres(Load / Data Warehouse), MinIO (Data Lake), dbt(Transform), Docker Compose(containerization), Apache Airflow (Orchestration)







<img width="811" height="742" alt="Letterboxd_ELT_Data_Pipeline drawio" src="https://github.com/user-attachments/assets/f7222c6a-97b0-453c-878c-c30214e8b464" />

