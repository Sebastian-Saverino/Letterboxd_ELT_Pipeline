• # Letterboxd Pipeline Operator Cheat Sheet

  ## Stack lifecycle

  ### Start the stack

  make up

  Raw equivalent:

  docker compose up -d

  ### Rebuild and restart

  make rebuild

  Raw equivalent:

  docker compose up -d --build

  ### Stop the stack

  make down

  Raw equivalent:

  docker compose down --remove-orphans

  ## Container status

  ### Check running services

  make ps

  Raw equivalent:

  docker compose ps

  ## Logs

  ### Stream all logs

  make logs

  ### Stream one service

  make logs SERVICE=api
  make logs SERVICE=dbt
  make logs SERVICE=bronze_loader

  Raw equivalent:

  docker compose logs -f api

  ## API and health

  ### Check API liveness and readiness

  make health-check

  Raw equivalents:

  curl.exe http://localhost:8000/health
  curl.exe -i http://localhost:8000/health/ready

  ### Open a shell in the API container

  make api-shell

  Raw equivalent:

  docker compose exec api sh

  ## Uploading data

  Upload through the API. Use filenames that match the loader convention:

  - *_ratings.csv
  - *_watched.csv
  - *_watchlist.csv
  - *_diary.csv
  - *_reviews.csv
  - *_profile.csv

  Example:

  curl.exe -X POST "http://localhost:8000/ingest/letterboxd/upload" -F "file=@C:\path\to\letterboxd_ratings.csv;type=te
  xt/csv"

  Expected response fields:

  - status
  - ingestion_id
  - bucket
  - object_key
  - size_bytes
  - sha256

  ## Bronze loader

  ### Run bronze loader for one dataset

  make bronze-loader DATASET=ratings

  Other examples:

  make bronze-loader DATASET=watched
  make bronze-loader DATASET=watchlist
  make bronze-loader DATASET=diary
  make bronze-loader DATASET=reviews
  make bronze-loader DATASET=profile

  Raw equivalent:

  docker compose --profile tooling run --rm --build bronze_loader --dataset ratings

  ## MinIO check

  ### Check MinIO health

  make minio-check

  Raw equivalent:

  docker compose exec minio sh -lc 'curl -fsS http://localhost:9000/minio/health/live'

  ## Metadata database

  ### Open metadata DB shell

  make meta-psql

  Raw equivalent:

  docker compose exec postgres-meta sh -lc 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"'

  ### Check recent ingestion rows

  select ingestion_id, source, original_filename, bucket, object_key, size_bytes, status, created_at
  from public.ingestion_runs
  order by created_at desc
  limit 10;

  ## Warehouse database

  ### Open warehouse DB shell

  make warehouse-psql

  Raw equivalent:

  docker compose exec postgres-warehouse sh -lc 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"'

  ### Confirm schemas exist

  select schema_name
  from information_schema.schemata
  where schema_name in ('bronze', 'silver', 'gold')
  order by schema_name;

  ## dbt operations

  ### Verify dbt connectivity

  make dbt-debug

  Raw equivalent:

  docker compose exec dbt dbt debug

  ### Run silver models

  make dbt-silver

  Raw equivalent:

  docker compose exec dbt dbt run --select silver

  ### Run gold models

  make dbt-gold

  Raw equivalent:

  docker compose exec dbt dbt run --select gold

  ### Run dbt tests

  make dbt-test

  Raw equivalent:

  docker compose exec dbt dbt test

  ## Quick verification SQL

  ### Bronze counts

  select count(*) as ratings_count from bronze.ratings;
  select count(*) as watched_count from bronze.watched;
  select count(*) as watchlist_count from bronze.watchlist;
  select count(*) as diary_count from bronze.diary;
  select count(*) as reviews_count from bronze.reviews;
  select count(*) as profile_count from bronze.profile;

  ### Silver counts

  select count(*) as silver_ratings_count from silver.silver_ratings;
  select count(*) as silver_watched_count from silver.silver_watched;
  select count(*) as silver_watchlist_count from silver.silver_watchlist;
  select count(*) as silver_diary_count from silver.silver_diary;
  select count(*) as silver_reviews_count from silver.silver_reviews;
  select count(*) as silver_profile_count from silver.silver_profile;

  ### Gold outputs

  select * from gold.gold_top_rated_movies limit 10;
  select * from gold.gold_recent_watch;
  select * from gold.gold_lowest_rated_movie;
  select * from gold.gold_best_movie_year limit 10;
  select * from gold.gold_watch_activity order by watch_month desc limit 12;
  select * from gold.gold_watchlist_by_year order by year desc limit 12;

  ## Common troubleshooting

  - api exits on startup:
      - check logs with make logs SERVICE=api
      - common causes: missing Python dependency, FastAPI import error, invalid response annotation
  - /health/ready returns 503:
      - MinIO bucket unreachable
      - metadata Postgres unreachable
      - check make minio-check and metadata DB connectivity
  - bronze loader cannot find a file:
      - uploaded filename/object key must end with _<dataset>.csv
  - bronze row counts look too high:
      - current bronze loader is append-only
      - repeated runs of the same dataset will duplicate bronze rows
  - dbt tests fail on bronze uniqueness:
      - expected if you have historical duplicate bronze loads
      - this is a data-state issue, not necessarily a broken pipeline
  - dbt source accepted-values fails:
      - bronze raw values may differ from typed silver/gold values
      - inspect raw distinct values before tightening source tests
  - stack won’t start:
      - check make ps
      - then make logs SERVICE=api or make logs SERVICE=db_bootstrap

  ## Typical demo flow

  make up
  make ps
  make health-check
  curl.exe -X POST "http://localhost:8000/ingest/letterboxd/upload" -F "file=@C:\path\to\letterboxd_ratings.csv;type=te
  xt/csv"
  make bronze-loader DATASET=ratings
  make dbt-silver
  make dbt-gold
  make dbt-test
  make warehouse-psql