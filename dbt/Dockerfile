FROM ghcr.io/dbt-labs/dbt-core:1.10.0

RUN python -m pip install --no-cache-dir dbt-postgres==1.10.0

WORKDIR /usr/app
ENV DBT_PROFILES_DIR=/root/.dbt