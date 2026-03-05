FROM python:3.11-slim

WORKDIR /usr/app

# Install system dependencies (git for dbt packages)
RUN apt-get update \
    && apt-get install -y git \
    && rm -rf /var/lib/apt/lists/*

# Install dbt
RUN pip install --no-cache-dir "dbt-postgres==1.10.0"