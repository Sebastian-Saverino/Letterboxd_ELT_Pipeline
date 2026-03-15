#!/bin/sh
set -eu

echo "Bootstrapping metadata database schema..."
PGPASSWORD="${POSTGRES_META_PASSWORD}" \
psql \
  -h postgres-meta \
  -U "${POSTGRES_META_USER}" \
  -d "${POSTGRES_META_DB}" \
  -v ON_ERROR_STOP=1 \
  -f /bootstrap/metadata.sql

echo "Bootstrapping warehouse schemas..."
PGPASSWORD="${POSTGRES_WAREHOUSE_PASSWORD}" \
psql \
  -h postgres-warehouse \
  -U "${POSTGRES_WAREHOUSE_USER}" \
  -d "${POSTGRES_WAREHOUSE_DB}" \
  -v ON_ERROR_STOP=1 \
  -f /bootstrap/warehouse.sql

echo "Database bootstrap complete."
