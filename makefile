.DEFAULT_GOAL := ps

COMPOSE := docker compose
TOOLING_PROFILE := --profile tooling
DATASET ?= ratings
SERVICE ?=

.PHONY: \
	up \
	down \
	rebuild \
	logs \
	ps \
	api-shell \
	bronze-loader \
	dbt-debug \
	dbt-silver \
	dbt-gold \
	dbt-test \
	meta-psql \
	warehouse-psql \
	minio-check \
	health-check

# Stack lifecycle
up:
	$(COMPOSE) up -d

down:
	$(COMPOSE) down --remove-orphans

rebuild:
	$(COMPOSE) up -d --build

logs:
	$(COMPOSE) logs -f $(SERVICE)

ps:
	$(COMPOSE) ps

# Service access
api-shell:
	$(COMPOSE) exec api sh

meta-psql:
	$(COMPOSE) exec postgres-meta sh -lc 'psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB"'

warehouse-psql:
	$(COMPOSE) exec postgres-warehouse sh -lc 'psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB"'

# Pipeline operations
bronze-loader:
	$(COMPOSE) $(TOOLING_PROFILE) run --rm --build bronze_loader --dataset "$(DATASET)"

dbt-debug:
	$(COMPOSE) exec dbt dbt debug

dbt-silver:
	$(COMPOSE) exec dbt dbt run --select silver

dbt-gold:
	$(COMPOSE) exec dbt dbt run --select gold

dbt-test:
	$(COMPOSE) exec dbt dbt test

# Health checks
minio-check:
	$(COMPOSE) exec minio sh -lc 'curl -fsS http://localhost:9000/minio/health/live'

health-check:
	curl.exe http://localhost:8000/health
	curl.exe -i http://localhost:8000/health/ready
