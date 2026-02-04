# Makefile for local dev workflow (Docker Compose + dbt + API)
# Usage: make help

SHELL := /bin/bash
.DEFAULT_GOAL := help

COMPOSE ?= docker compose
ENV_FILE ?= .env
PROJECT ?= letterboxd_pipeline

# Containers (must match docker-compose.yml service names)
SVC_MINIO ?= minio
SVC_API ?= api
SVC_DBT ?= dbt
SVC_META_DB ?= postgres_meta
SVC_WAREHOUSE_DB ?= postgres_warehouse
SVC_METABASE ?= metabase

# Ports (host ports, update if your compose differs)
MINIO_CONSOLE_URL ?= http://localhost:9001
MINIO_S3_URL ?= http://localhost:9000
API_URL ?= http://localhost:8000
METABASE_URL ?= http://localhost:3000

# Safety: require .env for most commands
define require_env
	@if [ ! -f "$(ENV_FILE)" ]; then \
		echo "Missing $(ENV_FILE). Create it first (ex: cp .env.example .env)."; \
		exit 1; \
	fi
endef

.PHONY: help
help: ## Show available commands
	@echo ""
	@echo "$(PROJECT) — Make targets"
	@echo "----------------------------------------"
	@grep -E '^[a-zA-Z0-9_\-]+:.*##' $(MAKEFILE_LIST) | awk 'BEGIN {FS=":.*##"}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'
	@echo ""

# -------------------------
# Core lifecycle
# -------------------------
.PHONY: up
up: ## Build and start all services (detached)
	$(call require_env)
	$(COMPOSE) up -d --build

.PHONY: down
down: ## Stop and remove services
	$(COMPOSE) down

.PHONY: restart
restart: ## Restart services
	$(COMPOSE) down
	$(call require_env)
	$(COMPOSE) up -d --build

.PHONY: ps
ps: ## Show running containers
	$(COMPOSE) ps

.PHONY: logs
logs: ## Tail logs for all services
	$(COMPOSE) logs -f --tail=200

.PHONY: logs-api
logs-api: ## Tail API logs
	$(COMPOSE) logs -f --tail=200 $(SVC_API)

.PHONY: logs-minio
logs-minio: ## Tail MinIO logs
	$(COMPOSE) logs -f --tail=200 $(SVC_MINIO)

# -------------------------
# Clean / reset
# -------------------------
.PHONY: clean
clean: ## Remove containers + networks (keeps named volumes)
	$(COMPOSE) down --remove-orphans

.PHONY: nuke
nuke: ## WARNING: Remove everything including volumes (data loss)
	@read -p "This will delete ALL volumes (data loss). Type 'NUKE' to continue: " ans; \
	if [ "$$ans" = "NUKE" ]; then \
		$(COMPOSE) down -v --remove-orphans; \
	else \
		echo "Aborted."; \
	fi

# -------------------------
# Shell access
# -------------------------
.PHONY: sh-api
sh-api: ## Open a shell inside the API container
	$(COMPOSE) exec $(SVC_API) bash || $(COMPOSE) exec $(SVC_API) sh

.PHONY: sh-dbt
sh-dbt: ## Open a shell inside the dbt container
	$(COMPOSE) exec $(SVC_DBT) bash || $(COMPOSE) exec $(SVC_DBT) sh

.PHONY: sh-minio
sh-minio: ## Open a shell inside the MinIO container
	$(COMPOSE) exec $(SVC_MINIO) sh

# -------------------------
# Health checks / quick opens
# -------------------------
.PHONY: urls
urls: ## Print useful local URLs
	@echo "MinIO (S3):      $(MINIO_S3_URL)"
	@echo "MinIO (Console): $(MINIO_CONSOLE_URL)"
	@echo "API:             $(API_URL)"
	@echo "Metabase:        $(METABASE_URL)"

.PHONY: check
check: ## Quick status check (containers + key endpoints)
	@$(COMPOSE) ps
	@echo ""
	@echo "Checking endpoints (best effort)..."
	@curl -fsS $(MINIO_CONSOLE_URL) >/dev/null && echo "✅ MinIO console reachable" || echo "⚠️  MinIO console not reachable"
	@curl -fsS $(API_URL)/docs >/dev/null && echo "✅ API docs reachable" || echo "⚠️  API docs not reachable"

# -------------------------
# dbt commands (run inside dbt container)
# -------------------------
.PHONY: dbt-debug
dbt-debug: ## dbt debug
	$(COMPOSE) run --rm $(SVC_DBT) dbt debug

.PHONY: dbt-deps
dbt-deps: ## dbt deps
	$(COMPOSE) run --rm $(SVC_DBT) dbt deps

.PHONY: dbt-run
dbt-run: ## dbt run
	$(COMPOSE) run --rm $(SVC_DBT) dbt run

.PHONY: dbt-test
dbt-test: ## dbt test
	$(COMPOSE) run --rm $(SVC_DBT) dbt test

.PHONY: dbt-build
dbt-build: ## dbt build (run + test)
	$(COMPOSE) run --rm $(SVC_DBT) dbt build

# -------------------------
# Postgres helpers (optional)
# -------------------------
.PHONY: psql-meta
psql-meta: ## Open psql to metadata DB (inside container)
	$(COMPOSE) exec $(SVC_META_DB) psql -U $$POSTGRES_META_USER -d $$POSTGRES_META_DB

.PHONY: psql-warehouse
psql-warehouse: ## Open psql to warehouse DB (inside container)
	$(COMPOSE) exec $(SVC_WAREHOUSE_DB) psql -U $$POSTGRES_WAREHOUSE_USER -d $$POSTGRES_WAREHOUSE_DB

# -------------------------
# Setup helpers
# -------------------------
.PHONY: env-example
env-example: ## Create .env.example from current .env (if present)
	@if [ ! -f "$(ENV_FILE)" ]; then \
		echo "Missing $(ENV_FILE). Nothing to export."; \
		exit 1; \
	fi
	@cp $(ENV_FILE) .env.example
	@echo "Wrote .env.example (review before committing)."
