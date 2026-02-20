.PHONY: bronze

bronze:
	docker compose exec api python -c "from app.services.bronze_pandas_loader import load_latest_to_bronze; load_latest_to_bronze('$(DATASET)')"