.PHONY: up down test lint dbt-run ingest-season format

up:
	docker compose up -d

down:
	docker compose down

test:
	uv run pytest tests/

lint:
	uv run ruff check .
	uv run ruff format --check .

format:
	uv run ruff check --fix .
	uv run ruff format .

dbt-run:
	cd transform && uv run dbt run

ingest-season:
	@test -n "$(SEASON)" || (echo "Usage: make ingest-season SEASON=2023-2024" && exit 1)
	@echo "Not yet implemented — will trigger Dagster asset materialization for season $(SEASON)."
