.PHONY: up down test lint dbt-run ingest-season backfill format

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
	uv run python scripts/ingest_season.py $(SEASON)

backfill:
	uv run python scripts/ingest_season.py 2023-2024
	uv run python scripts/ingest_season.py 2024-2025
	uv run python scripts/ingest_season.py 2025-2026
