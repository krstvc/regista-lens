# regista-lens

Football analytics data warehouse. Integrates match stats (FBref), expected goals (Understat), and market valuations (Transfermarkt) into a star schema on DuckDB, orchestrated by Dagster and transformed by dbt.

## Setup

### Prerequisites

- [Docker](https://docs.docker.com/get-docker/) and Docker Compose
- [uv](https://docs.astral.sh/uv/) (for local development outside Docker)

### Quick start

```bash
cp .env.example .env
docker compose up -d
```

Dagster UI will be available at [http://localhost:3000](http://localhost:3000).

### Local development

```bash
uv sync                   # Install all dependencies (including dev)
make lint                 # Run ruff linter and formatter check
make test                 # Run pytest
make dbt-run              # Run dbt models (requires DUCKDB_PATH)
```

### Makefile targets

| Target | Description |
|---|---|
| `make up` | Start Dagster services via Docker Compose |
| `make down` | Stop services |
| `make test` | Run Python tests |
| `make lint` | Lint and format check |
| `make format` | Auto-fix lint issues and format |
| `make dbt-run` | Run dbt models |
| `make ingest-season SEASON=2023-2024` | Trigger ingestion for a season (not yet implemented) |

## Architecture

See [CLAUDE.md](CLAUDE.md) for the full architecture document and [DECISIONS.md](DECISIONS.md) for architecture decision records.
