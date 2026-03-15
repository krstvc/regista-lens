# regista-lens — Football Analytics Warehouse

A local-first data warehouse that integrates football (soccer) data from three public sources into a clean star schema, enabling analytical queries across the top 5 European leagues.

## Architecture overview

### Sources

| Source | Data | Ingestion method | Key challenge |
|---|---|---|---|
| FBref (Sports Reference) | Match stats, player season stats, team stats | HTML scraping | Rate limiting (3s between requests), inconsistent table structures across leagues/seasons |
| Understat | xG, xA per player per match | JSON endpoints (undocumented) | Player/team name mismatches with FBref |
| Transfermarkt | Market values, player metadata (age, nationality, position, contract) | HTML scraping | Aggressive anti-bot protections, names in local scripts, complex page structures |

### Stack

| Component | Tool | Justification |
|---|---|---|
| Orchestration | **Dagster** | Asset-based model maps naturally to warehouse tables. Each asset has clear lineage, and partitioning (by season) is first-class. Better developer experience than Airflow for a greenfield project. |
| Transformation | **dbt** (dbt-core + dbt-duckdb) | Industry standard for SQL-based transformation. Enables declarative modeling, built-in testing, documentation generation, and clear layer separation. |
| Storage | **DuckDB** | Embedded analytical database — no server to manage. Excellent for local-first development. Columnar storage is ideal for analytical queries. Fast enough for this data volume (millions of rows, not billions). |
| Language | **Python 3.11+** | Ingestion scripts, Dagster definitions, glue code. Use `uv` for dependency management. |
| Containerization | **Docker Compose** | Dagster webserver + daemon as services. DuckDB is embedded (file on a volume), not a service. |

### Data layers

**Raw (Bronze)** — Python ingestion, orchestrated as Dagster assets
- One raw table per source per entity
- Naming: `raw_{source}__{entity}` (e.g., `raw_fbref__player_match_stats`)
- Store data as-is with metadata columns: `_ingested_at` (timestamp), `_source_url` (string), `_season` (string, e.g., "2023-2024")
- Partitioned by season in Dagster
- Idempotent: re-running a partition replaces it, never duplicates

**Staging (Silver)** — dbt models
- Clean, typed, deduplicated, single-source
- Naming: `stg_{source}__{entity}` (e.g., `stg_fbref__player_match_stats`)
- Unicode normalization, transliteration of names
- Source-aligned: no cross-source joins yet
- This layer produces the entity resolution cross-reference tables

**Intermediate** — dbt models
- Entity resolution logic lives here
- `int_player_xref`: maps `(source, source_player_id)` → `canonical_player_id`
- `int_team_xref`: maps `(source, source_team_id)` → `canonical_team_id`

**Marts (Gold)** — dbt models
- Star schema, analysis-ready
- Fact tables at well-defined grains
- Dimension tables with business-friendly columns

## Phase 1 scope

### In scope
- Top 5 European leagues: Premier League, La Liga, Bundesliga, Serie A, Ligue 1
- 3 seasons: current (2025-2026) + 2 historical (2024-2025, 2023-2024)
- All 3 sources
- Core star schema (see data model below)
- Entity resolution for players and teams
- dbt tests and data quality checks
- Dagster asset graph with partition support
- Docker Compose setup

### Out of scope
- Champions League, Europa League, national teams
- SCD Type 2 for player-team relationships (Phase 1 uses season snapshots)
- Career trajectory models
- Dashboard or visualization layer
- >3 seasons of history

## Data model

### Fact tables

**`fct_player_match_stats`** — grain: one row per player per match
- `player_key` (FK → dim_player)
- `match_key` (FK → dim_match)
- `team_key` (FK → dim_team)
- `competition_key` (FK → dim_competition)
- `season_key` (FK → dim_season)
- Minutes played, goals, assists, shots, shots on target, passes, pass completion %, tackles, interceptions, fouls, cards (from FBref)
- xG, xA, key passes (from Understat, joined via entity resolution)
- `_sources` (array or bitmask indicating which sources contributed to this row)

**`fct_player_season_valuations`** — grain: one row per player per season per valuation date
- `player_key`, `season_key`, `team_key`
- Market value (EUR), valuation date (from Transfermarkt)

**`fct_team_match_stats`** — grain: one row per team per match
- Possession %, pass accuracy, shots, shots on target, xG, corners, fouls
- Derived from aggregation of player stats + team-level source data

### Dimension tables

**`dim_player`**
- `player_key` (surrogate), `canonical_player_id`
- Full name, short name, date of birth, nationality, primary position, secondary position
- Source ID mappings (fbref_id, understat_id, transfermarkt_id)

**`dim_team`**
- `team_key` (surrogate), `canonical_team_id`
- Full name, short name, country, league
- Source ID mappings

**`dim_competition`**
- Competition name, country, tier

**`dim_match`**
- `match_key` (surrogate)
- Date, home team key, away team key, competition key, season key
- Final score, matchweek/round

**`dim_season`**
- Season label (e.g., "2023-2024"), start date, end date

## Entity resolution

### Player resolution

**Blocking keys** (to reduce comparison space):
- Normalized last name (transliterated to ASCII, lowercased)
- Active in same league + season

**Matching signals:**
- Jaro-Winkler similarity on full name (normalized) — primary signal
- Exact match on date of birth (when available) — strong confirming signal
- Position match (fuzzy — "Forward" ≈ "Centre-Forward") — weak signal
- Team overlap in same season — moderate signal

**Output:** `int_player_xref` table with confidence scores. High-confidence matches auto-merge. Low-confidence matches go to a manual review seed file (`seeds/player_match_overrides.csv`).

The resolution logic is deterministic and reproducible. No ML models, no non-deterministic steps. A reviewer can trace why any two source records were linked.

### Team resolution

Curated seed file (`seeds/team_name_mappings.csv`) mapping all known name variants per source to a canonical ID. ~100 teams x 3 sources = manageable manually.

## Project structure

```
regista-lens/
├── README.md
├── DECISIONS.md                 # ADR-style log of key design decisions
├── docker-compose.yml
├── pyproject.toml               # uv / pip dependencies
├── .env.example
│
├── ingestion/                   # Python — raw data extraction
│   ├── __init__.py
│   ├── fbref/
│   │   ├── __init__.py
│   │   ├── client.py            # HTTP client with rate limiting, retry, caching
│   │   ├── parsers.py           # HTML → structured data
│   │   └── schemas.py           # Pydantic models for raw data validation
│   ├── understat/
│   │   ├── __init__.py
│   │   ├── client.py
│   │   └── schemas.py
│   ├── transfermarkt/
│   │   ├── __init__.py
│   │   ├── client.py
│   │   └── parsers.py
│   └── common/
│       ├── http.py              # Shared HTTP utilities (retry, backoff, session mgmt)
│       ├── name_utils.py        # Unicode normalization, transliteration
│       └── storage.py           # Write to DuckDB raw tables
│
├── orchestration/               # Dagster
│   ├── __init__.py
│   ├── definitions.py           # Top-level Dagster Definitions object
│   ├── assets/
│   │   ├── raw.py               # Raw ingestion assets (partitioned by season)
│   │   └── dbt.py               # dbt asset group
│   ├── resources.py             # DuckDB resource, HTTP session resource
│   ├── sensors.py               # Optional: freshness sensors
│   └── partitions.py            # Season partition definitions
│
├── transform/                   # dbt project
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── packages.yml             # dbt-utils, dbt-expectations
│   ├── models/
│   │   ├── staging/
│   │   │   ├── fbref/
│   │   │   ├── understat/
│   │   │   └── transfermarkt/
│   │   ├── intermediate/
│   │   │   ├── int_player_xref.sql
│   │   │   ├── int_team_xref.sql
│   │   │   └── _intermediate__models.yml
│   │   └── marts/
│   │       ├── fct_player_match_stats.sql
│   │       ├── fct_player_season_valuations.sql
│   │       ├── fct_team_match_stats.sql
│   │       ├── dim_player.sql
│   │       ├── dim_team.sql
│   │       ├── dim_competition.sql
│   │       ├── dim_match.sql
│   │       ├── dim_season.sql
│   │       └── _marts__models.yml
│   ├── seeds/
│   │   ├── team_name_mappings.csv
│   │   └── player_match_overrides.csv
│   ├── tests/
│   │   └── generic/             # Custom generic tests
│   └── macros/
│       └── name_normalization.sql
│
├── tests/                       # Python tests
│   ├── unit/
│   │   ├── test_parsers.py
│   │   ├── test_name_utils.py
│   │   └── test_schemas.py
│   └── integration/
│       └── test_ingestion.py
│
└── docs/
    └── data_model.md            # ER diagram source (Mermaid or similar)
```

## Coding standards

### Python
- Type hints on all functions and methods.
- Pydantic models for all data crossing boundaries (API responses, raw table schemas).
- Explicit error handling — no bare `except`. Ingestion failures for one entity do not crash the pipeline.
- Logging with `structlog` — structured, not print statements.
- Tests for all parsers and name normalization. Parsers are the most brittle code (HTML scraping); tested with saved fixture HTML.

### dbt
- Every model has a `.yml` file with descriptions, column descriptions, and tests.
- Use `ref()` and `source()` exclusively — no hardcoded table names.
- CTEs over subqueries. One logical step per CTE, named clearly.
- `unique` and `not_null` tests on all primary keys. `relationships` tests on all foreign keys.
- Custom data quality tests: xG in [0, 1] range per shot, match dates within season bounds, no orphaned player keys.

### Dagster
- Assets, not ops. Each asset corresponds to a table or dbt model group.
- Season partitions defined once, shared across all raw assets.
- Resources for shared state (DuckDB connection, HTTP session with rate limiting).
- `@asset_check` for data freshness and row count anomaly detection.
- Backfill must work: running a historical season partition pulls and loads that season's data idempotently.

### General
- No secrets in code or config files. `.env` for local config, `.env.example` committed.
- `Makefile` commands for common operations: `make ingest-season SEASON=2023-2024`, `make dbt-run`, `make test`, `make up` (Docker Compose).

## Source access and rate limiting

All three sources are publicly accessible websites. The ingestion layer respects each source's rate limits and terms:

- **FBref:** Minimum 3-second delay between requests. FBref explicitly rate-limits aggressive clients.
- **Transfermarkt:** Requires realistic User-Agent headers. Conservative 5s+ delay between requests.
- **Understat:** JSON endpoints are lighter on server load, but still accessed with reasonable delays.
- Raw HTML/JSON responses are cached locally (`.cache/` directory, gitignored) during development to minimize requests to sources.
- `robots.txt` is respected on all sources.
