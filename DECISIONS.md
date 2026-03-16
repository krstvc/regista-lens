# Architecture Decision Records

## ADR-001: DuckDB over PostgreSQL / SQLite

**Date:** 2026-03-15

**Context:** We need an analytical database for a local-first data warehouse. The dataset is large enough to benefit from columnar storage (millions of rows across player match stats, valuations, and xG data) but not large enough to warrant a distributed system. The project must be easy to set up — ideally zero external services.

**Decision:** Use DuckDB as the storage engine.

**Rationale:**
- **Embedded, zero-config.** No server process to manage. The database is a single file on disk, which simplifies Docker setup (just a volume mount) and local development.
- **Columnar storage.** Analytical queries (aggregations, filters, joins across large fact tables) are the primary workload. DuckDB's columnar engine is purpose-built for this, unlike SQLite's row-oriented storage.
- **SQL dialect.** DuckDB supports a modern SQL dialect with window functions, CTEs, `UNNEST`, `LIST`, and direct Parquet/CSV reads — useful for ad-hoc exploration.
- **dbt integration.** `dbt-duckdb` is mature and well-maintained, making it a natural fit for the transformation layer.

**Alternatives considered:**
- **PostgreSQL** — excellent query engine, but requires running a server. Adds Docker Compose complexity and operational overhead for a project that doesn't need concurrent writes or network access. Overkill for a single-user local warehouse.
- **SQLite** — embedded and simple, but row-oriented storage is a poor fit for analytical queries. No native support for complex types (arrays, structs). Analytical query performance degrades significantly at the data volumes we're targeting.

**Tradeoffs:**
- DuckDB is single-writer. Concurrent writes from multiple processes would conflict. This is acceptable because our pipeline writes sequentially (ingestion → dbt), and only one Dagster run writes at a time.
- DuckDB is less battle-tested in production than PostgreSQL. Acceptable for a portfolio/analytics project, not for a transactional system.

---

## ADR-002: Dagster over Airflow

**Date:** 2026-03-15

**Context:** We need an orchestrator to manage ingestion jobs (partitioned by season), trigger dbt runs, and provide observability into pipeline health. The orchestrator must support partitioned backfills and have a web UI for monitoring.

**Decision:** Use Dagster as the orchestration layer.

**Rationale:**
- **Asset-based model.** Dagster's core abstraction is the software-defined asset, which maps directly to our warehouse tables. Each raw table, staging model, and mart is an asset with explicit dependencies. This is a more natural fit than Airflow's task-based DAGs for a data warehouse project.
- **First-class partitioning.** Season-based partitions are declared once and shared across all assets. Backfilling a historical season is a single UI action, not a custom script.
- **dbt integration.** `dagster-dbt` loads dbt models as Dagster assets automatically, preserving lineage through the full pipeline (ingestion → staging → marts).
- **Developer experience.** `dagster dev` provides a local web UI with asset lineage graphs, run history, and partition status out of the box. Faster feedback loop than Airflow during development.
- **Resource system.** Shared state (DuckDB connection, HTTP clients with rate limiting) is modeled as resources, injected into assets. Cleaner than Airflow's connection/hook pattern for this use case.

**Alternatives considered:**
- **Airflow** — industry standard, massive ecosystem. But its task-based DAG model is a poor fit for asset-centric data pipelines. Partition support exists but is bolted on. Local development requires more ceremony (scheduler, webserver, database). For a greenfield project with no legacy Airflow DAGs, the operational overhead isn't justified.
- **Prefect** — good developer experience and Pythonic API. But its asset/lineage story is weaker than Dagster's, and dbt integration is less mature.
- **No orchestrator (cron + scripts)** — simpler, but loses partition management, retry logic, run history, and lineage visualization. Not appropriate for a project that aims to demonstrate production-grade data engineering.

**Tradeoffs:**
- Dagster has a smaller community and job market presence than Airflow. This is a portfolio project, so demonstrating modern tooling is more valuable than using the incumbent.
- Dagster's learning curve is steeper for engineers coming from Airflow. The asset model requires a mental shift from "schedule tasks" to "declare data assets and their dependencies."

---

## ADR-003: Season-level partitioning

**Date:** 2026-03-15

**Context:** Raw data needs to be partitioned for incremental ingestion and idempotent backfills. FBref organizes stats pages by league-season (one page per league per season). We need to decide the partitioning grain: daily, monthly, or season-level.

**Decision:** Partition all raw assets by season (e.g., `2023-2024`).

**Rationale:**
- **Matches source grain.** FBref's Standard Stats pages are organized by season. One HTTP request returns the full season's data for a league. Season is the natural unit of work for ingestion.
- **Minimal partition count.** Phase 1 has 3 seasons — 3 partitions total. This keeps the Dagster UI clean and backfills trivial. Daily partitioning would create ~1,000+ partitions with no benefit since the source data doesn't update daily.
- **Idempotent replacement.** Re-running a season partition deletes and re-inserts all data for that season. At this data volume (~2,500 player-season rows per partition across 5 leagues), full replacement is cheap and avoids complex merge logic.

**Alternatives considered:**
- **Daily partitioning** — useful if data changed daily, but FBref stats pages are cumulative season tables. No benefit to daily granularity.
- **Monthly partitioning** — middle ground, but still doesn't match the source's natural grain. Adds partition management complexity for no gain.

**Tradeoffs:**
- Mid-season updates require re-ingesting the entire season. Acceptable at this volume (15 HTTP requests for a full season backfill across 5 leagues).
- If match-level stats are added later, a finer partition grain (by matchweek) might make sense for that specific asset, but season remains appropriate for aggregated stats.

---

## ADR-004: Sync httpx over async for ingestion

**Date:** 2026-03-15

**Context:** The ingestion layer makes HTTP requests to FBref, Understat, and Transfermarkt. We need to decide whether to use sync or async HTTP clients.

**Decision:** Use synchronous `httpx.Client`, not async.

**Rationale:**
- **Rate limiting is the bottleneck.** FBref requires 3s between requests, Transfermarkt 5s+. With mandatory delays between requests, there are no concurrent requests to benefit from async.
- **Dagster assets run synchronously.** Dagster's `@asset` functions are sync by default. Using async would require wrapping in `asyncio.run()` or using Dagster's experimental async support — added complexity with no throughput benefit.
- **Simpler code.** Sync code is easier to read, debug, and test. No `async/await` ceremony, no event loop management, no async context managers.

**Alternatives considered:**
- **Async httpx** — would enable concurrent requests to different sources. But rate limiting means we can't actually make concurrent requests to the same source, and cross-source parallelism is better handled at the Dagster level (separate assets run independently).

**Tradeoffs:**
- If we ever needed to make many concurrent requests (e.g., fetching hundreds of match pages from different sources simultaneously), async would help. But per-source rate limits mean this is unlikely to matter in practice.

---

## ADR-005: Player season stats as end-to-end minimal slice

**Date:** 2026-03-15

**Context:** The CLAUDE.md sequencing calls for proving one source end-to-end before adding complexity. We need to choose which FBref data to start with.

**Decision:** Start with player season stats (Standard Stats table), not match-level stats.

**Rationale:**
- **One request per league-season.** A single HTTP request to FBref's Standard Stats page yields ~500 player rows with ~25 stat columns. 5 leagues × 3 seasons = 15 requests for a full backfill. This proves every pipeline layer (ingestion → raw table → staging model → mart table) without crawling hundreds of match pages.
- **Tests the hardest parts.** The parser must handle FBref's HTML quirks (spacer rows, tables in comments, multi-team players, empty cells). The storage layer must handle idempotent partition replacement. The dbt models must handle type casting, name normalization, and surrogate key generation. All of this is exercised with season-level stats.
- **Stepping stone to match-level.** The `fct_player_season_stats` fact table is a useful intermediate artifact. When match-level scraping is added later, it becomes the aggregation target for validation (season totals should match sum of match stats).

**Alternatives considered:**
- **Match-level stats first** — more aligned with the target `fct_player_match_stats` schema, but requires crawling hundreds of match pages per season (5 leagues × ~380 matches × 2 pages per match). Too much HTTP complexity for a first proof-of-concept.

**Tradeoffs:**
- `fct_player_season_stats` is not in the final CLAUDE.md schema (which targets match-level grain). It will need to be either replaced or kept as a complementary fact table when match-level ingestion is added.

---

## ADR-006: Entity resolution — deterministic fuzzy matching in pure SQL

**Date:** 2026-03-16

**Context:** With two data sources (FBref and Understat), we need to link player and team records across sources. Player names differ (transliteration, abbreviation, ordering), and neither source provides IDs for the other. We need a reproducible, auditable resolution approach.

**Decision:** Deterministic fuzzy matching implemented entirely in dbt SQL, using DuckDB's built-in `jaro_winkler_similarity()` function. Team resolution via curated seed file; player resolution via blocking + weighted scoring.

**Rationale:**
- **Pure SQL, no Python in the transform layer.** Entity resolution runs as dbt models, maintaining the clean separation between ingestion (Python) and transformation (SQL). No additional runtime dependencies.
- **DuckDB built-ins.** `jaro_winkler_similarity()` is native to DuckDB — no UDFs or extensions needed. Performant enough for our data volume (~2,500 players per league-season).
- **Deterministic and reproducible.** Given the same inputs, the same matches are produced every time. No ML models, no randomness, no non-deterministic steps. A reviewer can trace exactly why any two records were linked by examining the confidence score breakdown (name: 0.6, team: 0.3, position: 0.1).
- **Two-tier approach:** Teams are few (~100) and their name variants are well-known, so a curated seed file is appropriate. Players are numerous (~2,500 per season) with unpredictable name variations, so automated fuzzy matching with a manual override escape hatch is the right trade-off.
- **Blocking for performance.** Comparing every FBref player to every Understat player would be O(n²). Blocking on (normalized last name, league, season) reduces the comparison space by ~100x while missing very few true matches (only cases where last names are transliterated differently, which the seed override handles).

**Alternatives considered:**
- **Python-based matching (e.g., recordlinkage, dedupe)** — more flexible and better for large-scale fuzzy matching, but adds Python to the transform layer, breaks the dbt-only transformation model, and is harder to audit. Overkill for our data volume.
- **ML-based entity resolution** — could improve match quality, but introduces non-determinism, requires training data, and is much harder to debug when matches are wrong. Not justified for this project's scale.
- **Exact-match only** — too many missed matches due to name variations (e.g., "Son Heung-min" vs "Heung-Min Son").

**Tradeoffs:**
- Blocking on last name will miss matches where the last name is transliterated differently across sources (e.g., different romanization of Arabic/Asian names). The manual override seed handles these cases.
- The 0.75 composite score threshold may need tuning as real data reveals edge cases. Players scoring between 0.75 and 0.90 are flagged as `review_needed` for manual inspection.
- The weighted scoring (name 60%, team 30%, position 10%) is a judgment call. Team overlap is weighted heavily because it's a strong signal — if two players share a name and team, they're almost certainly the same person.

---

## ADR-007: Transfermarkt market values page as primary endpoint

**Date:** 2026-03-16

**Context:** Transfermarkt exposes player data across several page types: squad pages (per team), player profiles (per player), and market values pages (per league, paginated). We need to choose which endpoint to scrape for Phase 1.

**Decision:** Use the league market values page (`/marktwerte/wettbewerb/{comp_id}`) as the single Transfermarkt ingestion endpoint.

**Rationale:**
- **Data density.** A single paginated endpoint yields player ID, team ID, position, DOB, nationality, and market value — all the fields needed for both the valuation fact table and entity resolution enrichment. No need to visit individual player or squad pages.
- **Minimal request count.** Each league has ~20-25 pages of 25 players. 5 leagues × ~22 pages = ~110 requests per season. At 5s rate limiting, that's ~9 minutes per season — acceptable for a batch pipeline.
- **Pagination is deterministic.** Total page count is available from the pagination element on page 1, so we know exactly how many requests to make upfront.

**Alternatives considered:**
- **Squad pages** — one page per team (~100 teams × 3 seasons = 300 requests). More granular team-level data, but doesn't include market values directly and requires visiting separate valuation sub-pages.
- **Player profiles** — most detailed data (contract dates, agent info, transfer history), but would require ~2,500+ requests per league-season. Far too many requests for Phase 1.

**Tradeoffs:**
- Market values pages don't include contract end dates or transfer history. These would require player profile scraping in a future phase.
- The market value is a point-in-time snapshot. Transfermarkt updates values periodically, but we only capture whatever value is current at ingestion time. Phase 1 treats this as a season snapshot, which is sufficient for the valuation fact table's grain.
