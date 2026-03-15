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
