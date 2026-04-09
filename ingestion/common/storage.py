"""DuckDB raw table writer with idempotent partition replacement."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import duckdb
import pandas as pd
import structlog

logger = structlog.get_logger()


def _derive_schema(table_name: str) -> str:
    """Derive the DuckDB schema from the raw table name.

    Convention: ``raw_{source}__{entity}`` → schema ``raw_{source}``.
    """
    parts = table_name.split("__", 1)
    return parts[0] if len(parts) == 2 else "main"


def write_raw_table(
    db_path: str,
    table_name: str,
    records: list[dict[str, Any]],
    season: str,
    source_url: str,
) -> int:
    """Write records to a raw DuckDB table with idempotent partition replacement.

    Adds metadata columns: _ingested_at, _source_url, _season.
    Deletes existing rows for the same season before inserting (idempotent).
    Tables are written into a schema derived from the table name
    (e.g. ``raw_fbref__player_season_stats`` → schema ``raw_fbref``).

    Returns the number of rows inserted.
    """
    if not records:
        logger.warning("write_raw_table_empty", table=table_name, season=season)
        return 0

    now = datetime.now(tz=UTC).isoformat()
    for record in records:
        record["_ingested_at"] = now
        record["_source_url"] = source_url
        record["_season"] = season

    schema = _derive_schema(table_name)
    qualified_name = f"{schema}.{table_name}"

    con = duckdb.connect(db_path)
    try:
        con.execute("BEGIN TRANSACTION")
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

        # Register records as a named view so DuckDB SQL can reference it
        df = pd.DataFrame(records)
        con.register("_records_view", df)

        table_exists = (
            con.execute(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_schema = ? AND table_name = ?",
                [schema, table_name],
            ).fetchone()[0]
            > 0
        )

        if not table_exists:
            con.execute(
                f"CREATE TABLE {qualified_name} AS SELECT * FROM _records_view WHERE 1=0"
            )
            logger.info("created_table", table=qualified_name)

        # Idempotent: delete existing partition data
        deleted = con.execute(
            f"DELETE FROM {qualified_name} WHERE _season = ?",  # noqa: S608
            [season],
        ).fetchone()[0]
        if deleted:
            logger.info("deleted_partition", table=qualified_name, season=season, rows=deleted)

        # Insert new data
        con.execute(f"INSERT INTO {qualified_name} SELECT * FROM _records_view")  # noqa: S608

        con.execute("COMMIT")
        logger.info("wrote_raw_table", table=qualified_name, season=season, rows=len(records))
        return len(records)
    except Exception:
        con.execute("ROLLBACK")
        raise
    finally:
        con.unregister("_records_view")
        con.close()
