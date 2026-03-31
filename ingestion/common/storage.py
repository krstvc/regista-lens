"""DuckDB raw table writer with idempotent partition replacement."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import duckdb
import pandas as pd
import structlog

logger = structlog.get_logger()


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

    con = duckdb.connect(db_path)
    try:
        con.execute("BEGIN TRANSACTION")

        # Register records as a named view so DuckDB SQL can reference it
        df = pd.DataFrame(records)
        con.register("_records_view", df)

        table_exists = (
            con.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
                [table_name],
            ).fetchone()[0]
            > 0
        )

        if not table_exists:
            con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM _records_view WHERE 1=0")
            logger.info("created_table", table=table_name)

        # Idempotent: delete existing partition data
        deleted = con.execute(
            f"DELETE FROM {table_name} WHERE _season = ?",  # noqa: S608
            [season],
        ).fetchone()[0]
        if deleted:
            logger.info("deleted_partition", table=table_name, season=season, rows=deleted)

        # Insert new data
        con.execute(f"INSERT INTO {table_name} SELECT * FROM _records_view")  # noqa: S608

        con.execute("COMMIT")
        logger.info("wrote_raw_table", table=table_name, season=season, rows=len(records))
        return len(records)
    except Exception:
        con.execute("ROLLBACK")
        raise
    finally:
        con.unregister("_records_view")
        con.close()
