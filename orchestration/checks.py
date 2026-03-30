"""Dagster asset checks for data quality validation."""

from __future__ import annotations

import duckdb
from dagster import AssetCheckResult, AssetCheckSeverity, asset_check

from orchestration.assets.raw import (
    raw_fbref__player_season_stats,
    raw_transfermarkt__player_valuations,
    raw_understat__player_season_stats,
)


def _row_count(db_path: str, table: str) -> int:
    """Return row count for a table, or 0 if it does not exist."""
    con = duckdb.connect(db_path, read_only=True)
    try:
        result = con.execute(f"select count(*) from {table}").fetchone()  # noqa: S608
        return result[0] if result else 0
    except duckdb.CatalogException:
        return 0
    finally:
        con.close()


@asset_check(asset=raw_fbref__player_season_stats, blocking=True)
def check_raw_fbref_row_count(duckdb_path: str) -> AssetCheckResult:
    """Raw FBref table must have rows after materialization."""
    count = _row_count(duckdb_path, "raw_fbref__player_season_stats")
    return AssetCheckResult(
        passed=count > 0,
        metadata={"row_count": count},
        severity=AssetCheckSeverity.ERROR,
    )


@asset_check(asset=raw_understat__player_season_stats, blocking=True)
def check_raw_understat_row_count(duckdb_path: str) -> AssetCheckResult:
    """Raw Understat table must have rows after materialization."""
    count = _row_count(duckdb_path, "raw_understat__player_season_stats")
    return AssetCheckResult(
        passed=count > 0,
        metadata={"row_count": count},
        severity=AssetCheckSeverity.ERROR,
    )


@asset_check(asset=raw_transfermarkt__player_valuations, blocking=True)
def check_raw_transfermarkt_row_count(duckdb_path: str) -> AssetCheckResult:
    """Raw Transfermarkt table must have rows after materialization."""
    count = _row_count(duckdb_path, "raw_transfermarkt__player_valuations")
    return AssetCheckResult(
        passed=count > 0,
        metadata={"row_count": count},
        severity=AssetCheckSeverity.ERROR,
    )


@asset_check(
    asset=raw_fbref__player_season_stats,
    description="Entity resolution match rate FBref to Understat >= 50%",
)
def check_player_xref_match_rate(duckdb_path: str) -> AssetCheckResult:
    """Verify that at least 50% of FBref players have an Understat match."""
    con = duckdb.connect(duckdb_path, read_only=True)
    try:
        result = con.execute("""
            select
                count(distinct fbref_player_id) as matched,
                (select count(distinct fbref_player_id)
                 from stg_fbref__player_season_stats) as total
            from int_player_xref
            where understat_player_id is not null
        """).fetchone()

        if result is None or result[1] == 0:
            return AssetCheckResult(
                passed=False,
                metadata={"detail": "No FBref players found"},
                severity=AssetCheckSeverity.WARN,
            )

        matched, total = result
        rate = matched / total
        return AssetCheckResult(
            passed=rate >= 0.5,
            metadata={"matched": matched, "total": total, "match_rate": round(rate, 3)},
            severity=AssetCheckSeverity.WARN,
        )
    except duckdb.CatalogException:
        return AssetCheckResult(
            passed=False,
            metadata={"detail": "xref or staging table not found — run dbt first"},
            severity=AssetCheckSeverity.WARN,
        )
    finally:
        con.close()


@asset_check(asset=raw_fbref__player_season_stats, description="Mart fact tables have rows")
def check_mart_row_counts(duckdb_path: str) -> AssetCheckResult:
    """Verify mart fact tables are populated (joins not broken)."""
    con = duckdb.connect(duckdb_path, read_only=True)
    try:
        counts = {}
        for table in ("fct_player_season_stats", "fct_player_season_valuations"):
            try:
                row = con.execute(f"select count(*) from {table}").fetchone()  # noqa: S608
                counts[table] = row[0] if row else 0
            except duckdb.CatalogException:
                counts[table] = 0

        all_populated = all(c > 0 for c in counts.values())
        return AssetCheckResult(
            passed=all_populated,
            metadata=counts,
            severity=AssetCheckSeverity.WARN,
        )
    finally:
        con.close()
