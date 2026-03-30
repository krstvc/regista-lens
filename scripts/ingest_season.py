"""Materialize raw assets for a given season without a running Dagster instance."""

from __future__ import annotations

import argparse
import os
import sys

from dagster import materialize
from dagster_dbt import DbtCliResource

from orchestration.assets.dbt import DBT_PROJECT_DIR, regista_dbt_assets
from orchestration.assets.raw import (
    raw_fbref__player_season_stats,
    raw_transfermarkt__player_valuations,
    raw_understat__player_season_stats,
)
from orchestration.resources import (
    DuckDBPathResource,
    FbrefClientResource,
    TransfermarktClientResource,
    UnderstatClientResource,
)

VALID_SEASONS = {"2023-2024", "2024-2025", "2025-2026"}


def main() -> int:
    parser = argparse.ArgumentParser(description="Ingest raw data for a season.")
    parser.add_argument("season", help="Season to ingest, e.g. 2023-2024")
    args = parser.parse_args()

    if args.season not in VALID_SEASONS:
        print(f"Invalid season: {args.season}. Must be one of {sorted(VALID_SEASONS)}")
        return 1

    resources = {
        "fbref_client": FbrefClientResource(
            request_delay=float(os.getenv("FBREF_REQUEST_DELAY", "3")),
        ),
        "understat_client": UnderstatClientResource(
            request_delay=float(os.getenv("UNDERSTAT_REQUEST_DELAY", "2")),
        ),
        "transfermarkt_client": TransfermarktClientResource(
            request_delay=float(os.getenv("TRANSFERMARKT_REQUEST_DELAY", "5")),
        ),
        "duckdb_path": DuckDBPathResource(
            path=os.getenv("DUCKDB_PATH", "regista.duckdb"),
        ),
        "dbt": DbtCliResource(project_dir=DBT_PROJECT_DIR),
    }

    result = materialize(
        assets=[
            raw_fbref__player_season_stats,
            raw_understat__player_season_stats,
            raw_transfermarkt__player_valuations,
            regista_dbt_assets,
        ],
        resources=resources,
        partition_key=args.season,
    )

    if not result.success:
        print(f"Ingestion failed for season {args.season}")
        return 1

    print(f"Successfully ingested season {args.season}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
