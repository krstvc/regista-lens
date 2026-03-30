"""Top-level Dagster Definitions — wires assets, resources, and schedules."""

from __future__ import annotations

import os

from dagster import Definitions
from dagster_dbt import DbtCliResource

from orchestration.assets.dbt import DBT_PROJECT_DIR, regista_dbt_assets
from orchestration.assets.raw import (
    raw_fbref__player_season_stats,
    raw_transfermarkt__player_valuations,
    raw_understat__player_season_stats,
)
from orchestration.checks import (
    check_mart_row_counts,
    check_player_xref_match_rate,
    check_raw_fbref_row_count,
    check_raw_transfermarkt_row_count,
    check_raw_understat_row_count,
)
from orchestration.resources import (
    FbrefClientResource,
    TransfermarktClientResource,
    UnderstatClientResource,
)

defs = Definitions(
    assets=[
        raw_fbref__player_season_stats,
        raw_understat__player_season_stats,
        raw_transfermarkt__player_valuations,
        regista_dbt_assets,
    ],
    asset_checks=[
        check_raw_fbref_row_count,
        check_raw_understat_row_count,
        check_raw_transfermarkt_row_count,
        check_player_xref_match_rate,
        check_mart_row_counts,
    ],
    resources={
        "fbref_client": FbrefClientResource(
            request_delay=float(os.getenv("FBREF_REQUEST_DELAY", "3")),
        ),
        "understat_client": UnderstatClientResource(
            request_delay=float(os.getenv("UNDERSTAT_REQUEST_DELAY", "2")),
        ),
        "transfermarkt_client": TransfermarktClientResource(
            request_delay=float(os.getenv("TRANSFERMARKT_REQUEST_DELAY", "5")),
        ),
        "duckdb_path": os.getenv("DUCKDB_PATH", "regista.duckdb"),
        "dbt": DbtCliResource(project_dir=DBT_PROJECT_DIR),
    },
)
