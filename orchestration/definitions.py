"""Top-level Dagster Definitions — wires assets, resources, and schedules."""

from __future__ import annotations

import os

from dagster import Definitions
from dagster_dbt import DbtCliResource

from orchestration.assets.dbt import DBT_PROJECT_DIR, regista_dbt_assets
from orchestration.assets.raw import raw_fbref__player_season_stats
from orchestration.resources import FbrefClientResource

defs = Definitions(
    assets=[
        raw_fbref__player_season_stats,
        regista_dbt_assets,
    ],
    resources={
        "fbref_client": FbrefClientResource(
            request_delay=float(os.getenv("FBREF_REQUEST_DELAY", "3")),
        ),
        "duckdb_path": os.getenv("DUCKDB_PATH", "regista.duckdb"),
        "dbt": DbtCliResource(project_dir=DBT_PROJECT_DIR),
    },
)
