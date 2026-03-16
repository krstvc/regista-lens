"""dbt assets — loads dbt models as Dagster assets with proper lineage."""

from __future__ import annotations

from pathlib import Path

from dagster import AssetKey
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, DbtProject, dbt_assets

DBT_PROJECT_DIR = Path(__file__).parent.parent.parent / "transform"

dbt_project = DbtProject(project_dir=DBT_PROJECT_DIR)
dbt_project.prepare_if_dev()


class CustomDbtTranslator(DagsterDbtTranslator):
    """Maps dbt sources to Dagster asset keys so lineage connects raw → staging."""

    def get_asset_key(self, dbt_resource_props: dict) -> AssetKey:
        resource_type = dbt_resource_props.get("resource_type", "")

        if resource_type == "source":
            # Map dbt source `raw_fbref.raw_fbref__player_season_stats`
            # to Dagster asset key `raw_fbref__player_season_stats`
            table_name = dbt_resource_props.get("name", "")
            return AssetKey(table_name)

        return super().get_asset_key(dbt_resource_props)


@dbt_assets(
    manifest=dbt_project.manifest_path,
    dagster_dbt_translator=CustomDbtTranslator(),
)
def regista_dbt_assets(context, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
