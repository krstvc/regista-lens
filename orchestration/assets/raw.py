"""Raw ingestion assets — partitioned by season."""

from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, asset

from ingestion.common.storage import write_raw_table
from ingestion.fbref.client import LEAGUES
from ingestion.understat.client import LEAGUES as UNDERSTAT_LEAGUES
from orchestration.partitions import SEASON_PARTITIONS
from orchestration.resources import FbrefClientResource, UnderstatClientResource


@asset(
    partitions_def=SEASON_PARTITIONS,
    group_name="raw",
    kinds={"python", "duckdb"},
    description=(
        "Player season stats scraped from FBref Standard Stats tables, "
        "one row per player per team per season."
    ),
)
def raw_fbref__player_season_stats(
    context: AssetExecutionContext,
    fbref_client: FbrefClientResource,
    duckdb_path: str,
) -> MaterializeResult:
    """Fetch player season stats from FBref for all 5 leagues in the given season."""
    season = context.partition_key
    client = fbref_client.get_client()

    all_records: list[dict] = []
    source_urls: list[str] = []

    try:
        for league in LEAGUES:
            context.log.info(f"Fetching {league} {season}")
            records, url = client.fetch_player_season_stats(league, season)
            source_urls.append(url)
            for record in records:
                all_records.append(record.model_dump())
            context.log.info(f"  → {len(records)} players")
    finally:
        client.close()

    row_count = write_raw_table(
        db_path=duckdb_path,
        table_name="raw_fbref__player_season_stats",
        records=all_records,
        season=season,
        source_url="; ".join(source_urls),
    )

    return MaterializeResult(
        metadata={
            "row_count": MetadataValue.int(row_count),
            "leagues": MetadataValue.text(", ".join(LEAGUES)),
            "season": MetadataValue.text(season),
        },
    )


@asset(
    partitions_def=SEASON_PARTITIONS,
    group_name="raw",
    kinds={"python", "duckdb"},
    description=(
        "Player season stats from Understat league pages, "
        "one row per player per team per season with xG/xA metrics."
    ),
)
def raw_understat__player_season_stats(
    context: AssetExecutionContext,
    understat_client: UnderstatClientResource,
    duckdb_path: str,
) -> MaterializeResult:
    """Fetch player season stats from Understat for all 5 leagues in the given season."""
    season = context.partition_key
    client = understat_client.get_client()

    all_records: list[dict] = []
    source_urls: list[str] = []

    try:
        for league in UNDERSTAT_LEAGUES:
            context.log.info(f"Fetching {league} {season}")
            records, url = client.fetch_player_season_stats(league, season)
            source_urls.append(url)
            for record in records:
                all_records.append(record.model_dump())
            context.log.info(f"  → {len(records)} players")
    finally:
        client.close()

    row_count = write_raw_table(
        db_path=duckdb_path,
        table_name="raw_understat__player_season_stats",
        records=all_records,
        season=season,
        source_url="; ".join(source_urls),
    )

    return MaterializeResult(
        metadata={
            "row_count": MetadataValue.int(row_count),
            "leagues": MetadataValue.text(", ".join(UNDERSTAT_LEAGUES)),
            "season": MetadataValue.text(season),
        },
    )
