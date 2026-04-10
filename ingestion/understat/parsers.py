"""Parsers for Understat league pages — extract hex-encoded JSON from script tags."""

from __future__ import annotations

import json
import re

import structlog

from ingestion.understat.schemas import UnderstatPlayerSeasonStatsRaw

logger = structlog.get_logger()

_PLAYERS_DATA_PATTERN = re.compile(r"var\s+playersData\s*=\s*JSON\.parse\('(.+?)'\)")

_HEX_ESCAPE_PATTERN = re.compile(r"\\x([0-9a-fA-F]{2})")

# Mapping from Understat JSON keys to our schema field names
_FIELD_MAP: dict[str, str] = {
    "id": "player_id",
    "player_name": "player_name",
    "team_title": "team_name",
    "position": "position",
    "games": "games",
    "time": "minutes",
    "goals": "goals",
    "assists": "assists",
    "npg": "npg",
    "xG": "xg",
    "xA": "xg_assist",
    "npxG": "npxg",
    "xGChain": "xg_chain",
    "xGBuildup": "xg_buildup",
    "shots": "shots",
    "key_passes": "key_passes",
    "yellow_cards": "yellow_cards",
    "red_cards": "red_cards",
}

_INT_FIELDS = {
    "games",
    "minutes",
    "goals",
    "assists",
    "npg",
    "shots",
    "key_passes",
    "yellow_cards",
    "red_cards",
}

_FLOAT_FIELDS = {"xg", "xg_assist", "npxg", "xg_chain", "xg_buildup"}


def _decode_hex_escapes(encoded: str) -> str:
    """Decode ``\\xHH`` escape sequences to their character equivalents."""
    return _HEX_ESCAPE_PATTERN.sub(lambda m: chr(int(m.group(1), 16)), encoded)


def _coerce_value(value: str | None, field_name: str) -> int | float | str | None:
    """Coerce a string value to the appropriate type."""
    if value is None or value == "":
        return None
    if field_name in _INT_FIELDS:
        try:
            return int(value)
        except ValueError:
            # Understat sometimes stores ints as floats (e.g. "3.0")
            try:
                return int(float(value))
            except ValueError:
                return None
    if field_name in _FLOAT_FIELDS:
        try:
            return float(value)
        except ValueError:
            return None
    return value


def parse_player_season_stats(
    html: str,
    league: str,
    season: str,
) -> list[UnderstatPlayerSeasonStatsRaw]:
    """Parse Understat league page HTML to extract player season stats.

    Understat embeds player data as hex-encoded JSON inside a ``<script>`` tag::

        var playersData = JSON.parse('\\x5B\\x7B...')

    This function finds the encoded string, decodes ``\\xHH`` sequences, parses
    the JSON, and returns validated Pydantic models.

    Args:
        html: Raw HTML content of the Understat league page.
        league: League name (e.g., "Premier League").
        season: Season string (e.g., "2023-2024").

    Returns:
        List of validated player season stats records.
    """
    match = _PLAYERS_DATA_PATTERN.search(html)
    if match is None:
        logger.error("understat_players_data_not_found", league=league, season=season)
        return []

    encoded_json = match.group(1)
    decoded_json = _decode_hex_escapes(encoded_json)

    try:
        raw_data = json.loads(decoded_json)
    except json.JSONDecodeError as e:
        logger.error("understat_json_parse_error", league=league, season=season, error=str(e))
        return []

    if not isinstance(raw_data, list):
        logger.error("understat_unexpected_format", league=league, season=season)
        return []

    records: list[UnderstatPlayerSeasonStatsRaw] = []
    for player_data in raw_data:
        fields: dict[str, int | float | str | None] = {}
        for json_key, schema_field in _FIELD_MAP.items():
            raw_value = player_data.get(json_key)
            fields[schema_field] = _coerce_value(
                str(raw_value) if raw_value is not None else None,
                schema_field,
            )

        fields["league"] = league
        fields["season"] = season

        try:
            record = UnderstatPlayerSeasonStatsRaw(**fields)
            records.append(record)
        except Exception as e:
            logger.warning(
                "understat_record_validation_error",
                player=player_data.get("player_name"),
                error=str(e),
            )

    logger.info(
        "parsed_understat_player_season_stats",
        league=league,
        season=season,
        count=len(records),
    )
    return records


def parse_player_season_stats_json(
    json_str: str,
    league: str,
    season: str,
) -> list[UnderstatPlayerSeasonStatsRaw]:
    """Parse player season stats from a JSON string (extracted via JS evaluation).

    This is the preferred entry point when using a browser client that
    evaluates ``JSON.stringify(playersData)`` on the page.
    """
    if not json_str:
        logger.error("understat_empty_json", league=league, season=season)
        return []

    try:
        raw_data = json.loads(json_str)
    except json.JSONDecodeError as e:
        logger.error("understat_json_parse_error", league=league, season=season, error=str(e))
        return []

    if not isinstance(raw_data, list):
        logger.error("understat_unexpected_format", league=league, season=season)
        return []

    records: list[UnderstatPlayerSeasonStatsRaw] = []
    for player_data in raw_data:
        fields: dict[str, int | float | str | None] = {}
        for json_key, schema_field in _FIELD_MAP.items():
            raw_value = player_data.get(json_key)
            fields[schema_field] = _coerce_value(
                str(raw_value) if raw_value is not None else None,
                schema_field,
            )

        fields["league"] = league
        fields["season"] = season

        try:
            record = UnderstatPlayerSeasonStatsRaw(**fields)
            records.append(record)
        except Exception as e:
            logger.warning(
                "understat_record_validation_error",
                player=player_data.get("player_name"),
                error=str(e),
            )

    logger.info(
        "parsed_understat_player_season_stats",
        league=league,
        season=season,
        count=len(records),
    )
    return records
