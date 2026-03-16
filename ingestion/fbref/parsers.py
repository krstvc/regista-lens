"""HTML parsers for FBref pages."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

import structlog
from bs4 import BeautifulSoup, Comment

from ingestion.fbref.schemas import FbrefPlayerSeasonStatsRaw

if TYPE_CHECKING:
    from bs4 import Tag

logger = structlog.get_logger()

# Mapping from FBref data-stat attribute to our schema field names
_STAT_FIELD_MAP: dict[str, str] = {
    "nationality": "nationality",
    "position": "position",
    "age": "age",
    "games": "games",
    "games_starts": "games_starts",
    "minutes": "minutes",
    "goals": "goals",
    "assists": "assists",
    "goals_pens": "goals_pens",
    "pens_made": "pens_made",
    "pens_att": "pens_att",
    "cards_yellow": "cards_yellow",
    "cards_red": "cards_red",
    "xg": "xg",
    "npxg": "npxg",
    "xg_assist": "xg_assist",
    "goals_per90": "goals_per90",
    "assists_per90": "assists_per90",
    "goals_assists_per90": "goals_assists_per90",
    "goals_pens_per90": "goals_pens_per90",
    "goals_assists_pens_per90": "goals_assists_pens_per90",
    "xg_per90": "xg_per90",
    "xg_assist_per90": "xg_assist_per90",
    "xg_xg_assist_per90": "xg_xg_assist_per90",
    "npxg_per90": "npxg_per90",
    "npxg_xg_assist_per90": "npxg_xg_assist_per90",
}

_INT_FIELDS = {
    "games",
    "games_starts",
    "minutes",
    "goals",
    "assists",
    "goals_pens",
    "pens_made",
    "pens_att",
    "cards_yellow",
    "cards_red",
}

_FLOAT_FIELDS = {
    "xg",
    "npxg",
    "xg_assist",
    "goals_per90",
    "assists_per90",
    "goals_assists_per90",
    "goals_pens_per90",
    "goals_assists_pens_per90",
    "xg_per90",
    "xg_assist_per90",
    "xg_xg_assist_per90",
    "npxg_per90",
    "npxg_xg_assist_per90",
}

_PLAYER_ID_PATTERN = re.compile(r"/players/([a-f0-9]+)/")
_TEAM_ID_PATTERN = re.compile(r"/squads/([a-f0-9]+)/")


def _find_stats_table(soup: BeautifulSoup, table_id: str = "stats_standard") -> Tag | None:
    """Find a stats table by ID, checking HTML comments if not in main DOM."""
    table = soup.find("table", id=table_id)
    if table is not None:
        return table

    # FBref wraps some tables in HTML comments
    for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
        if table_id in comment:
            comment_soup = BeautifulSoup(comment, "lxml")
            table = comment_soup.find("table", id=table_id)
            if table is not None:
                return table

    return None


def _extract_cell_text(row: Tag, data_stat: str) -> str | None:
    """Extract text from a cell identified by data-stat attribute."""
    cell = row.find(["td", "th"], attrs={"data-stat": data_stat})
    if cell is None:
        return None
    text = cell.get_text(strip=True)
    return text if text else None


def _extract_player_id(row: Tag) -> str | None:
    """Extract FBref player ID from the player link href."""
    cell = row.find(["td", "th"], attrs={"data-stat": "player"})
    if cell is None:
        return None
    link = cell.find("a", href=True)
    if link is None:
        return None
    match = _PLAYER_ID_PATTERN.search(link["href"])
    return match.group(1) if match else None


def _extract_team_id(row: Tag) -> str | None:
    """Extract FBref team ID from the team link href."""
    cell = row.find(["td", "th"], attrs={"data-stat": "team"})
    if cell is None:
        return None
    link = cell.find("a", href=True)
    if link is None:
        return None
    match = _TEAM_ID_PATTERN.search(link["href"])
    return match.group(1) if match else None


def _parse_stat_value(value: str | None, field_name: str) -> int | float | str | None:
    """Parse a stat value to the appropriate type."""
    if value is None:
        return None

    # Strip commas from numbers (e.g. "1,234")
    clean = value.replace(",", "")

    if field_name in _INT_FIELDS:
        try:
            return int(clean)
        except ValueError:
            return None
    if field_name in _FLOAT_FIELDS:
        try:
            return float(clean)
        except ValueError:
            return None
    return value


def parse_player_season_stats(
    html: str,
    league: str,
    season: str,
) -> list[FbrefPlayerSeasonStatsRaw]:
    """Parse FBref Standard Stats table into validated Pydantic models.

    Args:
        html: Raw HTML content of the FBref stats page.
        league: League name (e.g., "Premier League").
        season: Season string (e.g., "2023-2024").

    Returns:
        List of validated player season stats records.
    """
    soup = BeautifulSoup(html, "lxml")
    table = _find_stats_table(soup)

    if table is None:
        logger.error("stats_table_not_found", league=league, season=season)
        return []

    tbody = table.find("tbody")
    if tbody is None:
        logger.error("stats_tbody_not_found", league=league, season=season)
        return []

    records: list[FbrefPlayerSeasonStatsRaw] = []

    for row in tbody.find_all("tr"):
        # Skip spacer/header rows
        if "thead" in row.get("class", []) or row.find("th", attrs={"data-stat": "ranker"}) is None:
            continue

        player_id = _extract_player_id(row)
        player_name = _extract_cell_text(row, "player")
        team_name = _extract_cell_text(row, "team")
        team_id = _extract_team_id(row)

        if not player_id or not player_name:
            continue

        # Detect multi-team rows
        is_multi_team_total = False
        is_multi_team_row = False
        if team_name and re.search(r"\d+\s+Clubs?", team_name):
            is_multi_team_total = True
            team_id = team_id or ""
            team_name = team_name

        # Check if this is an individual team row for a multi-team player
        # (these rows don't have a ranker number)
        ranker_cell = row.find("th", attrs={"data-stat": "ranker"})
        if ranker_cell and not ranker_cell.get_text(strip=True):
            is_multi_team_row = True

        # Build stat dict
        stats: dict[str, int | float | str | None] = {}
        for data_stat, field_name in _STAT_FIELD_MAP.items():
            raw_value = _extract_cell_text(row, data_stat)
            stats[field_name] = _parse_stat_value(raw_value, field_name)

        # Handle nationality — FBref puts flag code, extract text
        nationality_cell = row.find("td", attrs={"data-stat": "nationality"})
        if nationality_cell:
            # Nationality is often in a span or as text after flag
            nat_text = nationality_cell.get_text(strip=True)
            stats["nationality"] = nat_text if nat_text else None

        record = FbrefPlayerSeasonStatsRaw(
            player_id=player_id,
            player_name=player_name,
            team_id=team_id or "",
            team_name=team_name or "",
            league=league,
            season=season,
            is_multi_team_total=is_multi_team_total,
            is_multi_team_row=is_multi_team_row,
            **stats,
        )
        records.append(record)

    logger.info("parsed_player_season_stats", league=league, season=season, count=len(records))
    return records
