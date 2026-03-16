"""FBref HTTP client — maps league/season pairs to URLs and fetches stats pages."""

from __future__ import annotations

from ingestion.common.http import RateLimitedClient
from ingestion.fbref.parsers import parse_player_season_stats
from ingestion.fbref.schemas import FbrefPlayerSeasonStatsRaw

# FBref competition IDs for the top 5 European leagues
COMP_IDS: dict[str, tuple[int, str]] = {
    "Premier League": (9, "Premier-League"),
    "La Liga": (12, "La-Liga"),
    "Bundesliga": (20, "Bundesliga"),
    "Serie A": (11, "Serie-A"),
    "Ligue 1": (13, "Ligue-1"),
}

LEAGUES = list(COMP_IDS.keys())


def _build_url(comp_id: int, comp_slug: str, season: str) -> str:
    """Build FBref Standard Stats page URL for a league-season."""
    return f"https://fbref.com/en/comps/{comp_id}/{season}/stats/{season}-{comp_slug}-Stats"


class FbrefClient:
    """High-level client for fetching and parsing FBref data."""

    def __init__(self, http_client: RateLimitedClient) -> None:
        self._http = http_client

    def fetch_player_season_stats(
        self,
        league: str,
        season: str,
    ) -> tuple[list[FbrefPlayerSeasonStatsRaw], str]:
        """Fetch and parse player season stats for a league-season.

        Returns:
            Tuple of (parsed records, source URL).
        """
        comp_id, comp_slug = COMP_IDS[league]
        url = _build_url(comp_id, comp_slug, season)
        html = self._http.get(url)
        records = parse_player_season_stats(html, league, season)
        return records, url

    def close(self) -> None:
        self._http.close()
