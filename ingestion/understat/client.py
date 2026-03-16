"""Understat HTTP client — maps league/season pairs to URLs and fetches stats pages."""

from __future__ import annotations

from ingestion.common.http import RateLimitedClient
from ingestion.understat.parsers import parse_player_season_stats
from ingestion.understat.schemas import UnderstatPlayerSeasonStatsRaw

# Understat URL slugs for the top 5 European leagues
LEAGUE_SLUGS: dict[str, str] = {
    "Premier League": "EPL",
    "La Liga": "La_Liga",
    "Bundesliga": "Bundesliga",
    "Serie A": "Serie_A",
    "Ligue 1": "Ligue_1",
}

LEAGUES = list(LEAGUE_SLUGS.keys())


def _season_to_year(season: str) -> str:
    """Convert season string '2023-2024' to start year '2023'."""
    return season.split("-")[0]


class UnderstatClient:
    """High-level client for fetching and parsing Understat data."""

    def __init__(self, http_client: RateLimitedClient) -> None:
        self._http = http_client

    def fetch_player_season_stats(
        self,
        league: str,
        season: str,
    ) -> tuple[list[UnderstatPlayerSeasonStatsRaw], str]:
        """Fetch and parse player season stats for a league-season.

        Returns:
            Tuple of (parsed records, source URL).
        """
        slug = LEAGUE_SLUGS[league]
        year = _season_to_year(season)
        url = f"https://understat.com/league/{slug}/{year}"
        html = self._http.get(url)
        records = parse_player_season_stats(html, league, season)
        return records, url

    def close(self) -> None:
        self._http.close()
