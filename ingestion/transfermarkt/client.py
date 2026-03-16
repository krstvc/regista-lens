"""Transfermarkt HTTP client — fetches market values pages for all leagues."""

from __future__ import annotations

from bs4 import BeautifulSoup

from ingestion.common.http import RateLimitedClient
from ingestion.transfermarkt.parsers import _extract_total_pages, parse_market_values_page
from ingestion.transfermarkt.schemas import TransfermarktPlayerValuationRaw

# Transfermarkt competition IDs and URL slugs for the top 5 European leagues
COMP_IDS: dict[str, tuple[str, str]] = {
    "Premier League": ("GB1", "premier-league"),
    "La Liga": ("ES1", "la-liga"),
    "Bundesliga": ("L1", "1-bundesliga"),
    "Serie A": ("IT1", "serie-a"),
    "Ligue 1": ("FR1", "ligue-1"),
}

LEAGUES = list(COMP_IDS.keys())


def _season_to_year(season: str) -> str:
    """Convert season string '2023-2024' to start year '2023'."""
    return season.split("-")[0]


def _build_url(comp_id: str, league_slug: str, season: str, page: int = 1) -> str:
    """Build Transfermarkt market values page URL."""
    year = _season_to_year(season)
    return (
        f"https://www.transfermarkt.com/{league_slug}"
        f"/marktwerte/wettbewerb/{comp_id}"
        f"/saison_id/{year}/page/{page}"
    )


class TransfermarktClient:
    """High-level client for fetching and parsing Transfermarkt market values."""

    def __init__(self, http_client: RateLimitedClient) -> None:
        self._http = http_client

    def fetch_player_valuations(
        self,
        league: str,
        season: str,
    ) -> tuple[list[TransfermarktPlayerValuationRaw], str]:
        """Fetch and parse player valuations for a league-season (all pages).

        Returns:
            Tuple of (parsed records, base URL for page 1).
        """
        comp_id, league_slug = COMP_IDS[league]

        # Fetch page 1 to determine total pages
        url_page1 = _build_url(comp_id, league_slug, season, page=1)
        html_page1 = self._http.get(url_page1)

        soup = BeautifulSoup(html_page1, "lxml")
        total_pages = _extract_total_pages(soup)

        all_records = parse_market_values_page(html_page1, league, season)

        # Fetch remaining pages
        for page in range(2, total_pages + 1):
            url = _build_url(comp_id, league_slug, season, page=page)
            html = self._http.get(url)
            records = parse_market_values_page(html, league, season)
            all_records.extend(records)

        return all_records, url_page1

    def close(self) -> None:
        self._http.close()
