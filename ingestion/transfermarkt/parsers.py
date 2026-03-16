"""HTML parsers for Transfermarkt market values pages."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

import structlog
from bs4 import BeautifulSoup

from ingestion.transfermarkt.schemas import TransfermarktPlayerValuationRaw

if TYPE_CHECKING:
    from bs4 import Tag

logger = structlog.get_logger()

_PLAYER_ID_PATTERN = re.compile(r"/spieler/(\d+)")
_TEAM_ID_PATTERN = re.compile(r"/verein/(\d+)")
_VALUE_PATTERN = re.compile(
    r"€(\d+(?:\.\d+)?)\s*(bn|m|k|Th\.)", re.IGNORECASE
)

_VALUE_MULTIPLIERS: dict[str, int] = {
    "bn": 1_000_000_000,
    "m": 1_000_000,
    "k": 1_000,
    "th.": 1_000,
}


def _parse_market_value(text: str | None) -> int | None:
    """Parse a Transfermarkt market value string to integer EUR.

    Handles formats like: €180.00m, €25.00k, €500Th., €1.20bn
    """
    if not text:
        return None

    match = _VALUE_PATTERN.search(text)
    if not match:
        return None

    amount = float(match.group(1))
    suffix = match.group(2).lower()
    multiplier = _VALUE_MULTIPLIERS.get(suffix)
    if multiplier is None:
        return None

    return int(amount * multiplier)


def _extract_total_pages(soup: BeautifulSoup) -> int:
    """Extract total page count from Transfermarkt pagination."""
    pager = soup.find("ul", class_="tm-pagination")
    if pager is None:
        return 1

    # Find all page number links
    page_links = pager.find_all("a", class_="tm-pagination__link")
    max_page = 1
    for link in page_links:
        text = link.get_text(strip=True)
        try:
            page_num = int(text)
            max_page = max(max_page, page_num)
        except ValueError:
            continue

    return max_page


def _extract_player_id(row: Tag) -> str | None:
    """Extract Transfermarkt player ID from the player link href."""
    link = row.select_one("td.hauptlink a[href*='/spieler/']")
    if link is None:
        return None
    match = _PLAYER_ID_PATTERN.search(link.get("href", ""))
    return match.group(1) if match else None


def _extract_team_id(row: Tag) -> str | None:
    """Extract Transfermarkt team ID from the team link href."""
    link = row.select_one("td a[href*='/verein/']")
    if link is None:
        return None
    match = _TEAM_ID_PATTERN.search(link.get("href", ""))
    return match.group(1) if match else None


def parse_market_values_page(
    html: str,
    league: str,
    season: str,
) -> list[TransfermarktPlayerValuationRaw]:
    """Parse a Transfermarkt market values page into validated Pydantic models.

    Args:
        html: Raw HTML content of the market values page.
        league: League name (e.g., "Premier League").
        season: Season string (e.g., "2023-2024").

    Returns:
        List of validated player valuation records.
    """
    soup = BeautifulSoup(html, "lxml")

    table = soup.find("table", class_="items")
    if table is None:
        logger.error("transfermarkt_table_not_found", league=league, season=season)
        return []

    tbody = table.find("tbody")
    if tbody is None:
        logger.error("transfermarkt_tbody_not_found", league=league, season=season)
        return []

    records: list[TransfermarktPlayerValuationRaw] = []

    for row in tbody.find_all("tr", recursive=False):
        cells = row.find_all("td")
        if len(cells) < 4:
            continue

        player_id = _extract_player_id(row)
        if not player_id:
            continue

        # Player name from hauptlink cell
        name_cell = row.select_one("td.hauptlink a")
        player_name = name_cell.get_text(strip=True) if name_cell else None
        if not player_name:
            continue

        # Team
        team_id = _extract_team_id(row)
        team_link = row.select_one("td a[href*='/verein/']")
        team_name = team_link.get("title", team_link.get_text(strip=True)) if team_link else None
        if not team_id or not team_name:
            continue

        # Position
        position: str | None = None
        pos_cells = row.select("td.posrela table td")
        if pos_cells:
            for td in pos_cells:
                text = td.get_text(strip=True)
                if text and text != player_name:
                    position = text
                    break
        if position is None:
            # Fallback: some layouts use inline-table structure
            pos_el = row.select_one("td.posrela tr:last-child td")
            if pos_el:
                position = pos_el.get_text(strip=True) or None

        # Date of birth and age — scan all zentriert cells for the DOB pattern
        dob: str | None = None
        age: int | None = None
        for cell in row.select("td.zentriert"):
            cell_text = cell.get_text(strip=True)
            dob_match = re.match(r"([A-Z][a-z]{2}\s+\d{1,2},\s+\d{4})", cell_text)
            if dob_match:
                dob = dob_match.group(1)
                age_match = re.search(r"\((\d+)\)", cell_text)
                if age_match:
                    age = int(age_match.group(1))
                break

        # Nationality
        nationality: str | None = None
        flag_imgs = row.select("td.zentriert img.flaggenrahmen")
        if flag_imgs:
            nationality = flag_imgs[0].get("title")

        # Market value — last cell typically
        value_cell = row.select_one("td.rechts.hauptlink")
        market_value_eur = _parse_market_value(
            value_cell.get_text(strip=True) if value_cell else None
        )

        try:
            record = TransfermarktPlayerValuationRaw(
                player_id=player_id,
                player_name=player_name,
                team_id=team_id,
                team_name=team_name,
                league=league,
                season=season,
                position=position,
                date_of_birth=dob,
                nationality=nationality,
                age=age,
                market_value_eur=market_value_eur,
            )
            records.append(record)
        except Exception as e:
            logger.warning(
                "transfermarkt_record_validation_error",
                player=player_name,
                error=str(e),
            )

    logger.info(
        "parsed_transfermarkt_market_values",
        league=league,
        season=season,
        count=len(records),
    )
    return records
