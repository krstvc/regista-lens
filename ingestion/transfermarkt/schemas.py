"""Pydantic models for Transfermarkt raw data validation."""

from __future__ import annotations

from pydantic import BaseModel


class TransfermarktPlayerValuationRaw(BaseModel):
    """Raw player valuation data from Transfermarkt market values pages.

    All stat fields except identifiers are optional — some players may have
    incomplete metadata on Transfermarkt.
    """

    # Identifiers
    player_id: str
    player_name: str
    team_id: str
    team_name: str
    league: str
    season: str

    # Bio
    position: str | None = None
    date_of_birth: str | None = None  # e.g., "Jul 21, 2000"
    nationality: str | None = None
    age: int | None = None

    # Valuation
    market_value_eur: int | None = None  # parsed from display string (e.g., €180.00m → 180000000)
