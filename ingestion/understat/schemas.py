"""Pydantic models for Understat raw data validation."""

from __future__ import annotations

from pydantic import BaseModel


class UnderstatPlayerSeasonStatsRaw(BaseModel):
    """Raw player season stats from Understat league pages.

    All stat fields are optional — Understat may omit fields for players
    with very few appearances.
    """

    # Identifiers
    player_id: str
    player_name: str
    team_name: str
    league: str
    season: str

    # Position (Understat codes: "F S", "M C", "D C", "GK", "Sub", etc.)
    position: str | None = None

    # Appearances
    games: int | None = None
    minutes: int | None = None

    # Goals & assists
    goals: int | None = None
    assists: int | None = None
    npg: int | None = None  # non-penalty goals

    # Expected
    xg: float | None = None
    xg_assist: float | None = None
    npxg: float | None = None
    xg_chain: float | None = None
    xg_buildup: float | None = None

    # Other
    shots: int | None = None
    key_passes: int | None = None
    yellow_cards: int | None = None
    red_cards: int | None = None
