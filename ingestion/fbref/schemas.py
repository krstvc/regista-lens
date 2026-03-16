"""Pydantic models for FBref raw data validation."""

from __future__ import annotations

from pydantic import BaseModel


class FbrefPlayerSeasonStatsRaw(BaseModel):
    """Raw player season stats from FBref's Standard Stats table.

    All stat fields are optional — FBref leaves cells empty for players
    with very few appearances.
    """

    # Identifiers
    player_id: str
    player_name: str
    team_id: str
    team_name: str
    league: str
    season: str

    # Bio
    nationality: str | None = None
    position: str | None = None
    age: str | None = None  # FBref format: "25-123" (years-days)

    # Appearance
    games: int | None = None
    games_starts: int | None = None
    minutes: int | None = None

    # Performance
    goals: int | None = None
    assists: int | None = None
    goals_pens: int | None = None
    pens_made: int | None = None
    pens_att: int | None = None
    cards_yellow: int | None = None
    cards_red: int | None = None

    # Expected (when available on FBref)
    xg: float | None = None
    npxg: float | None = None
    xg_assist: float | None = None

    # Per 90
    goals_per90: float | None = None
    assists_per90: float | None = None
    goals_assists_per90: float | None = None
    goals_pens_per90: float | None = None
    goals_assists_pens_per90: float | None = None
    xg_per90: float | None = None
    xg_assist_per90: float | None = None
    xg_xg_assist_per90: float | None = None
    npxg_per90: float | None = None
    npxg_xg_assist_per90: float | None = None

    # Multi-team flag
    is_multi_team_row: bool = False
    is_multi_team_total: bool = False
