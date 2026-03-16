"""Tests for Pydantic schema validation."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from ingestion.fbref.schemas import FbrefPlayerSeasonStatsRaw


def _minimal_record(**overrides: object) -> dict:
    """Build a minimal valid record with optional overrides."""
    base = {
        "player_id": "abc123",
        "player_name": "Test Player",
        "team_id": "def456",
        "team_name": "Test FC",
        "league": "Premier League",
        "season": "2023-2024",
    }
    base.update(overrides)
    return base


class TestFbrefPlayerSeasonStatsRaw:
    def test_minimal_valid_record(self) -> None:
        record = FbrefPlayerSeasonStatsRaw(**_minimal_record())
        assert record.player_id == "abc123"
        assert record.goals is None
        assert record.is_multi_team_row is False

    def test_full_record(self) -> None:
        record = FbrefPlayerSeasonStatsRaw(
            **_minimal_record(
                goals=10,
                assists=5,
                minutes=2000,
                xg=8.5,
                goals_per90=0.45,
            )
        )
        assert record.goals == 10
        assert record.xg == 8.5

    def test_missing_required_field_raises(self) -> None:
        with pytest.raises(ValidationError):
            FbrefPlayerSeasonStatsRaw(
                player_name="Test",
                team_id="x",
                team_name="Y",
                league="L",
                season="S",
                # missing player_id
            )

    def test_none_stats_allowed(self) -> None:
        record = FbrefPlayerSeasonStatsRaw(**_minimal_record(goals=None, xg=None, minutes=None))
        assert record.goals is None
        assert record.xg is None

    def test_multi_team_flags(self) -> None:
        record = FbrefPlayerSeasonStatsRaw(
            **_minimal_record(is_multi_team_total=True, is_multi_team_row=False)
        )
        assert record.is_multi_team_total is True
        assert record.is_multi_team_row is False
