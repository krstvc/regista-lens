"""Tests for Understat Pydantic schema validation."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from ingestion.understat.schemas import UnderstatPlayerSeasonStatsRaw


def _minimal_record(**overrides: object) -> dict:
    """Build a minimal valid record with optional overrides."""
    base = {
        "player_id": "1234",
        "player_name": "Test Player",
        "team_name": "Test FC",
        "league": "Premier League",
        "season": "2023-2024",
    }
    base.update(overrides)
    return base


class TestUnderstatPlayerSeasonStatsRaw:
    def test_minimal_valid_record(self) -> None:
        record = UnderstatPlayerSeasonStatsRaw(**_minimal_record())
        assert record.player_id == "1234"
        assert record.goals is None
        assert record.xg is None

    def test_full_record(self) -> None:
        record = UnderstatPlayerSeasonStatsRaw(
            **_minimal_record(
                goals=18,
                assists=10,
                minutes=2614,
                xg=16.2,
                xg_assist=8.5,
                npxg=13.1,
                xg_chain=22.5,
                xg_buildup=8.3,
                shots=98,
                key_passes=45,
                position="F S",
            )
        )
        assert record.goals == 18
        assert record.xg == 16.2
        assert record.xg_chain == 22.5

    def test_missing_required_field_raises(self) -> None:
        with pytest.raises(ValidationError):
            UnderstatPlayerSeasonStatsRaw(
                player_name="Test",
                team_name="Y",
                league="L",
                season="S",
                # missing player_id
            )

    def test_none_stats_allowed(self) -> None:
        record = UnderstatPlayerSeasonStatsRaw(
            **_minimal_record(goals=None, xg=None, minutes=None, xg_chain=None)
        )
        assert record.goals is None
        assert record.xg is None
        assert record.xg_chain is None

    def test_position_codes(self) -> None:
        record = UnderstatPlayerSeasonStatsRaw(**_minimal_record(position="M C"))
        assert record.position == "M C"

    def test_zero_values(self) -> None:
        record = UnderstatPlayerSeasonStatsRaw(
            **_minimal_record(goals=0, xg=0.0, minutes=0, shots=0)
        )
        assert record.goals == 0
        assert record.xg == 0.0
        assert record.minutes == 0
