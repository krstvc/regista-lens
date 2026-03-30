"""Tests for Transfermarkt Pydantic schema validation."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from ingestion.transfermarkt.schemas import TransfermarktPlayerValuationRaw


def _minimal_record(**overrides: object) -> dict:
    """Build a minimal valid record with optional overrides."""
    base = {
        "player_id": "418560",
        "player_name": "Erling Haaland",
        "team_id": "281",
        "team_name": "Manchester City",
        "league": "Premier League",
        "season": "2023-2024",
    }
    base.update(overrides)
    return base


class TestTransfermarktPlayerValuationRaw:
    def test_minimal_valid_record(self) -> None:
        record = TransfermarktPlayerValuationRaw(**_minimal_record())
        assert record.player_id == "418560"
        assert record.market_value_eur is None
        assert record.position is None
        assert record.date_of_birth is None

    def test_full_record(self) -> None:
        record = TransfermarktPlayerValuationRaw(
            **_minimal_record(
                position="Centre-Forward",
                date_of_birth="Jul 21, 2000",
                nationality="Norway",
                age=23,
                market_value_eur=180_000_000,
            )
        )
        assert record.position == "Centre-Forward"
        assert record.date_of_birth == "Jul 21, 2000"
        assert record.nationality == "Norway"
        assert record.age == 23
        assert record.market_value_eur == 180_000_000

    def test_missing_required_field_raises(self) -> None:
        with pytest.raises(ValidationError):
            TransfermarktPlayerValuationRaw(
                player_name="Test",
                team_id="1",
                team_name="Y",
                league="L",
                season="S",
                # missing player_id
            )

    def test_none_stats_allowed(self) -> None:
        record = TransfermarktPlayerValuationRaw(
            **_minimal_record(
                position=None,
                date_of_birth=None,
                nationality=None,
                age=None,
                market_value_eur=None,
            )
        )
        assert record.position is None
        assert record.market_value_eur is None

    def test_zero_market_value(self) -> None:
        record = TransfermarktPlayerValuationRaw(**_minimal_record(market_value_eur=0))
        assert record.market_value_eur == 0
