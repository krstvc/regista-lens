"""Tests for FBref HTML parsers using saved fixture HTML."""

from __future__ import annotations

from pathlib import Path

import pytest

from ingestion.fbref.parsers import parse_player_season_stats

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"


@pytest.fixture
def sample_html() -> str:
    return (FIXTURES_DIR / "fbref_player_season_stats_sample.html").read_text()


@pytest.fixture
def parsed_records(sample_html: str) -> list:
    return parse_player_season_stats(sample_html, "Premier League", "2023-2024")


class TestParsePlayerSeasonStats:
    def test_row_count(self, parsed_records: list) -> None:
        """Should parse all data rows, including multi-team sub-rows, but skip spacer rows."""
        # 3 normal + 1 multi-team total + 2 multi-team sub-rows + 1 sparse = 7
        assert len(parsed_records) == 7

    def test_skips_spacer_rows(self, parsed_records: list) -> None:
        """Spacer rows with class='thead' should not appear in output."""
        names = [r.player_name for r in parsed_records]
        # No "Player" header text should leak through
        assert "Player" not in names

    def test_player_id_extraction(self, parsed_records: list) -> None:
        """Player IDs should be extracted from href attributes."""
        salah = parsed_records[0]
        assert salah.player_id == "e342ad68"
        assert salah.player_name == "Mohamed Salah"

    def test_team_id_extraction(self, parsed_records: list) -> None:
        """Team IDs should be extracted from href attributes."""
        salah = parsed_records[0]
        assert salah.team_id == "822bd0ba"
        assert salah.team_name == "Liverpool"

    def test_stat_parsing(self, parsed_records: list) -> None:
        """Numeric stats should be parsed correctly."""
        salah = parsed_records[0]
        assert salah.goals == 18
        assert salah.assists == 10
        assert salah.minutes == 2614  # commas stripped
        assert salah.xg == 16.2
        assert salah.goals_per90 == 0.62

    def test_empty_cells(self, parsed_records: list) -> None:
        """Empty stat cells should result in None values."""
        young_player = parsed_records[-1]
        assert young_player.player_name == "Young Player"
        assert young_player.xg is None
        assert young_player.goals_per90 is None
        assert young_player.goals == 0  # Zero is not empty

    def test_multi_team_total_row(self, parsed_records: list) -> None:
        """Multi-team total rows should be flagged."""
        felix_total = next(
            r for r in parsed_records if r.player_name == "João Félix" and r.is_multi_team_total
        )
        assert felix_total.team_name == "2 Clubs"
        assert felix_total.games == 20

    def test_multi_team_individual_rows(self, parsed_records: list) -> None:
        """Multi-team individual team rows should be flagged."""
        felix_rows = [
            r for r in parsed_records if r.player_name == "João Félix" and r.is_multi_team_row
        ]
        assert len(felix_rows) == 2
        team_names = {r.team_name for r in felix_rows}
        assert "Barcelona" in team_names
        assert "Atlético Madrid" in team_names

    def test_accented_name_preserved_in_raw(self, parsed_records: list) -> None:
        """Raw parser should preserve original names — normalization happens in staging."""
        vlahovic = next(r for r in parsed_records if r.player_id == "abcdef01")
        assert vlahovic.player_name == "Dušan Vlahović"

    def test_league_and_season_set(self, parsed_records: list) -> None:
        """League and season should be set on all records."""
        for record in parsed_records:
            assert record.league == "Premier League"
            assert record.season == "2023-2024"

    def test_nationality_extracted(self, parsed_records: list) -> None:
        """Nationality text should be extracted."""
        salah = parsed_records[0]
        assert salah.nationality is not None
        assert "EGY" in salah.nationality

    def test_empty_html_returns_empty_list(self) -> None:
        """Empty or irrelevant HTML should return empty list, not crash."""
        result = parse_player_season_stats("<html><body></body></html>", "Test", "2023-2024")
        assert result == []
