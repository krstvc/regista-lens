"""Tests for Understat parsers using saved fixture HTML."""

from __future__ import annotations

from pathlib import Path

import pytest

from ingestion.understat.parsers import (
    _decode_hex_escapes,
    parse_player_season_stats,
)

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"


@pytest.fixture
def sample_html() -> str:
    return (FIXTURES_DIR / "understat_league_page_sample.html").read_text()


@pytest.fixture
def parsed_records(sample_html: str) -> list:
    return parse_player_season_stats(sample_html, "Premier League", "2023-2024")


class TestDecodeHexEscapes:
    def test_basic_decode(self) -> None:
        assert _decode_hex_escapes(r"\x48\x65\x6c\x6c\x6f") == "Hello"

    def test_mixed_content(self) -> None:
        assert _decode_hex_escapes(r"abc\x20def") == "abc def"

    def test_json_structure(self) -> None:
        encoded = r"\x5b\x7b\x22\x69\x64\x22\x3a\x22\x31\x22\x7d\x5d"
        assert _decode_hex_escapes(encoded) == '[{"id":"1"}]'

    def test_no_escapes_passthrough(self) -> None:
        assert _decode_hex_escapes("plain text") == "plain text"


class TestParsePlayerSeasonStats:
    def test_record_count(self, parsed_records: list) -> None:
        """Should parse all 5 players from fixture."""
        assert len(parsed_records) == 5

    def test_player_id_extraction(self, parsed_records: list) -> None:
        salah = parsed_records[0]
        assert salah.player_id == "1234"
        assert salah.player_name == "Mohamed Salah"

    def test_team_extraction(self, parsed_records: list) -> None:
        salah = parsed_records[0]
        assert salah.team_name == "Liverpool"

    def test_stat_parsing_int(self, parsed_records: list) -> None:
        salah = parsed_records[0]
        assert salah.goals == 18
        assert salah.assists == 10
        assert salah.minutes == 2614
        assert salah.shots == 98

    def test_stat_parsing_float(self, parsed_records: list) -> None:
        """Understat xG values should parse as floats, truncating noise."""
        salah = parsed_records[0]
        assert salah.xg == pytest.approx(16.2, abs=0.01)
        assert salah.xg_assist == 8.5
        assert salah.npxg == 13.1
        assert salah.xg_chain == 22.5
        assert salah.xg_buildup == 8.3

    def test_position_codes(self, parsed_records: list) -> None:
        salah = parsed_records[0]
        assert salah.position == "F S"
        odegaard = parsed_records[2]
        assert odegaard.position == "M C"
        van_dijk = parsed_records[3]
        assert van_dijk.position == "D C"

    def test_zero_minute_player(self, parsed_records: list) -> None:
        """Zero-minute players should still be parsed (filtering happens in staging)."""
        bench = parsed_records[4]
        assert bench.player_name == "Bench Warmer"
        assert bench.minutes == 0
        assert bench.goals == 0
        assert bench.xg == 0.0

    def test_league_and_season_set(self, parsed_records: list) -> None:
        for record in parsed_records:
            assert record.league == "Premier League"
            assert record.season == "2023-2024"

    def test_second_player(self, parsed_records: list) -> None:
        haaland = parsed_records[1]
        assert haaland.player_id == "5678"
        assert haaland.player_name == "Erling Haaland"
        assert haaland.goals == 27
        assert haaland.xg == pytest.approx(23.4)
        assert haaland.team_name == "Manchester City"

    def test_empty_html_returns_empty_list(self) -> None:
        result = parse_player_season_stats("<html><body></body></html>", "Test", "2023-2024")
        assert result == []

    def test_invalid_json_returns_empty_list(self) -> None:
        html = "<script>var playersData = JSON.parse('not valid json')</script>"
        result = parse_player_season_stats(html, "Test", "2023-2024")
        assert result == []
