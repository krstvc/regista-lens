"""Tests for Transfermarkt parsers using saved fixture HTML."""

from __future__ import annotations

from pathlib import Path

import pytest
from bs4 import BeautifulSoup

from ingestion.transfermarkt.parsers import (
    _extract_total_pages,
    _parse_market_value,
    parse_market_values_page,
)

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"


@pytest.fixture
def sample_html() -> str:
    return (FIXTURES_DIR / "transfermarkt_market_values_sample.html").read_text()


@pytest.fixture
def parsed_records(sample_html: str) -> list:
    return parse_market_values_page(sample_html, "Premier League", "2023-2024")


class TestParseMarketValue:
    def test_millions(self) -> None:
        assert _parse_market_value("€180.00m") == 180_000_000

    def test_millions_fractional(self) -> None:
        assert _parse_market_value("€1.50m") == 1_500_000

    def test_thousands_k(self) -> None:
        assert _parse_market_value("€25.00k") == 25_000

    def test_thousands_th(self) -> None:
        assert _parse_market_value("€500Th.") == 500_000

    def test_billions(self) -> None:
        assert _parse_market_value("€1.20bn") == 1_200_000_000

    def test_none_input(self) -> None:
        assert _parse_market_value(None) is None

    def test_empty_string(self) -> None:
        assert _parse_market_value("") is None

    def test_invalid_format(self) -> None:
        assert _parse_market_value("not a value") is None

    def test_dash_means_no_value(self) -> None:
        assert _parse_market_value("-") is None


class TestExtractTotalPages:
    def test_with_pagination(self, sample_html: str) -> None:
        soup = BeautifulSoup(sample_html, "lxml")
        assert _extract_total_pages(soup) == 3

    def test_no_pagination(self) -> None:
        html = "<html><body><table class='items'></table></body></html>"
        soup = BeautifulSoup(html, "lxml")
        assert _extract_total_pages(soup) == 1


class TestParseMarketValuesPage:
    def test_record_count(self, parsed_records: list) -> None:
        assert len(parsed_records) == 5

    def test_player_id_extraction(self, parsed_records: list) -> None:
        haaland = parsed_records[0]
        assert haaland.player_id == "418560"
        assert haaland.player_name == "Erling Haaland"

    def test_team_extraction(self, parsed_records: list) -> None:
        haaland = parsed_records[0]
        assert haaland.team_id == "281"
        assert haaland.team_name == "Manchester City"

    def test_market_value_millions(self, parsed_records: list) -> None:
        haaland = parsed_records[0]
        assert haaland.market_value_eur == 180_000_000

    def test_market_value_thousands(self, parsed_records: list) -> None:
        bellingham = parsed_records[2]
        assert bellingham.market_value_eur == 25_000

    def test_market_value_thousands_th(self, parsed_records: list) -> None:
        prospect = parsed_records[4]
        assert prospect.market_value_eur == 500_000

    def test_dob_extraction(self, parsed_records: list) -> None:
        haaland = parsed_records[0]
        assert haaland.date_of_birth == "Jul 21, 2000"

    def test_age_extraction(self, parsed_records: list) -> None:
        haaland = parsed_records[0]
        assert haaland.age == 23

    def test_nationality_extraction(self, parsed_records: list) -> None:
        haaland = parsed_records[0]
        assert haaland.nationality == "Norway"

    def test_position_extraction(self, parsed_records: list) -> None:
        haaland = parsed_records[0]
        assert haaland.position == "Centre-Forward"
        saka = parsed_records[1]
        assert saka.position == "Right Winger"
        prospect = parsed_records[4]
        assert prospect.position == "Goalkeeper"

    def test_league_and_season_set(self, parsed_records: list) -> None:
        for record in parsed_records:
            assert record.league == "Premier League"
            assert record.season == "2023-2024"

    def test_second_player(self, parsed_records: list) -> None:
        saka = parsed_records[1]
        assert saka.player_id == "433177"
        assert saka.player_name == "Bukayo Saka"
        assert saka.team_id == "11"
        assert saka.team_name == "Arsenal FC"
        assert saka.market_value_eur == 140_000_000
        assert saka.nationality == "England"

    def test_empty_html_returns_empty_list(self) -> None:
        result = parse_market_values_page(
            "<html><body></body></html>", "Test", "2023-2024"
        )
        assert result == []

    def test_no_tbody_returns_empty_list(self) -> None:
        html = "<html><body><table class='items'></table></body></html>"
        result = parse_market_values_page(html, "Test", "2023-2024")
        assert result == []
