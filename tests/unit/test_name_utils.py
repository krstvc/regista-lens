"""Tests for name normalization utilities."""

from ingestion.common.name_utils import normalize_name, normalize_team_name


class TestNormalizeName:
    def test_ascii_passthrough(self) -> None:
        assert normalize_name("John Smith") == "john smith"

    def test_accented_characters(self) -> None:
        assert normalize_name("José María") == "jose maria"

    def test_german_umlauts(self) -> None:
        assert normalize_name("Müller") == "muller"

    def test_turkish_characters(self) -> None:
        assert normalize_name("Çalhanoğlu") == "calhanoglu"

    def test_mixed_case(self) -> None:
        assert normalize_name("ERLING HAALAND") == "erling haaland"

    def test_extra_whitespace(self) -> None:
        assert normalize_name("  Mohamed   Salah  ") == "mohamed salah"

    def test_empty_string(self) -> None:
        assert normalize_name("") == ""

    def test_unicode_normalization(self) -> None:
        # é as combining characters vs precomposed
        assert normalize_name("e\u0301") == normalize_name("é")

    def test_slavic_characters(self) -> None:
        assert normalize_name("Dušan Vlahović") == "dusan vlahovic"

    def test_nordic_characters(self) -> None:
        assert normalize_name("Ødegaard") == "odegaard"


class TestNormalizeTeamName:
    def test_strip_fc_prefix(self) -> None:
        assert normalize_team_name("FC Barcelona") == "barcelona"

    def test_strip_fc_suffix(self) -> None:
        assert normalize_team_name("Liverpool FC") == "liverpool"

    def test_strip_sc_prefix(self) -> None:
        assert normalize_team_name("SC Freiburg") == "freiburg"

    def test_strip_ac_prefix(self) -> None:
        assert normalize_team_name("AC Milan") == "milan"

    def test_strip_rcd_prefix(self) -> None:
        assert normalize_team_name("RCD Mallorca") == "mallorca"

    def test_accented_team(self) -> None:
        assert normalize_team_name("Atlético Madrid") == "atletico madrid"

    def test_numbered_prefix(self) -> None:
        assert normalize_team_name("1. FC Köln") == "koln"

    def test_no_prefix_or_suffix(self) -> None:
        assert normalize_team_name("Arsenal") == "arsenal"

    def test_cf_prefix(self) -> None:
        assert normalize_team_name("CF Montréal") == "montreal"
