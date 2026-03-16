"""Unicode normalization and transliteration utilities for player/team names."""

from __future__ import annotations

import re
import unicodedata

from unidecode import unidecode


def normalize_name(name: str) -> str:
    """Normalize a person name: NFC → transliterate to ASCII → lowercase → collapse whitespace."""
    name = unicodedata.normalize("NFC", name)
    name = unidecode(name)
    name = name.lower().strip()
    name = re.sub(r"\s+", " ", name)
    return name


_TEAM_PREFIXES = re.compile(
    r"^(1\.\s*fc|fc|cf|sc|ac|as|ss|ssc|us|rc|rcd|ca|cd|afc)[\s.]",
    re.IGNORECASE,
)
_TEAM_SUFFIXES = re.compile(
    r"\s+(fc|cf|sc|ac)$",
    re.IGNORECASE,
)


def normalize_team_name(name: str) -> str:
    """Normalize a team name: transliterate, strip common prefixes/suffixes, lowercase."""
    name = normalize_name(name)
    name = _TEAM_PREFIXES.sub("", name)
    name = _TEAM_SUFFIXES.sub("", name)
    return name.strip()
