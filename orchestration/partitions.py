"""Season partition definitions shared across all raw assets."""

from dagster import StaticPartitionsDefinition

SEASON_PARTITIONS = StaticPartitionsDefinition(["2023-2024", "2024-2025", "2025-2026"])
