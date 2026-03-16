"""Dagster resources for shared state: HTTP clients, DuckDB connection."""

from __future__ import annotations

from pathlib import Path

from dagster import ConfigurableResource

from ingestion.common.http import RateLimitedClient
from ingestion.fbref.client import FbrefClient


class FbrefClientResource(ConfigurableResource):
    """Dagster resource wrapping the FBref HTTP client with rate limiting."""

    request_delay: float = 3.0
    cache_dir: str = ".cache/fbref"

    def get_client(self) -> FbrefClient:
        http_client = RateLimitedClient(
            delay_seconds=self.request_delay,
            cache_dir=Path(self.cache_dir),
        )
        return FbrefClient(http_client)
