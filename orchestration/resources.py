"""Dagster resources for shared state: HTTP clients, DuckDB connection."""

from __future__ import annotations

from pathlib import Path

from dagster import ConfigurableResource

from ingestion.common.http import RateLimitedClient
from ingestion.fbref.client import FbrefClient
from ingestion.transfermarkt.client import TransfermarktClient
from ingestion.understat.client import UnderstatClient


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


class UnderstatClientResource(ConfigurableResource):
    """Dagster resource wrapping the Understat HTTP client with rate limiting."""

    request_delay: float = 2.0
    cache_dir: str = ".cache/understat"

    def get_client(self) -> UnderstatClient:
        http_client = RateLimitedClient(
            delay_seconds=self.request_delay,
            cache_dir=Path(self.cache_dir),
        )
        return UnderstatClient(http_client)


class TransfermarktClientResource(ConfigurableResource):
    """Dagster resource wrapping the Transfermarkt HTTP client with rate limiting."""

    request_delay: float = 5.0
    cache_dir: str = ".cache/transfermarkt"

    def get_client(self) -> TransfermarktClient:
        http_client = RateLimitedClient(
            delay_seconds=self.request_delay,
            cache_dir=Path(self.cache_dir),
        )
        return TransfermarktClient(http_client)
