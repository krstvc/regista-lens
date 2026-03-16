"""Rate-limited HTTP client with local file caching and retry logic."""

from __future__ import annotations

import hashlib
import time
from pathlib import Path

import httpx
import structlog

logger = structlog.get_logger()

_DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
}

_CACHE_DIR = Path(".cache")


class RateLimitedClient:
    """Sync HTTP client with per-request delay, local file cache, and retry on 429/5xx."""

    def __init__(
        self,
        delay_seconds: float = 3.0,
        max_retries: int = 3,
        cache_dir: Path = _CACHE_DIR,
        headers: dict[str, str] | None = None,
    ) -> None:
        self._delay = delay_seconds
        self._max_retries = max_retries
        self._cache_dir = cache_dir
        self._last_request_time: float = 0.0
        self._client = httpx.Client(
            headers={**_DEFAULT_HEADERS, **(headers or {})},
            timeout=30.0,
            follow_redirects=True,
        )

    def _cache_path(self, url: str) -> Path:
        url_hash = hashlib.sha256(url.encode()).hexdigest()[:16]
        return self._cache_dir / f"{url_hash}.html"

    def _wait_for_rate_limit(self) -> None:
        elapsed = time.monotonic() - self._last_request_time
        if elapsed < self._delay:
            sleep_time = self._delay - elapsed
            logger.debug("rate_limit_wait", sleep_seconds=round(sleep_time, 2))
            time.sleep(sleep_time)

    def get(self, url: str, *, use_cache: bool = True) -> str:
        """Fetch URL content as text, with caching and rate limiting."""
        if use_cache:
            cached = self._cache_path(url)
            if cached.exists():
                logger.debug("cache_hit", url=url)
                return cached.read_text(encoding="utf-8")

        self._wait_for_rate_limit()

        last_error: Exception | None = None
        for attempt in range(1, self._max_retries + 1):
            try:
                logger.info("http_request", url=url, attempt=attempt)
                response = self._client.get(url)
                self._last_request_time = time.monotonic()

                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", "60"))
                    logger.warning("rate_limited", url=url, retry_after=retry_after)
                    time.sleep(retry_after)
                    continue

                response.raise_for_status()
                text = response.text

                if use_cache:
                    self._cache_dir.mkdir(parents=True, exist_ok=True)
                    self._cache_path(url).write_text(text, encoding="utf-8")

                return text

            except httpx.HTTPStatusError as e:
                last_error = e
                if e.response.status_code >= 500:
                    wait = 2**attempt
                    logger.warning(
                        "server_error_retry", url=url, status=e.response.status_code, wait=wait
                    )
                    time.sleep(wait)
                    continue
                raise

            except httpx.TransportError as e:
                last_error = e
                wait = 2**attempt
                logger.warning("transport_error_retry", url=url, error=str(e), wait=wait)
                time.sleep(wait)
                continue

        msg = f"Failed to fetch {url} after {self._max_retries} retries"
        raise httpx.HTTPError(msg) from last_error

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> RateLimitedClient:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()
