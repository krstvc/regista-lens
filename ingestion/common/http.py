"""Rate-limited HTTP clients with local file caching and retry logic.

Two clients:
- ``RateLimitedClient`` — lightweight httpx client for sources without
  Cloudflare protection (Understat JSON endpoints).
- ``BrowserClient`` — uses *nodriver* (headed Chrome via CDP) to bypass
  Cloudflare Turnstile challenges (FBref, Transfermarkt).
"""

from __future__ import annotations

import asyncio
import hashlib
import time
from pathlib import Path

import httpx
import structlog

logger = structlog.get_logger()

_CACHE_DIR = Path(".cache")


# ---------------------------------------------------------------------------
# Lightweight httpx client (Understat, other non-Cloudflare sources)
# ---------------------------------------------------------------------------

class RateLimitedClient:
    """Sync HTTP client with per-request delay, local file cache, and retry."""

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
            headers=headers or {},
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


# ---------------------------------------------------------------------------
# Browser-based client (Cloudflare-protected sources: FBref, Transfermarkt)
# ---------------------------------------------------------------------------

class BrowserClient:
    """Headed Chrome client via nodriver for Cloudflare-protected sources.

    Launches a real (non-headless) Chrome instance that passes Cloudflare
    Turnstile challenges. Results are cached locally so each URL is only
    fetched once during development.
    """

    def __init__(
        self,
        delay_seconds: float = 3.0,
        cache_dir: Path = _CACHE_DIR,
        page_load_timeout: int = 60,
    ) -> None:
        self._delay = delay_seconds
        self._cache_dir = cache_dir
        self._page_load_timeout = page_load_timeout
        self._last_request_time: float = 0.0
        self._browser: object | None = None
        self._loop: asyncio.AbstractEventLoop | None = None

    def _cache_path(self, url: str) -> Path:
        url_hash = hashlib.sha256(url.encode()).hexdigest()[:16]
        return self._cache_dir / f"{url_hash}.html"

    def _wait_for_rate_limit(self) -> None:
        elapsed = time.monotonic() - self._last_request_time
        if elapsed < self._delay:
            sleep_time = self._delay - elapsed
            logger.debug("rate_limit_wait", sleep_seconds=round(sleep_time, 2))
            time.sleep(sleep_time)

    async def _ensure_browser(self) -> object:
        if self._browser is None:
            import nodriver as uc

            self._browser = await uc.start(headless=False)
        return self._browser

    async def _fetch(self, url: str, wait_selector: str | None) -> str:
        browser = await self._ensure_browser()
        page = await browser.get(url)  # type: ignore[union-attr]
        # Wait for Cloudflare challenge to resolve and content to load
        deadline = time.monotonic() + self._page_load_timeout
        while time.monotonic() < deadline:
            await asyncio.sleep(2)
            html = await page.get_content()
            if wait_selector and wait_selector in html:
                return html
            if wait_selector is None and "Just a moment" not in html and len(html) > 5000:
                return html
        # Return whatever we have
        return await page.get_content()

    def get(
        self,
        url: str,
        *,
        use_cache: bool = True,
        wait_selector: str | None = None,
    ) -> str:
        """Fetch URL via headed Chrome, with caching and rate limiting.

        Args:
            url: The URL to fetch.
            use_cache: Whether to use the local file cache.
            wait_selector: An HTML substring (e.g. an element id) to wait for
                before returning. If *None*, waits until the Cloudflare
                challenge page disappears.
        """
        if use_cache:
            cached = self._cache_path(url)
            if cached.exists():
                logger.debug("cache_hit", url=url)
                return cached.read_text(encoding="utf-8")

        self._wait_for_rate_limit()
        logger.info("browser_request", url=url)

        # Run the async fetch in a dedicated event loop
        if self._loop is None or self._loop.is_closed():
            self._loop = asyncio.new_event_loop()
        html = self._loop.run_until_complete(self._fetch(url, wait_selector))
        self._last_request_time = time.monotonic()

        if use_cache:
            self._cache_dir.mkdir(parents=True, exist_ok=True)
            self._cache_path(url).write_text(html, encoding="utf-8")

        return html

    def close(self) -> None:
        if self._browser is not None:
            try:
                self._browser.stop()  # type: ignore[union-attr]
            except Exception:
                pass
            self._browser = None
        if self._loop is not None and not self._loop.is_closed():
            self._loop.close()
            self._loop = None

    def __enter__(self) -> BrowserClient:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()
