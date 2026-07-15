"""Tests for the browser-based BGG ID fetcher.

Focus: the retry/backoff hardening on fetch_sitemap_index (the first and most
critical navigation of the scrape). See id_fetcher_browser.py.
"""

from unittest import mock

import pytest

from src.modules.id_fetcher_browser import BrowserIDFetcher

SITEMAP_INDEX_HTML = """
<html><body>
https://boardgamegeek.com/sitemap_geekitems_boardgame_1
https://boardgamegeek.com/sitemap_geekitems_boardgame_2
https://boardgamegeek.com/sitemap_geekitems_boardgameexpansion_1
https://boardgamegeek.com/sitemap_geekitems_boardgameaccessory_1
</body></html>
"""


def _make_page(goto_side_effect, content=SITEMAP_INDEX_HTML):
    """Build a fake Playwright page with a scripted goto()."""
    page = mock.Mock()
    page.goto.side_effect = goto_side_effect
    page.title.return_value = ""  # no Cloudflare interstitial -> _wait returns at once
    page.content.return_value = content
    return page


@pytest.fixture(autouse=True)
def _no_sleep():
    """Skip the real exponential-backoff waits so tests run instantly."""
    with mock.patch("src.modules.id_fetcher_browser.time.sleep"):
        yield


def test_fetch_sitemap_index_success_first_try():
    fetcher = BrowserIDFetcher()
    page = _make_page(goto_side_effect=[None])

    urls = fetcher.fetch_sitemap_index(page)

    assert len(urls) == 4
    # boardgame sorts before expansion before accessory (last-write-wins typing)
    assert urls[0].endswith("boardgame_1")
    assert urls[-1].endswith("boardgameaccessory_1")
    assert page.goto.call_count == 1


def test_fetch_sitemap_index_retries_then_succeeds():
    fetcher = BrowserIDFetcher()
    # Fail the first two navigations (network blips), succeed on the third.
    page = _make_page(
        goto_side_effect=[
            RuntimeError("net::ERR_INTERNET_DISCONNECTED"),
            RuntimeError("Timeout 30000ms exceeded"),
            None,
        ]
    )

    urls = fetcher.fetch_sitemap_index(page)

    assert len(urls) == 4
    assert page.goto.call_count == 3


def test_fetch_sitemap_index_raises_after_max_retries():
    fetcher = BrowserIDFetcher()
    err = RuntimeError("net::ERR_NAME_NOT_RESOLVED")
    page = _make_page(goto_side_effect=[err, err, err])

    with pytest.raises(RuntimeError, match="ERR_NAME_NOT_RESOLVED"):
        fetcher.fetch_sitemap_index(page)

    assert page.goto.call_count == BrowserIDFetcher.MAX_RETRIES


def test_fetch_sitemap_index_empty_content_is_retryable():
    fetcher = BrowserIDFetcher()
    # goto() "succeeds" but the page has no sitemap URLs (a block/challenge
    # page) - this must be treated as a retryable failure, not a silent 0.
    page = _make_page(goto_side_effect=[None, None, None], content="<html>blocked</html>")

    with pytest.raises(RuntimeError, match="No sitemap URLs"):
        fetcher.fetch_sitemap_index(page)

    assert page.goto.call_count == BrowserIDFetcher.MAX_RETRIES
