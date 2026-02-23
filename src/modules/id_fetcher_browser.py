"""Module for fetching BoardGameGeek IDs using browser automation to bypass Cloudflare."""

import logging
import re
import time
from pathlib import Path

import requests as http_requests
from playwright.sync_api import sync_playwright, Browser, Page

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# User agent shared between browser and requests
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"


class BrowserIDFetcher:
    """Fetches BGG game IDs by scraping sitemaps with a real browser."""

    SITEMAP_INDEX_URL = "https://boardgamegeek.com/sitemapindex"

    # Pattern to match sitemap URLs for board games
    SITEMAP_PATTERN = re.compile(
        r'https://boardgamegeek\.com/sitemap_geekitems_boardgame(expansion|accessory|)_\d+'
    )

    # Pattern to extract game IDs from sitemap entries
    GAME_ID_PATTERN = re.compile(
        r'https://boardgamegeek\.com/boardgame(expansion|accessory|)/(\d+)'
    )

    # Sitemap type ordering for correct type assignment.
    # More specific types must come AFTER less specific types so they
    # overwrite in the last-write-wins deduplication dict.
    # This matches the activityclub.org Perl script behavior.
    SITEMAP_TYPE_ORDER = {"boardgame": 0, "boardgameexpansion": 1, "boardgameaccessory": 2}

    def __init__(self, headless: bool = True):
        """Initialize the browser-based fetcher.

        Args:
            headless: Run browser in headless mode (default True)
        """
        self.headless = headless
        self._browser: Browser | None = None
        self._page: Page | None = None

    def _wait_for_cloudflare(self, page: Page, timeout: int = 30) -> None:
        """Wait for Cloudflare challenge to complete.

        Args:
            page: Playwright page object
            timeout: Max seconds to wait
        """
        start = time.time()
        while time.time() - start < timeout:
            title = page.title()
            if "Just a moment" not in title and "Cloudflare" not in title:
                return
            logger.info("Waiting for Cloudflare challenge...")
            time.sleep(2)
        raise TimeoutError("Cloudflare challenge did not complete in time")

    def _sitemap_sort_key(self, url: str) -> tuple:
        """Sort key for sitemap URLs: boardgame < boardgameexpansion < boardgameaccessory.

        Args:
            url: Sitemap URL

        Returns:
            Tuple of (type_order, page_number) for sorting
        """
        match = self.SITEMAP_PATTERN.search(url)
        if match:
            suffix = match.group(1)  # '', 'expansion', or 'accessory'
            sitemap_type = f"boardgame{suffix}"
            type_order = self.SITEMAP_TYPE_ORDER.get(sitemap_type, 99)
            # Extract page number from end of URL
            page_num = int(url.rsplit("_", 1)[-1])
            return (type_order, page_num)
        return (99, 0)

    def fetch_sitemap_index(self, page: Page) -> list[str]:
        """Fetch and parse the sitemap index to get individual sitemap URLs.

        Uses the browser to bypass Cloudflare on the index page.

        Args:
            page: Playwright page object

        Returns:
            List of sitemap URLs for board games, sorted by type
        """
        logger.info(f"Fetching sitemap index: {self.SITEMAP_INDEX_URL}")
        page.goto(self.SITEMAP_INDEX_URL)
        self._wait_for_cloudflare(page)

        content = page.content()

        full_urls = []
        for match in re.finditer(self.SITEMAP_PATTERN, content):
            full_urls.append(match.group(0))

        # Sort: boardgame sitemaps first, then expansion, then accessory
        full_urls.sort(key=self._sitemap_sort_key)

        logger.info(f"Found {len(full_urls)} board game sitemaps")
        for url in full_urls:
            logger.info(f"  {url}")
        return full_urls

    def fetch_sitemap_page(self, url: str) -> list[dict]:
        """Fetch a single sitemap page via HTTP and extract game IDs.

        Individual sitemap XML pages are not behind Cloudflare,
        so plain HTTP requests work and avoid browser memory issues.

        Args:
            url: Sitemap URL to fetch

        Returns:
            List of dicts with game_id and type
        """
        logger.info(f"Fetching sitemap: {url}")
        resp = http_requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=60)
        resp.raise_for_status()

        content = resp.text
        games = []

        for match in re.finditer(self.GAME_ID_PATTERN, content):
            game_type_suffix = match.group(1)  # '', 'expansion', or 'accessory'
            game_id = int(match.group(2))
            game_type = f"boardgame{game_type_suffix}"
            games.append({"game_id": game_id, "type": game_type})

        logger.info(f"Found {len(games)} games in {url}")
        return games

    def fetch_all_ids(self) -> list[dict]:
        """Fetch all game IDs from BGG sitemaps.

        Uses browser only for the sitemap index (Cloudflare protected),
        then plain HTTP for individual sitemaps (not Cloudflare protected).
        Sitemaps are processed in order so that more specific types
        (expansion, accessory) overwrite less specific types (boardgame).

        Returns:
            List of dicts with game_id and type
        """
        all_games: dict[int, str] = {}  # game_id -> type (deduped, last-write-wins)

        # Step 1: Use browser only for the Cloudflare-protected sitemap index
        with sync_playwright() as p:
            logger.info("Launching browser for sitemap index...")
            browser = p.chromium.launch(headless=self.headless)
            context = browser.new_context(user_agent=USER_AGENT)
            page = context.new_page()

            try:
                sitemap_urls = self.fetch_sitemap_index(page)
            finally:
                browser.close()
                logger.info("Browser closed")

        # Step 2: Fetch individual sitemaps via plain HTTP (no browser needed)
        # All sitemaps must succeed — partial results cause type misclassification
        for i, sitemap_url in enumerate(sitemap_urls):
            logger.info(f"Processing sitemap {i+1}/{len(sitemap_urls)}")
            games = self.fetch_sitemap_page(sitemap_url)
            for game in games:
                all_games[game["game_id"]] = game["type"]

            # Be nice to the server
            time.sleep(1)

        # Convert back to list format
        result = [{"game_id": gid, "type": gtype} for gid, gtype in all_games.items()]
        logger.info(f"Total unique games found: {len(result)}")
        return result

    def save_to_file(self, games: list[dict], output_path: Path) -> None:
        """Save games to a thingids.txt format file.

        Args:
            games: List of game dicts
            output_path: Path to output file
        """
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Sort by game_id
        sorted_games = sorted(games, key=lambda g: g["game_id"])

        with open(output_path, "w") as f:
            for game in sorted_games:
                f.write(f"{game['game_id']} {game['type']}\n")

        logger.info(f"Saved {len(games)} games to {output_path}")


def main():
    """Run the browser-based ID fetcher."""
    fetcher = BrowserIDFetcher(headless=True)
    games = fetcher.fetch_all_ids()

    output_path = Path("temp/thingids_browser.txt")
    fetcher.save_to_file(games, output_path)

    print(f"Done! Found {len(games)} games. Output: {output_path}")


if __name__ == "__main__":
    main()
