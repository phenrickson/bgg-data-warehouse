"""Discover new BGG item IDs by probing the XML API above the known frontier.

BGG publishes no "list all items" or "what's new" endpoint, so IDs have to be
discovered. The sitemap scrape does this by crawling HTML behind Cloudflare;
this module instead walks the numeric ID space upward through the authenticated
XML API, which has no bot-protection surface.

IDs are drawn from one pool shared with RPGs, video games and sleeves, so each
item's own `type` decides whether it is kept.
"""

import logging
from typing import Dict, List, Optional

from ..api_client.client import BGGAPIClient

logger = logging.getLogger(__name__)

# Types that belong in raw.thing_ids. The API also returns rpgitem, videogame,
# bgsleeve and others from the same ID pool; those are not board game items.
BOARD_GAME_TYPES = frozenset({"boardgame", "boardgameexpansion", "boardgameaccessory"})

# Measured 2026-09-14 against the live frontier: the largest gap between
# consecutive real IDs was 8 near the frontier and 112 across populated space,
# and density collapses to zero within ~500 IDs of the last real item. 500
# consecutive misses is therefore a wide margin over anything observed.
DEFAULT_STOP_AFTER_MISSES = 500

# The API accepts comma-separated IDs; 20 keeps URLs short and matches the
# batch size the detail pipeline already uses.
DEFAULT_BATCH_SIZE = 20

# BGG returns 429 well before the documented 2 req/s when requests are
# sustained - a 0.5s cadence was rate-limited after ~48 requests.
DEFAULT_THROTTLE = 2.0


class ProbeIDFetcher:
    """Finds new BGG item IDs by walking the ID space above a starting point."""

    def __init__(
        self,
        client: Optional[BGGAPIClient] = None,
        batch_size: int = DEFAULT_BATCH_SIZE,
        stop_after_misses: int = DEFAULT_STOP_AFTER_MISSES,
    ) -> None:
        """Initialize the prober.

        Args:
            client: API client to use. Defaults to one throttled for bulk use
                and with per-request BigQuery logging disabled.
            batch_size: IDs per API request.
            stop_after_misses: Consecutive non-existent IDs that end the walk.
        """
        self.client = client or BGGAPIClient(throttle_delay=DEFAULT_THROTTLE, log_requests=False)
        self.batch_size = batch_size
        self.stop_after_misses = stop_after_misses

    @staticmethod
    def _items_from_response(response: Optional[Dict]) -> List[Dict]:
        """Normalize the parsed XML into a list of item dicts.

        xmltodict collapses a single `<item>` into a dict and omits the key
        entirely when a batch matches nothing.
        """
        if not response:
            return []
        items = (response.get("items") or {}).get("item")
        if not items:
            return []
        return [items] if isinstance(items, dict) else items

    def probe(self, start_id: int, max_ids: Optional[int] = None) -> List[Dict]:
        """Walk the ID space upward from start_id, collecting board game items.

        Args:
            start_id: First ID to probe (typically max known ID + 1).
            max_ids: Optional hard cap on how many IDs to examine, as a
                backstop against walking indefinitely.

        Returns:
            List of {"game_id": int, "type": str} for board game items found.
        """
        found: List[Dict] = []
        consecutive_misses = 0
        examined = 0
        current = start_id

        logger.info(
            "Probing for new IDs from %d (stop after %d consecutive misses)",
            start_id,
            self.stop_after_misses,
        )

        while consecutive_misses < self.stop_after_misses:
            if max_ids is not None and examined >= max_ids:
                logger.warning("Probe hit max_ids cap of %d at ID %d", max_ids, current)
                break

            batch = list(range(current, current + self.batch_size))
            response = self.client.get_thing(batch, stats=False, type_filter=None)

            # A failed request is not evidence that the IDs are absent; treating
            # it as a miss could end the walk early and silently skip real items.
            if response is None:
                raise RuntimeError(f"API request failed while probing IDs {batch[0]}-{batch[-1]}")

            items = self._items_from_response(response)
            for item in items:
                item_type = item.get("@type")
                if item_type in BOARD_GAME_TYPES:
                    found.append({"game_id": int(item["@id"]), "type": item_type})

            if items:
                consecutive_misses = 0
            else:
                consecutive_misses += len(batch)

            examined += len(batch)
            current += self.batch_size

        logger.info(
            "Probe examined %d IDs from %d, found %d board game items",
            examined,
            start_id,
            len(found),
        )
        return found
