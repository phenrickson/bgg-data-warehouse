"""Discover new BGG item IDs by probing the XML API around the known frontier.

BGG publishes no "list all items" or "what's new" endpoint, so IDs have to be
discovered. The sitemap scrape does this by crawling HTML behind Cloudflare;
this module instead walks the numeric ID space upward through the authenticated
XML API, which has no bot-protection surface.

BGG allocates an ID on submission but publishes the item only after approval,
often weeks later - so most newly visible IDs sit *below* the frontier, in
space the walk already passed. The walk therefore starts some way back (the
caller's lookback), skipping IDs already in raw.thing_ids. Below the frontier a
run of empty IDs is normal and never ends the walk; only misses at or above the
frontier count toward the stop. See
docs/superpowers/specs/2026-09-18-id-probe-lookback-design.md.

IDs are drawn from one pool shared with RPGs, video games and sleeves, so each
item's own `type` decides whether it is kept.
"""

import logging
from typing import Dict, FrozenSet, Iterator, List, Optional

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

    def _batches(self, start_id: int, skip: FrozenSet[int]) -> Iterator[List[int]]:
        """Yield successive batches of unknown IDs, walking upward from start_id.

        A batch may span a gap where known IDs were skipped, so its IDs are not
        necessarily contiguous.
        """
        current = start_id
        while True:
            batch: List[int] = []
            while len(batch) < self.batch_size:
                if current not in skip:
                    batch.append(current)
                current += 1
            yield batch

    def probe(
        self,
        start_id: int,
        *,
        frontier_id: Optional[int] = None,
        skip: FrozenSet[int] = frozenset(),
        max_ids: Optional[int] = None,
    ) -> List[Dict]:
        """Walk the ID space upward from start_id, collecting board game items.

        Args:
            start_id: First ID to consider. Equal to the frontier for a pure
                frontier walk; `max_known - lookback` to re-probe the space
                below it.
            frontier_id: The highest known ID. Misses below it are expected and
                do not count toward the stop; misses at or above it do.
                Defaults to start_id.
            skip: IDs already known (present in raw.thing_ids); never requested.
            max_ids: Optional hard cap on how many IDs to examine, as a
                backstop against walking indefinitely.

        Returns:
            List of {"game_id": int, "type": str} for board game items found.
        """
        if frontier_id is None:
            frontier_id = start_id

        found: List[Dict] = []
        consecutive_misses = 0
        examined = 0

        logger.info(
            "Probing for new IDs from %d (frontier %d, %d known IDs skipped, "
            "stop after %d consecutive misses above the frontier)",
            start_id,
            frontier_id,
            len(skip),
            self.stop_after_misses,
        )

        for batch in self._batches(start_id, skip):
            if consecutive_misses >= self.stop_after_misses:
                break
            if max_ids is not None and examined >= max_ids:
                logger.warning("Probe hit max_ids cap of %d at ID %d", max_ids, batch[0])
                break

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
            elif batch[-1] >= frontier_id:
                # Only misses at/above the frontier are evidence we've run out
                # of ID space; below it, gaps are unpublished submissions.
                consecutive_misses += sum(1 for i in batch if i >= frontier_id)

            examined += len(batch)

        logger.info(
            "Probe examined %d IDs from %d (frontier %d), found %d board game items",
            examined,
            start_id,
            frontier_id,
            len(found),
        )
        return found
