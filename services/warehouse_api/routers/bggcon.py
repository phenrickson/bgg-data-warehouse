"""BGG.CON library analytics router (TEMPORARY, BGG.CON 2026).

Thin HTTP shell: ``src.warehouse.readers.bggcon`` reads, ``src.bggcon.analytics`` computes.
Serves bgg-viewer's /bggcon/analytics page.
"""

import time

from fastapi import APIRouter

from src.bggcon import analytics
from src.warehouse.readers import bggcon as reader

router = APIRouter(prefix="/bggcon", tags=["bggcon"])

# The recorder writes once a minute, so a minute's cache costs nothing in freshness. Both
# endpoints share one read of the events, the latest run and the library.
_CACHE_TTL_SECONDS = 60
_cache: dict[str, tuple[float, tuple]] = {}


def _reset_cache() -> None:
    """Test seam: clears the module-level cache between tests."""
    _cache.clear()


def _inputs() -> tuple:
    now = time.time()
    hit = _cache.get("inputs")
    if hit is not None and now - hit[0] < _CACHE_TTL_SECONDS:
        return hit[1]
    inputs = (
        reader.fetch_events(),
        reader.fetch_as_of(),
        reader.fetch_copies(),
        reader.fetch_baseline(),
    )
    _cache["inputs"] = (now, inputs)
    return inputs


@router.get("/activity")
def get_activity() -> dict:
    """Checkouts per 15 minutes since the recorder's window opened, with the library hours."""
    events, as_of, _, baseline = _inputs()
    return analytics.activity(events, as_of, baseline)


@router.get("/games")
def get_games() -> dict:
    """Per-game checkouts, recent trend, availability and time out."""
    events, as_of, copies, baseline = _inputs()
    return analytics.games(events, as_of, copies, baseline)
