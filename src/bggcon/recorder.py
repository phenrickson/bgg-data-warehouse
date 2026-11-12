"""BGG.CON library recorder: what changed since the last run, and whether to send an alert.

Pure logic: no network, no clock. The job (`src.pipeline.bggcon_recorder`) does the I/O.

Tabletop.Events lists physical copies. Each run fetches the copies checked out right now and
compares them with the previous run's set:

- a copy that wasn't out before is an ``out`` event;
- a copy that was out and isn't any more is an ``in`` event;
- a copy still out under a different ``lastcheckout_id`` came back and went out again between
  runs, so it's an ``in`` and an ``out``.

A copy that goes out and comes back within one minute is never seen. Accepted (spec §8).

No borrower data: Tabletop.Events checkout records name the borrower; we keep only copy,
game and time.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

# Tue 17 Nov 2026 00:00 (test day) to Sun 22 Nov 15:00, America/Chicago (CST, UTC−6).
WINDOW_START = datetime(2026, 11, 17, 6, 0, tzinfo=UTC)
WINDOW_END = datetime(2026, 11, 22, 21, 0, tzinfo=UTC)

BACKOFF = timedelta(minutes=5)


@dataclass(frozen=True)
class Copy:
    """One checked-out copy, as much of it as we keep."""

    copy_id: str
    bgg_id: int | None
    name: str
    lastcheckout_id: str | None
    #: Tabletop.Events' own timestamp for the checkout (its local time, as it sends it).
    last_checkout_date: str | None

    @classmethod
    def from_row(cls, row: dict) -> Copy:
        bgg = row.get("bgg_id")
        return cls(
            copy_id=row["id"],
            bgg_id=int(bgg) if bgg else None,
            name=row.get("name") or "",
            lastcheckout_id=row.get("lastcheckout_id"),
            last_checkout_date=row.get("last_checkout_date"),
        )


def _event(kind: str, cid: str, bgg_id, name, last_id, when: datetime, tte_date=None) -> dict:
    return {
        "copy_id": cid,
        "bgg_id": bgg_id,
        "name": name,
        "event": kind,
        "observed_at": when.isoformat(),
        "lastcheckout_id": last_id,
        "tte_checkout_date": tte_date,
    }


def diff(
    prev: dict[str, str | None] | None,
    current: list[Copy],
    observed_at: datetime,
    known: dict[str, dict] | None = None,
) -> list[dict]:
    """Events between the previous run's copies and this run's.

    ``prev`` maps copy id to its ``lastcheckout_id`` (None on the first run, when every copy
    already out is recorded as ``out``). ``known`` holds the previous run's game and name
    for each copy, for the ``in`` events of copies that are no longer listed.
    """
    prev = prev or {}
    known = known or {}
    events: list[dict] = []
    now_ids = {c.copy_id for c in current}

    for c in current:
        if c.copy_id in prev and prev[c.copy_id] == c.lastcheckout_id:
            continue
        if c.copy_id in prev:  # same copy, new checkout: back in, then out again
            events.append(_event("in", c.copy_id, c.bgg_id, c.name, prev[c.copy_id], observed_at))
        events.append(
            _event("out", c.copy_id, c.bgg_id, c.name, c.lastcheckout_id, observed_at, c.last_checkout_date)
        )

    for cid, last_id in prev.items():
        if cid not in now_ids:
            k = known.get(cid, {})
            events.append(_event("in", cid, k.get("bgg_id"), k.get("name"), last_id, observed_at))

    return events


def checked_out_by_game(current: list[Copy]) -> dict[int, int]:
    """Copies out per BGG id (what the dashboard reads)."""
    out: dict[int, int] = {}
    for c in current:
        if c.bgg_id:
            out[c.bgg_id] = out.get(c.bgg_id, 0) + 1
    return out


def back_on_shelf(
    prev_out: dict[int, int], now_out: dict[int, int], copies: dict[int, int], starred: set[int]
) -> list[int]:
    """Starred games that went from every copy out to at least one on the shelf."""
    hits = []
    for bgg_id in sorted(starred):
        n = copies.get(bgg_id)
        if not n:
            continue
        if prev_out.get(bgg_id, 0) >= n and now_out.get(bgg_id, 0) < n:
            hits.append(bgg_id)
    return hits


def in_window(now: datetime) -> bool:
    return WINDOW_START <= now < WINDOW_END


def backoff_until(now: datetime, status: int | None) -> datetime | None:
    """After a 429 or 5xx, wait before asking again; other failures don't back off."""
    if status is not None and (status == 429 or status >= 500):
        return now + BACKOFF
    return None


def should_skip(now: datetime, next_allowed: str | None) -> bool:
    return bool(next_allowed) and now < datetime.fromisoformat(next_allowed)
