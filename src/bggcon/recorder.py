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
from zoneinfo import ZoneInfo

# Tue 17 Nov 2026 00:00 (test day) to Sun 22 Nov 15:00, America/Chicago (CST, UTC−6).
WINDOW_START = datetime(2026, 11, 17, 6, 0, tzinfo=UTC)
WINDOW_END = datetime(2026, 11, 22, 21, 0, tzinfo=UTC)

#: Checkouts count from the start of the window, the test day included (Phil, 2026-11-17). The
#: change deployed mid-Tuesday, so the totals start from that deploy, not from Tuesday 00:00.
COUNT_FROM = WINDOW_START

#: After the n-th failure in a row, wait BACKOFF_MINUTES[n-1] (the last one from then on).
BACKOFF_MINUTES = (2, 5, 10, 15)

#: The library is closed overnight, so poll only every QUIET_EVERY_MIN minutes from midnight to
#: 7 AM Chicago (counts sat flat through those hours on Wed and Thu).
CHICAGO = ZoneInfo("America/Chicago")
QUIET_HOURS = range(0, 7)
QUIET_EVERY_MIN = 15


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


def add_checkouts(total: dict[int, int], events: list[dict]) -> dict[int, int]:
    """The running count of checkouts per game since ``COUNT_FROM``, plus this run's ``out`` events.

    A return and re-checkout between runs is its own ``out`` event, so it counts too.
    """
    out = dict(total)
    for e in events:
        if e["event"] == "out" and e["bgg_id"] and datetime.fromisoformat(e["observed_at"]) >= COUNT_FROM:
            out[e["bgg_id"]] = out.get(e["bgg_id"], 0) + 1
    return out


def came_back(prev_out: dict[int, int], now_out: dict[int, int]) -> bool:
    """Whether any game has fewer copies out than last run: the cheap check that lets a quiet
    minute skip reading every user's alert files."""
    return any(now_out.get(bgg_id, 0) < n for bgg_id, n in prev_out.items())


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


def backoff_until(now: datetime, failures: int) -> datetime:
    """After any failed fetch (timeout, 404, 429, 5xx...), wait before asking again, longer each
    failure in a row, so a struggling Tabletop.Events doesn't get a request every minute."""
    i = min(max(failures, 1), len(BACKOFF_MINUTES)) - 1
    return now + timedelta(minutes=BACKOFF_MINUTES[i])


def quiet_skip(now: datetime) -> bool:
    """True for a minute the recorder sits out: overnight, all but every QUIET_EVERY_MIN-th."""
    local = now.astimezone(CHICAGO)
    return local.hour in QUIET_HOURS and local.minute % QUIET_EVERY_MIN != 0


def should_skip(now: datetime, next_allowed: str | None) -> bool:
    return bool(next_allowed) and now < datetime.fromisoformat(next_allowed)
