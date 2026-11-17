"""BGG.CON library analytics: what the recorder's events say about the library.

Pure logic: no network, no clock, like ``recorder.py``. The warehouse API
(``services/warehouse_api/routers/bggcon.py``) reads the events and calls these.

Events are dicts: ``copy_id, bgg_id, name, event ('out' | 'in'), observed_at (UTC datetime),
lastcheckout_id``.

The recorder's first run wrote an ``out`` for every copy already out (the **baseline**). Those
start the copies-out count but are never checkouts. The caller passes the first run's time
(from ``raw.bggcon_runs``); without it, the earliest event's time stands in.

Spec: bgg-viewer docs/superpowers/specs/2026-11-17-bggcon-analytics-design.md.
"""

from __future__ import annotations

import statistics
from collections import defaultdict
from datetime import UTC, datetime, timedelta

from src.bggcon.recorder import WINDOW_START

#: Library hours, UTC (Chicago is UTC-6 all con week): Wed 9 AM-midnight, Thu-Sat 8 AM-midnight,
#: Sun 8 AM-1 PM. Last year's hours; 2026's weren't posted (Phil, 2026-11-17). Change them here.
LIBRARY_HOURS: list[tuple[datetime, datetime]] = [
    (datetime(2026, 11, 18, 15, 0, tzinfo=UTC), datetime(2026, 11, 19, 6, 0, tzinfo=UTC)),
    (datetime(2026, 11, 19, 14, 0, tzinfo=UTC), datetime(2026, 11, 20, 6, 0, tzinfo=UTC)),
    (datetime(2026, 11, 20, 14, 0, tzinfo=UTC), datetime(2026, 11, 21, 6, 0, tzinfo=UTC)),
    (datetime(2026, 11, 21, 14, 0, tzinfo=UTC), datetime(2026, 11, 22, 6, 0, tzinfo=UTC)),
    (datetime(2026, 11, 22, 14, 0, tzinfo=UTC), datetime(2026, 11, 22, 19, 0, tzinfo=UTC)),
]

#: The con's opening, Wed 9 AM Chicago: demand and open minutes count from here.
CON_OPEN = LIBRARY_HOURS[0][0]

BUCKET = timedelta(minutes=15)

#: A return checked in within this long of opening is an overnight front-desk drop-off.
DROP_OFF = timedelta(hours=1)


def _iso(t: datetime | None) -> str | None:
    return t.isoformat() if t else None


def dedupe(events: list[dict]) -> list[dict]:
    """One event per ``(copy_id, event, lastcheckout_id)``, the earliest; sorted by time.

    Two overlapping recorder runs can both insert the same events a minute apart.
    """
    best: dict[tuple, dict] = {}
    for e in events:
        key = (e["copy_id"], e["event"], e["lastcheckout_id"])
        if key not in best or e["observed_at"] < best[key]["observed_at"]:
            best[key] = e
    return sorted(best.values(), key=lambda e: e["observed_at"])


def _baseline(events: list[dict], baseline: datetime | None) -> datetime | None:
    """The first run's time; events at or before it are the baseline, never checkouts."""
    if baseline is not None:
        return baseline
    return events[0]["observed_at"] if events else None


def _checkouts(events: list[dict], baseline: datetime | None) -> list[dict]:
    """The ``out`` events that are real checkouts: everything but the baseline."""
    base = _baseline(events, baseline)
    return [e for e in events if e["event"] == "out" and (base is None or e["observed_at"] > base)]


def activity(events: list[dict], as_of: datetime | None, baseline: datetime | None = None) -> dict:
    """Checkouts per 15 minutes, from the window's start to the bucket holding ``as_of``."""
    hours = [{"open": o.isoformat(), "close": c.isoformat()} for o, c in LIBRARY_HOURS]
    if as_of is None or as_of < WINDOW_START:
        return {"as_of": _iso(as_of), "hours": hours, "buckets": []}
    n = int((as_of - WINDOW_START) / BUCKET) + 1
    counts = [0] * n
    for e in _checkouts(dedupe(events), baseline):
        i = int((e["observed_at"] - WINDOW_START) / BUCKET)
        if 0 <= i < n:
            counts[i] += 1
    buckets = [
        {"start": (WINDOW_START + i * BUCKET).isoformat(), "checkouts": c}
        for i, c in enumerate(counts)
    ]
    return {"as_of": as_of.isoformat(), "hours": hours, "buckets": buckets}


def _open_windows(as_of: datetime) -> list[tuple[datetime, datetime]]:
    """Library hours from the con's opening to ``as_of``."""
    out = []
    for opens, closes in LIBRARY_HOURS:
        o, c = max(opens, CON_OPEN), min(closes, as_of)
        if o < c:
            out.append((o, c))
    return out


def _minutes_open(start: datetime, end: datetime, windows: list) -> float:
    total = timedelta()
    for o, c in windows:
        lo, hi = max(start, o), min(end, c)
        if lo < hi:
            total += hi - lo
    return total.total_seconds() / 60


def _minutes_all_out(evs: list[dict], copies: int, as_of: datetime, windows) -> int:
    """Minutes, within library hours, with at least ``copies`` copies of the game out."""
    count, since, total = 0, None, 0.0
    for e in evs:
        count = count + 1 if e["event"] == "out" else max(0, count - 1)
        if count >= copies and since is None:
            since = e["observed_at"]
        elif count < copies and since is not None:
            total += _minutes_open(since, e["observed_at"], windows)
            since = None
    if since is not None:
        total += _minutes_open(since, as_of, windows)
    return round(total)


def _end_of_play(start: datetime, end: datetime) -> datetime:
    """An overnight drop-off (checked in just after opening, taken out before it) ends at the
    previous close."""
    for i, (o, _) in enumerate(LIBRARY_HOURS):
        if i and o <= end < o + DROP_OFF and start < o:
            # Never before it went out: a checkout first seen after close counts as 0 min.
            return max(LIBRARY_HOURS[i - 1][1], start)
    return end


def _durations(evs: list[dict], baseline: datetime) -> list[float]:
    """Minutes out for each checkout that came back (paired by copy and checkout id)."""
    outs = {
        (e["copy_id"], e["lastcheckout_id"]): e["observed_at"]
        for e in evs
        if e["event"] == "out" and e["observed_at"] > baseline
    }
    mins = []
    for e in evs:
        start = outs.get((e["copy_id"], e["lastcheckout_id"])) if e["event"] == "in" else None
        if start is not None:
            mins.append((_end_of_play(start, e["observed_at"]) - start).total_seconds() / 60)
    return mins


def _quartiles(mins: list[float]) -> tuple[int | None, int | None, int | None]:
    if not mins:
        return None, None, None
    if len(mins) == 1:
        m = round(mins[0])
        return m, m, m
    q1, med, q3 = statistics.quantiles(mins, n=4, method="inclusive")
    return round(q1), round(med), round(q3)


def games(
    events: list[dict],
    as_of: datetime | None,
    copies: dict[int, int],
    baseline: datetime | None = None,
) -> dict:
    """Per-game checkouts, recent trend, availability and time out, by checkouts (most first)."""
    evs = dedupe(events)
    if as_of is None or not evs:
        return {"as_of": _iso(as_of), "games": []}
    baseline = _baseline(evs, baseline)
    windows = _open_windows(as_of)
    three_ago, six_ago = as_of - timedelta(hours=3), as_of - timedelta(hours=6)
    open_minutes = round(sum((c - o).total_seconds() / 60 for o, c in windows))
    by_game: dict[int, list[dict]] = defaultdict(list)
    for e in evs:
        if e["bgg_id"]:
            by_game[e["bgg_id"]].append(e)

    rows = []
    for bgg_id, ge in by_game.items():
        outs = [
            e["observed_at"] for e in ge if e["event"] == "out" and e["observed_at"] > baseline
        ]
        n = copies.get(bgg_id)
        q1, med, q3 = _quartiles(_durations(ge, baseline))
        rows.append(
            {
                "bgg_id": bgg_id,
                "name": next((e["name"] for e in reversed(ge) if e["name"]), None),
                "copies": n,
                "checkouts": len(outs),
                "checkouts_last_3h": sum(three_ago < t <= as_of for t in outs),
                "checkouts_prev_3h": sum(six_ago < t <= three_ago for t in outs),
                "minutes_all_out": _minutes_all_out(ge, n, as_of, windows) if n else None,
                "open_minutes": open_minutes,
                "time_out_median": med,
                "time_out_q1": q1,
                "time_out_q3": q3,
            }
        )
    rows.sort(key=lambda r: (-r["checkouts"], r["bgg_id"]))
    return {"as_of": as_of.isoformat(), "games": rows}
