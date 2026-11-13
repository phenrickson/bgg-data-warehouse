"""TEMPORARY (BGG.CON 2026): record the convention library's checkouts once a minute.

Cloud Scheduler runs this Cloud Run job every minute during the con (spec §8 in bgg-viewer's
docs/superpowers/specs/2026-11-12-bggcon-library-design.md). Each run:

1. exits at once outside the recording window, or while backing off after a 429 or 5xx;
2. fetches the copies checked out right now from Tabletop.Events (public API, a page or a
   few, one at a time);
3. inserts one event per change (``raw.bggcon_checkout_events``) and one heartbeat row
   (``raw.bggcon_runs``), so gaps in coverage show in the report;
4. writes the current per-game counts to ``bggcon/checked_out.json`` in the viewer bucket,
   which the dashboard's availability endpoint reads, with each game's checkouts since the
   con opened (``checkouts``, a running total kept in the state);
5. sends a phone push (ntfy) when a game in the alert user's focus list comes back on the
   shelf;
6. saves its state (``bggcon/recorder/state.json``) for the next run to compare against.

Events are inserted before the state is saved: if two runs ever overlap, the worst case is
duplicate rows (the report drops them by copy, event and checkout id), never lost minutes.

No borrower data is fetched or stored.

Run: ``python -m src.pipeline.bggcon_recorder``. Env: ``BGGCON_BUCKET`` (default the viewer
artifacts bucket), ``NTFY_TOPIC`` and ``BGGCON_ALERT_USER`` (both optional; no alerts
without them), ``BGGCON_TEST_RUN=1`` for a manual run outside the window (see ``run_once``).
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from src.bggcon import recorder as r

logger = logging.getLogger(__name__)

LIBRARY_URL = "https://tabletop.events/api/library/0AEB11DA-2B7D-11EC-B400-855F800FD618/librarygames"
PER_PAGE = 100  # the API's cap
PAUSE_S = 0.3
TIMEOUT_S = 20

STATE = "bggcon/recorder/state.json"
#: A manual test run's own state, so the first scheduled run starts clean.
TEST_STATE = "bggcon/recorder/state.test.json"
LATEST = "bggcon/checked_out.json"
LIBRARY = "bggcon/library.json"
EVENTS_TABLE = "raw.bggcon_checkout_events"
RUNS_TABLE = "raw.bggcon_runs"
DASHBOARD_URL = "https://boardgame-viz.com/bggcon"


def stars_name(user_id: str) -> str:
    return f"bggcon/stars/{user_id}.json"


class FetchError(Exception):
    def __init__(self, status: int | None):
        super().__init__(f"Tabletop.Events request failed ({status})")
        self.status = status


class Conflict(Exception):
    """Another run saved the state first."""


@dataclass
class Deps:
    fetch_checked_out: Callable[[], list[dict]]
    #: (data, generation); (None, None) when the object doesn't exist.
    read_json: Callable[[str], tuple[Any, int | None]]
    #: ``if_generation`` 0 means "must not exist yet"; None means overwrite.
    write_json: Callable[..., None]
    insert_rows: Callable[[str, list[dict]], None]
    notify: Callable[[str, str], None] | None
    now: Callable[[], datetime]


def _heartbeat(now: datetime, n_out: int | None, ok: bool, note: str | None) -> dict:
    return {"observed_at": now.isoformat(), "n_out": n_out, "ok": ok, "note": note}


def run_once(deps: Deps, alert_user: str | None = None, test_run: bool = False) -> str:
    """One minute's recording. ``test_run`` (a manual run, ``BGGCON_TEST_RUN=1``) goes ahead
    outside the window, keeps its own state file and notes "test run" on its heartbeat.

    Runs a minute apart can overlap, since a Cloud Run job can take most of a minute to start.
    Both read the same state; the one that saves second gets a ``Conflict`` and stops here,
    cleanly. Its events and heartbeat are already in (duplicates, never lost minutes), and it
    sends no alerts, since the run that won does."""
    try:
        return _record(deps, alert_user, test_run)
    except Conflict:
        return "another run saved first"


def _record(deps: Deps, alert_user: str | None, test_run: bool) -> str:
    now = deps.now()
    test_run = test_run or r.in_test_window(now)
    if not test_run and not r.in_window(now):
        return "outside the window"
    state_name = TEST_STATE if test_run else STATE
    ok_note = "test run" if test_run else None

    state, gen = deps.read_json(state_name)
    state = state or {}
    if r.should_skip(now, state.get("next_allowed")):
        deps.insert_rows(RUNS_TABLE, [_heartbeat(now, None, False, "backing off")])
        return "backing off"

    try:
        rows = deps.fetch_checked_out()
    except FetchError as e:
        until = r.backoff_until(now, e.status)
        deps.write_json(state_name, {**state, "next_allowed": until.isoformat() if until else None}, if_generation=gen or 0)
        deps.insert_rows(RUNS_TABLE, [_heartbeat(now, None, False, f"fetch failed ({e.status})")])
        return f"fetch failed ({e.status})"

    current = [r.Copy.from_row(row) for row in rows]
    had_state = "copies" in state
    prev_copies: dict[str, dict] = state.get("copies", {})
    events = r.diff(
        {cid: c["lastcheckout_id"] for cid, c in prev_copies.items()} if had_state else None,
        current,
        now,
        known=prev_copies,
    )
    now_out = r.checked_out_by_game(current)
    prev_out = {int(k): v for k, v in state.get("counts", {}).items()}
    checkouts = r.add_checkouts({int(k): v for k, v in state.get("checkouts", {}).items()}, events)

    if events:
        deps.insert_rows(EVENTS_TABLE, events)
    deps.insert_rows(RUNS_TABLE, [_heartbeat(now, len(current), True, ok_note)])
    deps.write_json(
        LATEST,
        {
            "as_of": now.isoformat(),
            "checked_out": {str(k): v for k, v in now_out.items()},
            "checkouts": {str(k): v for k, v in checkouts.items()},
        },
    )
    deps.write_json(
        state_name,
        {
            "at": now.isoformat(),
            "copies": {
                c.copy_id: {"lastcheckout_id": c.lastcheckout_id, "bgg_id": c.bgg_id, "name": c.name}
                for c in current
            },
            "counts": {str(k): v for k, v in now_out.items()},
            "checkouts": {str(k): v for k, v in checkouts.items()},
            "next_allowed": None,
        },
        if_generation=gen or 0,
    )

    if had_state and alert_user and deps.notify:
        try:
            _alert(deps, alert_user, prev_out, now_out)
        except Exception:  # an alert must never cost a recorded minute
            logger.exception("BGG.CON alert failed")

    return f"recorded {len(events)} events"


def _alert(deps: Deps, user: str, prev_out: dict[int, int], now_out: dict[int, int]) -> None:
    library, _ = deps.read_json(LIBRARY)
    starred, _ = deps.read_json(stars_name(user))
    if not library or not starred:
        return
    games = {g["bgg_id"]: g for g in library["games"]}
    copies = {i: g["copies"] for i, g in games.items()}
    for bgg_id in r.back_on_shelf(prev_out, now_out, copies, set(starred)):
        n = copies[bgg_id]
        deps.notify(f"BGG.CON: {games[bgg_id]['name']} is back", f"{n - now_out.get(bgg_id, 0)}/{n} available")


# --- the real services -----------------------------------------------------------------


def _fetch_checked_out() -> list[dict]:
    import requests

    out: list[dict] = []
    total, page = 1, 1
    while page <= total:
        if page > 1:
            time.sleep(PAUSE_S)
        try:
            res = requests.get(
                LIBRARY_URL,
                params={"is_checked_out": 1, "_items_per_page": PER_PAGE, "_page_number": page},
                timeout=TIMEOUT_S,
            )
        except requests.RequestException as e:
            raise FetchError(None) from e
        if res.status_code != 200:
            raise FetchError(res.status_code)
        result = res.json()["result"]
        out.extend(result["items"])
        total = int(result["paging"]["total_pages"]) or 1
        page += 1
    return out


def _real_deps() -> Deps:
    import requests
    from google.api_core.exceptions import NotFound, PreconditionFailed
    from google.cloud import bigquery, storage

    bucket = storage.Client().bucket(os.environ.get("BGGCON_BUCKET", "bgg-data-warehouse-bgg-viewer-artifacts"))
    bq = bigquery.Client()
    topic = os.environ.get("NTFY_TOPIC")

    def read_json(name: str):
        blob = bucket.blob(name)
        try:
            blob.reload()
            return json.loads(blob.download_as_text(if_generation_match=blob.generation)), blob.generation
        except NotFound:
            return None, None

    def write_json(name: str, data, if_generation: int | None = None):
        blob = bucket.blob(name)
        try:
            blob.upload_from_string(
                json.dumps(data), content_type="application/json", if_generation_match=if_generation
            )
        except PreconditionFailed as e:
            raise Conflict(name) from e

    def insert_rows(table: str, rows: list[dict]):
        errors = bq.insert_rows_json(f"{bq.project}.{table}", rows)
        if errors:
            raise RuntimeError(f"BigQuery insert into {table} failed: {errors}")

    def notify(title: str, message: str):
        requests.post(
            f"https://ntfy.sh/{topic}",
            data=message.encode(),
            headers={"Title": title, "Click": DASHBOARD_URL, "Tags": "game_die"},
            timeout=10,
        ).raise_for_status()

    return Deps(
        fetch_checked_out=_fetch_checked_out,
        read_json=read_json,
        write_json=write_json,
        insert_rows=insert_rows,
        notify=notify if topic else None,
        now=lambda: datetime.now(UTC),
    )


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    started = time.monotonic()
    result = run_once(
        _real_deps(),
        alert_user=os.environ.get("BGGCON_ALERT_USER"),
        test_run=os.environ.get("BGGCON_TEST_RUN") == "1",
    )
    logger.info("BGG.CON recorder: %s (%.1fs)", result, time.monotonic() - started)
    return 0


if __name__ == "__main__":
    sys.exit(main())
