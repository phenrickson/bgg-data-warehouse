"""BGG.CON library analytics: activity buckets and per-game stats from the recorder's events."""

from datetime import UTC, datetime, timedelta

from src.bggcon.analytics import CON_OPEN, activity, dedupe, games
from src.bggcon.recorder import WINDOW_START

T = CON_OPEN  # Wed 18 Nov 9:00 AM Chicago
BASE = WINDOW_START  # the recorder's first run: its events are the baseline


def ev(copy, bgg, event, t, last="co-1", name="Catan"):
    return {
        "copy_id": copy, "bgg_id": bgg, "name": name, "event": event,
        "observed_at": t, "lastcheckout_id": last,
    }


def row(result, bgg_id):
    [r] = [g for g in result["games"] if g["bgg_id"] == bgg_id]
    return r


def test_dedupe_keeps_the_earliest_of_a_duplicate():
    a = ev("c1", 13, "out", T + timedelta(minutes=5))
    b = ev("c1", 13, "out", T + timedelta(minutes=6))
    assert dedupe([b, a]) == [a]


def test_activity_counts_outs_per_15_minutes_and_skips_the_baseline():
    events = [
        ev("c0", 7, "out", BASE),
        ev("c1", 13, "out", T + timedelta(minutes=1)),
        ev("c2", 13, "out", T + timedelta(minutes=14)),
        ev("c1", 13, "in", T + timedelta(minutes=20)),
    ]
    a = activity(events, as_of=T + timedelta(minutes=20))
    by = {b["start"]: b["checkouts"] for b in a["buckets"]}
    assert by[T.isoformat()] == 2
    assert by[(T + timedelta(minutes=15)).isoformat()] == 0
    assert a["buckets"][0]["start"] == BASE.isoformat()
    assert sum(by.values()) == 2
    assert a["buckets"][-1]["start"] == (T + timedelta(minutes=15)).isoformat()
    assert len(a["hours"]) == 5
    assert a["hours"][0] == {"open": T.isoformat(), "close": "2026-11-19T06:00:00+00:00"}


def test_no_data_is_empty_not_an_error():
    a = activity([], None)
    assert a["buckets"] == [] and a["as_of"] is None
    assert games([], None, {}) == {"as_of": None, "games": []}


def test_games_counts_checkouts_and_the_two_3h_windows():
    now = T + timedelta(hours=6)
    events = [
        ev("c0", 13, "out", BASE),
        ev("c1", 13, "out", now - timedelta(hours=1)),
        ev("c2", 13, "out", now - timedelta(hours=2), last="co-2"),
        ev("c3", 13, "out", now - timedelta(hours=4), last="co-3"),
    ]
    g = row(games(events, now, {13: 5}), 13)
    assert (g["checkouts"], g["checkouts_last_3h"], g["checkouts_prev_3h"]) == (3, 2, 1)


def test_minutes_all_out_counts_only_library_hours_and_open_minutes_run_from_wed_9am():
    # One copy, out 11 PM to 1 AM Chicago (05:00-07:00 UTC Thu): only the hour to midnight counts.
    out_t = datetime(2026, 11, 19, 5, 0, tzinfo=UTC)
    in_t = datetime(2026, 11, 19, 7, 0, tzinfo=UTC)
    events = [ev("c0", 99, "out", BASE), ev("c1", 13, "out", out_t), ev("c1", 13, "in", in_t)]
    g = row(games(events, in_t, {13: 1}), 13)
    assert g["minutes_all_out"] == 60
    assert g["open_minutes"] == 15 * 60


def test_an_in_without_an_out_never_goes_below_zero_copies_out():
    events = [
        ev("c0", 99, "out", BASE),
        ev("cX", 13, "in", T + timedelta(hours=1), last="old"),
        ev("c1", 13, "out", T + timedelta(hours=2)),
        ev("c1", 13, "in", T + timedelta(hours=3)),
    ]
    g = row(games(events, T + timedelta(hours=4), {13: 1}), 13)
    assert g["minutes_all_out"] == 60


def test_time_out_quartiles_and_the_overnight_drop_off():
    # 30 and 90 min, plus one dropped overnight: out 11 PM Wed, in 8:20 AM Thu (Chicago),
    # which stops at midnight = 60 min.
    events = [
        ev("c0", 99, "out", BASE),
        ev("a", 13, "out", T, last="1"),
        ev("a", 13, "in", T + timedelta(minutes=30), last="1"),
        ev("b", 13, "out", T, last="2"),
        ev("b", 13, "in", T + timedelta(minutes=90), last="2"),
        ev("c", 13, "out", datetime(2026, 11, 19, 5, 0, tzinfo=UTC), last="3"),
        ev("c", 13, "in", datetime(2026, 11, 19, 14, 20, tzinfo=UTC), last="3"),
    ]
    g = row(games(events, datetime(2026, 11, 19, 15, 0, tzinfo=UTC), {13: 3}), 13)
    assert (g["time_out_q1"], g["time_out_median"], g["time_out_q3"]) == (45, 60, 75)


def test_a_game_missing_from_the_library_has_no_copies_or_demand():
    events = [ev("c0", 99, "out", BASE), ev("c1", 13, "out", T + timedelta(hours=1))]
    g = row(games(events, T + timedelta(hours=2), {}), 13)
    assert g["copies"] is None and g["minutes_all_out"] is None


def test_copies_without_a_bgg_id_are_left_out():
    events = [ev("c0", 99, "out", BASE), ev("c1", None, "out", T)]
    assert [r["bgg_id"] for r in games(events, T + timedelta(hours=1), {})["games"]] == [99]


def test_a_checkout_seen_just_after_close_never_has_a_negative_time_out():
    # First seen 5 min after close (late recorder, or the real hours differ), back at 8:20 AM.
    events = [
        ev("c0", 99, "out", BASE),
        ev("c", 13, "out", datetime(2026, 11, 19, 6, 5, tzinfo=UTC), last="3"),
        ev("c", 13, "in", datetime(2026, 11, 19, 14, 20, tzinfo=UTC), last="3"),
    ]
    g = row(games(events, datetime(2026, 11, 19, 15, 0, tzinfo=UTC), {13: 1}), 13)
    assert g["time_out_median"] == 0


def test_the_baseline_comes_from_the_first_run_not_the_first_event():
    # The first run found nothing out, so it wrote no events: the first checkout is a checkout.
    events = [ev("c1", 13, "out", T + timedelta(minutes=1))]
    a = activity(events, T + timedelta(minutes=5), baseline=BASE)
    assert sum(b["checkouts"] for b in a["buckets"]) == 1
    assert row(games(events, T + timedelta(minutes=5), {13: 1}, baseline=BASE), 13)["checkouts"] == 1
