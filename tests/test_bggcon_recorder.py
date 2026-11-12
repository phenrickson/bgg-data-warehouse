"""Unit tests for the BGG.CON library recorder's logic (pure: no network, no clock)."""

from datetime import UTC, datetime, timedelta

from src.bggcon import recorder as r

T = datetime(2026, 11, 18, 16, 0, tzinfo=UTC)


def copy(cid, bgg=13, last="co-1", name="Catan", when="2026-11-18 09:58:00"):
    return r.Copy(copy_id=cid, bgg_id=bgg, name=name, lastcheckout_id=last, last_checkout_date=when)


def kinds(events):
    return sorted((e["copy_id"], e["event"]) for e in events)


class TestDiff:
    def test_first_run_records_every_copy_already_out(self):
        events = r.diff(None, [copy("a"), copy("b", last="co-2")], T)
        assert kinds(events) == [("a", "out"), ("b", "out")]

    def test_no_change_records_nothing(self):
        assert r.diff({"a": "co-1"}, [copy("a")], T) == []

    def test_copies_going_out_and_coming_back(self):
        events = r.diff({"a": "co-1", "b": "co-2"}, [copy("a"), copy("c", last="co-3")], T)
        assert kinds(events) == [("b", "in"), ("c", "out")]

    def test_a_return_and_re_checkout_between_runs_is_an_in_and_an_out(self):
        # Still out, but under a new checkout: it came back and went out again within a minute.
        events = r.diff({"a": "co-1"}, [copy("a", last="co-9")], T)
        assert kinds(events) == [("a", "in"), ("a", "out")]

    def test_events_carry_the_game_and_the_times(self):
        [out] = r.diff({}, [copy("a", bgg=822, name="Carcassonne")], T)
        assert out == {
            "copy_id": "a",
            "bgg_id": 822,
            "name": "Carcassonne",
            "event": "out",
            "observed_at": T.isoformat(),
            "lastcheckout_id": "co-1",
            "tte_checkout_date": "2026-11-18 09:58:00",
        }

    def test_an_in_event_keeps_what_the_previous_run_knew(self):
        [ev] = r.diff({"b": "co-2"}, [], T, known={"b": {"bgg_id": 7, "name": "Go"}})
        assert (ev["bgg_id"], ev["name"], ev["event"], ev["lastcheckout_id"]) == (7, "Go", "in", "co-2")


class TestCounts:
    def test_counts_copies_out_per_game(self):
        out = [copy("a", bgg=13), copy("b", bgg=13), copy("c", bgg=822)]
        assert r.checked_out_by_game(out) == {13: 2, 822: 1}

    def test_copies_without_a_bgg_id_are_left_out(self):
        assert r.checked_out_by_game([copy("a", bgg=None)]) == {}


class TestAlerts:
    COPIES = {13: 2, 822: 1, 7: 1}

    def test_fires_when_a_starred_game_goes_from_all_out_to_available(self):
        assert r.back_on_shelf({13: 2, 822: 1}, {13: 1, 822: 1}, self.COPIES, starred={13, 822}) == [13]

    def test_quiet_for_games_that_were_not_all_out(self):
        assert r.back_on_shelf({13: 1}, {}, self.COPIES, starred={13}) == []

    def test_quiet_for_games_not_starred(self):
        assert r.back_on_shelf({822: 1}, {}, self.COPIES, starred={13}) == []

    def test_quiet_when_still_all_out(self):
        assert r.back_on_shelf({822: 1}, {822: 1}, self.COPIES, starred={822}) == []

    def test_quiet_for_games_the_snapshot_does_not_know(self):
        assert r.back_on_shelf({999: 1}, {}, self.COPIES, starred={999}) == []


class TestWindow:
    # Tue 17 Nov 00:00 to Sun 22 Nov 15:00, America/Chicago (CST, UTC−6).
    def test_records_from_the_test_day_to_the_close(self):
        assert not r.in_window(datetime(2026, 11, 17, 5, 59, tzinfo=UTC))
        assert r.in_window(datetime(2026, 11, 17, 6, 0, tzinfo=UTC))
        assert r.in_window(datetime(2026, 11, 22, 20, 59, tzinfo=UTC))
        assert not r.in_window(datetime(2026, 11, 22, 21, 0, tzinfo=UTC))


class TestBackoff:
    def test_waits_five_minutes_after_a_429_or_5xx(self):
        assert r.backoff_until(T, 429) == T + timedelta(minutes=5)
        assert r.backoff_until(T, 503) == T + timedelta(minutes=5)

    def test_no_wait_after_other_failures(self):
        assert r.backoff_until(T, 404) is None
        assert r.backoff_until(T, None) is None

    def test_skips_a_run_before_the_allowed_time(self):
        later = (T + timedelta(minutes=2)).isoformat()
        assert r.should_skip(T, later)
        assert not r.should_skip(T, None)
        assert not r.should_skip(T + timedelta(minutes=3), later)
