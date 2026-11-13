"""The BGG.CON recorder job's run, with Tabletop.Events, GCS, BigQuery and ntfy faked."""

from datetime import UTC, datetime, timedelta

import pytest

from src.pipeline import bggcon_recorder as job

T = datetime(2026, 11, 18, 16, 0, tzinfo=UTC)


def row(cid, bgg=13, last="co-1", name="Catan"):
    return {"id": cid, "bgg_id": bgg, "name": name, "lastcheckout_id": last, "last_checkout_date": "2026-11-18 09:58:00"}


class Fakes:
    """In-memory stand-ins for every outside service the job touches."""

    def __init__(self, rows=None, files=None, fetch_error=None, conflict=False):
        self.rows = rows or []
        self.files = dict(files or {})
        self.gens = {k: 1 for k in self.files}
        self.fetch_error = fetch_error
        self.conflict = conflict
        self.fetches = 0
        self.inserted: dict[str, list[dict]] = {}
        self.pushes: list[tuple[str, str]] = []

    def fetch(self):
        self.fetches += 1
        if self.fetch_error:
            raise self.fetch_error
        return self.rows

    def read(self, name):
        return self.files.get(name), self.gens.get(name)

    def write(self, name, data, if_generation=None):
        if if_generation is not None and (self.conflict or self.gens.get(name, 0) != if_generation):
            raise job.Conflict(name)
        self.files[name] = data
        self.gens[name] = self.gens.get(name, 0) + 1

    def insert(self, table, rows):
        self.inserted.setdefault(table, []).extend(rows)

    def notify(self, title, message):
        self.pushes.append((title, message))

    def deps(self, now=T):
        return job.Deps(
            fetch_checked_out=self.fetch,
            read_json=self.read,
            write_json=self.write,
            insert_rows=self.insert,
            notify=self.notify,
            now=lambda: now,
        )


def test_outside_the_window_it_does_nothing():
    f = Fakes(rows=[row("a")])
    assert job.run_once(f.deps(now=datetime(2026, 11, 23, tzinfo=UTC))) == "outside the window"
    assert f.fetches == 0 and f.inserted == {} and f.files == {}


def test_first_run_records_copies_out_and_saves_state_and_latest():
    f = Fakes(rows=[row("a"), row("b", bgg=822, last="co-2")])
    assert job.run_once(f.deps()) == "recorded 2 events"
    assert [e["event"] for e in f.inserted[job.EVENTS_TABLE]] == ["out", "out"]
    [run] = f.inserted[job.RUNS_TABLE]
    assert run == {"observed_at": T.isoformat(), "n_out": 2, "ok": True, "note": None}
    assert f.files[job.LATEST] == {
        "as_of": T.isoformat(),
        "checked_out": {"13": 1, "822": 1},
        "checkouts": {"13": 1, "822": 1},
    }
    assert f.files[job.STATE]["copies"]["a"] == {"lastcheckout_id": "co-1", "bgg_id": 13, "name": "Catan"}


def test_a_quiet_minute_writes_only_the_heartbeat():
    f = Fakes(rows=[row("a")])
    job.run_once(f.deps())
    job.run_once(f.deps(now=T + timedelta(minutes=1)))
    assert len(f.inserted[job.EVENTS_TABLE]) == 1
    assert len(f.inserted[job.RUNS_TABLE]) == 2


def test_a_copy_coming_back_is_an_in_event_with_its_game():
    f = Fakes(rows=[row("a", bgg=822, name="Carcassonne")])
    job.run_once(f.deps())
    f.rows = []
    job.run_once(f.deps(now=T + timedelta(minutes=1)))
    last = f.inserted[job.EVENTS_TABLE][-1]
    assert (last["event"], last["copy_id"], last["bgg_id"], last["name"]) == ("in", "a", 822, "Carcassonne")
    assert f.files[job.LATEST]["checked_out"] == {}


def test_a_429_backs_off_records_a_failed_heartbeat_and_skips_the_next_run():
    f = Fakes(rows=[row("a")])
    job.run_once(f.deps())
    f.fetch_error = job.FetchError(429)
    assert job.run_once(f.deps(now=T + timedelta(minutes=1))) == "fetch failed (429)"
    assert f.inserted[job.RUNS_TABLE][-1]["ok"] is False
    assert f.files[job.STATE]["copies"]  # the last good set is kept
    f.fetch_error = None
    fetches = f.fetches
    assert job.run_once(f.deps(now=T + timedelta(minutes=2))) == "backing off"
    assert f.fetches == fetches
    assert job.run_once(f.deps(now=T + timedelta(minutes=7))) == "recorded 0 events"


def test_alerts_when_a_starred_game_comes_back_on_the_shelf():
    library = {"games": [{"bgg_id": 822, "name": "Carcassonne", "copies": 1, "play_to_win": False}]}
    f = Fakes(
        rows=[row("a", bgg=822, name="Carcassonne")],
        files={job.LIBRARY: library, job.stars_name("u1"): [822]},
    )
    job.run_once(f.deps(), alert_user="u1")
    assert f.pushes == []  # first run: nothing to compare against
    f.rows = []
    job.run_once(f.deps(now=T + timedelta(minutes=1)), alert_user="u1")
    assert f.pushes == [("BGG.CON: Carcassonne is back", "1/1 available")]


def test_no_alerts_without_a_user_to_alert():
    library = {"games": [{"bgg_id": 822, "name": "Carcassonne", "copies": 1, "play_to_win": False}]}
    f = Fakes(rows=[row("a", bgg=822)], files={job.LIBRARY: library})
    job.run_once(f.deps())
    f.rows = []
    job.run_once(f.deps(now=T + timedelta(minutes=1)))
    assert f.pushes == []


def test_a_failing_push_does_not_fail_the_run():
    library = {"games": [{"bgg_id": 822, "name": "Carcassonne", "copies": 1, "play_to_win": False}]}
    f = Fakes(rows=[row("a", bgg=822)], files={job.LIBRARY: library, job.stars_name("u1"): [822]})
    job.run_once(f.deps(), alert_user="u1")
    f.rows = []

    def boom(title, message):
        raise RuntimeError("ntfy down")

    deps = f.deps(now=T + timedelta(minutes=1))
    deps.notify = boom
    assert job.run_once(deps, alert_user="u1") == "recorded 1 events"


def test_events_are_inserted_before_the_state_is_saved():
    # If another run saved the state first, this run's events are already in: duplicates the
    # report can drop (copy, event, checkout id), never lost minutes.
    f = Fakes(rows=[row("a")], conflict=True)
    with pytest.raises(job.Conflict):
        job.run_once(f.deps())
    assert len(f.inserted[job.EVENTS_TABLE]) == 1


def test_a_test_run_goes_ahead_outside_the_window_without_touching_the_real_state():
    f = Fakes(rows=[row("a")])
    before = datetime(2026, 11, 13, 16, 0, tzinfo=UTC)
    assert job.run_once(f.deps(now=before), test_run=True) == "recorded 1 events"
    assert job.STATE not in f.files
    assert f.files[job.TEST_STATE]["copies"]
    [run] = f.inserted[job.RUNS_TABLE]
    assert run["note"] == "test run"


def test_checkouts_build_up_across_runs_and_reach_the_snapshot():
    f = Fakes(rows=[row("a", bgg=13)])
    job.run_once(f.deps())
    f.rows = []  # back in
    job.run_once(f.deps(now=T + timedelta(minutes=1)))
    f.rows = [row("a", bgg=13, last="co-2"), row("b", bgg=822, last="co-3")]  # out again, and another game
    job.run_once(f.deps(now=T + timedelta(minutes=2)))
    assert f.files[job.LATEST]["checkouts"] == {"13": 2, "822": 1}
    assert f.files[job.STATE]["checkouts"] == {"13": 2, "822": 1}
