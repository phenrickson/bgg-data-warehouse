"""The BGG.CON recorder job's run, with Tabletop.Events, GCS, BigQuery and ntfy faked."""

from datetime import UTC, datetime, timedelta

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
        self.pushes: list[tuple[str, str, str]] = []
        self.listings = 0

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

    def list_names(self, prefix):
        self.listings += 1
        return sorted(k for k in self.files if k.startswith(prefix))

    def notify(self, topic, title, message):
        self.pushes.append((topic, title, message))

    def deps(self, now=T):
        return job.Deps(
            fetch_checked_out=self.fetch,
            read_json=self.read,
            write_json=self.write,
            insert_rows=self.insert,
            list_names=self.list_names,
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


CARCASSONNE = {"bgg_id": 822, "name": "Carcassonne", "copies": 1, "play_to_win": False}
CATAN = {"bgg_id": 13, "name": "Catan", "copies": 1, "play_to_win": False}
LIBRARY = {"games": [CARCASSONNE, CATAN]}


def alert_files(**users):
    """``u1=("t1", [822])`` -> that user's alerts file and stars file."""
    files = {}
    for uid, (topic, stars) in users.items():
        files[f"{job.ALERTS_PREFIX}{uid}.json"] = {"topic": topic} if topic else {}
        files[job.stars_name(uid)] = stars
    return files


def test_alerts_each_user_on_their_own_topic():
    f = Fakes(
        rows=[row("a", bgg=822, name="Carcassonne"), row("b", bgg=13)],
        files={job.LIBRARY: LIBRARY, **alert_files(u1=("t1", [822]), u2=("t2", [13]))},
    )
    job.run_once(f.deps())
    assert f.pushes == []  # first run: nothing to compare against
    f.rows = [row("b", bgg=13)]  # Carcassonne back; Catan still out
    job.run_once(f.deps(now=T + timedelta(minutes=1)))
    assert f.pushes == [("t1", "BGG.CON: Carcassonne is back", "1/1 available")]


def test_no_alerts_without_any_alert_files():
    f = Fakes(rows=[row("a", bgg=822)], files={job.LIBRARY: LIBRARY, job.stars_name("u1"): [822]})
    job.run_once(f.deps())
    f.rows = []
    job.run_once(f.deps(now=T + timedelta(minutes=1)))
    assert f.pushes == []


def test_a_quiet_minute_lists_no_alert_files():
    files = {job.LIBRARY: LIBRARY, **alert_files(u1=("t1", [822]))}
    f = Fakes(rows=[row("a", bgg=822)], files=files)
    job.run_once(f.deps())
    job.run_once(f.deps(now=T + timedelta(minutes=1)))  # still out: nothing came back
    assert f.listings == 0


def test_one_users_bad_file_or_failing_push_does_not_stop_the_others():
    f = Fakes(
        rows=[row("a", bgg=822, name="Carcassonne")],
        files={
            job.LIBRARY: LIBRARY,
            **alert_files(u1=(None, [822]), u2=("t2", [822]), u3=("t3", [822])),
        },
    )
    job.run_once(f.deps())
    f.rows = []
    deps = f.deps(now=T + timedelta(minutes=1))

    def flaky(topic, title, message):
        if topic == "t2":
            raise RuntimeError("ntfy down")
        f.notify(topic, title, message)

    deps.notify = flaky
    assert job.run_once(deps) == "recorded 1 events"
    assert f.pushes == [("t3", "BGG.CON: Carcassonne is back", "1/1 available")]


def test_a_failing_push_does_not_fail_the_run():
    files = {job.LIBRARY: LIBRARY, **alert_files(u1=("t1", [822]))}
    f = Fakes(rows=[row("a", bgg=822)], files=files)
    job.run_once(f.deps())
    f.rows = []

    def boom(topic, title, message):
        raise RuntimeError("ntfy down")

    deps = f.deps(now=T + timedelta(minutes=1))
    deps.notify = boom
    assert job.run_once(deps) == "recorded 1 events"


def test_losing_the_race_to_save_the_state_ends_the_run_cleanly():
    # Runs a minute apart can overlap (a Cloud Run job can take most of a minute to start).
    # The one that saves second stops quietly: its events are already in, so they're duplicates
    # the report can drop (copy, event, checkout id), never lost minutes. It sends no alerts;
    # the run that won does.
    f = Fakes(rows=[row("a")], conflict=True)
    assert job.run_once(f.deps()) == "another run saved first"
    assert len(f.inserted[job.EVENTS_TABLE]) == 1
    assert f.pushes == []


def test_losing_the_race_while_backing_off_ends_the_run_cleanly():
    f = Fakes(fetch_error=job.FetchError(429), conflict=True)
    assert job.run_once(f.deps()) == "another run saved first"


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



def test_a_manual_test_run_sends_no_alerts():
    # A test run compares against its own state, which can be days old: an alert from it
    # would be stale, and it would go to every user's real topic.
    files = {job.LIBRARY: LIBRARY, **alert_files(u1=("t1", [822]))}
    f = Fakes(rows=[row("a", bgg=822)], files=files)
    job.run_once(f.deps(), test_run=True)
    f.rows = []
    assert job.run_once(f.deps(now=T + timedelta(minutes=1)), test_run=True) == "recorded 1 events"
    assert f.pushes == []


def test_one_failed_push_does_not_drop_that_users_other_games():
    f = Fakes(
        rows=[row("a", bgg=822, name="Carcassonne"), row("b", bgg=13)],
        files={job.LIBRARY: LIBRARY, **alert_files(u1=("t1", [13, 822]))},
    )
    job.run_once(f.deps())
    f.rows = []
    deps = f.deps(now=T + timedelta(minutes=1))

    def flaky(topic, title, message):
        if "Catan" in title:
            raise RuntimeError("ntfy hiccup")
        f.notify(topic, title, message)

    deps.notify = flaky
    job.run_once(deps)
    assert f.pushes == [("t1", "BGG.CON: Carcassonne is back", "1/1 available")]


def test_push_carries_a_name_that_is_not_latin_1():
    """A game name with an en dash went in the Title header, which must be latin-1, so the
    push never left (2026-11-17, Night of the Goblin). Everything rides in a JSON body now."""
    import json

    import requests

    sent = {}

    def post(url, **kwargs):
        sent.update(url=url, **kwargs)
        return type("Ok", (), {"raise_for_status": lambda self: None})()

    name = "Marvel: Crisis Protocol Alliances – Night of the Goblin (English edition)"  # noqa: RUF001
    title = f"BGG.CON: {name} is back"
    job.send_push("bggcon-2026-someone", title, "1/1 available", post=post)

    prepared = requests.Request(
        "POST", sent["url"], json=sent.get("json"), data=sent.get("data"), headers=sent.get("headers")
    ).prepare()
    for value in prepared.headers.values():
        value.encode("latin-1")  # what http.client does to every header; it raised here
    body = json.loads(prepared.body)
    assert sent["url"] == "https://ntfy.sh/"
    assert body["topic"] == "bggcon-2026-someone"
    assert body["title"] == title
    assert body["message"] == "1/1 available"
    assert body["click"] == job.DASHBOARD_URL
