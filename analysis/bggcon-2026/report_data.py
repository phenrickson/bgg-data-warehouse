"""Numbers for the report's descriptive charts: counts, rankings and shares. Tests are in the stats_*.py files."""
import collections

import numpy as np

import stats_complexity_by_hour as hours
from availability import OBSERVED_SPANS, OPEN_SPANS, minute, table
from checkouts import GAMES, K, LIB, ct, meta

GAMES_TABLE = table()  # every checked-out game, most in demand first


def timeline():
    """[start minute, checkouts] for each 15 minutes of library time."""
    n = collections.Counter(int(minute(k["o"]) // 15 * 15) for k in K)
    return [[t, n[t]] for a, b in OPEN_SPANS for t in range(int(a), int(b), 15)]


def busiest_hour():
    """The clock hour with the most checkouts: (day, hour, checkouts)."""
    (day, hour), n = collections.Counter((ct(k["o"]).strftime("%a"), ct(k["o"]).hour) for k in K).most_common(1)[0]
    return day, f"{hour % 12 or 12} {'AM' if hour < 12 else 'PM'}", n


def by_day():
    """Checkouts each day, Wed-Sun, as bars."""
    n = collections.Counter(ct(k["o"]).strftime("%a") for k in K)
    names = dict(Wed="Wednesday", Thu="Thursday", Fri="Friday", Sat="Saturday", Sun="Sunday")
    assert set(n) == set(names)
    return [[d, n[d], name] for d, name in names.items()]


def by_hour_each_day():
    """Checkouts per clock hour, Wed-Sun. None when the library was closed or under half the hour was recorded
    (Thursday 2-9 PM, Saturday 1 PM).
    """
    n = collections.Counter((ct(k["o"]).day, ct(k["o"]).hour) for k in K)

    def hour(d, h):
        start = (d - 18) * 1440 + h * 60
        recorded = sum(max(0, min(start + 60, b) - max(start, a)) for a, b in OBSERVED_SPANS)
        return n[d, h] if recorded >= 30 else None

    return dict(H=list(range(8, 24)), days=[[hour(d, h) for h in range(8, 24)] for d in range(18, 23)])


def ranked(keep=lambda g: True):
    """Ids of checked-out games, most in demand first; games with no known copy count go last."""
    return [g["b"] for g in GAMES_TABLE if keep(g)]


# ---- top games, overall and by group

def top(measure, keep=lambda g: True, n=10):
    """Ids of the n games highest on a measure: "c" checkouts, or "p" share of the con with every copy out."""
    games = [g for g in GAMES_TABLE if keep(g) and (measure == "c" or g["p"])]
    other = "p" if measure == "c" else "c"
    return [g["b"] for g in sorted(games, key=lambda g: (-g[measure], -(g[other] or 0), g["n"]))[:n]]


# Group: which games are in it. A game with two of these categories is in both.
BY_CATEGORY = {name: (lambda g, tag=tag: tag in hours.CATEGORIES.get(g["b"], ()))
               for name, tag in (("Party games", "Party Game"), ("Economic games", "Economic"), ("Wargames", "Wargame"), ("Abstract games", "Abstract Strategy"))}
BY_YEAR = {name: (lambda g, a=a, b=b: bool(g["y"]) and a <= g["y"] <= b)
           for name, a, b in [(str(y), y, y) for y in range(2026, 2021, -1)] + [("2010–21", 2010, 2021), ("2000s", 2000, 2009), ("1990s", 1990, 1999)]}
BY_COMPLEXITY = {name: (lambda g, a=a, b=b: a <= hours.complexity(g["b"]) < b)
                 for name, a, b in (("Under 1.5", 0, 1.5), ("1.5–2", 1.5, 2), ("2–2.5", 2, 2.5), ("2.5–3", 2.5, 3), ("3 and up", 3, 9))}


def facets(measure, groups=None, n=10):
    """Data for a chart of top games: a panel per group, or one panel of everything."""
    panels = [[name, top(measure, keep, n)] for name, keep in groups.items()] if groups else [["", top(measure, n=n)]]
    return dict(by=measure, panels=panels)


def checkouts_per_game():
    """Library games never checked out, then games checked out once, twice, ... 10 or more times."""
    n = collections.Counter(min(g["c"], 10) for g in GAMES.values())
    never = sum(b not in GAMES for b in LIB)
    return [["0", never, "Never checked out"]] + [[str(i) if i < 10 else "10+", n[i], f"Checked out {'once' if i == 1 else f'{i} times' if i < 10 else '10 or more times'}"] for i in range(1, 11)]


def dots_and_share(column_of, value_of, names):
    """Every library game as [name, value, checkouts, column], and [games, checked out] for each column."""
    dots = [[meta(b)[0] or LIB[b].get("name"), value_of(b), GAMES[b]["c"] if b in GAMES else 0, column_of(b)] for b in LIB if column_of(b) is not None]
    rate = [[sum(d[3] == i for d in dots), sum(d[3] == i and d[2] > 0 for d in dots)] for i in range(len(names))]
    return dots, rate


# By complexity: columns a quarter point wide, 4 and up together
COMPLEXITY_NAMES = [f"{1 + i / 4:g}–{1.25 + i / 4:g}" for i in range(12)] + ["4+"]
COMPLEXITY_TICKS = [[i * 2, f"{1 + i / 2:.1f}", i % 2 == 0] for i in range(6)] + [[12.5, "4+", True]]


def by_complexity():
    col = lambda b: None if np.isnan(hours.complexity(b)) else min(12, int((hours.complexity(b) - 1) * 4 + 1e-6))
    return dots_and_share(col, lambda b: f"{hours.complexity(b):.2f}", COMPLEXITY_NAMES)


# By year: before 2015 single years have too few library games, so they're grouped; from 2015 each has 200+
BUCKETS = [(-9999, 1989, "<1990"), (1990, 1999, "1990s"), (2000, 2004, "2000–04"), (2005, 2009, "2005–09"), (2010, 2014, "2010–14")]
BUCKETS += [(y, y, str(y)) for y in range(2015, 2026)] + [(2026, 9999, "2026+")]
YEAR_NAMES = [n for _, _, n in BUCKETS]
YEAR_TICKS = [[i + .5, n if len(n) < 7 else n[2:], n in ("1990s", "2015", "2020", "2026+")] for i, n in enumerate(YEAR_NAMES)]


def by_year():
    col = lambda b: next(i for i, (a, z, _) in enumerate(BUCKETS) if a <= meta(b)[1] <= z) if meta(b)[1] else None
    return dots_and_share(col, lambda b: str(meta(b)[1]), YEAR_NAMES)


BAND_NAMES = ["Under 1.5", "1.5–2", "2–2.5", "2.5–3", "3 and up"]


def complexity_mix(c):
    """Share of each hour's checkouts in each complexity band."""
    return [(np.histogram(c.value[c.hour == h], [0, 1.5, 2, 2.5, 3, 9])[0] / (c.hour == h).sum()).round(4).tolist() for h in hours.HOURS]
