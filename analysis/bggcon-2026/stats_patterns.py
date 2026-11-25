"""How each game's demand moved over the con, and which games show the clearest patterns.

Data       Games with a known copy count, by clock hour, Wed-Sat. Each hour is the share of it when every copy was out.
Demand     Average of the hours so far, where an hour counts half as much every HALF_LIFE recorded hours.
           Looks back only, so it could be worked out live.
Groups     Games with at least MIN_CHECKOUTS, the strongest few of each. These are descriptions, not tests.
             Got hotter / Cooled off   average demand on Fri-Sat against Wed noon-Thu
             In demand all con         the highest low point: demand never dropped far
             Night / Daytime games     all out more after 6 PM than before 4 PM (or less) on Wed, Fri and Sat;
                                       ordered by the day with the smallest difference
Output     Numbers only. The drawing is in build_trends.py.
"""
import numpy as np

from availability import STEPS, all_out_steps, minute
from checkouts import END, GAMES, K

HALF_LIFE = 4  # recorded hours
MIN_CHECKOUTS = 8
PER_GROUP = 6
LEFT_OUT = {"Braided Skies"}  # probably reached the library Friday night

HOURS = np.arange(8, 96)  # every clock hour from Wed 8 AM, counted from Wed midnight
FITTED = HOURS >= 12  # from Wed noon: before that the rise is just the library opening full
STEP_HOURS = (STEPS // 60).astype(int)
OBSERVED = np.array([(STEP_HOURS == h).sum() >= 3 for h in HOURS])  # at least half the hour recorded


def by_hour(steps):
    """Average of a 10-minute series over each clock hour; NaN where it wasn't recorded."""
    return np.array([steps[STEP_HOURS == h].mean() if ok else np.nan for h, ok in zip(HOURS, OBSERVED)])


def demand(y, half_life=HALF_LIFE):
    """The look-back average, and how many points it moved from one recorded hour to the next.

    Starts when the library opens; kept from Wednesday noon.
    """
    use = np.where(OBSERVED)[0]
    weight, average = .5 ** (1 / half_life), np.full(len(HOURS), np.nan)
    total = count = 0
    for i in use:
        total, count = total * weight + y[i], count * weight + 1
        average[i] = total / count
    change = np.full(len(HOURS), np.nan)
    change[use[1:]] = 100 * np.diff(average[use])
    return np.where(FITTED, average, np.nan), np.where(FITTED, change, np.nan)


def library_out():
    """All copies out by hour, as a share of the peak."""
    out = np.sort([minute(k["o"]) for k in K])
    back = np.sort([minute(k["i"] or END) for k in K])
    y = by_hour(np.searchsorted(out, STEPS, "right") - np.searchsorted(back, STEPS, "right"))
    return y / np.nanmax(y)


def night_gaps(y):
    """All-out rate after 6 PM minus before 4 PM, on Wednesday (from noon), Friday and Saturday.

    Thursday is left out: most of its evening wasn't recorded.
    """
    hour, day = HOURS % 24, HOURS // 24
    rate = lambda d, part: np.nanmean(y[FITTED & (day == d) & part])
    return [rate(d, hour >= 18) - rate(d, hour < 16) for d in (0, 2, 3)]


def groups():
    """{group: [game id, ...]}, strongest first. A game can be in more than one."""
    rows = []
    for b, g in GAMES.items():
        if not g["k"] or g["c"] < MIN_CHECKOUTS or g["n"] in LEFT_OUT:
            continue
        y = by_hour(all_out_steps(g))
        line, _ = demand(y)
        gaps = night_gaps(y)
        rows.append(dict(b=b, shift=np.nanmean(line[HOURS >= 48]) - np.nanmean(line[HOURS < 48]), low=np.nanmin(line), night=min(gaps), day=-max(gaps)))
    top = lambda key: [r["b"] for r in sorted(rows, key=key, reverse=True)[:PER_GROUP]]
    return {"Got hotter": top(lambda r: r["shift"]), "Cooled off": top(lambda r: -r["shift"]), "In demand all con": top(lambda r: r["low"]),
            "Night games": top(lambda r: r["night"]), "Daytime games": top(lambda r: r["day"])}


if __name__ == "__main__":
    for name, ids in groups().items():
        print(f"{name}:", [(GAMES[b]["n"], GAMES[b]["k"], GAMES[b]["c"]) for b in ids])
