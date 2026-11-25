"""When was every copy of a game out? The one place this is worked out for the report's charts.

Times are minutes since Wednesday midnight, Chicago. "Open" is library hours; "observed" is open
hours with the recorder running (the Thursday outage and a few short gaps are left out).
A game with no copy count (not in the Nov 19 library list) has no availability.
"""
import numpy as np

from checkouts import END, GAMES, HOT, OPEN, UNK, W, cd

START = cd(18, 0)


def minute(t):
    return (t - START).total_seconds() / 60


def spans(pairs):
    return [[round(minute(a), 1), round(minute(b), 1)] for a, b in pairs]


OPEN_SPANS, GAP_SPANS, OBSERVED_SPANS = spans(OPEN), spans(UNK), spans(W)
OBSERVED_MINUTES = sum(b - a for a, b in OBSERVED_SPANS)


def loans(game):
    """[out, back] for each checkout; a copy never returned counts as out to the end of recording."""
    return [[round(minute(k["o"]), 1), round(minute(k["i"] or END), 1)] for k in game["ks"]]


def all_out(game):
    """[from, to] for each stretch when every copy was out."""
    if not game["k"]:
        return []
    events = sorted([(minute(k["o"]), 1) for k in game["ks"]] + [(minute(k["i"] or END), -1) for k in game["ks"]])
    out, since, stretches = 0, None, []
    for t, change in events:
        out += change
        if out >= game["k"] and since is None:
            since = t
        elif out < game["k"] and since is not None:
            stretches.append([round(since, 1), round(t, 1)])
            since = None
    return stretches


def table():
    """One row per checked-out game, most in demand first. p = % of observed open time with every copy out."""
    order = sorted(GAMES.items(), key=lambda i: (-(i[1]["p"] if i[1]["p"] is not None else -1), -i[1]["c"], i[1]["n"]))
    return [dict(b=b, n=g["n"], y=g["y"], k=g["k"], c=g["c"], p=None if g["p"] is None else round(g["p"], 3),
                 hot=g["hot"], iv=loans(g), ao=all_out(g)) for b, g in order]


# 10-minute steps of observed open time, marked by their midpoints
STEPS = np.array([x + 5 for a, b in ((minute(a), minute(b)) for a, b in W) for x in np.arange(a, b - 10 + 1e-9, 10)])
STEP_DAY = (STEPS // 1440).astype(int)  # 0 = Wednesday
STEP_HOUR = STEPS % 1440 / 60


def all_out_steps(game):
    """For each step, 1 if every copy was out."""
    out = np.sort([minute(k["o"]) for k in game["ks"]])
    back = np.sort([minute(k["i"] or END) for k in game["ks"]])
    copies_out = np.searchsorted(out, STEPS, "right") - np.searchsorted(back, STEPS, "right")
    return (copies_out >= game["k"]).astype(float)
