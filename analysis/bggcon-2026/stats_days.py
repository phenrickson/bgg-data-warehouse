"""Do people check out the same games each day?

Data       Checkouts Wed-Sat at hours every day was observed: 9 AM-1 PM and 10 PM-midnight (Thursday's
           outage took the afternoon; 9-10 PM was catch-up). Timed by Tabletop.Events' own clock.
Compared   Overlap: of the games checked out on either of two days, the share checked out on both.
           Mix: the kinds of games checked out each day.
Chance     If days were alike, each checkout could have fallen on any day. Days are shuffled among
           checkouts made in the same clock hour, so every day keeps its number of checkouts per hour.
Output     Numbers only. No chart code here.
"""
from itertools import combinations

import numpy as np

from checkouts import CAT, HOT, K, OPEN, X, ct, meta

DAYS = {18: "Wed", 19: "Thu", 20: "Fri", 21: "Sat"}
N_SHUFFLES = 2000


def load():
    """Arrays for checkouts at the matched hours: game number, day, hour; and the list of game ids."""
    rows = []
    for k in K:
        t = ct(k["d"])
        hour = t.hour + t.minute / 60
        if t.day in DAYS and any(a <= k["d"] < b for a, b in OPEN) and (9 <= hour < 13 or hour >= 22):
            rows.append((k["b"], t.day, t.hour))
    ids = sorted({b for b, _, _ in rows})
    number = {b: i for i, b in enumerate(ids)}
    return np.array([number[b] for b, _, _ in rows]), np.array([d for _, d, _ in rows]), np.array([h for _, _, h in rows]), ids


def shuffle_days(day, hour, rng):
    out = day.copy()
    for h in np.unique(hour):
        i = np.where(hour == h)[0]
        out[i] = day[rng.permutation(i)]
    return out


# ---------------------------------------------------------------- overlap between days

def overlap(game, day, a, b, n_games):
    """Of the games checked out on day a or day b, the share checked out on both."""
    on_a = np.bincount(game[day == a], minlength=n_games) > 0
    on_b = np.bincount(game[day == b], minlength=n_games) > 0
    return (on_a & on_b).sum() / (on_a | on_b).sum()


def overlaps(rng):
    """For each pair of days: observed overlap, the middle 95% of shuffles, and a two-sided p-value."""
    game, day, hour, ids = load()
    pairs = list(combinations(DAYS, 2))
    measure = lambda d: [overlap(game, d, a, b, len(ids)) for a, b in pairs]
    observed = np.array(measure(day))
    chance = np.array([measure(shuffle_days(day, hour, rng)) for _ in range(N_SHUFFLES)])
    result = []
    for i, (a, b) in enumerate(pairs):
        fewer, more = (chance[:, i] <= observed[i]).sum(), (chance[:, i] >= observed[i]).sum()
        low, high = np.percentile(chance[:, i], [2.5, 97.5])
        result.append(dict(a=DAYS[a], b=DAYS[b], v=float(observed[i]), lo=float(low), hi=float(high), mean=float(chance[:, i].mean()),
                           p=min(1, 2 * (min(fewer, more) + 1) / (N_SHUFFLES + 1))))
    return result


# ---------------------------------------------------------------- mix of games each day

def complexity(b):
    rating = CAT[b]["average_weight"] if b in CAT else float(X[b]["average_weight"] or 0) if b in X else 0
    return rating or np.nan


def year_is(test):
    return lambda b: float(test(meta(b)[1])) if meta(b)[1] else np.nan


KINDS = {  # each gives a value per game; NaN = unknown, left out
    "From 2026": year_is(lambda y: y == 2026),
    "From before 2020": year_is(lambda y: y < 2020),
    "On the Hot Games list": lambda b: float(b in HOT),
    "Average complexity": complexity,
}


def mix(rng):
    """For each kind: its average (or share) of each day's checkouts, and a p-value for the days differing."""
    game, day, hour, ids = load()
    shuffles = [shuffle_days(day, hour, rng) for _ in range(N_SHUFFLES)]
    result = []
    for name, value_of in KINDS.items():
        value = np.array([value_of(b) for b in ids])[game]
        by_day = lambda d: np.array([np.nanmean(value[d == x]) for x in DAYS])
        observed = by_day(day)
        spread = np.array([np.var(by_day(s)) for s in shuffles])
        result.append(dict(name=name, v=observed.tolist(), p=float((1 + (spread >= np.var(observed)).sum()) / (N_SHUFFLES + 1))))
    return result


if __name__ == "__main__":
    rng = np.random.default_rng(2026)
    for r in overlaps(rng):
        print(f"{r['a']}-{r['b']}: {r['v']:.3f}  chance {r['mean']:.3f} ({r['lo']:.3f} to {r['hi']:.3f})  p={r['p']:.4f}")
    for r in mix(rng):
        print(f"{r['name']:24}", " ".join(f"{v:.4f}" for v in r["v"]), f"p={r['p']:.4f}")
