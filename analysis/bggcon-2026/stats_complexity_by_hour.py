"""Does the complexity of the games people check out change through the day?

Data       Every checkout, Wed-Sat, 8 AM to midnight, of a game with a BGG complexity rating.
           One row per checkout, so a game checked out 10 times counts 10 times.
Compared   Average complexity by clock hour, after removing differences between days.
Chance     If time of day didn't matter, the complexities could be dealt out to that day's
           checkouts in any order. Two versions of "any order" (see below).
Output     Prints only. No chart code here.
"""
from collections import namedtuple

import numpy as np

from checkouts import CAT, K, X, _t, ct

DAYS = {18: "Wed", 19: "Thu", 20: "Fri", 21: "Sat"}  # day of November
HOURS = np.arange(8, 24)
N_SHUFFLES = 5000
MIN_CHECKOUTS = 10  # fewest checkouts for showing one day's average in one hour

Checkouts = namedtuple("Checkouts", "day hour value")  # three arrays, in time order


# ---------------------------------------------------------------- data

def complexity(game):
    """BGG's average complexity rating (1-5), or NaN if nobody has rated it."""
    if game in CAT:
        rating = CAT[game]["average_weight"]
    elif game in X:  # games missing from the catalog, loaded separately
        rating = float(X[game]["average_weight"] or 0)
    else:
        rating = 0
    return rating or np.nan  # BGG stores "no ratings" as 0


CATEGORIES = {str(r["game_id"]): set(r["categories"] or []) for r in _t.select(["game_id", "categories"]).to_pylist()}


def in_category(name):
    """1 if the game is in this BGG category, 0 if not, NaN if its categories aren't known."""
    return lambda game: float(name in CATEGORIES[game]) if game in CATEGORIES else np.nan


def load(value=complexity):
    """Checkouts in time order with value(game) for each; checkouts without a value are dropped.

    The time is when the recorder first saw the checkout, usually within 2 minutes of it.
    """
    rows = []
    for k in K:
        t = ct(k["o"])  # Chicago time
        if t.day in DAYS and t.hour >= 8:
            rows.append((t, value(k["b"])))
    rows = sorted((r for r in rows if not np.isnan(r[1])), key=lambda r: r[0])
    return Checkouts(np.array([t.day for t, _ in rows]), np.array([t.hour for t, _ in rows]), np.array([v for _, v in rows]))


def subset(c, keep):
    return Checkouts(c.day[keep], c.hour[keep], c.value[keep])


# ---------------------------------------------------------------- what we measure

def hour_means(hour, value):
    return np.array([value[hour == h].mean() for h in HOURS])


def remove_day_means(c):
    """Subtract each day's average, so a heavier day can't look like a time-of-day effect."""
    out = c.value.copy()
    for d in np.unique(c.day):
        out[c.day == d] -= c.value[c.day == d].mean()
    return out


def share_explained(hour, value):
    """Share of the variation in value that lies between clock hours (R-squared): 0 = none, 1 = all.

    Picks up any pattern over the day, not just a steady rise or fall.
    value must already average zero, as remove_day_means leaves it.
    """
    h = hour - hour.min()
    n = np.bincount(h)
    total = np.bincount(h, weights=value)
    used = n > 0
    return (total[used] ** 2 / n[used]).sum() / (value ** 2).sum()


def trend(hour, value):
    """Correlation between clock hour and value: negative = falls through the day."""
    return np.corrcoef(hour, value)[0, 1]


# ---------------------------------------------------------------- what chance looks like

def shuffle_within_days(day, value, rng):
    """Deal each day's values out in random order.

    Assumes checkouts are independent of each other.
    """
    out = value.copy()
    for d in np.unique(day):
        out[day == d] = rng.permutation(value[day == d])
    return out


def rotate_within_days(day, value, rng):
    """Slide each day's whole sequence by a random amount, wrapping the end to the start.

    Keeps runs of similar games together, so it doesn't assume checkouts are independent.
    The stricter of the two, and weak on a single day: a smooth fall is itself one long run.
    """
    out = value.copy()
    for d in np.unique(day):
        n = (day == d).sum()
        out[day == d] = np.roll(value[day == d], rng.integers(n))
    return out


def p_value(observed, simulated):
    """Share of simulations at least as large as observed, counting the observed one.

    The smallest possible value is 1 / (N_SHUFFLES + 1) = 0.0002.
    """
    return (1 + (simulated >= observed).sum()) / (len(simulated) + 1)


def test(c, rng):
    """Is the hour-to-hour variation, or the trend, more than chance would give?"""
    value = remove_day_means(c)
    result = dict(n=len(value), share_explained=share_explained(c.hour, value), trend=trend(c.hour, value))
    for name, rearrange in (("shuffle", shuffle_within_days), ("rotate", rotate_within_days)):
        explained, slope = np.empty(N_SHUFFLES), np.empty(N_SHUFFLES)
        for i in range(N_SHUFFLES):
            fake = rearrange(c.day, value, rng)
            explained[i] = share_explained(c.hour, fake)
            slope[i] = trend(c.hour, fake)
        result[f"p_hours_{name}"] = p_value(result["share_explained"], explained)
        result[f"p_trend_{name}"] = p_value(abs(result["trend"]), abs(slope))  # two-sided
    return result


def chance_band(c, rng, n=2000):
    """For each hour, the range holding the middle 95% of averages when values are shuffled within days.

    Each hour on its own: with 16 hours, about one would fall outside by luck.
    """
    sims = [hour_means(c.hour, shuffle_within_days(c.day, c.value, rng)) for _ in range(n)]
    return np.percentile(sims, [2.5, 97.5], axis=0)


# ---------------------------------------------------------------- report

def print_hours(c, rng):
    low, high = chance_band(c, rng)
    print(f"{'hour':>4} {'n':>5} {'mean':>6}  {'chance range':14}" + "".join(f"{d:>7}" for d in DAYS.values()))
    for i, h in enumerate(HOURS):
        at = c.hour == h
        daily = [c.value[at & (c.day == d)] for d in DAYS]
        cells = "".join(f"{v.mean():7.2f}" if len(v) >= MIN_CHECKOUTS else f"{'-':>7}" for v in daily)
        print(f"{h:>4} {at.sum():>5} {c.value[at].mean():6.2f}  {low[i]:.2f} to {high[i]:.2f}  {cells}")


def print_test(name, r):
    print(f"{name:26} n={r['n']:<6} explained={r['share_explained']:.2%}  trend={r['trend']:+.3f}"
          f"  | p hours: shuffle {r['p_hours_shuffle']:.4f}, rotate {r['p_hours_rotate']:.4f}"
          f"  | p trend: shuffle {r['p_trend_shuffle']:.4f}, rotate {r['p_trend_rotate']:.4f}")


def main():
    rng = np.random.default_rng(1)
    c = load()
    print_hours(c, rng)

    print("\nComplexity")
    print_test("all checkouts", test(c, rng))
    print("\nSame test on parts of the data")
    print_test("without Thursday", test(subset(c, c.day != 19), rng))  # the outage day
    print_test("without Thu 9 PM on", test(subset(c, ~((c.day == 19) & (c.hour >= 21))), rng))  # just after the outage
    print_test("10 AM on", test(subset(c, c.hour >= 10), rng))  # is it only the early morning?
    for d, name in DAYS.items():
        if d != 19:  # Thursday's outage leaves too few hours
            print_test(f"{name} only", test(subset(c, c.day == d), rng))

    print("\nShare of checkouts in a category (six tried)")
    for name in ("Party Game", "Humor", "Card Game", "Dice", "Wargame", "Economic"):
        print_test(name, test(load(in_category(name)), rng))


if __name__ == "__main__":
    main()
