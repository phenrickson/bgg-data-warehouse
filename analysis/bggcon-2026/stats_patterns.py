"""Which games' availability changed or cycled over the con?

Data       Games with a known copy count and at least MIN_CHECKOUTS checkouts, in 10-minute steps of
           observed open time, each marked 1 if every copy was out.
Compared   Gained / cooled: the share of time every copy was out before vs. after the moment that
           makes the two differ most. Night / day: after 6 PM vs. before 4 PM, day by day.
Chance     Not tested here. These are cutoffs for finding examples. The test is in steam.py: 9 of 357
           games pass p < .05 for gaining, where luck alone gives about 18.
Output     Numbers only. A game goes in the first group it fits.
"""
import numpy as np

from availability import STEP_DAY, STEP_HOUR, STEPS, all_out_steps
from checkouts import GAMES

MIN_CHECKOUTS = 6
MIN_CHANGE = 0.45       # gained / cooled: difference in share of time, before vs. after
MIN_SIDE = 48           # at least 8 observed hours on each side of the moment
SATURDAY_NOON = 3 * 1440 + 720  # cooling after this is the con winding down
MIN_DAYS = 3            # night / day: on at least 3 of the 4 days...
MIN_DAY_GAP = 0.15      # ...by more than this each day...
MIN_MEAN_GAP = 0.25     # ...and by this much on average
LEFT_OUT = {"Braided Skies"}  # probably reached the library Friday night

WED_TO_SAT = STEP_DAY < 4  # on Sunday nearly everything cools


def biggest_change(out):
    """Largest rise and largest fall in share of time out, over every moment to split at."""
    u = out[WED_TO_SAT]
    n, total = len(u), np.cumsum(u)
    split = np.arange(MIN_SIDE - 1, n - MIN_SIDE)
    before = total[split] / (split + 1)
    after = (total[-1] - total[split]) / (n - split - 1)
    fall = np.argmax(before - after)
    fell_at = STEPS[WED_TO_SAT][split[fall]] + 5
    return (after - before).max(), (before - after).max(), fell_at


def night_and_day(out):
    """Share of time out, per day (Wed-Sat): after 6 PM, and before 4 PM."""
    night = np.array([out[(STEP_DAY == d) & (STEP_HOUR >= 18)].mean() for d in range(4)])
    day = np.array([out[(STEP_DAY == d) & (STEP_HOUR < 16)].mean() for d in range(4)])
    return night, day


def groups():
    """{group: [game id, ...]}, strongest first."""
    rows = []
    for b, g in GAMES.items():
        if not g["k"] or g["c"] < MIN_CHECKOUTS or g["n"] in LEFT_OUT:
            continue
        out = all_out_steps(g)
        rise, fall, fell_at = biggest_change(out)
        night, day = night_and_day(out)
        rows.append(dict(b=b, rise=rise, fall=fall, fell_at=fell_at, gaps=night - day, gap=night.mean() - day.mean()))
    rules = {
        "Gained steam": (lambda r: r["rise"] >= MIN_CHANGE, lambda r: -r["rise"]),
        "Cooled off": (lambda r: r["fall"] >= MIN_CHANGE and r["fell_at"] <= SATURDAY_NOON, lambda r: -r["fall"]),
        "Night games": (lambda r: (r["gaps"] > MIN_DAY_GAP).sum() >= MIN_DAYS and r["gap"] >= MIN_MEAN_GAP, lambda r: -r["gap"]),
        "Daytime games": (lambda r: (r["gaps"] < -MIN_DAY_GAP).sum() >= MIN_DAYS and -r["gap"] >= MIN_MEAN_GAP, lambda r: r["gap"]),
    }
    used, result = set(), {}
    for name, (fits, order) in rules.items():
        picked = sorted((r for r in rows if fits(r) and r["b"] not in used), key=order)
        used.update(r["b"] for r in picked)
        result[name] = [r["b"] for r in picked]
    return result


if __name__ == "__main__":
    for name, ids in groups().items():
        print(f"{name}: {len(ids)}", [GAMES[b]["n"] for b in ids[:8]])
