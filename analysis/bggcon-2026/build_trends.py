"""Writes trends.html: how each game's availability moved over the con, one small chart per game.

Data    Games with a known copy count and at least MIN_CHECKOUTS checkouts, Wed-Sat.
Shown   The share of each hour when every copy was out; a look-back average of it, from Wednesday noon
        (before that the rise is just the library opening full); and that average's change each hour.
        The library line is all copies out, as a share of its own peak. It isn't used in the fit.
"""
import html

import numpy as np

from availability import STEPS, all_out_steps, minute
from checkouts import END, GAMES, K
from page import DATA, HERE

MIN_CHECKOUTS = 3
HALF_LIVES = [8, 6, 4, 2]  # recorded hours
DEFAULT_HALF_LIFE = 2  # 4 hours

HOURS = np.arange(8, 96)  # every clock hour from Wed 8 AM, counted from Wed midnight
FITTED = HOURS >= 12  # from Wed noon
STEP_HOURS = (STEPS // 60).astype(int)
OBSERVED = np.array([(STEP_HOURS == h).sum() >= 3 for h in HOURS])  # at least half the hour recorded


def by_hour(steps):
    """Average of a 10-minute series over each clock hour; NaN where it wasn't recorded."""
    return np.array([steps[STEP_HOURS == h].mean() if ok else np.nan for h, ok in zip(HOURS, OBSERVED)])


def trend(y, half_life):
    """Average of the hours so far, where an hour counts half as much every half_life recorded hours, and how many
    points it moved from one recorded hour to the next. Looks back only, so it can be worked out live.
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


# ---------------------------------------------------------------- drawing

W, H, D = 300, 74, 28  # chart width, height, and height of the rate-of-change strip
MAX_RATE = 20  # points an hour, at the strip's edge
BAR = W / len(HOURS)


def at(v):
    return H - 3 - (H - 16) * max(-.06, min(1.06, v))


def rate_at(v):
    return D / 2 - max(-MAX_RATE, min(MAX_RATE, v)) * (D - 4) / (2 * MAX_RATE)


def line(values, y):
    parts, pen = [], "M"
    for j, v in enumerate(values):
        if np.isfinite(v):
            parts.append(f"{pen}{(j + .5) * BAR:.1f},{y(v):.1f}")
        pen = "L" if np.isfinite(v) else "M"
    return "".join(parts)


def area(rates):
    """Between the rate of change and zero, one shape per stretch of recorded hours."""
    shapes, run = [], []
    for j, v in enumerate([*rates, np.nan]):
        if np.isfinite(v):
            run.append(((j + .5) * BAR, rate_at(v)))
        elif run:
            shapes.append(f"M{run[0][0]:.1f},{D / 2}" + "".join(f"L{x:.1f},{y:.1f}" for x, y in run) + f"L{run[-1][0]:.1f},{D / 2}Z")
            run = []
    return "".join(shapes)


def end_labels(library, fitted):
    """Names at the right: the two lines where they end, moved apart if they'd overlap, and the bars at their base."""
    last = lambda v: v[np.isfinite(v)][-1]
    labels = sorted([[at(last(library)), "lib", "library"], [at(last(fitted)), "tr", "demand"]])
    bars = H - 4
    labels[0][0] = max(labels[0][0], 12)
    labels[1][0] = min(max(labels[1][0], labels[0][0] + 11), bars - 11)
    labels[0][0] = min(labels[0][0], labels[1][0] - 11)
    return "".join(f'<text class="{c}" x="{W + 5}" y="{p + 3:.0f}">{t}</text>' for p, c, t in labels + [[bars, "hr", "all out"]])


def baseline():
    """A line at 0% for the recorded hours; closed and unrecorded hours are left empty."""
    return "".join(f'<rect class="base" x="{j * BAR:.1f}" width="{BAR + .1:.1f}" y="{at(0):.1f}" height="1"/>' for j, ok in enumerate(OBSERVED) if ok)


def svg(height, inner):
    return f'<svg viewBox="0 0 {W} {height}">{inner}</svg>'


def card(game, library, base):
    y = by_hour(all_out_steps(game))
    fits = [trend(y, h) for h in HALF_LIVES]
    start, mid = np.argmax(FITTED), D // 2
    gaps = night_gaps(y)
    bars = "".join(f'<rect class="hrs" x="{j * BAR:.1f}" width="{BAR:.1f}" y="{at(v):.1f}" height="{at(0) - at(v):.1f}"/>'
                   for j, v in enumerate(y) if np.isfinite(v) and v > 0)
    curves = "".join(f'<path class="ln tr v{i}" d="{line(np.where(OBSERVED, f, np.nan), at)}"/>' for i, (f, _) in enumerate(fits))
    rates = "".join(f'<path class="area v{i}" d="{area(r)}"/><path class="ln rt{" flat" if np.nanmax(abs(r)) < 1 else ""} v{i}" d="{line(r, rate_at)}"/>' for i, (_, r) in enumerate((f, np.where(OBSERVED, r, np.nan)) for f, r in fits))
    days = "".join(f'<text x="{max(0, d * 24) * BAR:.0f}" y="8">{n}</text>' for d, n in enumerate(("Wed", "Thu", "Fri", "Sat")))
    axis = f'<text x="-5" y="{at(1) + 3:.0f}" text-anchor="end">100%</text><text x="-5" y="{at(0) + 3:.0f}" text-anchor="end">0%</text>'
    names = "".join(f'<g class="v{i}">{end_labels(library, f)}</g>' for i, (f, _) in enumerate(fits))
    strip = (f'<text class="up" x="-5" y="{mid - 2}" text-anchor="end">rising</text><text class="dn" x="-5" y="{mid + 10}" text-anchor="end">cooling</text>')
    numbers = lambda f: ",".join(f"{f(v):.4f}" for v, _ in fits)
    name, share = html.escape(game["n"]), np.nanmean(y)
    return (f'<div class="c" data-n="{name.lower()}" data-k="{game["k"]}" data-c="{game["c"]}" data-m="{share:.4f}" data-night="{min(gaps):.3f}" data-day="{-max(gaps):.3f}" data-t="{",".join(f"{v:.2f}" for v in fits[DEFAULT_HALF_LIFE][0][FITTED & OBSERVED])}"'
            f' data-nets="{numbers(lambda v: v[OBSERVED][-1] - v[start])}" data-amps="{numbers(lambda v: np.ptp(v[FITTED & OBSERVED]))}">'
            f'<b>{name}</b><span>{game["k"]} cop{"y" if game["k"] == 1 else "ies"} · {game["c"]} checkouts · all out {share:.0%}</span>'
            + svg(H, base + bars + f'<path class="ln lib" d="{line(library, at)}"/>' + curves + days + f'<g class="tag axis">{axis}</g><g class="tag names">{names}</g>')
            + svg(D, f'<line class="zero" x2="{W}" y1="{mid}" y2="{mid}"/>{rates}<g class="tag axis">{strip}</g>') + "</div>")


def main():
    library, base = library_out(), baseline()
    games = [g for g in GAMES.values() if g["k"] and g["c"] >= MIN_CHECKOUTS]
    page = (HERE / "trends.template.html").read_text(encoding="utf-8")
    for key, value in (("/*CSS*/", (HERE / "report.css").read_text(encoding="utf-8")), ("/*SMOOTH*/", str(HALF_LIVES)), ("/*DEFAULT*/", str(DEFAULT_HALF_LIFE)), ("/*STRIP*/", str(D)),
                       ("<!--CARDS-->", "".join(card(g, library, base) for g in games))):
        page = page.replace(key, value)
    (DATA / "trends.html").write_text(page, encoding="utf-8")
    print(len(games), "games")


if __name__ == "__main__":
    main()
