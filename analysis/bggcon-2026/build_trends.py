"""Writes trends.html: how each game's availability moved over the con, one small chart per game.

Data    Games with a known copy count and at least MIN_CHECKOUTS checkouts, Wed-Sat.
Shown   The share of each hour when every copy was out, and a smoothing spline fit to it from Wednesday
        noon (before that the rise is just the library opening full), with the spline's derivative.
        The library line is all copies out, as a share of its own peak. It isn't used in the fit.
"""
import html

import numpy as np
from scipy.interpolate import make_smoothing_spline

from availability import STEPS, all_out_steps, minute
from checkouts import END, GAMES, K
from page import DATA, HERE

MIN_CHECKOUTS = 3
SMOOTHNESS = [1000, 300, 100, 30]  # the spline's penalty: higher is smoother
DEFAULT_SMOOTHNESS = 2  # 100

HOURS = np.arange(8, 96)  # every clock hour from Wed 8 AM, counted from Wed midnight
FITTED = HOURS >= 12  # from Wed noon
STEP_HOURS = (STEPS // 60).astype(int)
OBSERVED = np.array([(STEP_HOURS == h).sum() >= 3 for h in HOURS])  # at least half the hour recorded


def by_hour(steps):
    """Average of a 10-minute series over each clock hour; NaN where it wasn't recorded."""
    return np.array([steps[STEP_HOURS == h].mean() if ok else np.nan for h, ok in zip(HOURS, OBSERVED)])


def trend(y, smoothness):
    """Spline fit to hourly values, and its derivative in percentage points per day."""
    use = OBSERVED & FITTED
    f = make_smoothing_spline(HOURS[use].astype(float), y[use], lam=smoothness)
    return np.where(FITTED, f(HOURS), np.nan), np.where(FITTED, f.derivative()(HOURS) * 24 * 100, np.nan)


def library_out():
    """All copies out by hour, as a share of the peak."""
    out = np.sort([minute(k["o"]) for k in K])
    back = np.sort([minute(k["i"] or END) for k in K])
    y = by_hour(np.searchsorted(out, STEPS, "right") - np.searchsorted(back, STEPS, "right"))
    return y / np.nanmax(y)


def days_alike(y):
    """Average correlation between Wednesday, Friday and Saturday, noon to midnight, hour by hour.

    A day that's all out or all available counts as 0. Thursday is left out: most of it wasn't recorded.
    """
    days = [y[(HOURS >= 24 * d + 12) & (HOURS < 24 * d + 24)] for d in (0, 2, 3)]
    pairs = [(days[i][ok], days[j][ok]) for i, j in ((0, 1), (0, 2), (1, 2)) for ok in [np.isfinite(days[i]) & np.isfinite(days[j])]]
    return np.mean([np.corrcoef(a, b)[0, 1] if a.std() > 0 and b.std() > 0 else 0 for a, b in pairs])


# ---------------------------------------------------------------- drawing

W, H, D = 300, 58, 40  # chart width, height, and height of the rate-of-change strip
LEFT, RIGHT = 40, 56  # room for labels
MAX_RATE = 400  # points a day at the strip's edge
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
    """Outline between the rate of change and zero."""
    xs = [(j + .5) * BAR for j, v in enumerate(rates) if np.isfinite(v)]
    return f"{line(rates, rate_at)}L{xs[-1]:.1f},{D / 2}L{xs[0]:.1f},{D / 2}Z"


def end_labels(library, y, fitted):
    """Names at the right end of each line, moved apart where they'd overlap."""
    last = lambda v: v[np.isfinite(v)][-1]
    labels = sorted([[at(last(library)), "lib", "library"], [at(last(fitted)), "tr", "trend"], [at(last(y)), "hr", "each hour"]])
    for i, label in enumerate(labels):
        label[0] = max(label[0], 12, labels[i - 1][0] + 11 if i else 0)
    for i in (2, 1, 0):
        labels[i][0] = min(labels[i][0], H - 5 - 11 * (2 - i))
    return "".join(f'<text class="{c}" x="{W + 5}" y="{p + 3:.0f}">{t}</text>' for p, c, t in labels)


def baseline():
    """A line at 0% for the recorded hours; closed and unrecorded hours are left empty."""
    return "".join(f'<rect class="base" x="{j * BAR:.1f}" width="{BAR + .1:.1f}" y="{at(0):.1f}" height="1"/>' for j, ok in enumerate(OBSERVED) if ok)


def svg(height, inner):
    return f'<svg viewBox="{-LEFT} 0 {LEFT + W + RIGHT} {height}">{inner}</svg>'


def card(game, library, base):
    y = by_hour(all_out_steps(game))
    fits = [trend(y, s) for s in SMOOTHNESS]
    start, mid = np.argmax(FITTED), D // 2
    bars = "".join(f'<rect class="hrs" x="{j * BAR:.1f}" width="{BAR:.1f}" y="{at(v):.1f}" height="{at(0) - at(v):.1f}"/>'
                   for j, v in enumerate(y) if np.isfinite(v) and v > 0)
    curves = "".join(f'<path class="ln tr v{i}" d="{line(np.where(OBSERVED, f, np.nan), at)}"/>' for i, (f, _) in enumerate(fits))
    rates = "".join(f'<path class="area v{i}" d="{area(r)}"/><path class="ln rt{" flat" if np.nanmax(abs(r)) < 8 else ""} v{i}" d="{line(r, rate_at)}"/>' for i, (_, r) in enumerate(fits))
    days = "".join(f'<text x="{max(0, d * 24) * BAR:.0f}" y="8">{n}</text>' for d, n in enumerate(("Wed", "Thu", "Fri", "Sat")))
    labels = f'<text x="-5" y="{at(1) + 3:.0f}" text-anchor="end">100%</text><text x="-5" y="{at(0) + 3:.0f}" text-anchor="end">0%</text>'
    labels += "".join(f'<g class="v{i}">{end_labels(library, y, f)}</g>' for i, (f, _) in enumerate(fits))
    strip = (f'<text class="up" x="-5" y="{mid - 6}" text-anchor="end">rising</text><text class="dn" x="-5" y="{mid + 13}" text-anchor="end">cooling</text>'
             f'<text x="{W + 5}" y="{mid - 2}">rate of</text><text x="{W + 5}" y="{mid + 9}">change</text>')
    numbers = lambda f: ",".join(f"{f(v):.4f}" for v, _ in fits)
    name, share = html.escape(game["n"]), np.nanmean(y)
    return (f'<div class="c" data-n="{name.lower()}" data-k="{game["k"]}" data-c="{game["c"]}" data-m="{share:.4f}" data-d="{days_alike(y):.3f}" data-t="{",".join(f"{v:.2f}" for v in fits[DEFAULT_SMOOTHNESS][0][FITTED & OBSERVED])}"'
            f' data-nets="{numbers(lambda v: v[-1] - v[start])}" data-amps="{numbers(lambda v: np.ptp(v[start:]))}">'
            f'<b>{name}</b><span>{game["k"]} cop{"y" if game["k"] == 1 else "ies"} · {game["c"]} checkouts · out {share:.0%}</span>'
            + svg(H, base + bars + f'<path class="ln lib" d="{line(library, at)}"/>' + curves + days + f'<g class="tag">{labels}</g>')
            + svg(D, f'<line class="zero" x2="{W}" y1="{mid}" y2="{mid}"/>{rates}<g class="tag">{strip}</g>') + "</div>")


def main():
    library, base = library_out(), baseline()
    games = [g for g in GAMES.values() if g["k"] and g["c"] >= MIN_CHECKOUTS]
    page = (HERE / "trends.template.html").read_text(encoding="utf-8")
    for key, value in (("/*CSS*/", (HERE / "report.css").read_text(encoding="utf-8")), ("/*SMOOTH*/", str(SMOOTHNESS)), ("/*DEFAULT*/", str(DEFAULT_SMOOTHNESS)), ("/*STRIP*/", str(D)),
                       ("<!--CARDS-->", "".join(card(g, library, base) for g in games))):
        page = page.replace(key, value)
    (DATA / "trends.html").write_text(page, encoding="utf-8")
    print(len(games), "games")


if __name__ == "__main__":
    main()
