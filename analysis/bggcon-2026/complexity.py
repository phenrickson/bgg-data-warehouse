"""Complexity vs time of day: permutation tests. Prints only."""
from checkouts import *
import numpy as np, checkouts as con, sys
rng = np.random.default_rng(1)
def w(b):
    v = CAT[b]['average_weight'] if b in CAT else float(X[b]['average_weight'] or 0) if b in X else 0
    return v or np.nan
CATS = {str(r["game_id"]): set(r["categories"] or []) for r in con._t.select(["game_id", "categories"]).to_pylist()}
R = sorted(((ct(k['o']), k['b']) for k in K), key=lambda r: r[0])
R = [(t, b, w(b)) for t, b in R if 18 <= t.day <= 21 and t.hour >= 8]
def prep(keep=lambda t: True, val=lambda b, x: x):
    r = [(t.day, t.hour, val(b, x), b) for t, b, x in R if keep(t)]
    r = [x for x in r if not np.isnan(x[2])]
    return [np.array(v) for v in list(zip(*r))[:3]] + [np.array([x[3] for x in r])]
def hourly(h, v, H): return np.array([v[h == x].mean() for x in H])
def test(d, h, v, n=5000):
    # statistic: between-hour variation in means, net of day means. Nulls: (a) shuffle within day, (b) rotate each day's sequence in time order, which keeps runs of similar games together
    dm = v - np.array([v[d == x].mean() for x in range(18, 22)])[d - 18]
    H = np.unique(h); cnt = np.array([(h == x).sum() for x in H]); ix = np.searchsorted(H, h); gi = [np.where(d == x)[0] for x in range(18, 22)]
    st = lambda z: (np.bincount(ix, z) ** 2 / cnt).sum() / (z ** 2).sum()
    sl = lambda z: np.corrcoef(h, z)[0, 1]
    o = st(dm), sl(dm); A, B = [], []
    for _ in range(n):
        a, b = dm.copy(), dm.copy()
        for g in gi:
            a[g] = rng.permutation(a[g])
            if len(g): b[g] = np.roll(b[g], rng.integers(len(g)))
        A.append((st(a), sl(a))); B.append((st(b), sl(b)))
    A, B = np.array(A), np.array(B); P = lambda x, f: (1 + f(x).sum()) / (n + 1)
    return dict(r2=round(o[0], 4), r=round(o[1], 3), p_shuffle=P(A, lambda x: x[:, 0] >= o[0]), p_rotate=P(B, lambda x: x[:, 0] >= o[0]),
                p_trend_shuffle=P(A, lambda x: abs(x[:, 1]) >= abs(o[1])), p_trend_rotate=P(B, lambda x: abs(x[:, 1]) >= abs(o[1])))
if __name__ == '__main__':
    d, h, v, b = prep(); H = np.arange(8, 24)
    for x in H:
        s = v[h == x]; print(x, len(s), 'mean %.2f median %.2f' % (s.mean(), np.median(s)), '<1.5 %.0f%% 3+ %.0f%%' % (100 * (s < 1.5).mean(), 100 * (s >= 3).mean()), [round(v[(h == x) & (d == y)].mean(), 2) if ((h == x) & (d == y)).sum() > 9 else None for y in range(18, 22)])
    pr = lambda n, *a: print(n, test(*a))
    pr('all', d, h, v)
    k = ~((d == 19) & (h >= 21)); pr('no Thu 9-12PM', d[k], h[k], v[k])
    k = d != 19; pr('no Thursday', d[k], h[k], v[k])
    for y in (18, 20, 21):
        k = d == y; pr('day %d' % y, d[k], h[k], v[k])
    k = h >= 10; pr('from 10 AM', d[k], h[k], v[k])
    for c in ('Party Game', 'Humor', 'Card Game', 'Dice', 'Wargame', 'Economic'):
        dd, hh, vv, _ = prep(val=lambda b, x: float(c in CATS[b]) if b in CATS else np.nan)
        print(c, [round(100 * vv[hh == x].mean(), 1) for x in H]); pr('  ', dd, hh, vv)
