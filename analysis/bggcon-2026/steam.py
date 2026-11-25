"""Games that gained steam: availability before vs after the best split point, against a reshuffled-checkouts null. Prints only."""
from checkouts import *
import numpy as np

rng = np.random.default_rng(3)
z0 = cd(18, 0)
mn = lambda t: (t - z0).total_seconds() / 60
# 10-minute bins of observed open time
bins = []
for a, b in W:
    x = mn(a)
    while x + 10 <= mn(b) + 1e-9:
        bins.append(x + 5)
        x += 10
bins = np.array(bins); NB = len(bins)
close = np.array([mn(c) for _, c in OPEN])
MINSIDE = 48  # at least 8 observed open hours on each side of the split


def series(starts, ends, k):
    """1 where every copy is out."""
    o = np.sort(starts); e = np.sort(ends)
    return ((np.searchsorted(o, bins, 'right') - np.searchsorted(e, bins, 'right')) >= k).astype(float)


def best(u):
    c = np.cumsum(u); n = np.arange(1, NB + 1); tot = c[-1]
    s = np.arange(MINSIDE - 1, NB - MINSIDE)
    bef = c[s] / n[s]; aft = (tot - c[s]) / (NB - n[s])
    j = np.argmax(aft - bef)
    return aft[j] - bef[j], s[j], bef[j], aft[j]


allstarts = np.array([mn(k['o']) for k in K])


def run(minc=8, nsim=1000):
    R = []
    for b, g in GAMES.items():
        if not g['k'] or g['c'] < minc:
            continue
        o = np.array([mn(x['o']) for x in g['ks']]); e = np.array([mn(x['i'] or END) for x in g['ks']])
        d, s, bf, af = best(series(o, e, g['k']))
        dur = e - o; sim = np.empty(nsim)
        for i in range(nsim):
            so = rng.choice(allstarts, len(o)); se = so + rng.permutation(dur)
            cl = close[np.minimum(np.searchsorted(close, so), len(close) - 1)]
            sim[i] = best(series(so, np.minimum(se, cl), g['k']))[0]
        R.append(dict(b=b, n=g['n'], y=g['y'], k=g['k'], c=g['c'], hot=g['hot'], d=d, t=bins[s] + 5, bf=bf, af=af, exp=float(np.median(sim)), p=(1 + (sim >= d - 1e-12).sum()) / (nsim + 1),
                      cb=int((o <= bins[s] + 5).sum())))
    return R


def bh(p):
    o = np.argsort(p); m = len(p); q = np.empty(m); r = 1
    for i in range(m - 1, -1, -1):
        r = min(r, p[o[i]] * m / (i + 1)); q[o[i]] = r
    return q


def clock(t):
    d = int(t // 1440); h = int(t % 1440 // 60)
    return ['Wed', 'Thu', 'Fri', 'Sat', 'Sun'][d] + f" {(h % 12) or 12}:{int(t % 60):02d}{'am' if h < 12 else 'pm'}"


if __name__ == '__main__':
    R = run(); P = np.array([r['p'] for r in R]); Q = bh(P)
    print('games tested', len(R), 'bins', NB, 'p<.05:', (P < .05).sum(), 'expected by luck', round(.05 * len(R), 1), 'p<.01:', (P < .01).sum(), 'exp', round(.01 * len(R), 1), 'q<.1:', (Q < .1).sum(), 'q<.2', (Q < .2).sum())
    for r, q in zip(R, Q): r['q'] = q
    for r in sorted(R, key=lambda r: (r['p'], -r['d']))[:35]:
        print(f"p={r['p']:.3f} q={r['q']:.2f} {r['n'][:38]:38s} {r['y']} copies {r['k']} outs {r['c']} {'HOT' if r['hot'] else ''} {r['bf']:.0%}->{r['af']:.0%} at {clock(r['t'])} (luck gap {r['exp']:.0%}), {r['cb']} checkouts before")
    # day-by-day availability for the top ones, plus the biggest raw jumps
    print('biggest raw jumps')
    for r in sorted(R, key=lambda r: -r['d'])[:15]:
        print(f"{r['n'][:38]:38s} {r['bf']:.0%}->{r['af']:.0%} at {clock(r['t'])} p={r['p']:.3f} q={r['q']:.2f} c={r['c']}")
    # are gains, in aggregate, different from losses? mean jump observed vs null
    print('mean jump', round(np.mean([r['d'] for r in R]), 3), 'null median mean', round(np.mean([r['exp'] for r in R]), 3))
