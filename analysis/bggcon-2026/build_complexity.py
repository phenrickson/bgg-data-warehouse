"""Data for complexity.html: histogram, mix by hour, hourly mean vs chance, party share vs chance."""
import warnings; warnings.filterwarnings("ignore")
from complexity import *

H = np.arange(8, 24)


def band(d, h, v, n=2000):
    """Middle 95% of each hour's mean when values are shuffled among that day's checkouts."""
    gi = [np.where(d == x)[0] for x in range(18, 22)]
    S = []
    for _ in range(n):
        a = v.copy()
        for g in gi:
            a[g] = rng.permutation(a[g])
        S.append(hourly(h, a, H))
    return np.percentile(S, [2.5, 97.5], axis=0).round(4).tolist()


def series(d, h, v):
    return dict(m=hourly(h, v, H).round(4).tolist(), n=[int((h == x).sum()) for x in H], band=band(d, h, v),
                days=[[round(float(v[(h == x) & (d == y)].mean()), 4) if ((h == x) & (d == y)).sum() >= 20 else None for x in H]
                      for y in range(18, 22)])


d, h, v, b = prep()
cut = [0, 1.5, 2, 2.5, 3, 9]
mix = [(np.histogram(v[h == x], cut)[0] / (h == x).sum()).round(4).tolist() for x in H]
arr = lambda ids: (lambda x: x[~np.isnan(x)])(np.array([w(x) for x in ids]))
out, never = arr([x for x in LIB if x in GAMES]), arr([x for x in LIB if x not in GAMES])  # each library game once
for n_, x in (('out', out), ('never', never)):
    print(n_, len(x), 'mean %.2f median %.2f' % (x.mean(), np.median(x)), '<1.5 %.0f%% 3+ %.0f%%' % (100 * (x < 1.5).mean(), 100 * (x >= 3).mean()))
print('library games out / never', sum(x in GAMES for x in LIB), sum(x not in GAMES for x in LIB))
al = np.concatenate([out, never]); io_ = np.r_[np.ones(len(out)), np.zeros(len(never))]
f2 = lambda q: f"{q:.2f}"[:-1] if f"{q:.2f}".endswith("0") else f"{q:.2f}"
names = [f"{f2(q)}–{f2(q + .25)}" for q in np.arange(1, 4, .25)] + ["4+"]
dots = [[(meta(x)[0] or LIB[x].get('name')), f"{w(x):.2f}", GAMES[x]['c'] if x in GAMES else 0, min(12, int((w(x) - 1) * 4 + 1e-6))] for x in LIB if not np.isnan(w(x))]
rate = [[sum(g[3] == i for g in dots), sum(g[3] == i and g[2] > 0 for g in dots)] for i in range(13)]
df = [abs((lambda z: al[z == 1].mean() - al[z == 0].mean())(rng.permutation(io_))) for _ in range(5000)]
print('diff p', (1 + (np.array(df) >= abs(out.mean() - never.mean())).sum()) / 5001)
pd_, ph, pv, _ = prep(val=lambda b, x: float('Party Game' in CATS[b]) if b in CATS else np.nan)
data = dict(H=H.tolist(), mix=mix, cx=series(d, h, v), party=series(pd_, ph, pv), dots=dots, rate=rate, names=names)
from page import page
page('complexity', data)
