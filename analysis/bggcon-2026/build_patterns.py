"""patterns.html: games whose availability changed or cycled, one heatmap row per game."""
from patterns import *
from page import page

KEEP = day < 4  # Wed to Sat: nearly everything looks like it "cools" when the con winds down Saturday night and Sunday
N2 = int(KEEP.sum()); SIDE = 48


def split(u):
    u = u[KEEP]; c = np.cumsum(u); s = np.arange(SIDE - 1, N2 - SIDE)
    bef, aft = c[s] / (s + 1), (c[-1] - c[s]) / (N2 - s - 1)
    return s, bef, aft


R = []
for b, g in GAMES.items():
    if not g['k']:
        continue
    o = np.array([mn(x['o']) for x in g['ks']]); e = np.array([mn(x['i'] or END) for x in g['ks']])
    u = series(o, e, g['k']); s, bef, aft = split(u)
    up, dn = np.argmax(aft - bef), np.argmax(bef - aft)
    at = lambda j: float(bins[KEEP][s[j]] + 5)
    dd = np.array([u[(day == d) & (loc >= 18)].mean() - u[(day == d) & (loc < 16)].mean() for d in range(4)])
    nt = np.mean([u[(day == d) & (loc >= 18)].mean() for d in range(4)]); dy = np.mean([u[(day == d) & (loc < 16)].mean() for d in range(4)])
    iv, cur, st = [], 0, None
    for t, x in sorted([(q, 1) for q in o] + [(q, -1) for q in e]):
        cur += x
        if cur >= g['k'] and st is None: st = t
        elif cur < g['k'] and st is not None: iv.append([st, t]); st = None
    R.append(dict(p=g['p'], n=g['n'], y=g['y'], k=g['k'], c=g['c'], iv=[[round(a, 1), round(z, 1)] for a, z in iv],
                  up=(aft - bef)[up], dn=(bef - aft)[dn], dt=at(dn), dd=dd, nt=nt, dy=dy))
pct = lambda v: f"{v:.0%}"
row = lambda r, note: dict(n=r['n'], y=r['y'], k=r['k'], c=r['c'], iv=r['iv'], note=note)
SAT12 = 3 * 1440 + 720
con = lambda r: f"{r['p']:.0f}% of the con"
used = set()
DEMAND = sorted(R, key=lambda r: -r['p'])[:100]  # the list by demand; a game here can also be in a pattern group below
R = [r for r in R if r['c'] >= 6 and r['n'] != 'Braided Skies']  # Braided Skies probably arrived Friday night
groups = [dict(title='Most in demand', rows=[row(r, con(r)) for r in DEMAND])]
print('Most in demand', [(r['n'][:24], round(r['p']), r['c']) for r in DEMAND[:12] + DEMAND[-3:]])


def take(title, cond, key, note):
    L = [r for r in sorted((r for r in R if cond(r) and r['n'] not in used), key=key)]
    used.update(r['n'] for r in L)
    groups.append(dict(title=title, rows=[row(r, note(r)) for r in L]))
    print(title, len(L), [r['n'][:28] for r in L[:8]])


take('Gained steam', lambda r: r['up'] >= .45, lambda r: -r['up'], con)
take('Cooled off', lambda r: r['dn'] >= .45 and r['dt'] <= SAT12, lambda r: -r['dn'], con)
take('Night games', lambda r: (r['dd'] > .15).sum() >= 3 and r['nt'] - r['dy'] >= .25, lambda r: r['dy'] - r['nt'], lambda r: f"{pct(r['dy'])} before 4 PM, {pct(r['nt'])} after 6 PM")
take('Daytime games', lambda r: (r['dd'] < -.15).sum() >= 3 and r['dy'] - r['nt'] >= .25, lambda r: r['nt'] - r['dy'], lambda r: f"{pct(r['dy'])} before 4 PM, {pct(r['nt'])} after 6 PM")
mm = lambda L: [[round(mn(a), 1), round(mn(b), 1)] for a, b in L]
page('patterns', dict(groups=groups, O=mm(OPEN), U=mm(UNK)))
