"""Availability patterns: gained, cooled, night and daytime games. Prints only when run."""
from steam import *
loc = bins % 1440 / 60; day = (bins // 1440).astype(int)
def rows(minc=6):
    out = []
    for b, g in GAMES.items():
        if not g['k'] or g['c'] < minc: continue
        o = np.array([mn(x['o']) for x in g['ks']]); e = np.array([mn(x['i'] or END) for x in g['ks']])
        u = series(o, e, g['k'])
        dn = []  # per day: all-copies-out share 6 PM on, minus before 4 PM
        for d in range(4):
            a, c = u[(day == d) & (loc >= 18)], u[(day == d) & (loc < 16)]
            dn.append((a.mean(), c.mean()))
        dn = np.array(dn)
        up, dw = best(u), best(1 - u)
        out.append(dict(b=b, n=g['n'], c=g['c'], k=g['k'], y=g['y'], hot=g['hot'], gain=up[0], gt=bins[up[1]] + 5, gb=up[2], ga=up[3], drop=dw[0], dt=bins[dw[1]] + 5, db=1 - dw[2], da=1 - dw[3],
                        night=dn[:, 0].mean(), dayt=dn[:, 1].mean(), diff=dn[:, 0] - dn[:, 1]))
    return out
if __name__ == '__main__':
    R = rows()
    print(len(R))
    for nm, key, f in (('GAINED', lambda r: -r['gain'], lambda r: f"{r['gb']:.0%}->{r['ga']:.0%} {clock(r['gt'])}"), ('COOLED', lambda r: -r['drop'], lambda r: f"{r['db']:.0%}->{r['da']:.0%} {clock(r['dt'])}"),
                       ('NIGHT', lambda r: -(r['night'] - r['dayt']) - .1 * (r['diff'] > .15).sum(), lambda r: f"day {r['dayt']:.0%} night {r['night']:.0%} {np.round(r['diff'], 1)}"), ('DAY', lambda r: (r['night'] - r['dayt']) - .1 * (r['diff'] < -.15).sum(), lambda r: f"day {r['dayt']:.0%} night {r['night']:.0%} {np.round(r['diff'], 1)}")):
        print('==', nm)
        for r in sorted(R, key=key)[:16]: print(f"  {r['n'][:40]:40s} {r['y']} k{r['k']} c{r['c']} {f(r)}")
    D = np.array([r['diff'] for r in R]); rise3 = (D > .15).sum(1) >= 3; fall3 = (D < -.15).sum(1) >= 3
    print('night on 3+ days', rise3.sum(), 'day on 3+ days', fall3.sum())
