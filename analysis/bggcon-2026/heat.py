from checkouts import *
def cells(g):
    """per day, 64 cells 8AM-midnight: None=closed, '?'=unknown, else mean copies out (over observed minutes)"""
    out={}
    for d in range(18,23):
        row=[]
        for q in range(64):
            a=cd(d,8)+T(minutes=15*q);b=a+T(minutes=15)
            m=mins(a,b)
            if not any(o<=a<c for o,c in OPEN):row.append(None);continue
            if m<7.5:row.append('?');continue
            row.append(sum(mins(max(a,k['o']),min(b,k['i'] or END)) for k in g['ks'] if k['o']<b and (k['i'] or END)>a)/m)
        out[d]=row
    return out
def line(g,row):
    n=g['k'] or 1;s=''
    for x in row:
        s+=' ' if x is None else '?' if x=='?' else '#' if x>=n-.5 else '+' if x>=.5 else '.'
    return s
if __name__=='__main__':
    for b in('465669','338960','463605','454103','473508','467816','366013'):
        g=GAMES.get(b)
        if not g:continue
        print(g['n'],'| copies',g['k'],'| co',g['c'],'| all-out',round(g['p'] or 0))
        for d,r in cells(g).items():print('  ',('Wed','Thu','Fri','Sat','Sun')[d-18],line(g,r))
    # consistency: cell-based all-out vs minute-based; unknown/closed cell counts
    diffs=[];
    for g in GAMES.values():
        if g['k'] and g['c']>=5:
            v=[x for r in cells(g).values() for x in r if x not in(None,'?')];diffs.append(100*sum(x>=g['k']-.5 for x in v)/len(v)-g['p'])
    c=[x for r in cells(GAMES['465669']).values() for x in r]
    print('cells open',sum(x is not None for x in c),'unknown',c.count('?'),'| cell minus minute all-out pct: mean',sum(diffs)/len(diffs),'min',min(diffs),'max',max(diffs),'n',len(diffs))
