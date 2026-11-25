from checkouts import *
import statistics as st
R=list(GAMES.values());print('CHECK',len(K),round(OPEN_MIN/60,2),len(HOT),[(g['n'],g['c'],round(g['p'])) for g in sorted(R,key=lambda g:-g['c'])[:3]],round(GAMES['465669']['p'],1),sum(g['c'] for g in R if g['y']==2026),sum(g['c'] for g in R if g['y'] is None))
one=[g for g in R if g['c']==1];print('games out',len(R),'once',len(one),round(100*len(one)/len(R),1),'share of checkouts',round(100*len(one)/len(K),1))
print('dist',sorted(collections.Counter(min(g['c'],10) for g in R).items()))
nev=[b for b in LIB if b not in GAMES];print('library games',len(LIB),'never',len(nev),'once in lib',sum(g['b'] in LIB for g in one),'out in lib',sum(g['b'] in LIB for g in R))
def yb(y):return 'none' if not y else '2026' if y==2026 else '2027' if y>2026 else '2024-25' if y>2023 else '2020-23' if y>2019 else '2010-19' if y>2009 else '2000-09' if y>1999 else '1990-99' if y>1989 else '<1990'
L=collections.Counter(yb(meta(b)[1]) for b in LIB);O=collections.Counter(yb(g['y']) for g in one);A=collections.Counter(yb(g['y']) for g in R);NV=collections.Counter(yb(meta(b)[1]) for b in nev)
for z in L:print(' ',z,'lib',L[z],'never',NV[z],f'{100*NV[z]/L[z]:.0f}%','once',O[z],'out',A[z],f'once/out {100*O[z]/max(1,A[z]):.0f}%')
print('once by day',sorted(collections.Counter(ct(g['ks'][0]['d']).strftime('%d %a') for g in one).items()));print('all by day',sorted(collections.Counter(ct(k['d']).strftime('%d %a') for k in K).items()))
du=lambda k:(k['i']-k['d']).total_seconds()/60 if k['i'] else None
s=[x for g in one if (x:=du(g['ks'][0])) is not None];print('once dur: median',st.median(s),'<=20min',sum(x<=20 for x in s),'never returned',len(one)-len(s))
print('once not-lib',sum(g['b'] not in LIB for g in one),'multi-copy once',sum((g['k'] or 1)>1 for g in one))
ur=lambda b:(CAT[b]['users_rated'] if b in CAT else int(X[b]['users_rated']) if b in X else None)
print('POPULAR once');[print('  ',g['n'],g['y'],ur(g['b']),'copies',g['k']) for g in sorted(one,key=lambda g:-(ur(g['b']) or 0))[:25]]
print('POPULAR never');[print('  ',meta(b)[0],meta(b)[1],ur(b),'copies',LIB[b]['copies']) for b in sorted(nev,key=lambda b:-(ur(b) or 0))[:12]]
print('2026 once',[(g['n'],g['k']) for g in one if g['y']==2026][:10]);print('once with most copies',[(g['n'],g['y'],g['k']) for g in sorted(one,key=lambda g:-(g['k'] or 0))[:8]])
