let M = document.getElementById('m'); // where charts are added
const tip = document.getElementById('tip');
const hr = h => (h % 12 || 12) + (h < 12 ? ' AM' : ' PM'), pc = x => Math.round(100 * x) + '%', two = v => v.toFixed(2);
const W = 960, X0 = 48, X1 = 940;
const panel = (title, key, h, body) => M.insertAdjacentHTML('beforeend', `<h2>${title}</h2><div class="key">${key}</div><svg viewBox="0 0 ${W} ${h}">${body(h)}</svg>`);
const sw = (c, t) => `<span><i style="background:${c}"></i>${t}</span>`, ln = (c, w, t) => `<span><i class="ln" style="border-top:${w}px solid ${c}"></i>${t}</span>`;
const hit = (x, w, h, t) => `<rect x="${x}" y="0" width="${w}" height="${h}" fill="transparent" data-t="${t}"/>`;
const yaxis = (ticks, y, f) => ticks.map(t => `<line x1="${X0}" x2="${X1}" y1="${y(t)}" y2="${y(t)}" stroke="var(--border)"/><text x="${X0 - 8}" y="${y(t) + 4}" text-anchor="end">${f(t)}</text>`).join('');
const line = (r, x, y) => { let p = '', up = 1; r.forEach((v, i) => { if (v == null) { up = 1; return; } p += (up ? 'M' : 'L') + x(i) + ',' + y(v); up = 0; }); return p; };

// Every library game as a dot, in one column per name; each column is filled in rows from the bottom, most checked out first.
// games: [name, value shown in the tooltip, checkouts, column]
function rad(n) { return n ? 1.4 + 2.6 * Math.min(n, 25) / 25 : 1.2; }
function dotChart(title, what, games, names) {
  M.insertAdjacentHTML('beforeend', `<h2>${title}</h2><div class="key">${[1, 10, 25].map(n => `<span><i style="background:var(--lit);border-radius:50%;width:${2 * rad(n)}px;height:${2 * rad(n)}px;vertical-align:middle"></i>${n === 1 ? '1 checkout' : n === 25 ? '25+' : n}</span>`).join('')}<span><i style="background:var(--never);border-radius:50%;width:4px;height:4px;vertical-align:middle"></i>Never checked out</span></div><canvas style="display:block;width:100%"></canvas>`);
  const C = M.lastElementChild, B = names.length, cw = (X1 - X0) / B, cols = Array.from({ length: B }, () => []), P = [];
  for (const g of games) cols[g[3]].push(g);
  let H0 = 0;
  cols.forEach((c, i) => {
    c.sort((a, b) => b[2] - a[2]); let y = 0, k = 0;
    while (k < c.length) {
      const h = 2 * rad(c[k][2]) + .6; let wd = 0, row = [];
      while (k < c.length) { const d = 2 * rad(c[k][2]) + .6; if (row.length && wd + d > cw - 8) break; row.push([c[k], d]); wd += d; k++; }
      let px = X0 + i * cw + 1 + (cw - 2 - wd) / 2;
      for (const [g, d] of row) { P.push({ g, x: px + d / 2, y: y + h / 2, r: rad(g[2]) }); px += d; }
      y += h;
    }
    H0 = Math.max(H0, y);
  });
  const HT = Math.ceil(H0) + 40, base = H0 + 8; C.width = W; C.height = HT;
  function draw() {
    const k = C.clientWidth / W, r = devicePixelRatio || 1, css = getComputedStyle(document.documentElement), v = n => css.getPropertyValue(n).trim();
    C.width = C.clientWidth * r; C.height = HT * k * r; C.style.height = HT * k + 'px';
    const x = C.getContext('2d'); x.setTransform(k * r, 0, 0, k * r, 0, 0); x.clearRect(0, 0, W, HT);
    for (const p of P) { x.fillStyle = v(p.g[2] ? '--lit' : '--never'); x.beginPath(); x.arc(p.x, base - p.y, p.r, 0, 7); x.fill(); }
    x.strokeStyle = v('--border'); x.beginPath(); x.moveTo(X0, base + 2); x.lineTo(X1, base + 2); x.stroke();
    x.fillStyle = v('--muted'); x.font = '12px system-ui'; x.textAlign = 'center'; for (let i = 0; i < B; i++) x.fillText(names[i], X0 + (i + .5) * cw, base + 22);
  }
  draw(); addEventListener('resize', draw); matchMedia('(prefers-color-scheme: dark)').addEventListener('change', draw);
  C.addEventListener('pointermove', e => {
    const b = C.getBoundingClientRect(), k = W / b.width, mx = (e.clientX - b.left) * k, my = (e.clientY - b.top) * k;
    let best = null, bd = 3; for (const p of P) { const d = Math.hypot(p.x - mx, base - p.y - my) - p.r; if (d < bd) { bd = d; best = p; } }
    C.dataset.t = best ? `${best.g[0]}\n${what} ${best.g[1]} · ${best.g[2] ? best.g[2] + ' checkout' + (best.g[2] === 1 ? '' : 's') : 'never checked out'}` : '';
  });
}
// rate: one [games, checked out] per column, same columns as the dots
function shareChart(title, what, rate, names, max) {
  const all = rate.reduce((t, r) => [t[0] + r[0], t[1] + r[1]], [0, 0]), avg = all[1] / all[0];
  panel(title, sw('var(--lit)', 'Checked out at least once') + '<span><i class="ln" style="border-top:2px dashed var(--fg)"></i>All games (' + pc(avg) + ')</span>', 250, h => {
    const y = v => h - 40 - (h - 56) * v / max, b = (X1 - X0) / names.length, ticks = []; for (let t = 0; t <= max + 1e-9; t += .2) ticks.push(t);
    let o = yaxis(ticks, y, pc);
    rate.forEach(([c, k], i) => {
      o += `<rect x="${X0 + i * b + 4}" y="${y(k / c)}" width="${b - 8}" height="${y(0) - y(k / c)}" fill="var(--lit)"/>` + hit(X0 + i * b, b, h - 40, `${what} ${names[i]}\n${k.toLocaleString()} of ${c.toLocaleString()} games checked out (${pc(k / c)})`);
      o += `<text x="${X0 + (i + .5) * b}" y="${h - 20}" text-anchor="middle">${names[i]}</text>`;
    });
    return o + `<line x1="${X0}" x2="${X1}" y1="${y(avg)}" y2="${y(avg)}" stroke="var(--fg)" stroke-width="2" stroke-dasharray="5 4" pointer-events="none"/>`;
  });
}
// Value by clock hour: all days, each day, and the range chance alone would give.
// S: {H hours, n checkouts, m all-days values, days per-day values, band [low, high]}
function hourChart(title, S, ticks, f) {
  const DN = ['Wed', 'Thu', 'Fri', 'Sat'], s = (X1 - X0) / S.H.length, x = i => X0 + (i + .5) * s;
  const xax = h => S.H.map((v, i) => i % 2 ? '' : `<text x="${x(i)}" y="${h - 12}" text-anchor="middle">${hr(v)}</text>`).join('');
  panel(title, ln('var(--lit)', 3, 'All days') + ln('var(--day)', 1, 'Each day') + sw('var(--bandc)', 'Range from chance alone'), 300, h => {
    const lo = ticks[0], hi = ticks[ticks.length - 1], y = v => h - 36 - (h - 52) * (v - lo) / (hi - lo);
    let o = yaxis(ticks, y, f) + xax(h);
    o += `<path d="${line(S.band[0], x, y)}${S.band[1].map((v, i) => [v, i]).reverse().map(([v, i]) => 'L' + x(i) + ',' + y(v)).join('')}Z" fill="var(--bandc)"/>`;
    S.days.forEach(r => o += `<path d="${line(r, x, y)}" fill="none" stroke="var(--day)"/>`);
    o += `<path d="${line(S.m, x, y)}" fill="none" stroke="var(--lit)" stroke-width="2.5"/>`;
    S.m.forEach((v, i) => o += `<circle cx="${x(i)}" cy="${y(v)}" r="4.5" fill="var(--lit)" stroke="var(--bg)" stroke-width="2"/>`);
    S.m.forEach((v, i) => o += hit(x(i) - s / 2, s, h - 36, `${hr(S.H[i])} · ${S.n[i].toLocaleString()} checkouts\nAll days  ${f(v)}\n` + DN.map((d, k) => S.days[k][i] == null ? '' : d + '  ' + f(S.days[k][i]) + '\n').join('') + `Chance  ${f(S.band[0][i])} to ${f(S.band[1][i])}`));
    return o;
  });
}
document.addEventListener('pointermove', e => {
  const t = e.target.dataset && e.target.dataset.t; tip.style.display = t ? 'block' : 'none';
  if (t) { tip.textContent = t; tip.style.left = Math.min(e.clientX + 14, innerWidth - 190) + 'px'; tip.style.top = Math.min(e.clientY + 14, innerHeight - 130) + 'px'; }
});
