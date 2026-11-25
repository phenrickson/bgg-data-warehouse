// top: share of the con, all copies out, that gets the hottest in-demand color.
// The map of every library game. still: the whole con at once, with no playback or controls; color: weight, category or unavailable.
R.map = (root, DATA, { still = false, color = 'weight', top = DATA.topNA } = {}) => {
// Times: minutes since Wed 18 Nov 00:00 Chicago. O = library hours, U = not-observed windows.
// Playback time p runs over open hours only (nights are skipped): total length = sum of O.
const $ = i => root.querySelector(`[data-m="${i}"]`), G = DATA.G.map(g => ({ ...g })), DAYS = ['Wed', 'Thu', 'Fri', 'Sat', 'Sun'];
let total = 0; const SEG = DATA.O.map(([a, b]) => { const s = { a, b, p: total }; total += b - a; return s; });
const toT = p => { const s = SEG.findLast(s => p >= s.p); return s.a + (p - s.p); };
const clock = t => { const d = Math.floor(t / 1440), m = Math.floor(t) % 1440, h = Math.floor(m / 60); return `${DAYS[d]} ${(h + 11) % 12 + 1}:${String(m % 60).padStart(2, '0')} ${h < 12 ? 'AM' : 'PM'}`; };
const isOutage = ([a, b]) => b - a > 120;
const inOutage = t => DATA.U.some(w => isOutage(w) && t >= w[0] && t < w[1]);
const unk = t => DATA.U.some(([a, b]) => t >= a && t < b);
const outCount = (g, t) => { let n = 0; for (const [a, b] of g.iv) if (a <= t && t < b) n++; return n; };
const FADE = 10; // con minutes a game takes to fade in when checked out and out when returned
const OPEN_MIN = DATA.W.reduce((t, [a, b]) => t + b - a, 0); // observed open minutes over the whole con
for (const g of G) { // observed parts of the time every copy was out
  g.na = []; for (const [a, b] of g.ao) for (const [x, y] of DATA.W) if (Math.max(a, x) < Math.min(b, y)) g.na.push([Math.max(a, x), Math.min(b, y)]);
}
// Share of the con's observed open time, up to t, when every copy was out. Divides by the whole con, so it only grows.
const unavailable = (g, t) => { let n = 0; for (const [a, b] of g.na) if (t > a) n += Math.min(t, b) - a; return n / OPEN_MIN; };
const NOWFAINT = .3;
// Size is checkouts so far, on a straight scale up to DATA.top, fixed for the whole con so dots grow to their end size.
const radius = n => 2 + 4 * Math.min(n, DATA.top) / DATA.top;
const soFar = (g, t) => { let n = 0; for (const [a] of g.iv) if (a <= t) n += Math.min(1, (t - a) / FADE); return n; };
const OUT = G.filter(g => g.iv.length).sort((a, b) => a.iv.length - b.iv.length); // most checked out drawn last, on top
if (still && color === 'unavailable') { const d = new Map(OUT.map(g => [g, unavailable(g, 1e9)])); OUT.sort((a, b) => d.get(a) - d.get(b)); } // hottest on top
function fade(list, t) {
  let m = 0;
  for (const [a, b] of list) if (t >= a && t < b + FADE) m = Math.max(m, Math.min(1, (t - a) / FADE, t < b ? 1 : 1 - (t - b) / FADE));
  return m;
}
// Category slots copied from the viewer's map; complexity is our own scale.
const CATNAMES = ['Other', 'Trains', 'Economic', 'Wargame', 'Party Game', "Children's Game", 'Abstract Strategy', 'Card Game'];
let SEQ = []; // complexity, 1 to 4+: teal, then white (black on a light page), then red
let DEM = []; // demand ramp, 16 steps, rebuilt when the theme changes
const rgb = c => c.match(/[\d.]+/g).slice(0, 3).map(Number);
const mix = (keys, N) => { const [a, b, c] = keys.map(k => rgb(cols[k])); return Array.from({ length: N }, (_, i) => { const u = i / (N - 1) * 2, [p, q, f] = u <= 1 ? [a, b, u] : [b, c, u - 1]; return `rgb(${p.map((v, j) => Math.round(v + (q[j] - v) * f)).join(',')})`; }); };
let NOW = []; // share of copies out right now
// Copies out right now as a share of the game's copies; each loan eases in and out over FADE minutes.
const shareOut = (g, t) => Math.min(1, g.iv.reduce((n, [a, b]) => n + (t >= a && t < b + FADE ? Math.min(1, (t - a) / FADE, t < b ? 1 : 1 - (t - b) / FADE) : 0), 0) / g.k);
function buildRamps() { NOW = mix(['--n0', '--d1', '--d2'], 16); DEM = mix(['--d0', '--d1', '--d2'], 16); SEQ = mix(['--w0', '--w1', '--w2'], 13); }
const demand = (g, t) => Math.min(1, unavailable(g, t) / top);
const step = (v, lo, hi, n = 12) => Math.min(n - 1, Math.max(0, Math.floor((v - lo) / (hi - lo) * n)));
const BY = {
  weight: g => g.w ? SEQ[step(g.w, 1, 4, SEQ.length)] : cols['--c0'],
  category: g => cols['--c' + g.c],
  unavailable: (g, t) => DEM[Math.min(15, Math.floor(16 * demand(g, t)))],
};
function key() {
  const K = $('key'); K.replaceChildren();
  const item = (bg, text) => { const s = document.createElement('span'), i = document.createElement('i'); i.style.background = bg; s.append(i, text); K.append(s); };
  const ramp = (lo, hi, stops) => { const s = document.createElement('span'), b = document.createElement('b'); s.className = 'ramp'; b.style.background = `linear-gradient(90deg,${stops.join(',')})`; s.append(lo, b, hi); K.append(s); };
  item(cols['--context'], 'Not checked out');
  const by = $('by').value;
  const now = p >= 0 && $('mode').value === 'now';
  const pct = Math.round(top * 100);
  if (now && by !== 'unavailable') for (const [o, x] of [[NOWFAINT, 'Some copies out'], [1, 'All copies out']]) {
    const s = document.createElement('span'), i = document.createElement('i'); i.style.background = cols['--fg']; i.style.opacity = o; s.append(i, x); K.append(s);
  }
  if (!now) for (const n of [1, 10, DATA.top]) {
    const s = document.createElement('span'), i = document.createElement('i'), d = 2 * radius(n); i.style.cssText = `background:${cols['--muted']};width:${d}px;height:${d}px;vertical-align:middle`; s.append(i, n === 1 ? '1 checkout' : n === DATA.top ? n + '+' : String(n)); K.append(s);
  }
  if (by === 'unavailable') { if (now) { ramp('Few copies out', 'All copies out', NOW); item(cols['--c0'], 'Number of copies unknown'); } else ramp('Never ran out', `All copies out ${pct}%+ of the con`, DEM); }
  if (by === 'weight') { ramp('1 (light)', '4+ (heavy)', SEQ); item(cols['--c0'], 'No complexity rating'); }
  if (by === 'category') for (const c of [1, 2, 3, 4, 5, 6, 7, 0]) item(cols['--c' + c], CATNAMES[c]);
}
const probe = document.body.appendChild(document.createElement('i')); probe.hidden = true;
const css = n => { probe.style.color = `var(${n})`; return getComputedStyle(probe).color; };
const map = $('map'), mx = map.getContext('2d'), line = $('line'), lx = line.getContext('2d');
let base, W, H, dpr, cols, p = -1, hold = 0, playing = false, last = 0; // p: open minutes played, -1 = whole con
let X, Y, x0, x1, y0, y1; // which pair of coordinates, and their extent
function layout() { [X, Y] = $('proj').value === 'pca' ? ['a', 'b2'] : ['x', 'y']; const xs = G.map(g => g[X]), ys = G.map(g => g[Y]); x0 = Math.min(...xs); x1 = Math.max(...xs); y0 = Math.min(...ys); y1 = Math.max(...ys); }
function place(g) { const s = Math.min((W - 32) / (x1 - x0), (H - 32) / (y1 - y0)); return [W / 2 + (g[X] - (x0 + x1) / 2) * s, H / 2 - (g[Y] - (y0 + y1) / 2) * s]; }
function size() {
  layout();
  dpr = devicePixelRatio || 1; W = $('wrap').clientWidth; H = Math.round(Math.min(W * (W < 560 ? 1.1 : .9), 640));
  map.width = W * dpr; map.height = H * dpr; map.style.height = H + 'px';
  line.width = line.clientWidth * dpr; line.height = 40 * dpr;
  cols = Object.fromEntries(['--bg', '--fg', '--muted', '--border', '--context', '--lit', '--flash', '--closed', '--d0', '--d1', '--d2', '--n0', '--w0', '--w1', '--w2', ...CATNAMES.map((_, i) => '--c' + i)].map(k => [k, css(k)]));
  buildRamps();
  base = document.createElement('canvas'); base.width = map.width; base.height = map.height;
  const b = base.getContext('2d'); b.scale(dpr, dpr); b.fillStyle = cols['--context'];
  for (const g of G) { [g.px, g.py] = place(g); b.beginPath(); b.arc(g.px, g.py, 1.6, 0, 7); b.fill(); }
  placeLabels();
  key(); draw();
}
const pos = () => Math.max(0, p);
// Region names sit in the nearest open space beside their cluster: each tries spots around the cluster's center and takes
// the one covering the fewest dots without straying far. A short thin line joins the ones that can't get close.
// PLACEHOLDER copy (Phil's to write)
function outageNote() {
  const a = 'Systems outage', b = 'Checkouts were on paper; most are not in this data';
  mx.setTransform(dpr, 0, 0, dpr, 0, 0); mx.globalAlpha = 1; mx.textAlign = 'center'; mx.textBaseline = 'middle';
  mx.font = '12px system-ui, sans-serif'; const w = Math.min(W - 24, mx.measureText(b).width + 32), x = 12 + w / 2, y = 12;
  mx.fillStyle = cols['--bg']; mx.strokeStyle = cols['--muted']; mx.lineWidth = 1; mx.beginPath(); mx.roundRect(x - w / 2, y, w, 48, 6); mx.fill(); mx.stroke();
  mx.fillStyle = cols['--muted']; mx.fillText(b, x, y + 33); mx.font = '600 14px system-ui, sans-serif'; mx.fillStyle = cols['--fg']; mx.fillText(a, x, y + 16);
}
let spots = [], placedFor = '';
const cross = (ax, ay, bx, by, cx, cy, dx, dy) => { const o = (px, py, qx, qy, rx, ry) => Math.sign((qx - px) * (ry - py) - (qy - py) * (rx - px)); return o(ax, ay, bx, by, cx, cy) !== o(ax, ay, bx, by, dx, dy) && o(cx, cy, dx, dy, ax, ay) !== o(cx, cy, dx, dy, bx, by); };
function placeLabels() {
  mx.font = LABEL_FONT; spots = []; placedFor = $('lab').value; if (!placedFor) return;
  const order = DATA[placedFor].map(l => { const [x, y] = place(l); return { t: l.t, x, y, s: l.s || '', w: Math.max(mx.measureText(l.t).width, (l.s || '').length * 5.8) + 8, h: l.s ? 36 : 20 }; });
  for (const l of order) {
    let best = null;
    for (let r = 20; r <= 220; r += 8) for (let k = 0; k < 32; k++) {
      const an = k / 32 * 2 * Math.PI, cx = l.x + Math.cos(an) * (r + l.w / 2 * Math.abs(Math.cos(an))), cy = l.y + Math.sin(an) * r;
      const b = { x0: cx - l.w / 2, x1: cx + l.w / 2, y0: cy - l.h / 2, y1: cy + l.h / 2 };
      if (b.x0 < 6 || b.x1 > W - 6 || b.y0 < 6 || b.y1 > H - 6) continue;
      if (spots.some(q => b.x0 < q.x1 + 4 && b.x1 > q.x0 - 4 && b.y0 < q.y1 + 4 && b.y1 > q.y0 - 4)) continue;
      let n = 0; for (const g of G) if (g.px > b.x0 - 3 && g.px < b.x1 + 3 && g.py > b.y0 - 3 && g.py < b.y1 + 3) n += g.iv.length ? 1 : .4;
      const cost = r + 14 * n + 400 * spots.filter(q => cross(cx, cy, l.x, l.y, q.cx, q.cy, q.x, q.y)).length; if (!best || cost < best.cost) best = { ...b, cx, cy, cost };
    }
    if (best) spots.push({ ...l, ...best });
  }
}
const LABEL_FONT = '600 13px system-ui, sans-serif';
// The two PCA dimensions, named along the edges: complexity left to right, interactivity bottom to top.
function axes() {
  mx.setTransform(dpr, 0, 0, dpr, 0, 0); mx.globalAlpha = 1; mx.font = LABEL_FONT; mx.fillStyle = cols['--muted']; mx.textAlign = 'center'; mx.textBaseline = 'middle';
  mx.fillText('Complexity →', W / 2, H - 16);
  mx.save(); mx.translate(16, H / 2); mx.rotate(-Math.PI / 2); mx.fillText('Interactivity →', 0, 0); mx.restore();
}
function labels() {
  if (placedFor !== $('lab').value) placeLabels();
  mx.setTransform(dpr, 0, 0, dpr, 0, 0); mx.globalAlpha = 1; mx.font = LABEL_FONT; mx.textBaseline = 'middle'; mx.textAlign = 'center'; mx.lineJoin = 'round';
  for (const l of spots) {
    const ax = Math.max(l.x0, Math.min(l.x1, l.x)), ay = Math.max(l.y0, Math.min(l.y1, l.y)), d = Math.hypot(l.x - ax, l.y - ay);
    if (d > 30) { const q = (d - 12) / d; mx.lineWidth = 1; mx.strokeStyle = cols['--muted']; mx.beginPath(); mx.moveTo(ax, ay); mx.lineTo(ax + (l.x - ax) * q, ay + (l.y - ay) * q); mx.stroke(); }
    const put = (t, y, font, ink) => { mx.font = font; mx.lineWidth = 4; mx.strokeStyle = cols['--bg']; mx.strokeText(t, l.cx, y); mx.fillStyle = ink; mx.fillText(t, l.cx, y); };
    put(l.t, l.cy - (l.s ? 8 : 0), LABEL_FONT, cols['--fg']); if (l.s) put(l.s, l.cy + 9, '11px system-ui, sans-serif', cols['--muted']);
  }
}
// The color a checked-out game's dot has right now; the hover tooltip uses it too.
const dotColor = (g, t, now) => now && $('by').value === 'unavailable' ? (g.k ? NOW[Math.round(15 * shareOut(g, t))] : cols['--c0']) : BY[$('by').value](g, t);
let shown = [], mouse = null; // dots in the order drawn, with their size; where the pointer is
function draw() {
  const whole = p < 0, build = $('mode').value === 'build', t = toT(pos()), un = !whole && unk(t), u = un && !build, now = !whole && !build, na = $('by').value === 'unavailable'; let ng = 0; shown = [];
  mx.setTransform(1, 0, 0, 1, 0, 0); mx.clearRect(0, 0, map.width, map.height);
  mx.globalAlpha = u ? .5 : 1; mx.drawImage(base, 0, 0); mx.globalAlpha = 1; mx.scale(dpr, dpr);
  if (!u) {
    for (const g of OUT) {
      if (whole || (build ? g.iv.some(([a]) => a <= t) : outCount(g, t))) ng++;
      const tt = whole ? 1e9 : t, n = soFar(g, tt), f = !now ? Math.min(1, n) : na ? fade(g.iv, t) : Math.max(NOWFAINT * fade(g.iv, t), fade(g.ao, t)); if (f <= 0) continue;
      mx.globalAlpha = f; mx.fillStyle = dotColor(g, tt, now);
      const r = now ? 3 : radius(n); shown.push([g, r]);
      mx.beginPath(); mx.arc(g.px, g.py, r, 0, 7); mx.fill();
      if (r > 3) { mx.lineWidth = 1; mx.strokeStyle = cols['--bg']; mx.stroke(); }
    }
    for (const g of DATA.OFF) if (whole || (build ? g.iv.some(([a]) => a <= t) : outCount(g, t))) ng++;
    mx.globalAlpha = 1;
  }
  if ($('proj').value === 'pca') axes();
  if ($('lab').value) labels();
  $('clock').textContent = whole ? 'Whole con' : clock(Math.floor(t / 10) * 10);
  $('n').textContent = un && !build ? '' : whole ? `${ng.toLocaleString()} of ${DATA.N.toLocaleString()} games checked out*` : build ? `${ng.toLocaleString()} games checked out so far*` : `${ng.toLocaleString()} games out*`;
  $('warn').textContent = un && !inOutage(t) ? 'Not observed' : '';
  if (!whole && inOutage(t)) outageNote();
  timeline(t); hover();
}
// checkouts per 15 minutes of open time, as a small profile under the scrubber

const bins = new Array(Math.ceil(total / 15)).fill(0);
for (const g of [...G, ...DATA.OFF]) for (const [a] of g.iv) { const s = SEG.find(s => a >= s.a && a < s.b); if (s) bins[Math.floor((s.p + a - s.a) / 15)]++; }
const peak = Math.max(...bins);
function timeline() {
  const w = line.clientWidth, h = 40; lx.setTransform(dpr, 0, 0, dpr, 0, 0); lx.clearRect(0, 0, w, h);
  bins.forEach((n, i) => { const x = i * 15 / total * w; lx.fillStyle = p < 0 || i * 15 <= pos() ? cols['--lit'] : cols['--closed']; lx.fillRect(x, 26 - 24 * n / peak, Math.max(1, 15 / total * w - 1), 24 * n / peak); });
  lx.fillStyle = cols['--border'];
  for (const w_ of DATA.U) {
    const [a, b] = w_, s = SEG.find(s => a >= s.a && a < s.b), xa = (s.p + a - s.a) / total * w, xb = (s.p + b - s.a) / total * w;
    lx.save(); lx.beginPath(); lx.rect(xa, 2, xb - xa, 24); lx.clip(); lx.fillStyle = cols['--bg']; lx.fill();
    lx.strokeStyle = cols['--muted']; lx.globalAlpha = .6; lx.lineWidth = 1; for (let x = xa - 24; x < xb; x += 5) { lx.beginPath(); lx.moveTo(x, 26); lx.lineTo(x + 24, 2); lx.stroke(); } lx.restore();
    if (isOutage(w_)) { lx.font = '600 11px system-ui'; const tw = lx.measureText('Outage').width + 8, cx = (xa + xb) / 2; lx.fillStyle = cols['--bg']; lx.fillRect(cx - tw / 2, 6, tw, 16); lx.fillStyle = cols['--fg']; lx.textAlign = 'center'; lx.fillText('Outage', cx, 18); lx.textAlign = 'left'; }
  }
  lx.fillStyle = cols['--muted']; lx.font = '11px system-ui';
  SEG.forEach((s, i) => { const x = s.p / total * w; if (i) { lx.fillStyle = cols['--muted']; lx.fillRect(x, 2, 1, 24); } lx.fillStyle = cols['--muted']; lx.fillText(DAYS[i], x + 3, 38); });
  if (p >= 0) { lx.fillStyle = cols['--fg']; lx.fillRect(pos() / total * w - 1, 0, 2, 28); }
  line.setAttribute('aria-valuetext', p < 0 ? 'Whole con' : clock(toT(pos())));
}
function tick(ts) {
  const dt = Math.min(100, ts - last) / 1000; last = ts;
  if (playing) {
    if (p < 0) { hold -= dt; if (hold <= 0) { p = 0; key(); } }
    else { p += dt * $('speed').value * (unk(toT(p)) ? 4 : 1); if (p >= total) { p = -1; hold = 3; key(); } } // loop: hold the whole con for 3 seconds
    draw();
  }
  requestAnimationFrame(tick);
}
const label = () => $('play').textContent = playing ? 'Pause' : 'Play';
const seek = e => { const r = line.getBoundingClientRect(); p = Math.max(0, Math.min(total - 1, (e.clientX - r.left) / r.width * total)); key(); draw(); };
$('play').onclick = () => { playing = !playing; hold = 0; label(); };
$('all').onclick = () => { playing = false; p = -1; key(); label(); draw(); };
line.onpointerdown = e => { line.setPointerCapture(e.pointerId); seek(e); };
line.onpointermove = e => { if (e.buttons) seek(e); };
line.onkeydown = e => { const d = { ArrowRight: 10, ArrowLeft: -10 }[e.key]; if (d) { p = Math.max(0, Math.min(total - 1, p + d)); key(); draw(); e.preventDefault(); } };
map.onpointermove = e => { const r = map.getBoundingClientRect(); mouse = [e.clientX - r.left, e.clientY - r.top]; hover(); };
// The game under the pointer is the dot you see there: the topmost one covering it, else the nearest edge within 4px.
// The small gray dots of games that aren't showing count only when no showing dot is that close.
function pick(x, y) {
  let best = null, bd = 4;
  for (let i = shown.length - 1; i >= 0; i--) {
    const [g, r] = shown[i], d = Math.hypot(g.px - x, g.py - y) - r;
    if (d <= 0) return g;
    if (d < bd) { bd = d; best = g; }
  }
  if (best) return best;
  for (const g of G) { const d = Math.hypot(g.px - x, g.py - y); if (d < bd) { bd = d; best = g; } }
  return best;
}
function hover() {
  if (!mouse) return;
  const [x, y] = mouse, best = pick(x, y);
  const tip = $('tip'); tip.style.display = best ? 'block' : 'none'; if (!best) return;
  const t = toT(pos()), cum = p < 0 || $('mode').value === 'build', n = p < 0 ? best.iv.length : cum ? best.iv.filter(([a]) => a <= t).length : outCount(best, t);
  tip.textContent = best.n + (best.y4 ? ` (${best.y4})` : '') + (cum ? ` · ${n} checkout${n === 1 ? '' : 's'}` + (n && best.k ? `, all copies out ${Math.round(100 * unavailable(best, p < 0 ? 1e9 : t))}% of the con` : '') : unk(t) ? '' : n ? ` · ${n}${best.k ? ` of ${best.k}` : ''} out` : ' · none out');
  const isNow = p >= 0 && $('mode').value === 'now', lit = isNow ? !unk(t) && n > 0 : n > 0;
  tip.style.color = lit ? dotColor(best, p < 0 ? 1e9 : t, isNow) : cols['--fg'];
  tip.style.borderColor = lit ? dotColor(best, p < 0 ? 1e9 : t, isNow) : cols['--border'];
  tip.style.left = Math.min(x + 12, W - 230) + 'px'; tip.style.top = y + 12 + 'px';
};
map.onpointerleave = () => { mouse = null; $('tip').style.display = 'none'; };
R.seg($('spd'), 'Speed', [['40', 'Slow'], ['80', 'Normal'], ['160', 'Fast']], '80', v => { $('speed').value = v; });
root.classList.toggle('still', still); $('by').value = color;
let setLab;
for (const i of ['proj', 'mode', 'by', 'lab']) { // the viewer's segmented control in place of each menu
  const s = $(i); if (i === 'lab') s.value = $('wrap').clientWidth < 560 ? '' : 'R'; // no room for region names on a phone
  const set = R.seg(s.parentNode, s.parentNode.firstChild.textContent.trim(), [...s.options].map(o => [o.value, o.text]), s.value, v => { s.value = v; s.onchange(); });
  if (i === 'lab') setLab = v => { s.value = v; set(v); };
}
const regions = $('lab').value; setLab(regions);
$('proj').onchange = () => { setLab($('proj').value === 'pca' ? '' : regions); size(); }; // PCA opens with just its two dimensions named
$('lab').onchange = $('by').onchange = $('mode').onchange = () => { key(); draw(); }; label(); let lastW = $('wrap').clientWidth; new ResizeObserver(() => { const w = $('wrap').clientWidth; if (w && w !== lastW) { lastW = w; size(); } }).observe($('wrap')); matchMedia('(prefers-color-scheme: dark)').addEventListener('change', size);
size(); if (!still) requestAnimationFrame(ts => { last = ts; tick(ts); });
};
