// Charts for report.qmd. Every chart is drawn at the size it has, so its text never scales.
// Each takes (plot element, data). Labels here are placeholder copy.
const R = (() => {
  const DN = ['Wed', 'Thu', 'Fri', 'Sat', 'Sun'];
  const hr = h => (h % 12 || 12) + (h < 12 ? ' AM' : ' PM'), pc = v => Math.round(100 * v) + '%', two = v => v.toFixed(2);
  const esc = s => String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/"/g, '&quot;');
  const svg = (w, h, o) => `<svg width="${w}" height="${h}" viewBox="0 0 ${w} ${h}">${o}</svg>`;
  const make = (tag, cls, parent) => { const e = document.createElement(tag); if (cls) e.className = cls; parent.append(e); return e; };

  // ---- tooltip
  const tip = document.getElementById('rtip');
  const row = (a, b) => `<div><span>${esc(a)}</span><span>${esc(b)}</span></div>`;
  const show = (e, title, rows) => {
    tip.innerHTML = `<b>${esc(title)}</b>` + rows.join(''); tip.style.display = 'block';
    const r = tip.getBoundingClientRect(), W = document.documentElement.clientWidth;
    tip.style.left = Math.max(8, e.clientX + 14 + r.width > W - 8 ? e.clientX - r.width - 14 : e.clientX + 14) + 'px';
    tip.style.top = Math.max(8, e.clientY + 14 + r.height > innerHeight - 8 ? e.clientY - r.height - 14 : e.clientY + 14) + 'px';
  };
  const hide = () => { tip.style.display = 'none'; };
  addEventListener('scroll', hide, { passive: true });
  const n1 = (n, w) => n.toLocaleString() + ' ' + w + (n === 1 ? '' : 's');

  // ---- helpers
  function fit(el, f) { // draw now, and again whenever the width changes
    let last = 0; const go = () => { const w = Math.floor(el.clientWidth); if (w && w !== last) { last = w; f(w); } };
    go(); new ResizeObserver(go).observe(el);
  }
  function legend(el, items) {
    const d = document.createElement('div'); d.className = 'lg';
    d.innerHTML = items.map(([c, t, ln]) => `<span><i${ln ? ` class="ln" style="border-top:${ln} ${c}"` : ` style="background:${c}"`}></i>${esc(t)}</span>`).join('');
    el.before(d); return d;
  }
  function seg(parent, label, options, value, onchange) { // the viewer's segmented control
    const g = make('div', 'seg', parent); g.setAttribute('role', 'group'); g.setAttribute('aria-label', label);
    const set = v => [...g.children].forEach(b => { const on = b.dataset.v === String(v); b.classList.toggle('on', on); b.setAttribute('aria-pressed', on); });
    for (const [v, t] of options) { const b = make('button', '', g); b.type = 'button'; b.dataset.v = v; b.textContent = t; b.onclick = () => { set(v); onchange(v); }; }
    set(value); return set;
  }
  const path = (r, x, y) => { let p = '', up = 1; r.forEach((v, i) => { if (v == null) { up = 1; return; } p += (up ? 'M' : 'L') + x(i).toFixed(1) + ',' + y(v).toFixed(1); up = 0; }); return p; };
  const grid = (ticks, y, f, L, w) => ticks.map(t => `<line x1="${L}" x2="${w}" y1="${y(t)}" y2="${y(t)}" stroke="var(--border)"/><text x="${L - 8}" y="${y(t) + 4}" text-anchor="end">${f(t)}</text>`).join('');
  const HATCH = 'repeating-linear-gradient(45deg,var(--bg) 0 3px,var(--muted) 3px 4px)';
  const hatch = id => `<defs><pattern id="${id}" width="5" height="5" patternUnits="userSpaceOnUse" patternTransform="rotate(45)"><rect width="5" height="5" fill="var(--bg)"/><rect width="1.2" height="5" fill="var(--muted)"/></pattern></defs>`;
  // columns with a hover target; f(i) gives [title, rows]
  function columns(el, L, n, w, h, f) {
    const s = el.querySelector('svg'), b = (w - L) / n;
    s.onpointermove = e => { const i = Math.floor((e.clientX - s.getBoundingClientRect().left - L) / b); s.querySelectorAll('.hov').forEach(x => x.setAttribute('opacity', 0)); if (i < 0 || i >= n) return hide(); const c = s.querySelector(`.hov[data-i="${i}"]`); if (c) c.setAttribute('opacity', 1); show(e, ...f(i)); };
    s.onpointerleave = () => { hide(); s.querySelectorAll('.hov').forEach(x => x.setAttribute('opacity', 0)); };
  }
  const hovs = (L, n, w, T, B) => Array.from({ length: n }, (_, i) => `<rect class="hov" data-i="${i}" opacity="0" x="${L + i * (w - L) / n}" width="${(w - L) / n}" y="${T}" height="${B - T}" fill="var(--hover)"/>`).join('');
  const hours = (H, x, h, sm) => H.map((v, i) => i % (sm ? 4 : 2) ? '' : `<text x="${x(i)}" y="${h - 8}" text-anchor="middle">${hr(v)}</text>`).join('');

  // ---- time axis over library hours only
  let CON; // {O open, U gaps, W observed}, minutes since Wed midnight
  const ov = (a, b, x, y) => Math.max(0, Math.min(b, y) - Math.max(a, x));
  const observed = (a, b) => CON.W.reduce((t, [x, y]) => t + ov(a, b, x, y), 0);
  function axis(L, w, gap) {
    const open = CON.O.reduce((t, [a, b]) => t + b - a, 0), k = (w - L - gap * (CON.O.length - 1)) / open; let x = L;
    return { k, seg: CON.O.map(([a, b], i) => { const s = { a, b, x, w: (b - a) * k, i }; x += s.w + gap; return s; }) };
  }
  const bars = (A, list, fill, y, h) => A.seg.map(s => list.map(([a, b]) => { const u = Math.max(a, s.a), v = Math.min(b, s.b); return u < v ? `<rect x="${(s.x + (u - s.a) * A.k).toFixed(1)}" y="${y}" width="${Math.max(1, (v - u) * A.k).toFixed(1)}" height="${h}" fill="${fill}" shape-rendering="crispEdges"/>` : ''; }).join('')).join('');

  // ---- games, shared by every chart of games
  let GAMES = [];
  const byId = new Map();
  const name = g => g.n + (g.y ? ` (${g.y})` : '');
  function data(con, games) { CON = con; GAMES = games; games.forEach(g => { byId.set(g.b, g); g.q = fold(name(g)); }); }
  const fold = s => s.normalize('NFD').replace(/[̀-ͯ]/g, '').toLowerCase();
  const gameRows = g => [row('Copies', g.k ?? 'unknown'), row('Checkouts', g.c), ...(g.p == null ? [] : [row('Every copy out', g.p.toFixed(0) + '% of the con')])];

  // Checkouts in each 15 minutes of library time.
  function timeline(el, bins) {
    legend(el, [['var(--lit)', 'Checkouts per 15 minutes'], [HATCH, 'Not observed']]);
    fit(el, w => {
      const sm = w < 520, h = sm ? 170 : 210, L = 32, T = 8, B = h - 26, A = axis(L, w, 6), max = Math.max(...bins.map(b => b[1])), top = Math.ceil(max / 20) * 20, y = v => B - (B - T) * v / top;
      let o = hatch('ht-tl') + grid([0, top / 2, top], y, v => v, L, w);
      for (const s of A.seg) o += `<text x="${s.x}" y="${h - 6}">${DN[s.i]}</text>`;
      for (const [t, n] of bins) { const s = A.seg.find(s => t >= s.a && t < s.b); if (s && n) o += `<rect x="${s.x + (t - s.a) * A.k}" width="${15 * A.k < 3 ? 15 * A.k + .2 : 15 * A.k - .7}" y="${y(n)}" height="${B - y(n)}" fill="var(--lit)"/>`; }
      o += bars(A, CON.U, 'url(#ht-tl)', T, B - T);
      el.innerHTML = svg(w, h, o); const sv = el.firstChild;
      sv.onpointermove = e => { const x = e.clientX - sv.getBoundingClientRect().left, s = A.seg.find(s => x >= s.x && x < s.x + s.w); if (!s) return hide(); const t = s.a + (x - s.x) / A.k, q = Math.floor(t / 15) * 15, b = bins.find(b => b[0] === q), m = q % 1440, un = observed(q, q + 15) < 7.5; show(e, `${DN[s.i]} ${hr(Math.floor(m / 60)).replace(' ', `:${String(m % 60).padStart(2, '0')} `)}`, [un ? row('Not observed', '') : row('Checkouts', b ? b[1] : 0)]); };
      sv.onpointerleave = hide;
    });
  }

  // Games as rows. views: [[label, [game id]]]; scrolls, drawing only the rows in sight.
  function games(el, { views, search = false, sort = false, height = 448 }) {
    const bar = document.createElement('div'); bar.className = 'bar'; el.before(bar);
    let view = 0, by = 'p', q = '', list = [];
    if (views.length > 1) seg(bar, 'Games', views.map((v, i) => [i, v[0]]), 0, v => { view = v; apply(); });
    if (sort) seg(bar, 'Sort', [['p', 'Time all out'], ['c', 'Checkouts']], 'p', v => { by = v; apply(); });
    const count = make('span', 'count', bar);
    if (search) { const i = make('input', '', bar); i.type = 'search'; i.placeholder = 'Find a game'; i.setAttribute('aria-label', 'Find a game'); i.oninput = () => { q = fold(i.value.trim()); apply(); }; }
    legend(el, [['var(--lit)', 'Every copy out'], ['var(--never)', 'Available'], [HATCH, 'Not observed']]);
    const head = make('div', 'rhead', el), box = make('div', 'rbox', el), inner = make('div', 'rin', box), layer = make('div', 'rlay', inner);
    const id = 'ht-' + (games.n = (games.n || 0) + 1);
    let w = 0, RH = 28, sm = false, A, L, from = -1;
    function apply() {
      list = views[view][1].map(b => byId.get(b)).filter(g => g && (!q || g.q.includes(q)));
      if (sort) list.sort((a, b) => by === 'c' ? b.c - a.c || (b.p ?? -1) - (a.p ?? -1) : (b.p ?? -1) - (a.p ?? -1) || b.c - a.c);
      count.textContent = n1(list.length, 'game'); box.scrollTop = 0; size();
    }
    function size() { inner.style.height = list.length * RH + 'px'; box.style.height = Math.max(RH, Math.min(height, list.length * RH)) + 'px'; from = -1; paint(); }
    function layout(width) {
      w = width - 14; sm = w < 520; RH = sm ? 46 : 28; L = sm ? 0 : Math.min(230, Math.round(w * .3)); const V = 44; A = axis(L, w - V, sm ? 3 : 5);
      head.innerHTML = svg(w, 20, A.seg.map(s => `<text x="${s.x}" y="14">${DN[s.i]}</text>`).join('') + `<text x="${w}" y="14" text-anchor="end">${'%'}</text>`); size();
    }
    function paint() {
      if (!w) return; const a = Math.max(0, Math.floor(box.scrollTop / RH) - 4); if (a === from) return; from = a;
      const part = list.slice(a, a + Math.ceil(height / RH) + 10); layer.style.transform = `translateY(${a * RH}px)`;
      if (!list.length) { layer.innerHTML = '<p class="none">No games match.</p>'; return; }
      let o = hatch(id), names = '';
      part.forEach((g, i) => {
        const y0 = i * RH, y = y0 + (sm ? 24 : 5), H = 18;
        names += `<div class="nm" style="top:${y0}px;width:${sm ? w : L - 12}px;${sm ? 'height:24px;line-height:24px' : `height:${RH}px;line-height:${RH}px`}">${esc(g.n)}</div>`;
        if (g.k) o += A.seg.map(s => `<rect x="${s.x}" y="${y}" width="${s.w}" height="${H}" fill="var(--never)"/>`).join('') + bars(A, g.ao, 'var(--lit)', y, H) + bars(A, CON.U.filter(([a, b]) => b - a > 5), `url(#${id})`, y, H);
        else o += `<text x="${L}" y="${y + 13}">Number of copies unknown</text>`;
        o += `<text class="v" x="${w}" y="${y + 13}" text-anchor="end">${by === 'c' ? g.c : g.p == null ? '' : g.p.toFixed(0) + '%'}</text><rect data-i="${a + i}" x="0" y="${y0}" width="${w}" height="${RH}" fill="transparent"/>`;
      });
      layer.innerHTML = svg(w, part.length * RH, o) + names;
      head.querySelector('text:last-child').textContent = by === 'c' ? 'Count' : 'All out';
    }
    box.onscroll = () => { paint(); hide(); };
    layer.onpointermove = e => { const g = list[e.target.dataset && e.target.dataset.i]; if (g) show(e, name(g), gameRows(g)); else hide(); };
    layer.onpointerleave = hide;
    fit(el, layout); apply();
  }

  // One game, day by day, in 15-minute cells; search to pick it.
  function lookup(el, first) {
    const bar = document.createElement('div'); bar.className = 'bar'; el.before(bar);
    const wrap = make('div', 'pick', bar), input = make('input', '', wrap), menu = make('div', 'menu', wrap); input.type = 'search'; input.setAttribute('aria-label', 'Find a game'); input.setAttribute('role', 'combobox'); menu.hidden = true;
    const info = make('span', 'count', bar); legend(el, [['var(--lit)', 'Every copy out'], ['var(--some)', 'Some copies out'], ['var(--never)', 'None out'], [HATCH, 'Not observed']]);
    const known = GAMES.filter(g => g.k); let g = byId.get(first) || known[0], found = [], hot = 0, w = 0;
    const choose = x => { g = x; input.value = name(g); menu.hidden = true; draw(); };
    const list = () => { const q = fold(input.value.trim()); found = known.filter(x => x.q.includes(q)).slice(0, 12); hot = 0; menu.hidden = !found.length; paint(); };
    const paint = () => menu.innerHTML = found.map((x, i) => `<div data-i="${i}" class="${i === hot ? 'on' : ''}">${esc(name(x))}<span>${n1(x.c, 'checkout')}</span></div>`).join('');
    input.onfocus = () => { input.select(); found = []; menu.hidden = true; }; input.oninput = list;
    input.onkeydown = e => { if (menu.hidden) return; if (e.key === 'ArrowDown' || e.key === 'ArrowUp') { hot = (hot + (e.key === 'ArrowDown' ? 1 : found.length - 1)) % found.length; paint(); e.preventDefault(); } else if (e.key === 'Enter') { choose(found[hot]); e.preventDefault(); } else if (e.key === 'Escape') menu.hidden = true; };
    menu.onpointerdown = e => { const d = e.target.closest('[data-i]'); if (d) { choose(found[d.dataset.i]); e.preventDefault(); } };
    input.onblur = () => { menu.hidden = true; input.value = name(g); };
    function cell(d, q) {
      const a = d * 1440 + 480 + 15 * q, b = a + 15; if (!CON.O.some(([x, y]) => x <= a && a < y)) return null;
      const m = observed(a, b); if (m < 7.5) return { t: 'u' };
      const time = L => L.reduce((s, [x, y]) => s + (x < b && y > a ? observed(Math.max(a, x), Math.min(b, y)) : 0), 0);
      const avg = time(g.iv) / m; return { t: time(g.ao) >= m / 2 ? 'o' : avg >= .5 ? 's' : 'a', n: Math.min(g.k, Math.round(avg)) };
    }
    function draw() {
      if (!w) return; info.textContent = `${n1(g.k, 'copy').replace('copys', 'copies')} · ${n1(g.c, 'checkout')} · every copy out ${g.p.toFixed(0)}% of the con`;
      const sm = w < 520, L = 40, T = 22, RH = sm ? 30 : 34, cw = (w - L) / 64, C = [];
      let o = hatch('ht-lk'); for (let h = 8; h < 24; h += sm ? 4 : 2) o += `<text x="${L + (h - 8) * 4 * cw}" y="14">${hr(h)}</text>`;
      for (let d = 0; d < 5; d++) { o += `<text class="nm" x="0" y="${T + d * RH + RH / 2 + 4}">${DN[d]}</text>`; C.push(Array.from({ length: 64 }, (_, q) => cell(d, q))); C[d].forEach((c, q) => { if (c) o += `<rect x="${(L + q * cw).toFixed(2)}" y="${T + d * RH + 1}" width="${Math.max(1, cw - (sm ? .5 : 1.2)).toFixed(2)}" height="${RH - 4}" rx="${sm ? 0 : 1.5}" fill="${{ u: 'url(#ht-lk)', o: 'var(--lit)', s: 'var(--some)', a: 'var(--never)' }[c.t]}"/>`; }); }
      el.innerHTML = svg(w, T + 5 * RH, o); const s = el.firstChild;
      s.onpointermove = e => { const r = s.getBoundingClientRect(), q = Math.floor((e.clientX - r.left - L) / cw), d = Math.floor((e.clientY - r.top - T) / RH), c = C[d] && C[d][q]; if (!c) return hide(); const m = 480 + 15 * q; show(e, `${DN[d]} ${Math.floor(m / 60) % 12 || 12}:${String(m % 60).padStart(2, '0')} ${m < 720 ? 'AM' : 'PM'}`, [c.t === 'u' ? row('Not observed', '') : row('Copies out', `${c.n} of ${g.k}`)]); };
      s.onpointerleave = hide;
    }
    input.value = name(g); fit(el, x => { w = x; draw(); });
  }

  // Top games as bars, one panel per group, all on the same scale. by: 'c' checkouts, 'p' share of the con with every copy out
  function facets(el, { by, panels, height = 440 }) {
    const val = g => by === 'c' ? g.c : g.p, lab = g => by === 'c' ? g.c : g.p.toFixed(0) + '%', fill = by === 'c' ? 'var(--lit)' : 'var(--hot)', mark = g => by === 'p' && g.hot ? ' hotname' : ''; // in demand: names of Hot Games room games are bold
    if (by === 'p') legend(el, []).innerHTML = '<span><b class="hotname">Bold</b> = in the Hot Games room</span>';
    const all = panels.map(([t, ids]) => [t, ids.map(b => byId.get(b))]), max = Math.max(...all.flatMap(([, g]) => g.map(val))), one = all.length === 1, RH = 24;
    const longest = () => { const c = facets.ctx = facets.ctx || document.createElement('canvas').getContext('2d'); c.font = '13px ' + getComputedStyle(el).fontFamily; return Math.max(...all.flatMap(([, l]) => l.map(g => c.measureText(g.n).width))); };
    el.classList.add('facets'); if (one) el.classList.add('one');
    for (const [t, list] of all) {
      const c = make('div', 'facet', el); if (t) make('h4', '', c).textContent = t;
      const box = make('div', one ? 'rbox' : '', c), p = make('div', 'fp', box); if (one) box.style.maxHeight = height + 'px';
      fit(p, w => { // names get the room the longest one in the figure needs, up to about half a panel (270px in a full-width chart); longer ones are cut off, so rows line up across panels (hover has the full name)
        const L = Math.ceil(Math.min(longest() + 14, w < 450 ? w * .54 : 270)), V = 38, k = (w - L - V) / max;
        p.innerHTML = svg(w, list.length * RH, list.map((g, i) => `<rect x="${L}" y="${i * RH + 4}" width="${Math.max(2, val(g) * k).toFixed(1)}" height="16" rx="2" fill="${fill}"/><text class="v" x="${(L + val(g) * k + 6).toFixed(1)}" y="${i * RH + 16}">${lab(g)}</text><rect data-i="${i}" y="${i * RH}" width="${w}" height="${RH}" fill="transparent"/>`).join('')) + list.map((g, i) => `<div class="nm${mark(g)}" style="top:${i * RH}px;width:${L - 12}px">${esc(g.n)}</div>`).join('');
      });
      p.onpointermove = e => { const g = list[e.target.dataset && e.target.dataset.i]; if (g) show(e, name(g), gameRows(g)); else hide(); }; p.onpointerleave = hide; box.onscroll = hide;
    }
  }

  // Bars: [[label, value, tooltip label]]
  function barChart(el, { items, what, muted = 0 }) {
    fit(el, w => {
      const sm = w < 520, h = sm ? 220 : 260, L = 0, T = 22, B = h - 26, max = Math.max(...items.map(d => d[1])), b = (w - L) / items.length, y = v => B - (B - T) * v / max;
      let o = hovs(L, items.length, w, 0, B);
      items.forEach((d, i) => { o += `<rect x="${L + i * b + b * .1}" width="${b * .8}" y="${y(d[1])}" height="${Math.max(1, B - y(d[1]))}" rx="3" fill="${i < muted ? 'var(--never)' : 'var(--lit)'}"/><text class="v" x="${L + (i + .5) * b}" y="${y(d[1]) - 6}" text-anchor="middle">${d[1].toLocaleString()}</text><text x="${L + (i + .5) * b}" y="${h - 8}" text-anchor="middle">${d[0]}</text>`; });
      el.innerHTML = svg(w, h, o + `<line x1="0" x2="${w}" y1="${B}" y2="${B}" stroke="var(--border)"/>`); columns(el, L, items.length, w, h, i => [items[i][2], [row(what, items[i][1].toLocaleString())]]);
    });
  }

  // Every library game as a dot, in columns filled from the bottom, most checked out first.
  // Sized like the map: area is checkouts to the power 0.7.
  // games: [name, value, checkouts, column]; ticks: [[position in columns, label, show when narrow]]
  function dots(el, { games: G, names, ticks, what }) {
    const top = Math.max(...GAMES.map(g => g.c)), base = n => n ? 6.4 * Math.sqrt((n / top) ** .7) : 1.2, lg = legend(el, []);
    lg.innerHTML = [1, 5, 25, top].map(n => `<span><i class="dot" style="background:var(--lit);width:${2 * base(n)}px;height:${2 * base(n)}px"></i>${n === 1 ? '1 checkout' : n}</span>`).join('') + '<span><i class="dot" style="background:var(--never);width:4px;height:4px"></i>Never checked out</span>';
    const C = make('canvas', '', el), cols = Array.from({ length: names.length }, () => []); G.forEach(g => cols[g[3]].push(g)); cols.forEach(c => c.sort((a, b) => b[2] - a[2]));
    let P = [], w = 0, H = 0, f = 1;
    const draw = () => {
      const r = devicePixelRatio || 1, css = getComputedStyle(document.documentElement), v = n => css.getPropertyValue(n).trim(); C.width = w * r; C.height = H * r; C.style.width = w + 'px'; C.style.height = H + 'px';
      const x = C.getContext('2d'); x.scale(r, r); for (const p of P) { x.fillStyle = v(p.g[2] ? '--lit' : '--never'); x.beginPath(); x.arc(p.x, H - 34 - p.y, p.r, 0, 7); x.fill(); }
      x.strokeStyle = v('--border'); x.beginPath(); x.moveTo(0, H - 31.5); x.lineTo(w, H - 31.5); x.stroke(); x.fillStyle = v('--muted'); x.font = getComputedStyle(el).fontFamily && '13px ' + getComputedStyle(el).fontFamily; x.textAlign = 'center';
      for (const [at, t, keep] of ticks) if (w >= 620 || keep) x.fillText(t, Math.max(x.measureText(t).width / 2, Math.min(w - x.measureText(t).width / 2, at * w / names.length)), H - 12);
    };
    fit(el, width => {
      w = width; f = Math.max(.62, Math.min(1, w / 900)); P = []; const cw = w / names.length; let top = 0;
      cols.forEach((c, i) => { let y = 0, k = 0; while (k < c.length) { const d = n => 2 * base(n) * f + .6, h = d(c[k][2]); let wd = 0, line = []; while (k < c.length) { const q = d(c[k][2]); if (line.length && wd + q > cw - (w < 520 ? 3 : 7)) break; line.push([c[k], q]); wd += q; k++; } let px = i * cw + (cw - wd) / 2; for (const [g, q] of line) { P.push({ g, x: px + q / 2, y: y + h / 2, r: base(g[2]) * f }); px += q; } y += h; } top = Math.max(top, y); });
      H = Math.ceil(top) + 38; draw();
    });
    matchMedia('(prefers-color-scheme: dark)').addEventListener('change', draw);
    C.onpointermove = e => { const b = C.getBoundingClientRect(), mx = e.clientX - b.left, my = H - 34 - (e.clientY - b.top); let best = null, bd = 3; for (const p of P) { const d = Math.hypot(p.x - mx, p.y - my) - p.r; if (d < bd) { bd = d; best = p; } } if (!best) return hide(); const g = best.g; show(e, g[0], [row(what, g[1]), row('Checkouts', g[2])]); };
    C.onpointerleave = hide;
  }

  // Share of games checked out, one bar per column of dots. rate: [games, checked out]
  function share(el, { rate, names, ticks, what, max }) {
    const all = rate.reduce((t, r) => [t[0] + r[0], t[1] + r[1]], [0, 0]), avg = all[1] / all[0];
    legend(el, [['var(--lit)', 'Checked out at least once'], ['var(--fg)', `All games (${pc(avg)})`, '2px dashed']]);
    fit(el, w => {
      const sm = w < 520, h = sm ? 210 : 250, L = 40, T = 8, B = h - 28, n = names.length, b = (w - L) / n, y = v => B - (B - T) * v / max, tk = []; for (let t = 0; t <= max + 1e-9; t += .2) tk.push(t);
      let o = hovs(L, n, w, T, B) + grid(tk, y, pc, L, w); rate.forEach(([c, k], i) => o += `<rect x="${L + i * b + b * .07}" width="${b * .86}" y="${y(k / c)}" height="${B - y(k / c)}" rx="2" fill="var(--lit)"/>`);
      o += `<line x1="${L}" x2="${w}" y1="${y(avg)}" y2="${y(avg)}" stroke="var(--fg)" stroke-width="2" stroke-dasharray="5 4"/>`;
      for (const [at, t, keep] of ticks) if (w >= 620 || keep) o += `<text x="${Math.min(w - t.length * 3.6, L + at * b)}" y="${h - 8}" text-anchor="middle">${t}</text>`;
      el.innerHTML = svg(w, h, o); columns(el, L, n, w, h, i => [`${what} ${names[i]}`, [row('Library games', rate[i][0].toLocaleString()), row('Checked out', `${rate[i][1].toLocaleString()} (${pc(rate[i][1] / rate[i][0])})`)]]);
    });
  }

  // Value by clock hour: all days, each day, and the range chance alone would give.
  function hourly(el, { S, ticks, percent }) {
    const f = percent ? pc : two, ax = percent ? pc : v => v.toFixed(1);
    legend(el, [['var(--lit)', 'All days', '3px solid'], ['var(--day)', 'Each day', '1px solid'], ['var(--bandc)', 'Range from chance alone']]);
    fit(el, w => {
      const sm = w < 520, h = sm ? 260 : 320, L = 40, T = 8, B = h - 30, n = S.H.length, s = (w - L) / n, x = i => L + (i + .5) * s, lo = ticks[0], hi = ticks[ticks.length - 1], y = v => T + (B - T) * (hi - v) / (hi - lo);
      let o = `<path d="${path(S.band[0], x, y)}${S.band[1].map((v, i) => [v, i]).reverse().map(([v, i]) => 'L' + x(i) + ',' + y(v)).join('')}Z" fill="var(--bandc)"/>` + hovs(L, n, w, T, B) + grid(ticks, y, ax, L, w) + hours(S.H, x, h, sm);
      S.days.forEach(r => o += `<path d="${path(r, x, y)}" fill="none" stroke="var(--day)"/>`);
      o += `<path d="${path(S.m, x, y)}" fill="none" stroke="var(--lit)" stroke-width="2.5" stroke-linejoin="round"/>` + S.m.map((v, i) => `<circle cx="${x(i)}" cy="${y(v)}" r="4.5" fill="var(--lit)" stroke="var(--bg)" stroke-width="2"/>`).join('');
      el.innerHTML = svg(w, h, o); columns(el, L, n, w, h, i => [hr(S.H[i]), [row('Checkouts', S.n[i].toLocaleString()), row('All days', f(S.m[i])), ...S.days.map((d, k) => d[i] == null ? '' : row(DN[k], f(d[i]))), row('Chance', f(S.band[0][i]) + ' to ' + f(S.band[1][i]))]]);
    });
  }

  // Checkouts per clock hour, one line per day. days: Wed-Sun, null = closed or not recorded
  function eachDay(el, { H, days }) {
    const col = i => `var(--day-${i + 1})`; legend(el, days.map((_, i) => [col(i), DN[i], '3px solid']));
    fit(el, w => {
      const sm = w < 520, h = sm ? 260 : 320, L = 36, T = 8, B = h - 30, n = H.length, s = (w - L) / n, x = i => L + (i + .5) * s, top = Math.ceil(Math.max(...days.flat()) / 100) * 100, y = v => B - (B - T) * v / top, tk = []; for (let t = 0; t <= top; t += 100) tk.push(t);
      let o = hovs(L, n, w, T, B) + grid(tk, y, v => v, L, w) + hours(H, x, h, sm);
      days.forEach((r, k) => { o += `<path d="${path(r, x, y)}" fill="none" stroke="${col(k)}" stroke-width="2.25" stroke-linejoin="round" stroke-linecap="round"/>`; r.forEach((v, i) => { if (v != null) o += `<circle cx="${x(i)}" cy="${y(v)}" r="${sm ? 2 : 3}" fill="${col(k)}"/>`; }); });
      el.innerHTML = svg(w, h, o); columns(el, L, n, w, h, i => [hr(H[i]), days.map((r, k) => `<div><span><i style="display:inline-block;width:9px;height:9px;border-radius:50%;background:${col(k)};margin-right:6px"></i>${DN[k]}</span><span>${r[i] == null ? (k === 0 || k === 4 ? 'closed' : 'not recorded') : r[i]}</span></div>`)]);
    });
  }

  // Stacked shares by hour. mix: one array of shares per hour, in band order
  function stack(el, { H, n, mix, names }) {
    const c = ['var(--w0)', 'color-mix(in oklab,var(--w0) 50%,var(--w1))', 'var(--w1)', 'color-mix(in oklab,var(--w2) 50%,var(--w1))', 'var(--w2)'];
    legend(el, names.map((t, i) => [c[i], t]));
    fit(el, w => {
      const sm = w < 520, h = sm ? 260 : 320, L = 44, T = 8, B = h - 30, s = (w - L) / H.length, x = i => L + (i + .5) * s, y = v => B - (B - T) * v;
      let o = hovs(L, H.length, w, T, B) + grid([0, .25, .5, .75, 1], y, pc, L, w) + hours(H, x, h, sm);
      mix.forEach((r, i) => { let a = 0; r.forEach((v, j) => { o += `<rect x="${L + i * s + s * .05}" width="${s * .9}" y="${y(a + v) + 1}" height="${Math.max(0, y(a) - y(a + v) - 2)}" fill="${c[j]}"/>`; a += v; }); });
      el.innerHTML = svg(w, h, o); columns(el, L, H.length, w, h, i => [hr(H[i]), [row('Checkouts', n[i].toLocaleString()), ...mix[i].map((v, j) => row(names[j], pc(v))).reverse()]]);
    });
  }

  // Pairs of days: what was seen against the range chance gives.
  function pairs(el, { items }) {
    legend(el, [['var(--lit)', 'Seen'], ['var(--bandc)', 'Range from chance alone']]);
    fit(el, w => {
      const RH = 38, T = 4, L = w < 520 ? 98 : 110, n = items.length, lo = .18, hi = .30, x = v => L + (w - L - 20) * (v - lo) / (hi - lo), h = T + n * RH + 26;
      let o = [.18, .20, .22, .24, .26, .28, .30].filter((_, i) => w >= 520 || i % 2 === 0).map(t => `<line x1="${x(t)}" x2="${x(t)}" y1="${T}" y2="${T + n * RH}" stroke="var(--border)"/><text x="${x(t)}" y="${h - 6}" text-anchor="middle">${pc(t)}</text>`).join('');
      items.forEach((d, i) => { const y = T + (i + .5) * RH; o += `<text class="nm" x="0" y="${y + 5}">${d.a} and ${d.b}</text><rect x="${x(d.lo)}" width="${x(d.hi) - x(d.lo)}" y="${y - 8}" height="16" rx="3" fill="var(--bandc)"/><circle cx="${x(d.v)}" cy="${y}" r="6" fill="var(--lit)"/><rect data-i="${i}" x="0" y="${y - RH / 2}" width="${w}" height="${RH}" fill="transparent"/>`; });
      el.innerHTML = svg(w, h, o); const s = el.firstChild;
      s.onpointermove = e => { const d = items[e.target.dataset.i]; if (d) show(e, `${d.a} and ${d.b}`, [row('Games on both days', (100 * d.v).toFixed(1) + '%'), row('Chance', `${(100 * d.lo).toFixed(1)}% to ${(100 * d.hi).toFixed(1)}%`), row('p', d.p.toFixed(3))]); else hide(); };
      s.onpointerleave = hide;
    });
  }

  // Small charts side by side, one per kind of game, a bar per day.
  function days(el, { items }) {
    el.classList.add('small');
    for (const d of items) {
      const num = d.name === 'Average complexity', f = v => num ? two(v) : (100 * v).toFixed(v < .1 ? 1 : 0) + '%', c = make('div', '', el); c.innerHTML = `<h4>${esc(d.name)}</h4>`; const p = make('div', '', c);
      fit(p, w => {
        const h = 130, T = 20, B = h - 22, b = w / 4, max = num ? 3 : Math.max(...d.v), y = v => B - (B - T) * v / max;
        p.innerHTML = svg(w, h, d.v.map((v, i) => `<rect x="${i * b + b * .12}" width="${b * .76}" y="${y(v)}" height="${B - y(v)}" rx="3" fill="var(--lit)"/><text class="v" x="${(i + .5) * b}" y="${y(v) - 6}" text-anchor="middle">${f(v)}</text><text x="${(i + .5) * b}" y="${h - 6}" text-anchor="middle">${DN[i]}</text>`).join(''));
      });
    }
  }
  return { data, seg, fit, show, hide, row, timeline, games, lookup, barChart, dots, share, hourly, facets, eachDay, stack, pairs, days };
})();
