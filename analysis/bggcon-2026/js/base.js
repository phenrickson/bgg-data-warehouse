// Shared by every chart: tooltip, legend, segmented control, time axis, the games. Labels are placeholder copy.
// Each chart is drawn at the size it has, so its text never scales.

const DN = ['Wed', 'Thu', 'Fri', 'Sat', 'Sun'];
const hr = (h) => (h % 12 || 12) + (h < 12 ? ' AM' : ' PM'),
  pc = (v) => Math.round(100 * v) + '%',
  two = (v) => v.toFixed(2);
const esc = (s) => String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/"/g, '&quot;');
const svg = (w, h, o) => `<svg width="${w}" height="${h}" viewBox="0 0 ${w} ${h}">${o}</svg>`;
const make = (tag, cls, parent) => {
  const e = document.createElement(tag);
  if (cls) e.className = cls;
  parent.append(e);
  return e;
};

// ---- tooltip
const tip = document.getElementById('rtip');
const row = (a, b) => `<div><span>${esc(a)}</span><span>${esc(b)}</span></div>`;
const show = (e, title, rows) => {
  tip.innerHTML = `<b>${esc(title)}</b>` + rows.join('');
  tip.style.display = 'block';
  const r = tip.getBoundingClientRect(),
    W = document.documentElement.clientWidth;
  tip.style.left = Math.max(8, e.clientX + 14 + r.width > W - 8 ? e.clientX - r.width - 14 : e.clientX + 14) + 'px';
  tip.style.top =
    Math.max(8, e.clientY + 14 + r.height > innerHeight - 8 ? e.clientY - r.height - 14 : e.clientY + 14) + 'px';
};
const hide = () => {
  tip.style.display = 'none';
};
addEventListener('scroll', hide, { passive: true });
const n1 = (n, w) => n.toLocaleString() + ' ' + w + (n === 1 ? '' : 's');

// ---- helpers
function fit(el, f) {
  // draw now, and again whenever the width changes
  let last = 0;
  const go = () => {
    const w = Math.floor(el.clientWidth);
    if (w && w !== last) {
      last = w;
      f(w);
    }
  };
  go();
  new ResizeObserver(go).observe(el);
}
function legend(el, items) {
  const d = document.createElement('div');
  d.className = 'lg';
  d.innerHTML = items
    .map(
      ([c, t, ln]) =>
        `<span><i${ln ? ` class="ln" style="border-top:${ln} ${c}"` : ` style="background:${c}"`}></i>${esc(t)}</span>`,
    )
    .join('');
  el.before(d);
  return d;
}
function seg(parent, label, options, value, onchange) {
  // the viewer's segmented control
  const g = make('div', 'seg', parent);
  g.setAttribute('role', 'group');
  g.setAttribute('aria-label', label);
  const set = (v) =>
    [...g.children].forEach((b) => {
      const on = b.dataset.v === String(v);
      b.classList.toggle('on', on);
      b.setAttribute('aria-pressed', on);
    });
  for (const [v, t] of options) {
    const b = make('button', '', g);
    b.type = 'button';
    b.dataset.v = v;
    b.textContent = t;
    b.onclick = () => {
      set(v);
      onchange(v);
    };
  }
  set(value);
  return set;
}
const path = (r, x, y) => {
  let p = '',
    up = 1;
  r.forEach((v, i) => {
    if (v == null) {
      up = 1;
      return;
    }
    p += (up ? 'M' : 'L') + x(i).toFixed(1) + ',' + y(v).toFixed(1);
    up = 0;
  });
  return p;
};
const grid = (ticks, y, f, L, w) =>
  ticks
    .map(
      (t) =>
        `<line x1="${L}" x2="${w}" y1="${y(t)}" y2="${y(t)}" stroke="var(--border)"/><text x="${L - 8}" y="${y(t) + 4}" text-anchor="end">${f(t)}</text>`,
    )
    .join('');
const HATCH = 'repeating-linear-gradient(45deg,var(--bg) 0 3px,var(--muted) 3px 4px)';
const hatch = (id) =>
  `<defs><pattern id="${id}" width="5" height="5" patternUnits="userSpaceOnUse" patternTransform="rotate(45)"><rect width="5" height="5" fill="var(--bg)"/><rect width="1.2" height="5" fill="var(--muted)"/></pattern></defs>`;
// columns with a hover target; f(i) gives [title, rows]
function columns(el, L, n, w, h, f) {
  const s = el.querySelector('svg'),
    b = (w - L) / n;
  s.onpointermove = (e) => {
    const i = Math.floor((e.clientX - s.getBoundingClientRect().left - L) / b);
    s.querySelectorAll('.hov').forEach((x) => x.setAttribute('opacity', 0));
    if (i < 0 || i >= n) return hide();
    const c = s.querySelector(`.hov[data-i="${i}"]`);
    if (c) c.setAttribute('opacity', 1);
    show(e, ...f(i));
  };
  s.onpointerleave = () => {
    hide();
    s.querySelectorAll('.hov').forEach((x) => x.setAttribute('opacity', 0));
  };
}
const hovs = (L, n, w, T, B) =>
  Array.from(
    { length: n },
    (_, i) =>
      `<rect class="hov" data-i="${i}" opacity="0" x="${L + (i * (w - L)) / n}" width="${(w - L) / n}" y="${T}" height="${B - T}" fill="var(--hover)"/>`,
  ).join('');
const hours = (H, x, h, sm) =>
  H.map((v, i) => (i % (sm ? 4 : 2) ? '' : `<text x="${x(i)}" y="${h - 8}" text-anchor="middle">${hr(v)}</text>`)).join(
    '',
  );

// ---- time axis over library hours only
let CON; // {O open, U gaps, W observed}, minutes since Wed midnight
const ov = (a, b, x, y) => Math.max(0, Math.min(b, y) - Math.max(a, x));
const observed = (a, b) => CON.W.reduce((t, [x, y]) => t + ov(a, b, x, y), 0);
function axis(L, w, gap) {
  const open = CON.O.reduce((t, [a, b]) => t + b - a, 0),
    k = (w - L - gap * (CON.O.length - 1)) / open;
  let x = L;
  return {
    k,
    seg: CON.O.map(([a, b], i) => {
      const s = { a, b, x, w: (b - a) * k, i };
      x += s.w + gap;
      return s;
    }),
  };
}
const bars = (A, list, fill, y, h) =>
  A.seg
    .map((s) =>
      list
        .map(([a, b]) => {
          const u = Math.max(a, s.a),
            v = Math.min(b, s.b);
          return u < v
            ? `<rect x="${(s.x + (u - s.a) * A.k).toFixed(1)}" y="${y}" width="${Math.max(1, (v - u) * A.k).toFixed(1)}" height="${h}" fill="${fill}" shape-rendering="crispEdges"/>`
            : '';
        })
        .join(''),
    )
    .join('');

// ---- games, shared by every chart of games
let GAMES = [];
const byId = new Map();
const name = (g) => g.n + (g.y ? ` (${g.y})` : '');
function data(con, games) {
  CON = con;
  GAMES = games;
  games.forEach((g) => {
    byId.set(g.b, g);
    g.q = fold(name(g));
  });
}
const fold = (s) => s.normalize('NFD').replace(/[̀-ͯ]/g, '').toLowerCase();
const gameRows = (g) => [
  row('Copies', g.k ?? 'unknown'),
  row('Checkouts', g.c),
  ...(g.p == null ? [] : [row('Every copy out', g.p.toFixed(0) + '% of the con')]),
];
