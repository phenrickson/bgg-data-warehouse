// Games as rows. views: [[label, [game id]]]; scrolls, drawing only the rows in sight.
function games(el, { views, search = false, sort = false, height = 448 }) {
  const bar = document.createElement('div');
  bar.className = 'bar';
  el.before(bar);
  let view = 0,
    by = 'p',
    q = '',
    list = [];
  if (views.length > 1)
    seg(
      bar,
      'Games',
      views.map((v, i) => [i, v[0]]),
      0,
      (v) => {
        view = v;
        apply();
      },
    );
  if (sort)
    seg(
      bar,
      'Sort',
      [
        ['p', 'Time all out'],
        ['c', 'Checkouts'],
      ],
      'p',
      (v) => {
        by = v;
        apply();
      },
    );
  const count = make('span', 'count', bar);
  if (search) {
    const i = make('input', '', bar);
    i.type = 'search';
    i.placeholder = 'Find a game';
    i.setAttribute('aria-label', 'Find a game');
    i.oninput = () => {
      q = fold(i.value.trim());
      apply();
    };
  }
  legend(el, [
    ['var(--lit)', 'Every copy out'],
    ['var(--never)', 'Available'],
    [HATCH, 'Not observed'],
  ]);
  const head = make('div', 'rhead', el),
    box = make('div', 'rbox', el),
    inner = make('div', 'rin', box),
    layer = make('div', 'rlay', inner);
  const id = 'ht-' + (games.n = (games.n || 0) + 1);
  let w = 0,
    RH = 28,
    sm = false,
    A,
    L,
    from = -1;
  function apply() {
    list = views[view][1].map((b) => byId.get(b)).filter((g) => g && (!q || g.q.includes(q)));
    if (sort)
      list.sort((a, b) =>
        by === 'c' ? b.c - a.c || (b.p ?? -1) - (a.p ?? -1) : (b.p ?? -1) - (a.p ?? -1) || b.c - a.c,
      );
    count.textContent = n1(list.length, 'game');
    box.scrollTop = 0;
    size();
  }
  function size() {
    inner.style.height = list.length * RH + 'px';
    box.style.height = Math.max(RH, Math.min(height, list.length * RH)) + 'px';
    from = -1;
    paint();
  }
  function layout(width) {
    w = width - 14;
    sm = w < 520;
    RH = sm ? 46 : 28;
    L = sm ? 0 : Math.min(230, Math.round(w * 0.3));
    const V = 44;
    A = axis(L, w - V, sm ? 3 : 5);
    head.innerHTML = svg(
      w,
      20,
      A.seg.map((s) => `<text x="${s.x}" y="14">${DN[s.i]}</text>`).join('') +
        `<text x="${w}" y="14" text-anchor="end">${'%'}</text>`,
    );
    size();
  }
  function paint() {
    if (!w) return;
    const a = Math.max(0, Math.floor(box.scrollTop / RH) - 4);
    if (a === from) return;
    from = a;
    const part = list.slice(a, a + Math.ceil(height / RH) + 10);
    layer.style.transform = `translateY(${a * RH}px)`;
    if (!list.length) {
      layer.innerHTML = '<p class="none">No games match.</p>';
      return;
    }
    let o = hatch(id),
      names = '';
    part.forEach((g, i) => {
      const y0 = i * RH,
        y = y0 + (sm ? 24 : 5),
        H = 18;
      names += `<div class="nm" style="top:${y0}px;width:${sm ? w : L - 12}px;${sm ? 'height:24px;line-height:24px' : `height:${RH}px;line-height:${RH}px`}">${esc(g.n)}</div>`;
      if (g.k)
        o +=
          A.seg.map((s) => `<rect x="${s.x}" y="${y}" width="${s.w}" height="${H}" fill="var(--never)"/>`).join('') +
          bars(A, g.ao, 'var(--lit)', y, H) +
          bars(
            A,
            CON.U.filter(([a, b]) => b - a > 5),
            `url(#${id})`,
            y,
            H,
          );
      else o += `<text x="${L}" y="${y + 13}">Number of copies unknown</text>`;
      o += `<text class="v" x="${w}" y="${y + 13}" text-anchor="end">${by === 'c' ? g.c : g.p == null ? '' : g.p.toFixed(0) + '%'}</text><rect data-i="${a + i}" x="0" y="${y0}" width="${w}" height="${RH}" fill="transparent"/>`;
    });
    layer.innerHTML = svg(w, part.length * RH, o) + names;
    head.querySelector('text:last-child').textContent = by === 'c' ? 'Count' : 'All out';
  }
  box.onscroll = () => {
    paint();
    hide();
  };
  layer.onpointermove = (e) => {
    const g = list[e.target.dataset && e.target.dataset.i];
    if (g) show(e, name(g), gameRows(g));
    else hide();
  };
  layer.onpointerleave = hide;
  fit(el, layout);
  apply();
}

// One game, day by day, in 15-minute cells; search to pick it.
function lookup(el, first) {
  const bar = document.createElement('div');
  bar.className = 'bar';
  el.before(bar);
  const wrap = make('div', 'pick', bar),
    input = make('input', '', wrap),
    menu = make('div', 'menu', wrap);
  input.type = 'search';
  input.setAttribute('aria-label', 'Find a game');
  input.setAttribute('role', 'combobox');
  menu.hidden = true;
  const info = make('span', 'count', bar);
  legend(el, [
    ['var(--lit)', 'Every copy out'],
    ['var(--some)', 'Some copies out'],
    ['var(--never)', 'None out'],
    [HATCH, 'Not observed'],
  ]);
  const known = GAMES.filter((g) => g.k);
  let g = byId.get(first) || known[0],
    found = [],
    hot = 0,
    w = 0;
  const choose = (x) => {
    g = x;
    input.value = name(g);
    menu.hidden = true;
    draw();
  };
  const list = () => {
    const q = fold(input.value.trim());
    found = known.filter((x) => x.q.includes(q)).slice(0, 12);
    hot = 0;
    menu.hidden = !found.length;
    paint();
  };
  const paint = () =>
    (menu.innerHTML = found
      .map(
        (x, i) =>
          `<div data-i="${i}" class="${i === hot ? 'on' : ''}">${esc(name(x))}<span>${n1(x.c, 'checkout')}</span></div>`,
      )
      .join(''));
  input.onfocus = () => {
    input.select();
    found = [];
    menu.hidden = true;
  };
  input.oninput = list;
  input.onkeydown = (e) => {
    if (menu.hidden) return;
    if (e.key === 'ArrowDown' || e.key === 'ArrowUp') {
      hot = (hot + (e.key === 'ArrowDown' ? 1 : found.length - 1)) % found.length;
      paint();
      e.preventDefault();
    } else if (e.key === 'Enter') {
      choose(found[hot]);
      e.preventDefault();
    } else if (e.key === 'Escape') menu.hidden = true;
  };
  menu.onpointerdown = (e) => {
    const d = e.target.closest('[data-i]');
    if (d) {
      choose(found[d.dataset.i]);
      e.preventDefault();
    }
  };
  input.onblur = () => {
    menu.hidden = true;
    input.value = name(g);
  };
  function cell(d, q) {
    const a = d * 1440 + 480 + 15 * q,
      b = a + 15;
    if (!CON.O.some(([x, y]) => x <= a && a < y)) return null;
    const m = observed(a, b);
    if (m < 7.5) return { t: 'u' };
    const time = (L) => L.reduce((s, [x, y]) => s + (x < b && y > a ? observed(Math.max(a, x), Math.min(b, y)) : 0), 0);
    const avg = time(g.iv) / m;
    return { t: time(g.ao) >= m / 2 ? 'o' : avg >= 0.5 ? 's' : 'a', n: Math.min(g.k, Math.round(avg)) };
  }
  function draw() {
    if (!w) return;
    info.textContent = `${n1(g.k, 'copy').replace('copys', 'copies')} · ${n1(g.c, 'checkout')} · every copy out ${g.p.toFixed(0)}% of the con`;
    const sm = w < 520,
      L = 40,
      T = 22,
      RH = sm ? 30 : 34,
      cw = (w - L) / 64,
      C = [];
    let o = hatch('ht-lk');
    for (let h = 8; h < 24; h += sm ? 4 : 2) o += `<text x="${L + (h - 8) * 4 * cw}" y="14">${hr(h)}</text>`;
    for (let d = 0; d < 5; d++) {
      o += `<text class="nm" x="0" y="${T + d * RH + RH / 2 + 4}">${DN[d]}</text>`;
      C.push(Array.from({ length: 64 }, (_, q) => cell(d, q)));
      C[d].forEach((c, q) => {
        if (c)
          o += `<rect x="${(L + q * cw).toFixed(2)}" y="${T + d * RH + 1}" width="${Math.max(1, cw - (sm ? 0.5 : 1.2)).toFixed(2)}" height="${RH - 4}" rx="${sm ? 0 : 1.5}" fill="${{ u: 'url(#ht-lk)', o: 'var(--lit)', s: 'var(--some)', a: 'var(--never)' }[c.t]}"/>`;
      });
    }
    el.innerHTML = svg(w, T + 5 * RH, o);
    const s = el.firstChild;
    s.onpointermove = (e) => {
      const r = s.getBoundingClientRect(),
        q = Math.floor((e.clientX - r.left - L) / cw),
        d = Math.floor((e.clientY - r.top - T) / RH),
        c = C[d] && C[d][q];
      if (!c) return hide();
      const m = 480 + 15 * q;
      show(e, `${DN[d]} ${Math.floor(m / 60) % 12 || 12}:${String(m % 60).padStart(2, '0')} ${m < 720 ? 'AM' : 'PM'}`, [
        c.t === 'u' ? row('Not observed', '') : row('Copies out', `${c.n} of ${g.k}`),
      ]);
    };
    s.onpointerleave = hide;
  }
  input.value = name(g);
  fit(el, (x) => {
    w = x;
    draw();
  });
}

// Top games as bars, one panel per group, all on the same scale. by: 'c' checkouts, 'p' share of the con with every copy out
function facets(el, { by, panels, height = panels.length > 1 ? 560 : 440 }) {
  const val = (g) => (by === 'c' ? g.c : g.p),
    lab = (g) => (by === 'c' ? g.c : g.p.toFixed(0) + '%'),
    fill = by === 'c' ? 'var(--lit)' : 'var(--hot)',
    mark = (g) => (g.hot ? ' hotname' : ''); // names of Hot Games room games are bold
  legend(el, []).innerHTML = '<span><b class="hotname">Bold</b> = in the Hot Games room</span>';
  const all = panels.map(([t, ids]) => [t, ids.map((b) => byId.get(b))]),
    max = Math.max(...all.flatMap(([, g]) => g.map(val))),
    one = all.length === 1,
    RH = 24;
  const longest = () => {
    const c = (facets.ctx = facets.ctx || document.createElement('canvas').getContext('2d'));
    c.font = '13px ' + getComputedStyle(el).fontFamily;
    return Math.max(...all.flatMap(([, l]) => l.map((g) => c.measureText(g.n).width)));
  };
  const box = make('div', 'rbox', el),
    grid_ = make('div', one ? 'facets one' : 'facets', box);
  box.style.maxHeight = `min(${height}px, 80vh)`;
  box.onscroll = hide; // taller than this scrolls
  for (const [t, list] of all) {
    const c = make('div', 'facet', grid_);
    if (t) make('h4', '', c).textContent = t;
    const p = make('div', 'fp', c);
    fit(p, (w) => {
      // names get the room the longest one in the figure needs, up to about half a panel (270px in a full-width chart); longer ones are cut off, so rows line up across panels (hover has the full name)
      const L = Math.ceil(Math.min(longest() + 14, w < 450 ? w * 0.54 : 270)),
        V = 38,
        k = (w - L - V) / max;
      p.innerHTML =
        svg(
          w,
          list.length * RH,
          list
            .map(
              (g, i) =>
                `<rect x="${L}" y="${i * RH + 4}" width="${Math.max(2, val(g) * k).toFixed(1)}" height="16" rx="2" fill="${fill}"/><text class="v" x="${(L + val(g) * k + 6).toFixed(1)}" y="${i * RH + 16}">${lab(g)}</text><rect data-i="${i}" y="${i * RH}" width="${w}" height="${RH}" fill="transparent"/>`,
            )
            .join(''),
        ) +
        list
          .map((g, i) => `<div class="nm${mark(g)}" style="top:${i * RH}px;width:${L - 12}px">${esc(g.n)}</div>`)
          .join('');
    });
    p.onpointermove = (e) => {
      const g = list[e.target.dataset && e.target.dataset.i];
      if (g) show(e, name(g), gameRows(g));
      else hide();
    };
    p.onpointerleave = hide;
  }
}

// Demand over the con: a tab per group of games, each a few small charts (drawn in build_trends.py). views: [[group, html]]
function trends(el, { views, strip }) {
  const bar = make('div', 'bar', el),
    key = make('div', '', el),
    box = make('div', 'tcs', el);
  box.insertAdjacentHTML(
    'beforebegin',
    `<svg width="0" height="0" style="position:absolute"><defs><linearGradient id="rate" gradientUnits="userSpaceOnUse" x1="0" x2="0" y1="0" y2="${strip}"><stop offset=".485" stop-color="var(--w2)"/><stop offset=".485" stop-color="var(--muted)"/><stop offset=".515" stop-color="var(--muted)"/><stop offset=".515" stop-color="var(--w0)"/></linearGradient></defs></svg>`,
  );
  legend(key, [
    ['color-mix(in oklab,var(--lit) 78%,var(--bg))', 'All out (share of the hour)'],
    ['var(--hot)', 'Demand', '2px solid'],
    ['var(--muted)', 'Library (share of its peak)', '1px solid'],
    ['var(--w2)', 'Rising'],
    ['var(--w0)', 'Cooling'],
  ]);
  const put = (i) => {
    box.innerHTML = views[i][1];
  };
  seg(
    bar,
    'Games',
    views.map(([t], i) => [i, t]),
    0,
    put,
  );
  put(0);
}
