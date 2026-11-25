// Checkouts in each 15 minutes of library time.
function timeline(el, bins) {
  legend(el, [
    ['var(--lit)', 'Checkouts per 15 minutes'],
    [HATCH, 'Not observed'],
  ]);
  fit(el, (w) => {
    const sm = w < 520,
      h = sm ? 170 : 210,
      L = 32,
      T = 8,
      B = h - 26,
      A = axis(L, w, 6),
      max = Math.max(...bins.map((b) => b[1])),
      top = Math.ceil(max / 20) * 20,
      y = (v) => B - ((B - T) * v) / top;
    let o = hatch('ht-tl') + grid([0, top / 2, top], y, (v) => v, L, w);
    for (const s of A.seg) o += `<text x="${s.x}" y="${h - 6}">${DN[s.i]}</text>`;
    for (const [t, n] of bins) {
      const s = A.seg.find((s) => t >= s.a && t < s.b);
      if (s && n)
        o += `<rect x="${s.x + (t - s.a) * A.k}" width="${15 * A.k < 3 ? 15 * A.k + 0.2 : 15 * A.k - 0.7}" y="${y(n)}" height="${B - y(n)}" fill="var(--lit)"/>`;
    }
    o += bars(A, CON.U, 'url(#ht-tl)', T, B - T);
    el.innerHTML = svg(w, h, o);
    const sv = el.firstChild;
    sv.onpointermove = (e) => {
      const x = e.clientX - sv.getBoundingClientRect().left,
        s = A.seg.find((s) => x >= s.x && x < s.x + s.w);
      if (!s) return hide();
      const t = s.a + (x - s.x) / A.k,
        q = Math.floor(t / 15) * 15,
        b = bins.find((b) => b[0] === q),
        m = q % 1440,
        un = observed(q, q + 15) < 7.5;
      show(e, `${DN[s.i]} ${hr(Math.floor(m / 60)).replace(' ', `:${String(m % 60).padStart(2, '0')} `)}`, [
        un ? row('Not observed', '') : row('Checkouts', b ? b[1] : 0),
      ]);
    };
    sv.onpointerleave = hide;
  });
}

// Bars: [[label, value, tooltip label]]
function barChart(el, { items, what, muted = 0 }) {
  fit(el, (w) => {
    const sm = w < 520,
      h = sm ? 220 : 260,
      L = 0,
      T = 22,
      B = h - 26,
      max = Math.max(...items.map((d) => d[1])),
      b = (w - L) / items.length,
      y = (v) => B - ((B - T) * v) / max;
    let o = hovs(L, items.length, w, 0, B);
    items.forEach((d, i) => {
      o += `<rect x="${L + i * b + b * 0.1}" width="${b * 0.8}" y="${y(d[1])}" height="${Math.max(1, B - y(d[1]))}" rx="3" fill="${i < muted ? 'var(--never)' : 'var(--lit)'}"/><text class="v" x="${L + (i + 0.5) * b}" y="${y(d[1]) - 6}" text-anchor="middle">${d[1].toLocaleString()}</text><text x="${L + (i + 0.5) * b}" y="${h - 8}" text-anchor="middle">${d[0]}</text>`;
    });
    el.innerHTML = svg(w, h, o + `<line x1="0" x2="${w}" y1="${B}" y2="${B}" stroke="var(--border)"/>`);
    columns(el, L, items.length, w, h, (i) => [items[i][2], [row(what, items[i][1].toLocaleString())]]);
  });
}

// Share of games checked out, one bar per column of dots. rate: [games, checked out]
function share(el, { rate, names, ticks, what, max }) {
  const all = rate.reduce((t, r) => [t[0] + r[0], t[1] + r[1]], [0, 0]),
    avg = all[1] / all[0];
  legend(el, [
    ['var(--lit)', 'Checked out at least once'],
    ['var(--fg)', `All games (${pc(avg)})`, '2px dashed'],
  ]);
  fit(el, (w) => {
    const sm = w < 520,
      h = sm ? 210 : 250,
      L = 40,
      T = 8,
      B = h - 28,
      n = names.length,
      b = (w - L) / n,
      y = (v) => B - ((B - T) * v) / max,
      tk = [];
    for (let t = 0; t <= max + 1e-9; t += 0.2) tk.push(t);
    let o = hovs(L, n, w, T, B) + grid(tk, y, pc, L, w);
    rate.forEach(
      ([c, k], i) =>
        (o += `<rect x="${L + i * b + b * 0.07}" width="${b * 0.86}" y="${y(k / c)}" height="${B - y(k / c)}" rx="2" fill="var(--lit)"/>`),
    );
    o += `<line x1="${L}" x2="${w}" y1="${y(avg)}" y2="${y(avg)}" stroke="var(--fg)" stroke-width="2" stroke-dasharray="5 4"/>`;
    for (const [at, t, keep] of ticks)
      if (w >= 620 || keep)
        o += `<text x="${Math.min(w - t.length * 3.6, L + at * b)}" y="${h - 8}" text-anchor="middle">${t}</text>`;
    el.innerHTML = svg(w, h, o);
    columns(el, L, n, w, h, (i) => [
      `${what} ${names[i]}`,
      [
        row('Library games', rate[i][0].toLocaleString()),
        row('Checked out', `${rate[i][1].toLocaleString()} (${pc(rate[i][1] / rate[i][0])})`),
      ],
    ]);
  });
}

// Value by clock hour: all days, each day, and the range chance alone would give.
function hourly(el, { S, ticks, percent }) {
  const f = percent ? pc : two,
    ax = percent ? pc : (v) => v.toFixed(1);
  legend(el, [
    ['var(--lit)', 'All days', '3px solid'],
    ['var(--day)', 'Each day', '1px solid'],
    ['var(--bandc)', 'Range from chance alone'],
  ]);
  fit(el, (w) => {
    const sm = w < 520,
      h = sm ? 260 : 320,
      L = 40,
      T = 8,
      B = h - 30,
      n = S.H.length,
      s = (w - L) / n,
      x = (i) => L + (i + 0.5) * s,
      lo = ticks[0],
      hi = ticks[ticks.length - 1],
      y = (v) => T + ((B - T) * (hi - v)) / (hi - lo);
    let o =
      `<path d="${path(S.band[0], x, y)}${S.band[1]
        .map((v, i) => [v, i])
        .reverse()
        .map(([v, i]) => 'L' + x(i) + ',' + y(v))
        .join('')}Z" fill="var(--bandc)"/>` +
      hovs(L, n, w, T, B) +
      grid(ticks, y, ax, L, w) +
      hours(S.H, x, h, sm);
    S.days.forEach((r) => (o += `<path d="${path(r, x, y)}" fill="none" stroke="var(--day)"/>`));
    o +=
      `<path d="${path(S.m, x, y)}" fill="none" stroke="var(--lit)" stroke-width="2.5" stroke-linejoin="round"/>` +
      S.m
        .map(
          (v, i) => `<circle cx="${x(i)}" cy="${y(v)}" r="4.5" fill="var(--lit)" stroke="var(--bg)" stroke-width="2"/>`,
        )
        .join('');
    el.innerHTML = svg(w, h, o);
    columns(el, L, n, w, h, (i) => [
      hr(S.H[i]),
      [
        row('Checkouts', S.n[i].toLocaleString()),
        row('All days', f(S.m[i])),
        ...S.days.map((d, k) => (d[i] == null ? '' : row(DN[k], f(d[i])))),
        row('Chance', f(S.band[0][i]) + ' to ' + f(S.band[1][i])),
      ],
    ]);
  });
}

// Checkouts per clock hour, one line per day. days: Wed-Sun, null = closed or not recorded
function eachDay(el, { H, days }) {
  const col = (i) => `var(--day-${i + 1})`;
  legend(
    el,
    days.map((_, i) => [col(i), DN[i], '3px solid']),
  );
  fit(el, (w) => {
    const sm = w < 520,
      h = sm ? 260 : 320,
      L = 36,
      T = 8,
      B = h - 30,
      n = H.length,
      s = (w - L) / n,
      x = (i) => L + (i + 0.5) * s,
      top = Math.ceil(Math.max(...days.flat()) / 100) * 100,
      y = (v) => B - ((B - T) * v) / top,
      tk = [];
    for (let t = 0; t <= top; t += 100) tk.push(t);
    let o = hovs(L, n, w, T, B) + grid(tk, y, (v) => v, L, w) + hours(H, x, h, sm);
    days.forEach((r, k) => {
      o += `<path d="${path(r, x, y)}" fill="none" stroke="${col(k)}" stroke-width="2.25" stroke-linejoin="round" stroke-linecap="round"/>`;
      r.forEach((v, i) => {
        if (v != null) o += `<circle cx="${x(i)}" cy="${y(v)}" r="${sm ? 2 : 3}" fill="${col(k)}"/>`;
      });
    });
    el.innerHTML = svg(w, h, o);
    columns(el, L, n, w, h, (i) => [
      hr(H[i]),
      days.map(
        (r, k) =>
          `<div><span><i style="display:inline-block;width:9px;height:9px;border-radius:50%;background:${col(k)};margin-right:6px"></i>${DN[k]}</span><span>${r[i] == null ? (k === 0 || k === 4 ? 'closed' : 'not recorded') : r[i]}</span></div>`,
      ),
    ]);
  });
}

// Stacked shares by hour. mix: one array of shares per hour, in band order
function stack(el, { H, n, mix, names }) {
  const c = names.map((_, i) => {
    const u = (i / (names.length - 1)) * 2;
    return u <= 1
      ? `color-mix(in oklab,var(--w1) ${100 * u}%,var(--w0))`
      : `color-mix(in oklab,var(--w2) ${100 * (u - 1)}%,var(--w1))`;
  }); // the map's complexity scale
  legend(
    el,
    names.map((t, i) => [c[i], t]),
  );
  fit(el, (w) => {
    const sm = w < 520,
      h = sm ? 260 : 320,
      L = 44,
      T = 8,
      B = h - 30,
      s = (w - L) / H.length,
      x = (i) => L + (i + 0.5) * s,
      y = (v) => B - (B - T) * v;
    let o = hovs(L, H.length, w, T, B) + grid([0, 0.25, 0.5, 0.75, 1], y, pc, L, w) + hours(H, x, h, sm);
    mix.forEach((r, i) => {
      let a = 0;
      r.forEach((v, j) => {
        o += `<rect x="${L + i * s + s * 0.05}" width="${s * 0.9}" y="${y(a + v) + 1}" height="${Math.max(0, y(a) - y(a + v) - 2)}" fill="${c[j]}"/>`;
        a += v;
      });
    });
    el.innerHTML = svg(w, h, o);
    columns(el, L, H.length, w, h, (i) => [
      hr(H[i]),
      [row('Checkouts', n[i].toLocaleString()), ...mix[i].map((v, j) => row(names[j], pc(v))).reverse()],
    ]);
  });
}
