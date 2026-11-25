// Every library game as a dot, in columns filled from the bottom, most checked out first.
// Sized like the map: area is checkouts to the power 0.7.
// games: [name, value, checkouts, column]; ticks: [[position in columns, label, show when narrow]]
function dots(el, { games: G, names, ticks, what }) {
  const top = Math.max(...GAMES.map((g) => g.c)),
    base = (n) => (n ? 6.4 * Math.sqrt((n / top) ** 0.7) : 1.2),
    lg = legend(el, []);
  lg.innerHTML =
    [1, 5, 25, top]
      .map(
        (n) =>
          `<span><i class="dot" style="background:var(--lit);width:${2 * base(n)}px;height:${2 * base(n)}px"></i>${n === 1 ? '1 checkout' : n}</span>`,
      )
      .join('') +
    '<span><i class="dot" style="background:var(--never);width:4px;height:4px"></i>Never checked out</span>';
  const C = make('canvas', '', el),
    cols = Array.from({ length: names.length }, () => []);
  G.forEach((g) => cols[g[3]].push(g));
  cols.forEach((c) => c.sort((a, b) => b[2] - a[2]));
  let P = [],
    w = 0,
    H = 0,
    f = 1;
  const draw = () => {
    const r = devicePixelRatio || 1,
      css = getComputedStyle(document.documentElement),
      v = (n) => css.getPropertyValue(n).trim();
    C.width = w * r;
    C.height = H * r;
    C.style.width = w + 'px';
    C.style.height = H + 'px';
    const x = C.getContext('2d');
    x.scale(r, r);
    for (const p of P) {
      x.fillStyle = v(p.g[2] ? '--lit' : '--never');
      x.beginPath();
      x.arc(p.x, H - 34 - p.y, p.r, 0, 7);
      x.fill();
    }
    x.strokeStyle = v('--border');
    x.beginPath();
    x.moveTo(0, H - 31.5);
    x.lineTo(w, H - 31.5);
    x.stroke();
    x.fillStyle = v('--muted');
    x.font = getComputedStyle(el).fontFamily && '13px ' + getComputedStyle(el).fontFamily;
    x.textAlign = 'center';
    for (const [at, t, keep] of ticks)
      if (w >= 620 || keep)
        x.fillText(
          t,
          Math.max(x.measureText(t).width / 2, Math.min(w - x.measureText(t).width / 2, (at * w) / names.length)),
          H - 12,
        );
  };
  fit(el, (width) => {
    w = width;
    f = Math.max(0.62, Math.min(1, w / 900));
    P = [];
    const cw = w / names.length;
    let top = 0;
    cols.forEach((c, i) => {
      let y = 0,
        k = 0;
      while (k < c.length) {
        const d = (n) => 2 * base(n) * f + 0.6,
          h = d(c[k][2]);
        let wd = 0,
          line = [];
        while (k < c.length) {
          const q = d(c[k][2]);
          if (line.length && wd + q > cw - (w < 520 ? 3 : 7)) break;
          line.push([c[k], q]);
          wd += q;
          k++;
        }
        let px = i * cw + (cw - wd) / 2;
        for (const [g, q] of line) {
          P.push({ g, x: px + q / 2, y: y + h / 2, r: base(g[2]) * f });
          px += q;
        }
        y += h;
      }
      top = Math.max(top, y);
    });
    H = Math.ceil(top) + 38;
    draw();
  });
  matchMedia('(prefers-color-scheme: dark)').addEventListener('change', draw);
  C.onpointermove = (e) => {
    const b = C.getBoundingClientRect(),
      mx = e.clientX - b.left,
      my = H - 34 - (e.clientY - b.top);
    let best = null,
      bd = 3;
    for (const p of P) {
      const d = Math.hypot(p.x - mx, p.y - my) - p.r;
      if (d < bd) {
        bd = d;
        best = p;
      }
    }
    if (!best) return hide();
    const g = best.g;
    show(e, g[0], [row(what, g[1]), row('Checkouts', g[2])]);
  };
  C.onpointerleave = hide;
}
