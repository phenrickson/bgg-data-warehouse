// The ordinary charts, drawn with Observable Plot. Columns are numbered from 0: column i runs from i to i + 1.

// Raw SVG as a mark, for what Plot has no mark for.
const raw = (markup) => () => {
  const g = document.createElementNS('http://www.w3.org/2000/svg', 'g');
  g.innerHTML = markup;
  return g;
};

// Text along the bottom edge: [{ x, text }], with its baseline dy pixels below the chart.
const bottomText = (labels, dy, options = {}) =>
  Plot.text(labels, { x: 'x', text: 'text', frameAnchor: 'bottom', dy, ...options });

// Gridlines with their labels at the left.
const gridLines = (ticks, format) => [
  Plot.gridY(ticks, { stroke: 'var(--border)', strokeOpacity: 1 }),
  Plot.axisY(ticks, { tickSize: 0, tickPadding: 8, label: null, tickFormat: format }),
];

// Clock hours along the bottom of a chart with a column per hour.
const hourLabels = (H, narrow) => {
  const labels = H.map((h, i) => ({ x: i + 0.5, text: hr(h) }));
  return bottomText(
    labels.filter((_, i) => i % (narrow ? 4 : 2) === 0),
    22,
  );
};

// Draws a chart at the width it has, and again when that changes.
// heights: [narrow, wide]; y: [low, high]; n: how many columns (none: x is in pixels); tip(i): [title, rows] for a column.
// marks(size) gives the marks; size has narrow, width, height and column (a column's width in pixels).
function plot(el, { heights, left = 0, top = 8, bottom = 30, n, y, tip, highlightTop = top, marks }) {
  fit(el, (width) => {
    const narrow = width < 520,
      height = heights[narrow ? 0 : 1],
      highlight = tip ? [raw(hovs(left, n, width, highlightTop, height - bottom))] : [];
    const chart = Plot.plot({
      width,
      height,
      marginLeft: left,
      marginRight: 0,
      marginTop: top,
      marginBottom: bottom,
      className: 'pl',
      style: { background: 'none', overflow: 'visible' },
      x: n ? { domain: [0, n], axis: null } : { type: 'identity', axis: null },
      y: { domain: y, axis: null },
      color: { type: 'identity' },
      marks: [...highlight, ...marks({ narrow, width, height, column: (width - left) / n })],
    });
    el.replaceChildren(chart);
    if (tip) columns(el, left, n, width, height, tip);
  });
}

// Checkouts in each 15 minutes of library time.
function timeline(el, bins) {
  legend(el, [
    ['var(--lit)', 'Checkouts per 15 minutes'],
    [HATCH, 'Not observed'],
  ]);
  const left = 32,
    top = 8,
    bottom = 26,
    most = Math.ceil(Math.max(...bins.map((b) => b[1])) / 20) * 20;
  plot(el, {
    heights: [170, 210],
    left,
    top,
    bottom,
    y: [0, most],
    marks: ({ width, height }) => {
      const A = axis(left, width, 6),
        quarter = 15 * A.k,
        barWidth = quarter < 3 ? quarter + 0.2 : quarter - 0.7;
      const quarters = bins.flatMap(([t, n]) => {
        const s = A.seg.find((s) => t >= s.a && t < s.b);
        if (!s || !n) return [];
        const x1 = s.x + (t - s.a) * A.k;
        return [{ x1, x2: x1 + barWidth, n }];
      });
      const days = A.seg.map((s) => ({ x: s.x, text: DN[s.i] }));
      return [
        ...gridLines([0, most / 2, most], (v) => v),
        bottomText(days, 20, { textAnchor: 'start' }),
        Plot.rect(quarters, { x1: 'x1', x2: 'x2', y1: 0, y2: 'n', fill: 'var(--lit)' }),
        raw(hatch('ht-tl') + bars(A, CON.U, 'url(#ht-tl)', top, height - bottom - top)),
      ];
    },
  });
  el.onpointermove = (e) => {
    const box = el.firstChild.getBoundingClientRect(),
      x = e.clientX - box.left,
      A = axis(left, box.width, 6),
      s = A.seg.find((s) => x >= s.x && x < s.x + s.w);
    if (!s) return hide();
    const start = Math.floor((s.a + (x - s.x) / A.k) / 15) * 15,
      bin = bins.find((b) => b[0] === start),
      time = hr(Math.floor((start % 1440) / 60)).replace(' ', `:${String(start % 60).padStart(2, '0')} `),
      unseen = observed(start, start + 15) < 7.5;
    show(e, `${DN[s.i]} ${time}`, [unseen ? row('Not observed', '') : row('Checkouts', bin ? bin[1] : 0)]);
  };
  el.onpointerleave = hide;
}

// Bars: [[label, value, tooltip label]]
function barChart(el, { items, what, muted = 0 }) {
  const data = items.map(([label, value], i) => ({ i, label, value, fill: i < muted ? 'var(--never)' : 'var(--lit)' })),
    labels = data.map((d) => ({ x: d.i + 0.5, text: d.label })),
    number = (d) => d.value.toLocaleString();
  plot(el, {
    heights: [220, 260],
    top: 22,
    bottom: 26,
    highlightTop: 0,
    n: items.length,
    y: [0, Math.max(...data.map((d) => d.value))],
    tip: (i) => [items[i][2], [row(what, number(data[i]))]],
    marks: () => [
      Plot.rect(data, { x1: (d) => d.i + 0.1, x2: (d) => d.i + 0.9, y1: 0, y2: 'value', rx: 3, fill: 'fill' }),
      Plot.text(data, { x: (d) => d.i + 0.5, y: 'value', text: number, lineAnchor: 'bottom', dy: -6, className: 'v' }),
      bottomText(labels, 18),
      Plot.ruleY([0], { stroke: 'var(--border)' }),
    ],
  });
}

// Share of games checked out, one bar per column of dots. rate: [games, checked out]
function share(el, { rate, names, ticks, what, max }) {
  const sum = (k) => rate.reduce((t, r) => t + r[k], 0),
    average = sum(1) / sum(0),
    data = rate.map(([all, out], i) => ({ i, all, out, value: out / all })),
    grid = Array.from({ length: Math.round(max / 0.2) + 1 }, (_, i) => i * 0.2);
  legend(el, [
    ['var(--lit)', 'Checked out at least once'],
    ['var(--fg)', `All games (${pc(average)})`, '2px dashed'],
  ]);
  plot(el, {
    heights: [210, 250],
    left: 40,
    bottom: 28,
    n: names.length,
    y: [0, max],
    tip: (i) => [
      `${what} ${names[i]}`,
      [
        row('Library games', data[i].all.toLocaleString()),
        row('Checked out', `${data[i].out.toLocaleString()} (${pc(data[i].value)})`),
      ],
    ],
    marks: ({ width, column }) => {
      const labels = ticks
        .filter(([, , keep]) => width >= 620 || keep)
        .map(([at, text]) => ({ x: Math.min(at, names.length - (text.length * 3.6) / column), text })); // inside the right edge
      return [
        ...gridLines(grid, pc),
        Plot.rect(data, {
          x1: (d) => d.i + 0.07,
          x2: (d) => d.i + 0.93,
          y1: 0,
          y2: 'value',
          rx: 2,
          fill: 'var(--lit)',
        }),
        Plot.ruleY([average], { stroke: 'var(--fg)', strokeWidth: 2, strokeDasharray: '5 4' }),
        bottomText(labels, 20),
      ];
    },
  });
}

// Value by clock hour: all days, each day, and the range chance alone would give.
function hourly(el, { S, ticks, percent }) {
  const format = percent ? pc : two,
    average = S.H.map((_, i) => ({ x: i + 0.5, value: S.m[i], low: S.band[0][i], high: S.band[1][i] })),
    byDay = S.days.flatMap((values, day) => values.map((v, i) => ({ x: i + 0.5, day, value: v ?? NaN })));
  legend(el, [
    ['var(--lit)', 'All days', '3px solid'],
    ['var(--day)', 'Each day', '1px solid'],
    ['var(--bandc)', 'Range from chance alone'],
  ]);
  plot(el, {
    heights: [260, 320],
    left: 40,
    n: S.H.length,
    y: [ticks[0], ticks.at(-1)],
    tip: (i) => [
      hr(S.H[i]),
      [
        row('Checkouts', S.n[i].toLocaleString()),
        row('All days', format(S.m[i])),
        ...S.days.map((d, k) => (d[i] == null ? '' : row(DN[k], format(d[i])))),
        row('Chance', `${format(S.band[0][i])} to ${format(S.band[1][i])}`),
      ],
    ],
    marks: ({ narrow }) => [
      Plot.areaY(average, { x: 'x', y1: 'low', y2: 'high', fill: 'var(--bandc)' }),
      ...gridLines(ticks, percent ? pc : (v) => v.toFixed(1)),
      hourLabels(S.H, narrow),
      Plot.lineY(byDay, { x: 'x', y: 'value', z: 'day', stroke: 'var(--day)', strokeWidth: 1 }),
      Plot.lineY(average, { x: 'x', y: 'value', stroke: 'var(--lit)', strokeWidth: 2.5 }),
      Plot.dot(average, { x: 'x', y: 'value', r: 4.5, fill: 'var(--lit)', stroke: 'var(--bg)', strokeWidth: 2 }),
    ],
  });
}

// Checkouts per clock hour, one line per day. days: Wed-Sun, null = closed or not recorded
function eachDay(el, { H, days }) {
  const color = (day) => `var(--day-${day + 1})`,
    most = Math.ceil(Math.max(...days.flat()) / 100) * 100,
    ticks = Array.from({ length: most / 100 + 1 }, (_, i) => i * 100),
    data = days.map((values) => values.map((v, i) => ({ x: i + 0.5, value: v ?? NaN })));
  legend(
    el,
    days.map((_, day) => [color(day), DN[day], '3px solid']),
  );
  const tipRow = (day, v) => {
    const missing = day === 0 || day === 4 ? 'closed' : 'not recorded',
      dot = `<i style="display:inline-block;width:9px;height:9px;border-radius:50%;background:${color(day)};margin-right:6px"></i>`;
    return `<div><span>${dot}${DN[day]}</span><span>${v ?? missing}</span></div>`;
  };
  plot(el, {
    heights: [260, 320],
    left: 36,
    n: H.length,
    y: [0, most],
    tip: (i) => [hr(H[i]), days.map((values, day) => tipRow(day, values[i]))],
    marks: ({ narrow }) => [
      ...gridLines(ticks, (v) => v),
      hourLabels(H, narrow),
      ...data.flatMap((points, day) => [
        Plot.lineY(points, { x: 'x', y: 'value', stroke: color(day), strokeWidth: 2.25, strokeLinecap: 'round' }),
        Plot.dot(points, { x: 'x', y: 'value', r: narrow ? 2 : 3, fill: color(day), stroke: 'none' }),
      ]),
    ],
  });
}

// Stacked shares by hour. mix: one array of shares per hour, in band order
function stack(el, { H, n, mix, names }) {
  // the map's complexity scale
  const colors = names.map((_, i) => {
    const u = (i / (names.length - 1)) * 2;
    return u <= 1
      ? `color-mix(in oklab,var(--w1) ${100 * u}%,var(--w0))`
      : `color-mix(in oklab,var(--w2) ${100 * (u - 1)}%,var(--w1))`;
  });
  const data = mix.flatMap((shares, i) => {
    let from = 0;
    return shares.map((v, j) => ({ i, y1: from, y2: (from += v), fill: colors[j] }));
  });
  legend(
    el,
    names.map((name, i) => [colors[i], name]),
  );
  const inset = { insetTop: 1, insetBottom: 1 };
  plot(el, {
    heights: [260, 320],
    left: 44,
    n: H.length,
    y: [0, 1],
    tip: (i) => [
      hr(H[i]),
      [row('Checkouts', n[i].toLocaleString()), ...mix[i].map((v, j) => row(names[j], pc(v))).reverse()],
    ],
    marks: ({ narrow }) => [
      ...gridLines([0, 0.25, 0.5, 0.75, 1], pc),
      hourLabels(H, narrow),
      Plot.rect(data, { x1: (d) => d.i + 0.05, x2: (d) => d.i + 0.95, y1: 'y1', y2: 'y2', fill: 'fill', ...inset }),
    ],
  });
}
