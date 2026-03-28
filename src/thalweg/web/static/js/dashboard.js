/* Thalweg — Dashboard rendering (D3.js + vanilla fetch) */

const COLORS = {
  CAD: '#e74c3c',
  USD: '#3498db',
  EUR: '#f1c40f',
  GBP: '#2ecc71',
};

const CURRENCY_ORDER = ['CAD', 'USD', 'EUR', 'GBP'];

const SPREAD_DISPLAY = {
  'USD-CAD': { name: 'GoC\u2013UST', sign: -1 },
  'EUR-GBP': { name: 'Bund\u2013Gilt', sign: 1 },
  'CAD-EUR': { name: 'GoC\u2013Bund', sign: 1 },
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Format a number as signed basis-point string, e.g. "+45bp" or "\u221228bp". */
function fmtBp(value) {
  if (value == null || isNaN(value)) return '\u2014';
  const rounded = Math.round(value);
  const sign = rounded > 0 ? '+' : '';
  return `${sign}${rounded}bp`;
}

/** Format yield percentage to 2 decimal places. */
function fmtYield(value) {
  if (value == null || isNaN(value)) return '\u2014';
  return value.toFixed(2);
}

/** Return CSS class for positive / negative / neutral. */
function signClass(value) {
  if (value == null || isNaN(value) || value === 0) return '';
  return value > 0 ? 'positive' : 'negative';
}

/** Convert change_pct (percentage-point change) to basis points. */
function pctToBp(changePct) {
  if (changePct == null) return null;
  return changePct * 100;
}

// ---------------------------------------------------------------------------
// Data fetching
// ---------------------------------------------------------------------------

async function fetchJSON(url) {
  try {
    const resp = await fetch(url);
    if (!resp.ok) return null;
    return resp.json();
  } catch {
    return null;
  }
}

async function fetchAll() {
  // Show loading indicators
  for (const sel of ['#curves-chart', '#rates-panel .panel-body', '#slopes-panel .panel-body', '#changes-panel .panel-body']) {
    const el = document.querySelector(sel);
    if (el) el.innerHTML = '<div class="loading-indicator">Loading\u2026</div>';
  }

  const [curvesRes, ratesRes, slopesRes, spreadsRes, changesRes, regimesRes] =
    await Promise.all([
      fetchJSON('/api/curves/latest'),
      fetchJSON('/api/rates/overnight'),
      fetchJSON('/api/analytics/slopes'),
      fetchJSON('/api/analytics/spreads'),
      fetchJSON('/api/curves/changes'),
      fetchJSON('/api/regimes/latest'),
    ]);

  renderCurves(curvesRes ? curvesRes.curves : []);
  renderRates(ratesRes ? ratesRes.rates : []);
  renderSlopes(
    slopesRes ? slopesRes.slopes : [],
    spreadsRes ? spreadsRes.spreads : [],
    regimesRes ? regimesRes.regimes : [],
  );
  renderChanges(changesRes ? changesRes.changes : [], curvesRes ? curvesRes.curves : []);

  // Update header date from curve data
  if (curvesRes && curvesRes.curves && curvesRes.curves.length > 0) {
    const dates = curvesRes.curves.map(c => c.date).sort();
    document.getElementById('header-date').textContent = dates[dates.length - 1];
  }
}

// ---------------------------------------------------------------------------
// Curves chart (D3)
// ---------------------------------------------------------------------------

function renderCurves(curves) {
  const container = document.getElementById('curves-chart');
  container.innerHTML = '';

  if (!curves || curves.length === 0) {
    container.innerHTML = '<div class="empty-message">No curve data available</div>';
    return;
  }

  const rect = container.getBoundingClientRect();
  const margin = { top: 20, right: 100, bottom: 40, left: 50 };
  const width = rect.width - margin.left - margin.right;
  const height = rect.height - margin.top - margin.bottom;

  if (width <= 0 || height <= 0) return;

  const svg = d3.select(container)
    .append('svg')
    .attr('viewBox', `0 0 ${rect.width} ${rect.height}`)
    .attr('preserveAspectRatio', 'xMidYMid meet');

  const g = svg.append('g')
    .attr('transform', `translate(${margin.left},${margin.top})`);

  // Group data by currency
  const byCurrency = d3.group(curves, d => d.currency);

  // Scales
  const allTenors = curves.map(d => d.tenor_years);
  const allYields = curves.map(d => d.yield_pct);

  const x = d3.scaleLinear()
    .domain([d3.min(allTenors), d3.max(allTenors)])
    .range([0, width]);

  const yMin = d3.min(allYields);
  const yMax = d3.max(allYields);
  const yPad = (yMax - yMin) * 0.1 || 0.5;

  const y = d3.scaleLinear()
    .domain([yMin - yPad, yMax + yPad])
    .range([height, 0]);

  // Grid lines
  g.append('g')
    .attr('class', 'grid')
    .attr('transform', `translate(0,${height})`)
    .call(
      d3.axisBottom(x)
        .tickSize(-height)
        .tickFormat('')
    );

  g.append('g')
    .attr('class', 'grid')
    .call(
      d3.axisLeft(y)
        .tickSize(-width)
        .tickFormat('')
    );

  // Axes
  g.append('g')
    .attr('class', 'axis')
    .attr('transform', `translate(0,${height})`)
    .call(d3.axisBottom(x).ticks(8).tickFormat(d => d + 'y'));

  g.append('g')
    .attr('class', 'axis')
    .call(d3.axisLeft(y).ticks(6).tickFormat(d => d.toFixed(1) + '%'));

  // Axis labels
  g.append('text')
    .attr('class', 'axis-label')
    .attr('x', width / 2)
    .attr('y', height + 35)
    .attr('text-anchor', 'middle')
    .text('Tenor (years)');

  g.append('text')
    .attr('class', 'axis-label')
    .attr('transform', 'rotate(-90)')
    .attr('x', -height / 2)
    .attr('y', -40)
    .attr('text-anchor', 'middle')
    .text('Yield (%)');

  // Line generator
  const line = d3.line()
    .x(d => x(d.tenor_years))
    .y(d => y(d.yield_pct))
    .curve(d3.curveMonotoneX);

  // Draw one line per currency
  for (const [currency, points] of byCurrency) {
    const sorted = [...points].sort((a, b) => a.tenor_years - b.tenor_years);
    const color = COLORS[currency] || '#999';

    g.append('path')
      .datum(sorted)
      .attr('class', 'curve-line')
      .attr('d', line)
      .attr('stroke', color);
  }

  // Legend
  const legendX = width + 12;
  const currencies = CURRENCY_ORDER.filter(c => byCurrency.has(c));

  currencies.forEach((currency, i) => {
    const legendG = g.append('g')
      .attr('class', 'legend-item')
      .attr('transform', `translate(${legendX}, ${i * 22})`);

    legendG.append('circle')
      .attr('r', 5)
      .attr('fill', COLORS[currency]);

    legendG.append('text')
      .attr('x', 12)
      .attr('dy', '0.35em')
      .text(currency);
  });
}

// ---------------------------------------------------------------------------
// Overnight rates
// ---------------------------------------------------------------------------

function renderRates(rates) {
  const body = document.querySelector('#rates-panel .panel-body');
  body.innerHTML = '';

  if (!rates || rates.length === 0) {
    body.innerHTML = '<div class="empty-message">No rate data available</div>';
    return;
  }

  for (const rate of rates) {
    const row = document.createElement('div');
    row.className = 'rate-row';

    const name = document.createElement('span');
    name.className = 'rate-name';
    name.textContent = rate.rate_name;

    const valueWrap = document.createElement('span');
    const val = document.createElement('span');
    val.className = 'rate-value';
    val.textContent = fmtYield(rate.value_pct);
    const unit = document.createElement('span');
    unit.className = 'rate-unit';
    unit.textContent = '%';
    valueWrap.appendChild(val);
    valueWrap.appendChild(unit);

    row.appendChild(name);
    row.appendChild(valueWrap);

    if (rate.change_bp != null) {
      const change = document.createElement('span');
      const absBp = Math.abs(Math.round(rate.change_bp));
      if (rate.change_bp > 0.5) {
        change.className = 'rate-change positive';
        change.textContent = `\u25b2 ${absBp}bp`;
      } else if (rate.change_bp < -0.5) {
        change.className = 'rate-change negative';
        change.textContent = `\u25bc ${absBp}bp`;
      } else {
        change.className = 'rate-change';
        change.textContent = '\u2014';
      }
      row.appendChild(change);
    }

    body.appendChild(row);
  }
}

// ---------------------------------------------------------------------------
// Slopes & spreads
// ---------------------------------------------------------------------------

function renderSlopes(slopes, spreads, regimes) {
  const body = document.querySelector('#slopes-panel .panel-body');
  body.innerHTML = '';

  // --- Slopes section ---
  if (slopes && slopes.length > 0) {
    const slopesSection = document.createElement('div');
    slopesSection.className = 'slopes-section';

    const slopesLabel = document.createElement('div');
    slopesLabel.className = 'slopes-label';
    slopesLabel.textContent = 'Slopes';
    slopesSection.appendChild(slopesLabel);

    // Group slopes by slope_name
    const byName = {};
    for (const s of slopes) {
      if (!byName[s.slope_name]) byName[s.slope_name] = {};
      byName[s.slope_name][s.currency] = s.value_bp;
    }

    for (const [slopeName, currencies] of Object.entries(byName)) {
      const row = document.createElement('div');
      row.className = 'slope-row';

      const nameEl = document.createElement('span');
      nameEl.className = 'slope-name';
      nameEl.textContent = slopeName;
      row.appendChild(nameEl);

      const valuesEl = document.createElement('span');
      valuesEl.className = 'slope-values';

      for (const cur of CURRENCY_ORDER) {
        if (currencies[cur] == null) continue;
        const chip = document.createElement('span');
        chip.className = `slope-chip ${signClass(currencies[cur])}`;

        const tag = document.createElement('span');
        tag.className = `currency-tag text-${cur.toLowerCase()}`;
        tag.textContent = cur;

        chip.appendChild(tag);
        chip.appendChild(document.createTextNode(' ' + fmtBp(currencies[cur])));
        valuesEl.appendChild(chip);
      }

      row.appendChild(valuesEl);
      slopesSection.appendChild(row);
    }

    body.appendChild(slopesSection);
  }

  // --- Spreads section ---
  if (spreads && spreads.length > 0) {
    const spreadsSection = document.createElement('div');
    spreadsSection.className = 'slopes-section';

    const spreadsLabel = document.createElement('div');
    spreadsLabel.className = 'slopes-label';
    spreadsLabel.textContent = 'Cross-Market Spreads (10yr)';
    spreadsSection.appendChild(spreadsLabel);

    // Filter to 10yr spreads and map to friendly names
    const tenYr = spreads.filter(s => s.tenor_years === 10.0);
    const shown = tenYr.length > 0 ? tenYr : spreads;

    for (const s of shown) {
      const display = SPREAD_DISPLAY[s.pair];
      if (!display) continue;

      const adjustedBp = s.spread_bp * display.sign;

      const row = document.createElement('div');
      row.className = 'spread-row';

      const pair = document.createElement('span');
      pair.className = 'spread-pair';
      pair.textContent = display.name;
      row.appendChild(pair);

      const val = document.createElement('span');
      val.className = `spread-value ${signClass(adjustedBp)}`;
      val.textContent = fmtBp(adjustedBp);
      row.appendChild(val);

      spreadsSection.appendChild(row);
    }

    body.appendChild(spreadsSection);
  }

  // --- Regimes section ---
  if (regimes && regimes.length > 0) {
    const regimeSection = document.createElement('div');
    regimeSection.className = 'regime-section';

    const regimeLabel = document.createElement('div');
    regimeLabel.className = 'regime-label';
    regimeLabel.textContent = 'Regime';
    regimeSection.appendChild(regimeLabel);

    const badgesEl = document.createElement('div');
    badgesEl.className = 'regime-badges';

    for (const cur of CURRENCY_ORDER) {
      const r = regimes.find(x => x.currency === cur);
      if (!r) continue;

      const badge = document.createElement('span');
      badge.className = 'regime-badge';
      badge.style.borderColor = COLORS[cur] || '#555';

      const curSpan = document.createElement('span');
      curSpan.className = `regime-currency text-${cur.toLowerCase()}`;
      curSpan.textContent = cur;

      const nameSpan = document.createElement('span');
      nameSpan.className = 'regime-name';
      nameSpan.textContent = r.regime.replace(/_/g, ' ');

      badge.appendChild(curSpan);
      badge.appendChild(nameSpan);
      badgesEl.appendChild(badge);
    }

    regimeSection.appendChild(badgesEl);
    body.appendChild(regimeSection);
  }

  if ((!slopes || slopes.length === 0) && (!spreads || spreads.length === 0) &&
      (!regimes || regimes.length === 0)) {
    body.innerHTML = '<div class="empty-message">No slope/spread data available</div>';
  }
}

// ---------------------------------------------------------------------------
// Curve history — 2×2 grid, one chart per currency with overlaid time traces
// ---------------------------------------------------------------------------

const TRACE_STYLES = [
  { key: 'today', label: 'Now',  opacity: 1.0,  width: 2,   dash: null },
  { key: '1d',    label: '1D',   opacity: 0.55, width: 1.5, dash: null },
  { key: '1w',    label: '1W',   opacity: 0.35, width: 1,   dash: null },
  { key: '1m',    label: '1M',   opacity: 0.2,  width: 1,   dash: null },
  { key: '1y',    label: '1Y',   opacity: 0.12, width: 1,   dash: '4,3' },
];

function renderChanges(changes, currentCurves) {
  const body = document.querySelector('#changes-panel .panel-body');
  body.innerHTML = '';

  if ((!changes || changes.length === 0) || (!currentCurves || currentCurves.length === 0)) {
    body.innerHTML = '<div class="empty-message">No curve history available</div>';
    return;
  }

  // Build current yield lookup: currency -> tenor -> yield_pct
  const currentByTenor = {};
  for (const c of currentCurves) {
    if (!currentByTenor[c.currency]) currentByTenor[c.currency] = {};
    currentByTenor[c.currency][c.tenor_years] = c.yield_pct;
  }

  // Reconstruct historical yields: historical = current - change
  const byCurrency = {};
  for (const c of changes) {
    const curYield = currentByTenor[c.currency] && currentByTenor[c.currency][c.tenor_years];
    if (curYield == null) continue;

    if (!byCurrency[c.currency]) byCurrency[c.currency] = {};
    if (!byCurrency[c.currency][c.horizon]) byCurrency[c.currency][c.horizon] = [];
    byCurrency[c.currency][c.horizon].push({
      tenor_years: c.tenor_years,
      yield_pct: curYield - (c.change_pct || 0),
    });
  }

  const currencies = CURRENCY_ORDER.filter(c => byCurrency[c]);
  if (currencies.length === 0) {
    body.innerHTML = '<div class="empty-message">No curve history available</div>';
    return;
  }

  // 2×2 grid of charts
  const grid = document.createElement('div');
  grid.className = 'history-grid';

  for (const cur of currencies) {
    const cell = document.createElement('div');
    cell.className = 'history-cell';

    const title = document.createElement('div');
    title.className = 'history-cell-title';
    title.style.color = COLORS[cur];
    title.textContent = cur;
    cell.appendChild(title);

    const chartDiv = document.createElement('div');
    chartDiv.className = 'history-cell-chart';
    cell.appendChild(chartDiv);

    grid.appendChild(cell);
  }

  body.appendChild(grid);

  // Render after DOM insertion for getBoundingClientRect
  requestAnimationFrame(() => {
    const charts = grid.querySelectorAll('.history-cell-chart');
    currencies.forEach((cur, i) => {
      renderHistoryChart(charts[i], cur, byCurrency[cur] || {}, currentCurves);
    });
  });
}

function renderHistoryChart(container, currency, horizonData, currentCurves) {
  const rect = container.getBoundingClientRect();
  const margin = { top: 4, right: 8, bottom: 18, left: 32 };
  const width = rect.width - margin.left - margin.right;
  const height = rect.height - margin.top - margin.bottom;
  if (width <= 0 || height <= 0) return;

  // Current curve points
  const curPoints = currentCurves
    .filter(c => c.currency === currency)
    .sort((a, b) => a.tenor_years - b.tenor_years);

  // Collect all yields for scale
  let allYields = curPoints.map(p => p.yield_pct);
  for (const hz of Object.keys(horizonData)) {
    allYields.push(...horizonData[hz].map(p => p.yield_pct));
  }

  if (allYields.length === 0) return;

  const yMin = d3.min(allYields);
  const yMax = d3.max(allYields);
  const yPad = (yMax - yMin) * 0.15 || 0.5;
  const allTenors = curPoints.map(p => p.tenor_years);

  const x = d3.scaleLinear()
    .domain([d3.min(allTenors), d3.max(allTenors)])
    .range([0, width]);

  const y = d3.scaleLinear()
    .domain([yMin - yPad, yMax + yPad])
    .range([height, 0]);

  const svg = d3.select(container)
    .append('svg')
    .attr('viewBox', `0 0 ${rect.width} ${rect.height}`)
    .attr('preserveAspectRatio', 'xMidYMid meet');

  const g = svg.append('g')
    .attr('transform', `translate(${margin.left},${margin.top})`);

  // Grid
  g.append('g').attr('class', 'grid').attr('transform', `translate(0,${height})`)
    .call(d3.axisBottom(x).tickSize(-height).tickFormat(''));
  g.append('g').attr('class', 'grid')
    .call(d3.axisLeft(y).tickSize(-width).tickFormat(''));

  // Axes
  g.append('g').attr('class', 'axis').attr('transform', `translate(0,${height})`)
    .call(d3.axisBottom(x).ticks(4).tickFormat(d => d + 'y'));
  g.append('g').attr('class', 'axis')
    .call(d3.axisLeft(y).ticks(3).tickFormat(d => d.toFixed(1) + '%'));

  const line = d3.line()
    .x(p => x(p.tenor_years))
    .y(p => y(p.yield_pct))
    .curve(d3.curveMonotoneX);

  const color = COLORS[currency] || '#999';

  // Draw traces oldest-first so newest is on top
  for (let i = TRACE_STYLES.length - 1; i >= 0; i--) {
    const style = TRACE_STYLES[i];
    let points;
    if (style.key === 'today') {
      points = curPoints;
    } else {
      points = (horizonData[style.key] || [])
        .sort((a, b) => a.tenor_years - b.tenor_years);
    }
    if (points.length === 0) continue;

    const path = g.append('path')
      .datum(points)
      .attr('d', line)
      .attr('fill', 'none')
      .attr('stroke', color)
      .attr('stroke-width', style.width)
      .attr('opacity', style.opacity);

    if (style.dash) {
      path.attr('stroke-dasharray', style.dash);
    }
  }
}

// ---------------------------------------------------------------------------
// Resize handler for D3 chart
// ---------------------------------------------------------------------------

let resizeTimer;
window.addEventListener('resize', () => {
  clearTimeout(resizeTimer);
  resizeTimer = setTimeout(() => {
    // Re-fetch and re-render on resize to recompute SVG dimensions
    fetchJSON('/api/curves/latest').then(res => {
      if (res) renderCurves(res.curves);
    });
  }, 250);
});

// ---------------------------------------------------------------------------
// Init
// ---------------------------------------------------------------------------

document.addEventListener('DOMContentLoaded', () => {
  fetchAll();
  setInterval(fetchAll, 900_000); // refresh every 15 minutes
});
