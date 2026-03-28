/* Thalweg — Dashboard rendering (D3.js + vanilla fetch) */

const COLORS = {
  CAD: '#e74c3c',
  USD: '#3498db',
  EUR: '#f1c40f',
  GBP: '#2ecc71',
};

const CURRENCY_ORDER = ['CAD', 'USD', 'EUR', 'GBP'];

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
  renderChanges(changesRes ? changesRes.changes : []);

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

    // Filter to 10yr spreads for cleanliness
    const tenYr = spreads.filter(s => s.tenor_years === 10.0);
    const shown = tenYr.length > 0 ? tenYr : spreads;

    for (const s of shown) {
      const row = document.createElement('div');
      row.className = 'spread-row';

      const pair = document.createElement('span');
      pair.className = 'spread-pair';
      pair.textContent = s.pair + (s.tenor_years ? ` ${s.tenor_years}yr` : '');
      row.appendChild(pair);

      const val = document.createElement('span');
      val.className = `spread-value ${signClass(s.spread_bp)}`;
      val.textContent = fmtBp(s.spread_bp);
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
// Curve changes table
// ---------------------------------------------------------------------------

function renderChanges(changes) {
  const body = document.querySelector('#changes-panel .panel-body');
  body.innerHTML = '';

  if (!changes || changes.length === 0) {
    body.innerHTML = '<div class="empty-message">No change data available</div>';
    return;
  }

  const horizons = ['1d', '1w', '1m', '1y'];
  const horizonLabels = { '1d': '1D', '1w': '1W', '1m': '1M', '1y': '1Y' };

  // Group by currency -> horizon -> [{tenor_years, change_pct}]
  const byCurrency = {};
  for (const c of changes) {
    if (!byCurrency[c.currency]) byCurrency[c.currency] = {};
    if (!byCurrency[c.currency][c.horizon]) byCurrency[c.currency][c.horizon] = [];
    byCurrency[c.currency][c.horizon].push({
      tenor_years: c.tenor_years,
      change_bp: pctToBp(c.change_pct),
    });
  }

  const currencies = CURRENCY_ORDER.filter(c => byCurrency[c]);
  if (currencies.length === 0) {
    body.innerHTML = '<div class="empty-message">No change data available</div>';
    return;
  }

  const grid = document.createElement('div');
  grid.className = 'changes-grid';

  // Header row: empty corner + horizon labels
  grid.appendChild(document.createElement('div'));
  for (const h of horizons) {
    const hdr = document.createElement('div');
    hdr.className = 'ch-header';
    hdr.textContent = horizonLabels[h];
    grid.appendChild(hdr);
  }

  // Mini chart dimensions
  const cellW = 100;
  const cellH = 44;
  const pad = { top: 4, right: 4, bottom: 4, left: 4 };
  const w = cellW - pad.left - pad.right;
  const h = cellH - pad.top - pad.bottom;

  for (const cur of currencies) {
    // Row label
    const label = document.createElement('div');
    label.className = `ch-label text-${cur.toLowerCase()}`;
    label.textContent = cur;
    grid.appendChild(label);

    for (const hz of horizons) {
      const cell = document.createElement('div');
      cell.className = 'ch-cell';

      const points = (byCurrency[cur][hz] || [])
        .sort((a, b) => a.tenor_years - b.tenor_years);

      if (points.length === 0) {
        cell.innerHTML = '<span style="color:#555;font-size:0.7rem">\u2014</span>';
        grid.appendChild(cell);
        continue;
      }

      const bpValues = points.map(p => p.change_bp);
      const maxAbs = Math.max(Math.abs(d3.min(bpValues)), Math.abs(d3.max(bpValues)), 1);

      const x = d3.scaleLinear()
        .domain([d3.min(points, p => p.tenor_years), d3.max(points, p => p.tenor_years)])
        .range([0, w]);

      const y = d3.scaleLinear()
        .domain([-maxAbs, maxAbs])
        .range([h, 0]);

      const line = d3.line()
        .x(p => x(p.tenor_years))
        .y(p => y(p.change_bp))
        .curve(d3.curveMonotoneX);

      const svg = d3.select(cell)
        .append('svg')
        .attr('viewBox', `0 0 ${cellW} ${cellH}`)
        .attr('preserveAspectRatio', 'xMidYMid meet');

      const g = svg.append('g')
        .attr('transform', `translate(${pad.left},${pad.top})`);

      // Zero line
      g.append('line')
        .attr('x1', 0).attr('x2', w)
        .attr('y1', y(0)).attr('y2', y(0))
        .attr('stroke', '#333')
        .attr('stroke-dasharray', '2,2');

      // Change curve
      g.append('path')
        .datum(points)
        .attr('d', line)
        .attr('fill', 'none')
        .attr('stroke', COLORS[cur] || '#999')
        .attr('stroke-width', 1.5);

      grid.appendChild(cell);
    }
  }

  body.appendChild(grid);
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
