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
  const [curvesRes, ratesRes, slopesRes, spreadsRes, changesRes] = await Promise.all([
    fetchJSON('/api/curves/latest'),
    fetchJSON('/api/rates/overnight'),
    fetchJSON('/api/analytics/slopes'),
    fetchJSON('/api/analytics/spreads'),
    fetchJSON('/api/curves/changes'),
  ]);

  renderCurves(curvesRes ? curvesRes.curves : []);
  renderRates(ratesRes ? ratesRes.rates : []);
  renderSlopes(
    slopesRes ? slopesRes.slopes : [],
    spreadsRes ? spreadsRes.spreads : [],
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

function renderSlopes(slopes, spreads) {
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

  if ((!slopes || slopes.length === 0) && (!spreads || spreads.length === 0)) {
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

  // Pivot: group by currency, pick 10yr tenor (or closest)
  const byCurrency = {};
  for (const c of changes) {
    if (!byCurrency[c.currency]) byCurrency[c.currency] = {};
    // Prefer 10yr, but store all and pick later
    if (!byCurrency[c.currency][c.tenor_years]) {
      byCurrency[c.currency][c.tenor_years] = {};
    }
    byCurrency[c.currency][c.tenor_years][c.horizon] = c.change_pct;
  }

  // For each currency, find 10yr or nearest long-end tenor
  const currencyRows = {};
  for (const [cur, tenorMap] of Object.entries(byCurrency)) {
    const tenors = Object.keys(tenorMap).map(Number).sort((a, b) => a - b);
    const target = tenors.includes(10) ? 10 : tenors[tenors.length - 1];
    currencyRows[cur] = { tenor: target, horizons: tenorMap[target] };
  }

  const table = document.createElement('table');
  table.className = 'changes-table';

  // Header
  const thead = document.createElement('thead');
  const headerRow = document.createElement('tr');

  const thCur = document.createElement('th');
  thCur.textContent = '10yr \u0394';
  headerRow.appendChild(thCur);

  for (const h of horizons) {
    const th = document.createElement('th');
    th.textContent = horizonLabels[h];
    headerRow.appendChild(th);
  }
  thead.appendChild(headerRow);
  table.appendChild(thead);

  // Body rows
  const tbody = document.createElement('tbody');
  const sortedCurrencies = CURRENCY_ORDER.filter(c => currencyRows[c]);

  for (const cur of sortedCurrencies) {
    const row = document.createElement('tr');

    const tdName = document.createElement('td');
    tdName.className = `text-${cur.toLowerCase()}`;
    tdName.textContent = cur;
    row.appendChild(tdName);

    for (const h of horizons) {
      const td = document.createElement('td');
      const raw = currencyRows[cur].horizons[h];
      const bp = pctToBp(raw);
      td.className = signClass(bp);
      td.textContent = fmtBp(bp);
      row.appendChild(td);
    }

    tbody.appendChild(row);
  }

  table.appendChild(tbody);
  body.appendChild(table);
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
