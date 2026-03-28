/* Thalweg — Curve Explorer (D3.js + vanilla fetch) */

const COLORS = {
  CAD: '#e74c3c',
  USD: '#3498db',
  EUR: '#f1c40f',
  GBP: '#2ecc71',
};

const CURRENCY_ORDER = ['CAD', 'USD', 'EUR', 'GBP'];

const REGIME_COLORS = {
  normal_steep: '#1a3a1a',
  normal:       '#1a1a2e',
  flat:         '#3a3a1a',
  inverted:     '#3a1a1a',
  bear_steep:   '#2a1a1a',
  bull_flat:    '#1a2a3a',
  bear_flat:    '#2a2a1a',
  bull_steep:   '#1a3a2a',
};

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

let allDates = [];
let overlayDates = [];  // max 3
let currentCurves = [];
let overlayCurves = {};  // date -> curves[]

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async function fetchJSON(url) {
  try {
    const resp = await fetch(url);
    if (!resp.ok) return null;
    return resp.json();
  } catch { return null; }
}

// ---------------------------------------------------------------------------
// Init — load available dates
// ---------------------------------------------------------------------------

async function init() {
  const res = await fetchJSON('/api/curves/dates');
  if (!res || !res.dates || res.dates.length === 0) {
    document.getElementById('slider-date-display').textContent = 'No data';
    return;
  }

  allDates = res.dates;
  const slider = document.getElementById('date-slider');
  slider.max = allDates.length - 1;
  slider.value = allDates.length - 1;

  slider.addEventListener('input', onSliderChange);
  document.getElementById('add-overlay-btn').addEventListener('click', onAddOverlay);
  document.getElementById('overlay-date-input').addEventListener('change', onOverlayDatePicked);

  await loadDate(allDates[allDates.length - 1]);
}

// ---------------------------------------------------------------------------
// Date slider
// ---------------------------------------------------------------------------

let sliderDebounce;

function onSliderChange() {
  const idx = parseInt(this.value);
  const selectedDate = allDates[idx];
  document.getElementById('slider-date-display').textContent = selectedDate;

  clearTimeout(sliderDebounce);
  sliderDebounce = setTimeout(() => loadDate(selectedDate), 80);
}

async function loadDate(dateStr) {
  document.getElementById('slider-date-display').textContent = dateStr;
  document.getElementById('header-date').textContent = dateStr;

  const [curvesRes, regimesRes] = await Promise.all([
    fetchJSON(`/api/curves?start_date=${dateStr}&end_date=${dateStr}`),
    fetchJSON(`/api/regimes?start_date=${dateStr}&end_date=${dateStr}`),
  ]);

  currentCurves = curvesRes ? curvesRes.curves : [];
  renderExplorerCurves();
  renderExplorerRegimes(regimesRes ? regimesRes.regimes : []);
}

// ---------------------------------------------------------------------------
// Overlay controls
// ---------------------------------------------------------------------------

function onAddOverlay() {
  const input = document.getElementById('overlay-date-input');
  input.style.display = input.style.display === 'none' ? 'inline-block' : 'none';
  if (input.style.display !== 'none') input.focus();
}

async function onOverlayDatePicked() {
  const dateStr = this.value;
  this.style.display = 'none';

  if (!dateStr || overlayDates.includes(dateStr) || overlayDates.length >= 3) return;

  const res = await fetchJSON(`/api/curves?start_date=${dateStr}&end_date=${dateStr}`);
  if (!res || !res.curves || res.curves.length === 0) return;

  overlayDates.push(dateStr);
  overlayCurves[dateStr] = res.curves;
  renderOverlayChips();
  renderExplorerCurves();
}

function removeOverlay(dateStr) {
  overlayDates = overlayDates.filter(d => d !== dateStr);
  delete overlayCurves[dateStr];
  renderOverlayChips();
  renderExplorerCurves();
}

function renderOverlayChips() {
  const container = document.getElementById('overlay-chips');
  container.innerHTML = '';
  for (const d of overlayDates) {
    const chip = document.createElement('span');
    chip.className = 'overlay-chip';
    chip.innerHTML = `${d} <button onclick="removeOverlay('${d}')">&times;</button>`;
    container.appendChild(chip);
  }
}

// ---------------------------------------------------------------------------
// Curve chart (shared renderer)
// ---------------------------------------------------------------------------

function renderExplorerCurves() {
  const container = document.getElementById('explorer-curves');
  container.innerHTML = '';

  const allData = [...currentCurves];
  if (allData.length === 0 && overlayDates.length === 0) {
    container.innerHTML = '<div class="empty-message">No curve data for this date</div>';
    return;
  }

  const rect = container.getBoundingClientRect();
  const margin = { top: 20, right: 100, bottom: 40, left: 50 };
  const width = rect.width - margin.left - margin.right;
  const height = rect.height - margin.top - margin.bottom;
  if (width <= 0 || height <= 0) return;

  // Collect all points for scale computation
  const allPoints = [...allData];
  for (const curves of Object.values(overlayCurves)) {
    allPoints.push(...curves);
  }

  const svg = d3.select(container)
    .append('svg')
    .attr('viewBox', `0 0 ${rect.width} ${rect.height}`)
    .attr('preserveAspectRatio', 'xMidYMid meet');

  const g = svg.append('g')
    .attr('transform', `translate(${margin.left},${margin.top})`);

  const allTenors = allPoints.map(d => d.tenor_years);
  const allYields = allPoints.map(d => d.yield_pct);
  const yMin = d3.min(allYields);
  const yMax = d3.max(allYields);
  const yPad = (yMax - yMin) * 0.1 || 0.5;

  const x = d3.scaleLinear()
    .domain([d3.min(allTenors), d3.max(allTenors)])
    .range([0, width]);

  const y = d3.scaleLinear()
    .domain([yMin - yPad, yMax + yPad])
    .range([height, 0]);

  // Grid
  g.append('g').attr('class', 'grid').attr('transform', `translate(0,${height})`)
    .call(d3.axisBottom(x).tickSize(-height).tickFormat(''));
  g.append('g').attr('class', 'grid')
    .call(d3.axisLeft(y).tickSize(-width).tickFormat(''));

  // Axes
  g.append('g').attr('class', 'axis').attr('transform', `translate(0,${height})`)
    .call(d3.axisBottom(x).ticks(8).tickFormat(d => d + 'y'));
  g.append('g').attr('class', 'axis')
    .call(d3.axisLeft(y).ticks(6).tickFormat(d => d.toFixed(1) + '%'));

  // Axis labels
  g.append('text').attr('class', 'axis-label')
    .attr('x', width / 2).attr('y', height + 35).attr('text-anchor', 'middle')
    .text('Tenor (years)');
  g.append('text').attr('class', 'axis-label')
    .attr('transform', 'rotate(-90)').attr('x', -height / 2).attr('y', -40)
    .attr('text-anchor', 'middle').text('Yield (%)');

  const line = d3.line()
    .x(d => x(d.tenor_years))
    .y(d => y(d.yield_pct))
    .curve(d3.curveMonotoneX);

  // Draw overlay curves first (dashed, dim)
  for (const [dateStr, curves] of Object.entries(overlayCurves)) {
    const byCurrency = d3.group(curves, d => d.currency);
    for (const [currency, points] of byCurrency) {
      const sorted = [...points].sort((a, b) => a.tenor_years - b.tenor_years);
      g.append('path')
        .datum(sorted)
        .attr('class', 'curve-line')
        .attr('d', line)
        .attr('stroke', COLORS[currency] || '#555')
        .attr('opacity', 0.35)
        .attr('stroke-dasharray', '4,3');
    }
  }

  // Draw current curves (solid)
  const byCurrency = d3.group(currentCurves, d => d.currency);
  for (const [currency, points] of byCurrency) {
    const sorted = [...points].sort((a, b) => a.tenor_years - b.tenor_years);
    const color = COLORS[currency] || '#999';

    g.append('path')
      .datum(sorted)
      .attr('class', 'curve-line')
      .attr('d', line)
      .attr('stroke', color);

    // Clickable tenor points
    for (const p of sorted) {
      g.append('circle')
        .attr('cx', x(p.tenor_years))
        .attr('cy', y(p.yield_pct))
        .attr('r', 6)
        .attr('fill', color)
        .attr('opacity', 0)
        .attr('cursor', 'pointer')
        .on('mouseover', function() { d3.select(this).attr('opacity', 0.6); })
        .on('mouseout', function() { d3.select(this).attr('opacity', 0); })
        .on('click', () => loadTenorHistory(currency, p.tenor_years));

      g.append('circle')
        .attr('cx', x(p.tenor_years))
        .attr('cy', y(p.yield_pct))
        .attr('r', 3)
        .attr('fill', color)
        .attr('pointer-events', 'none');
    }
  }

  // Legend
  const legendX = width + 12;
  const currencies = CURRENCY_ORDER.filter(c => byCurrency.has(c));
  currencies.forEach((currency, i) => {
    const lg = g.append('g')
      .attr('class', 'legend-item')
      .attr('transform', `translate(${legendX}, ${i * 22})`);
    lg.append('circle').attr('r', 5).attr('fill', COLORS[currency]);
    lg.append('text').attr('x', 12).attr('dy', '0.35em').text(currency);
  });
}

// ---------------------------------------------------------------------------
// Regime badges
// ---------------------------------------------------------------------------

function renderExplorerRegimes(regimes) {
  const container = document.getElementById('explorer-regimes');
  container.innerHTML = '';
  if (!regimes || regimes.length === 0) return;

  for (const cur of CURRENCY_ORDER) {
    const r = regimes.find(x => x.currency === cur);
    if (!r) continue;
    const badge = document.createElement('span');
    badge.className = 'regime-badge';
    badge.style.borderColor = COLORS[cur] || '#555';
    badge.innerHTML = `<span class="regime-currency text-${cur.toLowerCase()}">${cur}</span>` +
      `<span class="regime-name">${r.regime.replace(/_/g, ' ')}</span>`;
    container.appendChild(badge);
  }
}

// ---------------------------------------------------------------------------
// Tenor history chart
// ---------------------------------------------------------------------------

async function loadTenorHistory(currency, tenorYears) {
  const panel = document.getElementById('history-panel');
  panel.style.display = '';

  const title = document.getElementById('history-title');
  title.textContent = `${currency} ${tenorYears}yr History`;

  const [historyRes, regimesRes] = await Promise.all([
    fetchJSON(`/api/curves?currency=${currency}`),
    fetchJSON(`/api/regimes?currency=${currency}`),
  ]);

  const curves = historyRes ? historyRes.curves : [];
  const regimes = regimesRes ? regimesRes.regimes : [];

  const tenorData = curves
    .filter(c => c.tenor_years === tenorYears)
    .map(c => ({ date: new Date(c.date), yield_pct: c.yield_pct, dateStr: c.date }))
    .sort((a, b) => a.date - b.date);

  renderTenorChart(tenorData, regimes, currency);

  // Also show spreads panel
  showSpreadsPanel(currency);
}

function renderTenorChart(data, regimes, currency) {
  const container = document.getElementById('history-chart');
  container.innerHTML = '';

  if (data.length === 0) {
    container.innerHTML = '<div class="empty-message">No history</div>';
    return;
  }

  const rect = container.getBoundingClientRect();
  const margin = { top: 16, right: 20, bottom: 30, left: 50 };
  const width = rect.width - margin.left - margin.right;
  const height = rect.height - margin.top - margin.bottom;
  if (width <= 0 || height <= 0) return;

  const svg = d3.select(container)
    .append('svg')
    .attr('viewBox', `0 0 ${rect.width} ${rect.height}`)
    .attr('preserveAspectRatio', 'xMidYMid meet');

  const g = svg.append('g')
    .attr('transform', `translate(${margin.left},${margin.top})`);

  const x = d3.scaleTime()
    .domain(d3.extent(data, d => d.date))
    .range([0, width]);

  const yVals = data.map(d => d.yield_pct);
  const yPad = (d3.max(yVals) - d3.min(yVals)) * 0.1 || 0.5;
  const y = d3.scaleLinear()
    .domain([d3.min(yVals) - yPad, d3.max(yVals) + yPad])
    .range([height, 0]);

  // Regime shading bands
  if (regimes.length > 0) {
    const sorted = regimes
      .map(r => ({ date: new Date(r.date), regime: r.regime }))
      .sort((a, b) => a.date - b.date);

    for (let i = 0; i < sorted.length; i++) {
      const start = sorted[i].date;
      const end = i + 1 < sorted.length ? sorted[i + 1].date : data[data.length - 1].date;
      const color = REGIME_COLORS[sorted[i].regime] || REGIME_COLORS.normal;

      g.append('rect')
        .attr('x', x(start))
        .attr('y', 0)
        .attr('width', Math.max(0, x(end) - x(start)))
        .attr('height', height)
        .attr('fill', color)
        .attr('opacity', 0.5);
    }
  }

  // Grid + axes
  g.append('g').attr('class', 'grid').attr('transform', `translate(0,${height})`)
    .call(d3.axisBottom(x).tickSize(-height).tickFormat(''));
  g.append('g').attr('class', 'grid')
    .call(d3.axisLeft(y).tickSize(-width).tickFormat(''));

  g.append('g').attr('class', 'axis').attr('transform', `translate(0,${height})`)
    .call(d3.axisBottom(x).ticks(8));
  g.append('g').attr('class', 'axis')
    .call(d3.axisLeft(y).ticks(6).tickFormat(d => d.toFixed(1) + '%'));

  // Line
  const line = d3.line()
    .x(d => x(d.date))
    .y(d => y(d.yield_pct));

  g.append('path')
    .datum(data)
    .attr('fill', 'none')
    .attr('stroke', COLORS[currency] || '#999')
    .attr('stroke-width', 1.5)
    .attr('d', line);
}

// ---------------------------------------------------------------------------
// Spread time series (Task 6)
// ---------------------------------------------------------------------------

async function showSpreadsPanel(currency) {
  const panel = document.getElementById('spreads-panel');
  panel.style.display = '';

  const controlsEl = document.getElementById('spreads-controls');
  const chartsEl = document.getElementById('spreads-charts');
  controlsEl.innerHTML = '';
  chartsEl.innerHTML = '';

  // Slope selector
  const slopeSelect = document.createElement('select');
  slopeSelect.className = 'explorer-select';
  ['2s10s', '2s30s', '5s30s'].forEach(s => {
    const opt = document.createElement('option');
    opt.value = s; opt.textContent = s;
    slopeSelect.appendChild(opt);
  });

  const curSelect = document.createElement('select');
  curSelect.className = 'explorer-select';
  CURRENCY_ORDER.forEach(c => {
    const opt = document.createElement('option');
    opt.value = c; opt.textContent = c;
    if (c === currency) opt.selected = true;
    curSelect.appendChild(opt);
  });

  // Spread pair selector
  const pairSelect = document.createElement('select');
  pairSelect.className = 'explorer-select';
  ['USD-CAD', 'USD-EUR', 'EUR-GBP', 'CAD-GBP'].forEach(p => {
    const opt = document.createElement('option');
    opt.value = p; opt.textContent = p;
    pairSelect.appendChild(opt);
  });

  const loadBtn = document.createElement('button');
  loadBtn.className = 'btn-small';
  loadBtn.textContent = 'Load';
  loadBtn.addEventListener('click', () => {
    loadSpreadsCharts(curSelect.value, slopeSelect.value, pairSelect.value);
  });

  controlsEl.appendChild(slopeSelect);
  controlsEl.appendChild(curSelect);
  controlsEl.appendChild(pairSelect);
  controlsEl.appendChild(loadBtn);

  // Auto-load
  await loadSpreadsCharts(currency, '2s10s', 'USD-CAD');
}

async function loadSpreadsCharts(currency, slopeName, pair) {
  const chartsEl = document.getElementById('spreads-charts');
  chartsEl.innerHTML = '';

  const [slopesRes, spreadsRes, regimesRes] = await Promise.all([
    fetchJSON(`/api/analytics/slopes/history?currency=${currency}&slope_name=${slopeName}`),
    fetchJSON(`/api/analytics/spreads/history?pair=${pair}&tenor_years=10`),
    fetchJSON(`/api/regimes?currency=${currency}`),
  ]);

  const slopeData = slopesRes ? slopesRes.slopes : [];
  const spreadData = spreadsRes ? spreadsRes.spreads : [];
  const regimes = regimesRes ? regimesRes.regimes : [];

  // Create two side-by-side chart containers
  const row = document.createElement('div');
  row.className = 'spreads-row';

  const slopeDiv = document.createElement('div');
  slopeDiv.className = 'spread-chart-container';
  slopeDiv.id = 'slope-ts-chart';
  const slopeTitle = document.createElement('div');
  slopeTitle.className = 'spread-chart-title';
  slopeTitle.textContent = `${currency} ${slopeName}`;
  slopeDiv.appendChild(slopeTitle);
  const slopeChart = document.createElement('div');
  slopeChart.className = 'spread-chart-body';
  slopeDiv.appendChild(slopeChart);

  const spreadDiv = document.createElement('div');
  spreadDiv.className = 'spread-chart-container';
  spreadDiv.id = 'spread-ts-chart';
  const spreadTitle = document.createElement('div');
  spreadTitle.className = 'spread-chart-title';
  spreadTitle.textContent = `${pair} 10yr`;
  spreadDiv.appendChild(spreadTitle);
  const spreadChart = document.createElement('div');
  spreadChart.className = 'spread-chart-body';
  spreadDiv.appendChild(spreadChart);

  row.appendChild(slopeDiv);
  row.appendChild(spreadDiv);
  chartsEl.appendChild(row);

  // Render after DOM insertion for getBoundingClientRect
  requestAnimationFrame(() => {
    renderTimeSeriesChart(
      slopeChart,
      slopeData.map(s => ({ date: new Date(s.date), value: s.value_bp })),
      regimes, COLORS[currency] || '#999', 'bp'
    );
    renderTimeSeriesChart(
      spreadChart,
      spreadData.map(s => ({ date: new Date(s.date), value: s.spread_bp })),
      regimes, '#888', 'bp'
    );
  });
}

function renderTimeSeriesChart(container, data, regimes, color, unit) {
  if (data.length === 0) {
    container.innerHTML = '<div class="empty-message">No data</div>';
    return;
  }

  const rect = container.getBoundingClientRect();
  const margin = { top: 12, right: 16, bottom: 28, left: 46 };
  const width = rect.width - margin.left - margin.right;
  const height = rect.height - margin.top - margin.bottom;
  if (width <= 0 || height <= 0) return;

  const svg = d3.select(container)
    .append('svg')
    .attr('viewBox', `0 0 ${rect.width} ${rect.height}`)
    .attr('preserveAspectRatio', 'xMidYMid meet');

  const g = svg.append('g')
    .attr('transform', `translate(${margin.left},${margin.top})`);

  data.sort((a, b) => a.date - b.date);

  const x = d3.scaleTime()
    .domain(d3.extent(data, d => d.date))
    .range([0, width]);

  const vals = data.map(d => d.value);
  const maxAbs = Math.max(Math.abs(d3.min(vals)), Math.abs(d3.max(vals)), 1);
  const pad = maxAbs * 0.1;
  const y = d3.scaleLinear()
    .domain([d3.min(vals) - pad, d3.max(vals) + pad])
    .range([height, 0]);

  // Regime shading
  if (regimes && regimes.length > 0) {
    const sorted = regimes
      .map(r => ({ date: new Date(r.date), regime: r.regime }))
      .sort((a, b) => a.date - b.date);

    for (let i = 0; i < sorted.length; i++) {
      const start = sorted[i].date;
      const end = i + 1 < sorted.length ? sorted[i + 1].date : data[data.length - 1].date;
      const bgColor = REGIME_COLORS[sorted[i].regime] || REGIME_COLORS.normal;
      g.append('rect')
        .attr('x', x(start)).attr('y', 0)
        .attr('width', Math.max(0, x(end) - x(start)))
        .attr('height', height)
        .attr('fill', bgColor).attr('opacity', 0.5);
    }
  }

  // Zero line
  if (d3.min(vals) < 0 && d3.max(vals) > 0) {
    g.append('line')
      .attr('x1', 0).attr('x2', width)
      .attr('y1', y(0)).attr('y2', y(0))
      .attr('stroke', '#555').attr('stroke-dasharray', '3,3');
  }

  // Grid + axes
  g.append('g').attr('class', 'grid').attr('transform', `translate(0,${height})`)
    .call(d3.axisBottom(x).tickSize(-height).tickFormat(''));
  g.append('g').attr('class', 'grid')
    .call(d3.axisLeft(y).tickSize(-width).tickFormat(''));

  g.append('g').attr('class', 'axis').attr('transform', `translate(0,${height})`)
    .call(d3.axisBottom(x).ticks(6));
  g.append('g').attr('class', 'axis')
    .call(d3.axisLeft(y).ticks(5).tickFormat(d => Math.round(d) + unit));

  // Line
  const line = d3.line().x(d => x(d.date)).y(d => y(d.value));
  g.append('path')
    .datum(data)
    .attr('fill', 'none')
    .attr('stroke', color)
    .attr('stroke-width', 1.5)
    .attr('d', line);
}

// ---------------------------------------------------------------------------
// Init
// ---------------------------------------------------------------------------

document.addEventListener('DOMContentLoaded', init);
