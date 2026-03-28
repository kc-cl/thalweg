/* Thalweg — Analytics page (D3.js + vanilla fetch) */

const COLORS = {
  CAD: '#e74c3c',
  USD: '#3498db',
  EUR: '#f1c40f',
  GBP: '#2ecc71',
};

const REGIME_COLORS = {
  inverted:     '#e74c3c',
  flat:         '#95a5a6',
  bear_steep:   '#e67e22',
  bull_flat:    '#3498db',
  bear_flat:    '#9b59b6',
  bull_steep:   '#2ecc71',
  normal_steep: '#1abc9c',
  normal:       '#7f8c8d',
};

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
// State
// ---------------------------------------------------------------------------

let currentCurrency = 'USD';
let currentHorizon = 63;

// ---------------------------------------------------------------------------
// Init
// ---------------------------------------------------------------------------

document.addEventListener('DOMContentLoaded', () => {
  const currencySelect = document.getElementById('currency-select');
  currencySelect.addEventListener('change', () => {
    currentCurrency = currencySelect.value;
    loadAll();
  });

  document.querySelectorAll('.horizon-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('.horizon-btn').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      currentHorizon = parseInt(btn.dataset.horizon);
      loadFanAndForecast();
    });
  });

  loadAll();
});

// ---------------------------------------------------------------------------
// Data loading
// ---------------------------------------------------------------------------

function loadAll() {
  loadPhaseSpace();
  loadFanAndForecast();
}

function loadFanAndForecast() {
  loadFanChart();
  loadForecast();
}

async function loadPhaseSpace() {
  const [scoresRes, regimesRes] = await Promise.all([
    fetchJSON(`/api/analytics/pca/scores?currency=${currentCurrency}`),
    fetchJSON(`/api/regimes?currency=${currentCurrency}`),
  ]);

  const scores = scoresRes ? scoresRes.scores : [];
  const regimes = regimesRes ? regimesRes.regimes : [];

  renderPhaseSpace(scores, regimes);
}

async function loadFanChart() {
  const res = await fetchJSON(
    `/api/analytics/fan?currency=${currentCurrency}&horizon=${currentHorizon}`
  );

  const fan = res ? res.fan : [];
  const current = res ? res.current : [];

  renderFanChart(fan, current);
}

async function loadForecast() {
  const res = await fetchJSON(
    `/api/analytics/analogs?currency=${currentCurrency}&k=20&horizon=${currentHorizon}`
  );

  const analogs = res ? res.analogs : [];
  const forecasts = res ? res.forecasts : [];

  renderForecast(forecasts);
}

// ---------------------------------------------------------------------------
// Panel 1: Phase Space Portrait (PC1 vs PC2)
// ---------------------------------------------------------------------------

function renderPhaseSpace(scores, regimes) {
  const container = document.getElementById('phase-chart');
  container.innerHTML = '';

  // Remove any stale tooltip
  d3.select('.scatter-tooltip').remove();

  if (!scores || scores.length === 0) {
    container.innerHTML = '<div class="empty-message">No PCA data available</div>';
    return;
  }

  const rect = container.getBoundingClientRect();
  const margin = { top: 20, right: 20, bottom: 40, left: 50 };
  const width = rect.width - margin.left - margin.right;
  const height = rect.height - margin.top - margin.bottom;
  if (width <= 0 || height <= 0) return;

  // Build date -> regime lookup
  const regimeMap = {};
  if (regimes) {
    for (const r of regimes) {
      regimeMap[r.date] = r.regime;
    }
  }

  // Scales
  const pc1Vals = scores.map(s => s.pc1);
  const pc2Vals = scores.map(s => s.pc2);
  const pc1Pad = (d3.max(pc1Vals) - d3.min(pc1Vals)) * 0.1 || 1;
  const pc2Pad = (d3.max(pc2Vals) - d3.min(pc2Vals)) * 0.1 || 1;

  const x = d3.scaleLinear()
    .domain([d3.min(pc1Vals) - pc1Pad, d3.max(pc1Vals) + pc1Pad])
    .range([0, width]);

  const y = d3.scaleLinear()
    .domain([d3.min(pc2Vals) - pc2Pad, d3.max(pc2Vals) + pc2Pad])
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
    .call(d3.axisBottom(x).ticks(8));
  g.append('g').attr('class', 'axis')
    .call(d3.axisLeft(y).ticks(6));

  // Axis labels
  g.append('text').attr('class', 'axis-label')
    .attr('x', width / 2).attr('y', height + 35).attr('text-anchor', 'middle')
    .text('PC1 (Level)');
  g.append('text').attr('class', 'axis-label')
    .attr('transform', 'rotate(-90)').attr('x', -height / 2).attr('y', -40)
    .attr('text-anchor', 'middle').text('PC2 (Slope)');

  // Tooltip
  const tooltip = d3.select('body').append('div')
    .attr('class', 'scatter-tooltip')
    .style('opacity', 0);

  // Find latest date for "today" marker
  const sortedDates = scores.map(s => s.date).sort();
  const latestDate = sortedDates[sortedDates.length - 1];

  // Draw historical points
  g.selectAll('.phase-dot')
    .data(scores.filter(s => s.date !== latestDate))
    .join('circle')
    .attr('class', 'phase-dot')
    .attr('cx', d => x(d.pc1))
    .attr('cy', d => y(d.pc2))
    .attr('r', 3)
    .attr('fill', d => {
      const regime = regimeMap[d.date];
      return regime ? (REGIME_COLORS[regime] || '#7f8c8d') : '#555';
    })
    .attr('opacity', 0.6)
    .on('mouseover', function(event, d) {
      const regime = regimeMap[d.date] || 'unknown';
      tooltip.transition().duration(100).style('opacity', 1);
      tooltip.html(`<strong>${d.date}</strong><br>${regime.replace(/_/g, ' ')}`)
        .style('left', (event.pageX + 12) + 'px')
        .style('top', (event.pageY - 28) + 'px');
      d3.select(this).attr('r', 5).attr('opacity', 1);
    })
    .on('mouseout', function() {
      tooltip.transition().duration(200).style('opacity', 0);
      d3.select(this).attr('r', 3).attr('opacity', 0.6);
    });

  // Today marker (larger, with border)
  const today = scores.find(s => s.date === latestDate);
  if (today) {
    const todayRegime = regimeMap[today.date];
    const todayColor = todayRegime ? (REGIME_COLORS[todayRegime] || '#7f8c8d') : '#fff';

    g.append('circle')
      .attr('cx', x(today.pc1))
      .attr('cy', y(today.pc2))
      .attr('r', 8)
      .attr('fill', todayColor)
      .attr('stroke', '#fff')
      .attr('stroke-width', 2)
      .on('mouseover', function(event) {
        const regime = regimeMap[today.date] || 'unknown';
        tooltip.transition().duration(100).style('opacity', 1);
        tooltip.html(`<strong>${today.date} (Today)</strong><br>${regime.replace(/_/g, ' ')}`)
          .style('left', (event.pageX + 12) + 'px')
          .style('top', (event.pageY - 28) + 'px');
      })
      .on('mouseout', function() {
        tooltip.transition().duration(200).style('opacity', 0);
      });
  }

  // Legend for regime colors
  const usedRegimes = new Set();
  for (const s of scores) {
    const r = regimeMap[s.date];
    if (r) usedRegimes.add(r);
  }

  const regimeList = [...usedRegimes].sort();
  if (regimeList.length > 0) {
    const legendG = g.append('g')
      .attr('transform', `translate(${width - 120}, 0)`);

    regimeList.forEach((regime, i) => {
      const item = legendG.append('g')
        .attr('transform', `translate(0, ${i * 16})`);
      item.append('circle')
        .attr('r', 4)
        .attr('fill', REGIME_COLORS[regime] || '#555');
      item.append('text')
        .attr('x', 10)
        .attr('dy', '0.35em')
        .attr('fill', '#888')
        .attr('font-size', '10px')
        .text(regime.replace(/_/g, ' '));
    });
  }

  // Update header date
  document.getElementById('header-date').textContent = latestDate;
}

// ---------------------------------------------------------------------------
// Panel 2: Shock Fan Chart
// ---------------------------------------------------------------------------

function renderFanChart(fan, current) {
  const container = document.getElementById('fan-chart');
  container.innerHTML = '';

  if ((!fan || fan.length === 0) && (!current || current.length === 0)) {
    container.innerHTML = '<div class="empty-message">No fan chart data available</div>';
    return;
  }

  const rect = container.getBoundingClientRect();
  const margin = { top: 20, right: 20, bottom: 40, left: 50 };
  const width = rect.width - margin.left - margin.right;
  const height = rect.height - margin.top - margin.bottom;
  if (width <= 0 || height <= 0) return;

  // Collect all yield values for y-axis domain
  const allYields = [
    ...fan.map(d => d.yield_pct),
    ...current.map(d => d.yield_pct),
  ];
  const yMin = d3.min(allYields);
  const yMax = d3.max(allYields);
  const yPad = (yMax - yMin) * 0.1 || 0.5;

  const allTenors = [
    ...fan.map(d => d.tenor_years),
    ...current.map(d => d.tenor_years),
  ];

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

  // Build fan bands
  // Group fan data by quantile, then build paired bands
  const byQuantile = {};
  for (const d of fan) {
    const q = d.quantile;
    if (!byQuantile[q]) byQuantile[q] = [];
    byQuantile[q].push(d);
  }

  // Sort each quantile's data by tenor
  for (const q of Object.keys(byQuantile)) {
    byQuantile[q].sort((a, b) => a.tenor_years - b.tenor_years);
  }

  // Pair quantiles symmetrically around 0.5
  const quantiles = Object.keys(byQuantile).map(Number).sort((a, b) => a - b);
  const bandPairs = [];
  const lowerQuantiles = quantiles.filter(q => q < 0.5);
  const upperQuantiles = quantiles.filter(q => q > 0.5);

  // Match closest pairs from outer to inner
  for (const lo of lowerQuantiles) {
    const expected = 1 - lo;
    const hi = upperQuantiles.reduce((best, q) =>
      Math.abs(q - expected) < Math.abs(best - expected) ? q : best,
      upperQuantiles[0]
    );
    if (hi !== undefined) {
      const confidence = Math.round((hi - lo) * 100);
      bandPairs.push({ lo, hi, confidence });
    }
  }

  // Sort widest first (outer bands drawn first)
  bandPairs.sort((a, b) => b.confidence - a.confidence);

  const baseColor = COLORS[currentCurrency] || '#3498db';

  // Draw bands from widest to narrowest
  bandPairs.forEach((pair, i) => {
    const loData = byQuantile[pair.lo];
    const hiData = byQuantile[pair.hi];
    if (!loData || !hiData) return;

    // Build tenor-matched pairs
    const tenors = loData.map(d => d.tenor_years);
    const hiMap = {};
    for (const d of hiData) hiMap[d.tenor_years] = d.yield_pct;

    const bandData = tenors
      .filter(t => hiMap[t] !== undefined)
      .map(t => ({
        tenor_years: t,
        lo: loData.find(d => d.tenor_years === t).yield_pct,
        hi: hiMap[t],
      }));

    if (bandData.length === 0) return;

    // Opacity: widest band most transparent, narrowest most opaque
    const opacity = 0.12 + (bandPairs.length - 1 - i) * (0.25 / Math.max(bandPairs.length - 1, 1));

    const area = d3.area()
      .x(d => x(d.tenor_years))
      .y0(d => y(d.lo))
      .y1(d => y(d.hi))
      .curve(d3.curveMonotoneX);

    g.append('path')
      .datum(bandData)
      .attr('d', area)
      .attr('fill', baseColor)
      .attr('opacity', opacity);
  });

  // Draw median line from fan if available
  if (byQuantile[0.5]) {
    const medianLine = d3.line()
      .x(d => x(d.tenor_years))
      .y(d => y(d.yield_pct))
      .curve(d3.curveMonotoneX);

    g.append('path')
      .datum(byQuantile[0.5])
      .attr('d', medianLine)
      .attr('fill', 'none')
      .attr('stroke', baseColor)
      .attr('stroke-width', 1.5)
      .attr('stroke-dasharray', '4,3')
      .attr('opacity', 0.8);
  }

  // Draw current curve (solid white)
  if (current && current.length > 0) {
    const sorted = [...current].sort((a, b) => a.tenor_years - b.tenor_years);

    const line = d3.line()
      .x(d => x(d.tenor_years))
      .y(d => y(d.yield_pct))
      .curve(d3.curveMonotoneX);

    g.append('path')
      .datum(sorted)
      .attr('d', line)
      .attr('fill', 'none')
      .attr('stroke', '#e0e0e0')
      .attr('stroke-width', 2.5);

    // Data points on current curve
    g.selectAll('.current-dot')
      .data(sorted)
      .join('circle')
      .attr('cx', d => x(d.tenor_years))
      .attr('cy', d => y(d.yield_pct))
      .attr('r', 3)
      .attr('fill', '#e0e0e0');
  }

  // Band legend
  const legendG = g.append('g')
    .attr('transform', `translate(${width - 80}, 0)`);

  legendG.append('rect')
    .attr('x', -4).attr('y', -4)
    .attr('width', 84).attr('height', 18)
    .attr('fill', '#111118').attr('opacity', 0.8);
  legendG.append('line')
    .attr('x1', 0).attr('x2', 16).attr('y1', 6).attr('y2', 6)
    .attr('stroke', '#e0e0e0').attr('stroke-width', 2);
  legendG.append('text')
    .attr('x', 20).attr('y', 10)
    .attr('fill', '#888').attr('font-size', '10px')
    .text('Current');
}

// ---------------------------------------------------------------------------
// Panel 3: Conditional Forecast (Spaghetti)
// ---------------------------------------------------------------------------

function renderForecast(forecasts) {
  const container = document.getElementById('forecast-chart');
  container.innerHTML = '';

  if (!forecasts || forecasts.length === 0) {
    container.innerHTML = '<div class="empty-message">No forecast data available</div>';
    return;
  }

  const rect = container.getBoundingClientRect();
  const margin = { top: 20, right: 20, bottom: 40, left: 50 };
  const width = rect.width - margin.left - margin.right;
  const height = rect.height - margin.top - margin.bottom;
  if (width <= 0 || height <= 0) return;

  // Separate median from analog paths
  const medianPaths = forecasts.filter(d => d.is_median);
  const analogPaths = forecasts.filter(d => !d.is_median);

  // Also fetch current curve data from the fan endpoint (already loaded)
  // Use the analog data grouped by analog_date for spaghetti lines
  const byAnalog = d3.group(analogPaths, d => d.analog_date);
  const medianByDate = d3.group(medianPaths, d => d.future_date);

  // For spaghetti, each analog_date gives one curve (at a given future_date)
  // Group by future_date to get per-future-date curves, but we want per-analog curves
  // Each analog_date should give us tenor_years -> yield_pct

  // Collect all points for scales
  const allYields = forecasts.map(d => d.yield_pct);
  const allTenors = forecasts.map(d => d.tenor_years);

  const yMin = d3.min(allYields);
  const yMax = d3.max(allYields);
  const yPad = (yMax - yMin) * 0.1 || 0.5;

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

  const baseColor = COLORS[currentCurrency] || '#3498db';

  // Draw analog spaghetti lines
  for (const [analogDate, points] of byAnalog) {
    const sorted = [...points].sort((a, b) => a.tenor_years - b.tenor_years);

    g.append('path')
      .datum(sorted)
      .attr('d', line)
      .attr('fill', 'none')
      .attr('stroke', baseColor)
      .attr('stroke-width', 1)
      .attr('opacity', 0.25);
  }

  // Draw median path (dashed, full opacity)
  if (medianPaths.length > 0) {
    const medianSorted = [...medianPaths].sort((a, b) => a.tenor_years - b.tenor_years);

    g.append('path')
      .datum(medianSorted)
      .attr('d', line)
      .attr('fill', 'none')
      .attr('stroke', baseColor)
      .attr('stroke-width', 2.5)
      .attr('stroke-dasharray', '6,4');
  }

  // Legend
  const legendG = g.append('g')
    .attr('transform', `translate(${width - 120}, 0)`);

  // Background
  legendG.append('rect')
    .attr('x', -4).attr('y', -4)
    .attr('width', 128).attr('height', 38)
    .attr('fill', '#111118').attr('opacity', 0.8);

  // Analog line
  legendG.append('line')
    .attr('x1', 0).attr('x2', 16).attr('y1', 6).attr('y2', 6)
    .attr('stroke', baseColor).attr('stroke-width', 1).attr('opacity', 0.4);
  legendG.append('text')
    .attr('x', 20).attr('y', 10)
    .attr('fill', '#888').attr('font-size', '10px')
    .text('Analog paths');

  // Median line
  legendG.append('line')
    .attr('x1', 0).attr('x2', 16).attr('y1', 22).attr('y2', 22)
    .attr('stroke', baseColor).attr('stroke-width', 2).attr('stroke-dasharray', '4,2');
  legendG.append('text')
    .attr('x', 20).attr('y', 26)
    .attr('fill', '#888').attr('font-size', '10px')
    .text('Median forecast');
}

// ---------------------------------------------------------------------------
// Resize handler
// ---------------------------------------------------------------------------

let resizeTimer;
window.addEventListener('resize', () => {
  clearTimeout(resizeTimer);
  resizeTimer = setTimeout(() => loadAll(), 250);
});
