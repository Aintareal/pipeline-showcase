const DATA_URL = 'data.json';
const POLL_MS = 60000;
let lastGood = null;
let lastRows = {};

function themeTokens() {
  const s = getComputedStyle(document.documentElement);
  const v = (name) => s.getPropertyValue(name).trim();
  return {
    series1: v('--series-1'), series2: v('--series-2'),
    textSecondary: v('--text-secondary'), textMuted: v('--text-muted'),
    gridline: v('--gridline'), baseline: v('--baseline'), surface: v('--surface'),
  };
}

function hexToRgba(hex, alpha) {
  const h = hex.replace('#', '');
  const r = parseInt(h.substring(0, 2), 16), g = parseInt(h.substring(2, 4), 16), b = parseInt(h.substring(4, 6), 16);
  return `rgba(${r}, ${g}, ${b}, ${alpha})`;
}

function sharedScales(t) {
  return {
    x: { grid: { color: t.gridline, drawTicks: false }, border: { color: t.baseline }, ticks: { color: t.textMuted } },
    y: { grid: { color: t.gridline, drawTicks: false }, border: { display: false }, ticks: { color: t.textMuted }, beginAtZero: true },
  };
}

async function loadData() {
  const url = `${DATA_URL}?_=${Date.now()}`;
  const res = await fetch(url, { cache: 'no-store' });
  if (!res.ok) throw new Error(`fetch failed: ${res.status}`);
  return res.json();
}

function freshnessLabel(generatedAt) {
  if (!generatedAt) return { text: 'No data yet', cls: 'unknown' };
  const ageMin = (Date.now() - new Date(generatedAt).getTime()) / 60000;
  if (ageMin < 15) return { text: 'Live', cls: 'live' };
  if (ageMin < 120) return { text: 'Stale', cls: 'stale' };
  return { text: 'Unknown', cls: 'unknown' };
}

function renderTiles(d) {
  const s = d.summary;
  const topItem = d.top_items[0] || { item_name: '—', order_count: 0 };
  const tiles = [
    ['Total Orders', s.order_count_cumulative],
    ['Orders (24h)', s.order_count_rolling_24h],
    ['Total Amount', `$${(s.total_amount_cumulative ?? 0).toLocaleString()}`],
    ['Avg Order Value', `$${(s.avg_order_value_cumulative ?? 0).toFixed(2)}`],
    ['Most Popular Item', `${topItem.item_name} (${topItem.order_count})`],
    ['Rejection Rate', `${(s.rejection_rate_cumulative * 100).toFixed(1)}%`],
  ];
  document.getElementById('stat-tiles').innerHTML = tiles.map(([label, value], i) =>
    `<div class="tile" style="animation-delay:${i * 40}ms"><div class="value">${value}</div><div class="label">${label}</div></div>`
  ).join('');
}

function renderTable(id, headers, rows) {
  const el = document.getElementById(id);
  el.innerHTML = `<table><thead><tr>${headers.map(h => `<th>${h}</th>`).join('')}</tr></thead>` +
    `<tbody>${rows.map(r => `<tr>${r.map(c => `<td>${c}</td>`).join('')}</tr>`).join('')}</tbody></table>`;
}

let hourlyChart, dailyChart, leaderboardChart, scd2Chart;

function renderCharts(d) {
  const t = themeTokens();
  const hourlyLabels = d.hourly_volume_24h.map(r => new Date(r.hour).toLocaleTimeString([], { hour: '2-digit' }));
  const hourlyData = d.hourly_volume_24h.map(r => r.count);
  const dailyLabels = d.daily_volume_all_time.map(r => r.date);
  const dailyData = d.daily_volume_all_time.map(r => r.count);
  const itemLabels = d.top_items.map(r => r.item_name);
  const itemData = d.top_items.map(r => r.order_count);

  lastRows.hourly = hourlyLabels.map((l, i) => [l, hourlyData[i]]);
  lastRows.daily = dailyLabels.map((l, i) => [l, dailyData[i]]);
  lastRows.leaderboard = itemLabels.map((l, i) => [l, itemData[i]]);
  lastRows.scd2 = [['Active', d.scd2.active_rows], ['Superseded', d.scd2.superseded_rows]];
  for (const [id, headers] of [['hourly', ['Hour', 'Orders']], ['daily', ['Date', 'Orders']],
                                 ['leaderboard', ['Item', 'Orders']], ['scd2', ['State', 'Rows']]]) {
    renderTable(`${id}-table`, headers, lastRows[id]);
  }

  const commonBar = { borderRadius: 4, borderSkipped: false, maxBarThickness: 24 };
  if (!hourlyChart) {
    hourlyChart = new Chart(document.getElementById('hourly-chart'), {
      type: 'bar',
      data: { labels: hourlyLabels, datasets: [{ data: hourlyData, backgroundColor: t.series1, ...commonBar }] },
      options: { plugins: { legend: { display: false } }, scales: sharedScales(t), maintainAspectRatio: false },
    });
    dailyChart = new Chart(document.getElementById('daily-chart'), {
      type: 'line',
      data: { labels: dailyLabels, datasets: [{
        data: dailyData, borderColor: t.series1, backgroundColor: hexToRgba(t.series1, 0.1),
        fill: true, tension: 0.35, borderWidth: 2, pointRadius: 4, pointBackgroundColor: t.series1,
        pointBorderColor: t.surface, pointBorderWidth: 2,
      }] },
      options: { plugins: { legend: { display: false } }, scales: sharedScales(t), maintainAspectRatio: false },
    });
    leaderboardChart = new Chart(document.getElementById('leaderboard-chart'), {
      type: 'bar',
      data: { labels: itemLabels, datasets: [{ data: itemData, backgroundColor: t.series1, ...commonBar }] },
      options: { indexAxis: 'y', plugins: { legend: { display: false } }, scales: sharedScales(t), maintainAspectRatio: false },
    });
    scd2Chart = new Chart(document.getElementById('scd2-chart'), {
      type: 'bar',
      data: { labels: ['Silver rows'], datasets: [
        { label: 'Active', data: [d.scd2.active_rows], backgroundColor: t.series1, borderRadius: 4, borderSkipped: false },
        { label: 'Superseded', data: [d.scd2.superseded_rows], backgroundColor: t.series2, borderRadius: 4, borderSkipped: false },
      ] },
      options: {
        indexAxis: 'y',
        scales: { x: { ...sharedScales(t).x, stacked: true }, y: { ...sharedScales(t).y, stacked: true, beginAtZero: undefined } },
        plugins: { legend: { position: 'bottom', labels: { color: t.textSecondary, boxWidth: 12 } } },
        maintainAspectRatio: false,
      },
    });
  } else {
    hourlyChart.data.labels = hourlyLabels; hourlyChart.data.datasets[0].data = hourlyData; hourlyChart.update();
    dailyChart.data.labels = dailyLabels; dailyChart.data.datasets[0].data = dailyData; dailyChart.update();
    leaderboardChart.data.labels = itemLabels; leaderboardChart.data.datasets[0].data = itemData; leaderboardChart.update();
    scd2Chart.data.datasets[0].data = [d.scd2.active_rows]; scd2Chart.data.datasets[1].data = [d.scd2.superseded_rows]; scd2Chart.update();
  }
}

function restyleCharts() {
  if (!hourlyChart) return;
  const t = themeTokens();
  const scales = sharedScales(t);
  for (const chart of [hourlyChart, dailyChart, leaderboardChart, scd2Chart]) {
    chart.options.scales.x = { ...chart.options.scales.x, ...scales.x };
    chart.options.scales.y = { ...chart.options.scales.y, ...scales.y };
  }
  hourlyChart.data.datasets[0].backgroundColor = t.series1;
  leaderboardChart.data.datasets[0].backgroundColor = t.series1;
  dailyChart.data.datasets[0].borderColor = t.series1;
  dailyChart.data.datasets[0].backgroundColor = hexToRgba(t.series1, 0.1);
  dailyChart.data.datasets[0].pointBackgroundColor = t.series1;
  dailyChart.data.datasets[0].pointBorderColor = t.surface;
  scd2Chart.data.datasets[0].backgroundColor = t.series1;
  scd2Chart.data.datasets[1].backgroundColor = t.series2;
  if (scd2Chart.options.plugins.legend.labels) scd2Chart.options.plugins.legend.labels.color = t.textSecondary;
  for (const chart of [hourlyChart, dailyChart, leaderboardChart, scd2Chart]) chart.update();
}

async function refresh() {
  try {
    const d = await loadData();
    lastGood = d;
    renderTiles(d);
    renderCharts(d);
    const { text, cls } = freshnessLabel(d.generated_at);
    const badge = document.getElementById('freshness-badge');
    badge.className = `badge ${cls}`;
    badge.querySelector('.badge-text').textContent = text;
  } catch (e) {
    if (lastGood) {
      const badge = document.getElementById('freshness-badge');
      badge.className = 'badge unknown';
      badge.querySelector('.badge-text').textContent = "Couldn't refresh — showing last known data";
    }
    console.error(e);
  }
}

function setTheme(mode) {
  document.documentElement.setAttribute('data-theme', mode);
  localStorage.setItem('theme', mode);
  restyleCharts();
}

document.getElementById('theme-toggle').addEventListener('click', () => {
  const current = document.documentElement.getAttribute('data-theme')
    || (matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light');
  setTheme(current === 'dark' ? 'light' : 'dark');
});

document.querySelectorAll('.view-toggle').forEach(btn => {
  btn.addEventListener('click', () => {
    const id = btn.dataset.target;
    const card = btn.closest('.card');
    const canvas = card.querySelector('.chart-wrap');
    const table = document.getElementById(`${id}-table`);
    const showingTable = !table.hidden;
    table.hidden = showingTable;
    canvas.hidden = !showingTable;
    btn.textContent = showingTable ? 'View as table' : 'View as chart';
  });
});

document.getElementById('refresh-btn').addEventListener('click', refresh);
document.addEventListener('DOMContentLoaded', refresh);
setInterval(refresh, POLL_MS);
