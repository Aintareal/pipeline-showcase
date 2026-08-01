const DATA_URL = 'data.json';
const POLL_MS = 60000;
let lastGood = null;

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
  document.getElementById('stat-tiles').innerHTML = tiles.map(([label, value]) =>
    `<div class="tile"><div class="value">${value}</div><div class="label">${label}</div></div>`
  ).join('');
}

let hourlyChart, dailyChart, leaderboardChart, scd2Chart;

function renderCharts(d) {
  const hourlyLabels = d.hourly_volume_24h.map(r => new Date(r.hour).toLocaleTimeString([], { hour: '2-digit' }));
  const hourlyData = d.hourly_volume_24h.map(r => r.count);
  const dailyLabels = d.daily_volume_all_time.map(r => r.date);
  const dailyData = d.daily_volume_all_time.map(r => r.count);
  const itemLabels = d.top_items.map(r => r.item_name);
  const itemData = d.top_items.map(r => r.order_count);

  if (!hourlyChart) {
    hourlyChart = new Chart(document.getElementById('hourly-chart'), {
      type: 'bar', data: { labels: hourlyLabels, datasets: [{ data: hourlyData, backgroundColor: '#2a78d6' }] },
      options: { plugins: { legend: { display: false } } },
    });
    dailyChart = new Chart(document.getElementById('daily-chart'), {
      type: 'line', data: { labels: dailyLabels, datasets: [{ data: dailyData, borderColor: '#2a78d6', backgroundColor: 'rgba(42,120,214,0.1)', fill: true }] },
      options: { plugins: { legend: { display: false } } },
    });
    leaderboardChart = new Chart(document.getElementById('leaderboard-chart'), {
      type: 'bar', data: { labels: itemLabels, datasets: [{ data: itemData, backgroundColor: '#3987e5' }] },
      options: { indexAxis: 'y', plugins: { legend: { display: false } } },
    });
    scd2Chart = new Chart(document.getElementById('scd2-chart'), {
      type: 'bar',
      data: { labels: ['Silver rows'], datasets: [
        { label: 'Active', data: [d.scd2.active_rows], backgroundColor: '#2a78d6' },
        { label: 'Superseded', data: [d.scd2.superseded_rows], backgroundColor: '#eb6834' },
      ] },
      options: { indexAxis: 'y', scales: { x: { stacked: true }, y: { stacked: true } } },
    });
  } else {
    hourlyChart.data.labels = hourlyLabels; hourlyChart.data.datasets[0].data = hourlyData; hourlyChart.update();
    dailyChart.data.labels = dailyLabels; dailyChart.data.datasets[0].data = dailyData; dailyChart.update();
    leaderboardChart.data.labels = itemLabels; leaderboardChart.data.datasets[0].data = itemData; leaderboardChart.update();
    scd2Chart.data.datasets[0].data = [d.scd2.active_rows]; scd2Chart.data.datasets[1].data = [d.scd2.superseded_rows]; scd2Chart.update();
  }
}

async function refresh() {
  try {
    const d = await loadData();
    lastGood = d;
    renderTiles(d);
    renderCharts(d);
    const { text, cls } = freshnessLabel(d.generated_at);
    const badge = document.getElementById('freshness-badge');
    badge.textContent = text; badge.className = `badge ${cls}`;
  } catch (e) {
    if (lastGood) {
      const badge = document.getElementById('freshness-badge');
      badge.textContent = 'Couldn\'t refresh — showing last known data';
      badge.className = 'badge unknown';
    }
    console.error(e);
  }
}

document.getElementById('refresh-btn').addEventListener('click', refresh);
document.addEventListener('DOMContentLoaded', refresh);
setInterval(refresh, POLL_MS);
