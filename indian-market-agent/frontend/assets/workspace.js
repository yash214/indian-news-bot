'use strict';

const query = new URLSearchParams(window.location.search);
const state = {
  symbol: 'NIFTY',
  interval: '5m',
  range: '1d',
  useMock: query.get('mock') === 'true',
  activeSidebarModule: 'watchlist',
  activeDrawer: 'agents',
  summary: null,
  candles: [],
  overlays: null,
  newsLookbackHours: 24,
  newsFilter: 'all',
  newsArticles: [],
  newsAgentReport: null,
  newsLoading: false,
  newsError: '',
  terminalTab: 'pnl',
  chart: null,
  candleSeries: null,
  overlaySeries: [],
  priceLines: [],
  resizeObserver: null,
};

const ui = {};

document.addEventListener('DOMContentLoaded', () => {
  Object.assign(ui, {
    sidebar: document.getElementById('workspace-sidebar'),
    sidebarToggle: document.getElementById('sidebar-toggle'),
    sidebarSelection: document.getElementById('sidebar-selection'),
    sidebarWatchlist: document.getElementById('sidebar-watchlist'),
    nav: document.getElementById('workspace-nav'),
    marketStrip: document.getElementById('market-strip'),
    systemHealthChip: document.getElementById('system-health-chip'),
    proposalChip: document.getElementById('proposal-chip'),
    symbolTabs: document.getElementById('symbol-tabs'),
    intervalTabs: document.getElementById('interval-tabs'),
    mockToggle: document.getElementById('mock-toggle'),
    refreshWorkspace: document.getElementById('refresh-workspace'),
    chartStatus: document.getElementById('chart-status'),
    chartContainer: document.getElementById('main-chart'),
    drawer: document.getElementById('insights-drawer'),
    drawerKicker: document.getElementById('drawer-kicker'),
    drawerTitle: document.getElementById('drawer-title'),
    drawerToggle: document.getElementById('drawer-toggle'),
    agentGrid: document.getElementById('right-drawer-content'),
    terminalTabs: document.getElementById('terminal-tabs'),
    terminalBody: document.getElementById('terminal-body'),
  });
  bindEvents();
  renderStaticPanels();
  updateMockButton();
  initChart();
  refreshWorkspace();
  window.setInterval(refreshWorkspace, 60000);
});

function bindEvents() {
  ui.sidebarToggle.addEventListener('click', () => {
    ui.sidebar.classList.toggle('collapsed');
    ui.sidebarToggle.textContent = ui.sidebar.classList.contains('collapsed') ? '›' : '‹';
  });
  ui.drawerToggle.addEventListener('click', () => {
    ui.drawer.classList.toggle('collapsed');
    ui.drawerToggle.textContent = ui.drawer.classList.contains('collapsed') ? '‹' : '›';
    resizeChartSoon();
  });
  ui.nav.addEventListener('click', event => {
    const button = event.target.closest('.nav-item');
    if (!button) return;
    activateSidebarModule(button.dataset.panel || 'watchlist');
  });
  ui.symbolTabs.addEventListener('click', event => {
    const button = event.target.closest('button[data-symbol]');
    if (!button) return;
    state.symbol = button.dataset.symbol;
    ui.symbolTabs.querySelectorAll('button').forEach(item => item.classList.toggle('active', item === button));
    refreshWorkspace();
  });
  ui.intervalTabs.addEventListener('click', event => {
    const button = event.target.closest('button[data-interval]');
    if (!button) return;
    state.interval = button.dataset.interval;
    state.range = state.interval === '1d' ? '3m' : '1d';
    ui.intervalTabs.querySelectorAll('button').forEach(item => item.classList.toggle('active', item === button));
    refreshChart();
  });
  ui.mockToggle.addEventListener('click', () => {
    state.useMock = !state.useMock;
    updateMockButton();
    refreshWorkspace();
  });
  ui.refreshWorkspace.addEventListener('click', refreshWorkspace);
  ui.terminalTabs.addEventListener('click', event => {
    const button = event.target.closest('button[data-tab]');
    if (!button) return;
    state.terminalTab = button.dataset.tab;
    ui.terminalTabs.querySelectorAll('button').forEach(item => item.classList.toggle('active', item === button));
    renderTerminal();
  });
}

function activateSidebarModule(module) {
  state.activeSidebarModule = module || 'watchlist';
  ui.nav.querySelectorAll('.nav-item').forEach(item => {
    item.classList.toggle('active', item.dataset.panel === state.activeSidebarModule);
  });
  const activeButton = navButtonForModule(state.activeSidebarModule);
  ui.sidebarSelection.textContent = activeButton ? activeButton.textContent.trim() : 'Workspace';

  if (state.activeSidebarModule === 'news') {
    state.activeDrawer = 'news';
    openRightDrawer();
    renderRightDrawer();
    loadNewsCenter();
    return;
  }
  if (state.activeSidebarModule === 'agents') {
    state.activeDrawer = 'agents';
    renderRightDrawer();
    return;
  }
  if (state.activeSidebarModule === 'macro') {
    state.activeDrawer = 'macro';
    renderRightDrawer();
    return;
  }
  if (state.activeSidebarModule === 'fo') {
    state.activeDrawer = 'fo';
    renderRightDrawer();
    return;
  }
  if (state.activeSidebarModule === 'execution') {
    state.activeDrawer = 'execution';
    renderRightDrawer();
    return;
  }
  state.activeDrawer = state.activeSidebarModule === 'watchlist' ? 'agents' : 'placeholder';
  renderRightDrawer();
}

function openRightDrawer() {
  ui.drawer.classList.remove('collapsed');
  ui.drawerToggle.textContent = '›';
  resizeChartSoon();
}

function navButtonForModule(module) {
  return Array.from(ui.nav.querySelectorAll('.nav-item')).find(item => item.dataset.panel === module);
}

async function refreshWorkspace() {
  await Promise.all([loadWorkspaceSummary(), refreshChart()]);
  if (state.activeDrawer === 'news') {
    await loadNewsCenter();
  }
}

async function refreshChart() {
  setChartStatus('Loading chart...');
  try {
    let candlePayload = await fetchChartCandles(state.symbol, state.interval, state.range, state.useMock);
    if ((!candlePayload.candles || candlePayload.candles.length === 0) && !state.useMock) {
      candlePayload = await fetchChartCandles(state.symbol, state.interval, state.range, true);
      candlePayload.warnings = ['Live chart data unavailable; mock candles shown for workspace preview.'];
    }
    const overlayPayload = await fetchChartOverlays(state.symbol, state.interval, state.useMock || candlePayload.source === 'mock');
    state.candles = candlePayload.candles || [];
    state.overlays = overlayPayload.overlays || {};
    applyCandles(state.candles);
    applyOverlayLines(state.overlays);
    const warnings = [...(candlePayload.warnings || []), ...(overlayPayload.warnings || [])];
    setChartStatus(warnings.length ? warnings.join(' ') : '', !warnings.length);
  } catch (error) {
    setChartStatus(error.message || 'Chart failed to load.');
  }
}

async function loadWorkspaceSummary() {
  try {
    state.summary = await fetchWorkspaceSummary(state.symbol, state.useMock);
    if (!state.useMock && summaryNeedsMarketFallback(state.summary)) {
      const fallback = await fetchWorkspaceSummary(state.symbol, true);
      state.summary = {
        ...state.summary,
        market_bar: {
          ...(fallback.market_bar || {}),
          market_status: ((state.summary.market_bar || {}).market_status || (fallback.market_bar || {}).market_status),
          last_updated: ((state.summary.market_bar || {}).last_updated || (fallback.market_bar || {}).last_updated),
        },
        warnings: [...((state.summary && state.summary.warnings) || []), 'Live market strip unavailable; preview market values shown.'],
      };
    }
    renderMarketBar();
    renderRightDrawer();
    renderTerminal();
  } catch (error) {
    state.summary = { agents: {}, strategy_suggestions: [], market_bar: {}, warnings: [error.message] };
    renderMarketBar();
    renderRightDrawer();
    renderTerminal();
  }
}

async function loadNewsCenter() {
  state.newsLoading = true;
  state.newsError = '';
  renderRightDrawer();
  try {
    const [report, articlePayload] = await Promise.all([
      fetchNewsAgentReport(state.symbol, state.newsLookbackHours),
      fetchNewsAgentArticles(state.newsLookbackHours),
    ]);
    state.newsAgentReport = shouldUseMockNewsReport(report) ? mockNewsReport() : report;
    const articles = normalizeNewsArticles(articlePayload);
    state.newsArticles = state.useMock && !articles.length ? mockNewsArticles() : articles;
  } catch (error) {
    state.newsError = error.message || 'News Center failed to load.';
    if (state.useMock) {
      state.newsAgentReport = mockNewsReport();
      state.newsArticles = mockNewsArticles();
      state.newsError = '';
    }
  } finally {
    state.newsLoading = false;
    renderRightDrawer();
  }
}

async function refreshNewsArticles() {
  state.newsLoading = true;
  state.newsError = '';
  renderRightDrawer();
  try {
    if (!state.useMock) {
      await getJson('/api/news');
    }
  } catch (_) {
    // The News Center can still use stored News Agent article analyses.
  }
  await loadNewsCenter();
}

async function runNewsAgent() {
  state.newsLoading = true;
  state.newsError = '';
  renderRightDrawer();
  try {
    const report = await fetchNewsAgentReport(state.symbol, state.newsLookbackHours);
    state.newsAgentReport = shouldUseMockNewsReport(report) ? mockNewsReport() : report;
    const articlePayload = await fetchNewsAgentArticles(state.newsLookbackHours);
    const articles = normalizeNewsArticles(articlePayload);
    state.newsArticles = state.useMock && !articles.length ? mockNewsArticles() : articles;
  } catch (error) {
    state.newsError = error.message || 'News Agent run failed.';
    if (state.useMock) {
      state.newsAgentReport = mockNewsReport();
      state.newsArticles = mockNewsArticles();
      state.newsError = '';
    }
  } finally {
    state.newsLoading = false;
    renderRightDrawer();
  }
}

function initChart() {
  if (!window.LightweightCharts || !ui.chartContainer) {
    setChartStatus('Chart renderer unavailable. Backend data panels remain available.');
    return;
  }
  const options = {
    autoSize: true,
    layout: {
      background: { color: '#0b1320' },
      textColor: '#9aa8bd',
    },
    grid: {
      vertLines: { color: '#182335' },
      horzLines: { color: '#182335' },
    },
    rightPriceScale: { borderColor: '#263348' },
    timeScale: { borderColor: '#263348', timeVisible: true, secondsVisible: false },
    crosshair: { mode: LightweightCharts.CrosshairMode ? LightweightCharts.CrosshairMode.Normal : 0 },
  };
  state.chart = LightweightCharts.createChart(ui.chartContainer, options);
  state.candleSeries = addCandleSeries(state.chart);
  if (window.ResizeObserver) {
    state.resizeObserver = new ResizeObserver(resizeChartSoon);
    state.resizeObserver.observe(ui.chartContainer);
  } else {
    window.addEventListener('resize', resizeChartSoon);
  }
}

function addCandleSeries(chart) {
  const options = {
    upColor: '#20c987',
    downColor: '#ef5c6c',
    borderUpColor: '#20c987',
    borderDownColor: '#ef5c6c',
    wickUpColor: '#20c987',
    wickDownColor: '#ef5c6c',
  };
  if (chart.addSeries && LightweightCharts.CandlestickSeries) {
    return chart.addSeries(LightweightCharts.CandlestickSeries, options);
  }
  return chart.addCandlestickSeries(options);
}

function addLineSeries(chart, options) {
  if (chart.addSeries && LightweightCharts.LineSeries) {
    return chart.addSeries(LightweightCharts.LineSeries, options);
  }
  return chart.addLineSeries(options);
}

function applyCandles(candles) {
  if (!state.candleSeries) return;
  if (!candles.length) {
    state.candleSeries.setData([]);
    return;
  }
  state.candleSeries.setData(candles.map(candle => ({
    time: candle.time,
    open: Number(candle.open),
    high: Number(candle.high),
    low: Number(candle.low),
    close: Number(candle.close),
  })));
  if (state.chart && state.chart.timeScale) {
    state.chart.timeScale().fitContent();
  }
}

function applyOverlayLines(overlays) {
  if (!state.chart || !state.candleSeries) return;
  clearOverlays();
  const overlayConfig = [
    ['vwap', '#f4b942', 'VWAP'],
    ['ema_9', '#4f8cff', 'EMA 9'],
    ['ema_21', '#b784ff', 'EMA 21'],
  ];
  overlayConfig.forEach(([key, color, label]) => {
    const data = Array.isArray(overlays[key]) ? overlays[key] : [];
    if (!data.length) return;
    const series = addLineSeries(state.chart, {
      color,
      lineWidth: 2,
      priceLineVisible: false,
      lastValueVisible: true,
      title: label,
    });
    series.setData(data.map(item => ({ time: item.time, value: Number(item.value) })).filter(item => Number.isFinite(item.value)));
    state.overlaySeries.push(series);
  });
  const opening = overlays.opening_range || {};
  if (opening.high) addPriceLine(Number(opening.high), 'Opening Range High', '#f4b942');
  if (opening.low) addPriceLine(Number(opening.low), 'Opening Range Low', '#f4b942');
  (overlays.support_zones || []).forEach(zone => {
    if (zone.high) addPriceLine(Number(zone.high), `${zone.label || 'Support'} ${zone.strength || ''}`, '#20c987');
    if (zone.low) addPriceLine(Number(zone.low), 'Support Low', '#1c8c66');
  });
  (overlays.resistance_zones || []).forEach(zone => {
    if (zone.low) addPriceLine(Number(zone.low), `${zone.label || 'Resistance'} ${zone.strength || ''}`, '#ef5c6c');
    if (zone.high) addPriceLine(Number(zone.high), 'Resistance High', '#b94350');
  });
  (overlays.price_lines || []).forEach(line => {
    if (line.price) addPriceLine(Number(line.price), line.label || 'Level', '#3ec9d6');
  });
}

function addPriceLine(price, title, color) {
  if (!Number.isFinite(price) || !state.candleSeries.createPriceLine) return;
  const line = state.candleSeries.createPriceLine({
    price,
    color,
    lineWidth: 1,
    lineStyle: LightweightCharts.LineStyle ? LightweightCharts.LineStyle.Dashed : 2,
    axisLabelVisible: true,
    title,
  });
  state.priceLines.push(line);
}

function clearOverlays() {
  state.priceLines.forEach(line => {
    try { state.candleSeries.removePriceLine(line); } catch (_) {}
  });
  state.priceLines = [];
  state.overlaySeries.forEach(series => {
    try { state.chart.removeSeries(series); } catch (_) {}
  });
  state.overlaySeries = [];
}

function resizeChartSoon() {
  window.requestAnimationFrame(() => {
    if (!state.chart || !ui.chartContainer) return;
    const rect = ui.chartContainer.getBoundingClientRect();
    if (rect.width > 0 && rect.height > 0 && state.chart.resize) {
      state.chart.resize(Math.floor(rect.width), Math.floor(rect.height));
    }
  });
}

async function fetchWorkspaceSummary(symbol, mock) {
  return getJson(`/api/workspace/summary?symbol=${encodeURIComponent(symbol)}&mock=${mock ? 'true' : 'false'}`);
}

async function fetchChartCandles(symbol, interval, range, mock) {
  return getJson(`/api/chart/candles?symbol=${encodeURIComponent(symbol)}&interval=${encodeURIComponent(interval)}&range=${encodeURIComponent(range)}&mock=${mock ? 'true' : 'false'}`);
}

async function fetchChartOverlays(symbol, interval, mock) {
  return getJson(`/api/chart/overlays?symbol=${encodeURIComponent(symbol)}&interval=${encodeURIComponent(interval)}&mock=${mock ? 'true' : 'false'}`);
}

async function fetchNewsAgentReport(index, lookbackHours) {
  return getJson(`/api/news/agent/report?index=${encodeURIComponent(index)}&lookback_hours=${encodeURIComponent(lookbackHours)}`);
}

async function fetchNewsAgentArticles(lookbackHours) {
  return getJson(`/api/news/agent/articles?lookback_hours=${encodeURIComponent(lookbackHours)}`);
}

async function getJson(url) {
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`Request failed: ${response.status}`);
  }
  return response.json();
}

function renderStaticPanels() {
  ui.sidebarWatchlist.innerHTML = ['NIFTY', 'SENSEX', 'INDIA VIX', 'USD/INR'].map(label => (
    `<div class="mini-row"><span>${escapeHtml(label)}</span><span class="neutral">Watch</span></div>`
  )).join('');
}

function renderMarketBar() {
  const bar = (state.summary && state.summary.market_bar) || {};
  const items = [
    ['NIFTY', bar.nifty],
    ['SENSEX', bar.sensex],
    ['INDIA VIX', bar.india_vix],
    ['USD/INR', bar.usd_inr],
    ['GOLD', bar.gold],
    ['CRUDE', bar.crude],
    ['MARKET', { price: bar.market_status || 'UNKNOWN', pct: null }],
  ];
  ui.marketStrip.innerHTML = items.map(([label, quote]) => marketTile(label, quote || {})).join('');
  const health = getAgent('execution_health_agent');
  const overall = String(health.overall_health || 'UNKNOWN');
  ui.systemHealthChip.textContent = `System Health: ${overall}`;
  ui.systemHealthChip.className = `status-chip ${overall.toLowerCase()}`;
  const proposalAllowed = health.strategy_engine_guidance
    ? Boolean(health.strategy_engine_guidance.allow_trade_proposal)
    : !health.fresh_trade_blocked && overall === 'HEALTHY';
  ui.proposalChip.textContent = `Trade Proposal: ${proposalAllowed ? 'Allowed' : 'Blocked'}`;
  ui.proposalChip.className = `status-chip ${proposalAllowed ? 'healthy' : 'unhealthy'}`;
}

function summaryNeedsMarketFallback(summary) {
  const bar = (summary && summary.market_bar) || {};
  return ['nifty', 'sensex', 'india_vix'].some(key => !quoteHasDisplayValue(bar[key] || {}));
}

function quoteHasDisplayValue(quote) {
  return ['price', 'close', 'value', 'last'].some(key => {
    const number = Number(quote && quote[key]);
    return Number.isFinite(number);
  });
}

function marketTile(label, quote) {
  const price = quote.price ?? quote.close ?? '--';
  const pct = Number(quote.pct);
  const tone = Number.isFinite(pct) ? (pct >= 0 ? 'positive' : 'negative') : 'neutral';
  return `
    <div class="market-tile">
      <div class="market-label">${escapeHtml(label)}</div>
      <div class="market-value">${formatNumber(price)}</div>
      <div class="market-change ${tone}">${Number.isFinite(pct) ? signed(pct) : '--'}</div>
    </div>
  `;
}

function renderRightDrawer() {
  if (!ui.agentGrid) return;
  if (state.activeDrawer === 'news') {
    renderNewsDrawer();
    return;
  }
  if (state.activeDrawer === 'macro') {
    renderFocusedAgentDrawer('Macro Agent', getAgent('macro_agent'), 'Macro Context');
    return;
  }
  if (state.activeDrawer === 'fo') {
    renderFocusedAgentDrawer('F&O Structure', getAgent('fo_structure_agent'), 'Options Context');
    return;
  }
  if (state.activeDrawer === 'execution') {
    renderFocusedAgentDrawer('Execution Health', getAgent('execution_health_agent'), 'System Gate');
    return;
  }
  if (state.activeDrawer === 'placeholder') {
    renderPlaceholderDrawer();
    return;
  }
  renderAgentDrawer();
}

function setDrawerHeader(kicker, title) {
  ui.drawerKicker.textContent = kicker;
  ui.drawerTitle.textContent = title;
}

function renderAgentDrawer() {
  setDrawerHeader('Agent Insights', 'Live Context');
  ui.agentGrid.className = 'agent-card-grid';
  const cards = [
    ['News Agent', getAgent('news_agent')],
    ['Macro Agent', getAgent('macro_agent')],
    ['Market Regime', getAgent('market_regime_agent')],
    ['F&O Structure', getAgent('fo_structure_agent')],
    ['Execution Health', getAgent('execution_health_agent')],
    ['Risk Agent', getAgent('risk_agent')],
  ];
  ui.agentGrid.innerHTML = cards.map(([title, report]) => agentCard(title, report || {})).join('');
}

function renderFocusedAgentDrawer(title, report, kicker) {
  setDrawerHeader(kicker, title);
  ui.agentGrid.className = 'agent-card-grid';
  ui.agentGrid.innerHTML = `
    ${agentCard(title, report || {})}
    <div class="empty-state">This drawer is intentionally light for now. The chart and terminal remain active while the selected agent context is in focus.</div>
  `;
}

function renderPlaceholderDrawer() {
  const active = navButtonForModule(state.activeSidebarModule);
  const label = active ? active.textContent.trim() : 'Workspace';
  setDrawerHeader('Workspace Module', label);
  ui.agentGrid.className = 'agent-card-grid';
  ui.agentGrid.innerHTML = `<div class="empty-state">${escapeHtml(label)} tools are not implemented in this workspace phase.</div>`;
}

function renderNewsDrawer() {
  setDrawerHeader('News Center', `${state.symbol} News`);
  ui.agentGrid.className = 'agent-card-grid news-drawer-body';
  const filteredArticles = filteredNewsArticles();
  ui.agentGrid.innerHTML = `
    <div class="news-controls">
      <label class="lookback-control">
        <span>Lookback</span>
        <select id="news-lookback-select">
          ${[6, 12, 24, 48, 72].map(hours => `<option value="${hours}" ${state.newsLookbackHours === hours ? 'selected' : ''}>${hours}h</option>`).join('')}
        </select>
      </label>
      <button class="drawer-action" id="news-refresh-button">Refresh Articles</button>
      <button class="drawer-action primary" id="news-run-button">Run Agent</button>
    </div>
    ${renderNewsSummaryCard()}
    <div class="news-filter-row">
      ${newsFilterButton('all', 'All')}
      ${newsFilterButton('bullish', 'Bullish')}
      ${newsFilterButton('bearish', 'Bearish')}
      ${newsFilterButton('neutral', 'Neutral')}
      ${newsFilterButton('high-impact', 'High Impact')}
      ${newsFilterButton('bookmarked', 'Bookmarked')}
    </div>
    ${state.newsLoading ? '<div class="drawer-state">Loading News Center...</div>' : ''}
    ${state.newsError ? `<div class="drawer-state error">${escapeHtml(state.newsError)}</div>` : ''}
    <div class="news-list">
      ${!state.newsLoading && !state.newsError && filteredArticles.length
        ? filteredArticles.map(newsArticleCard).join('')
        : (!state.newsLoading && !state.newsError ? '<div class="empty-state">No recent news articles found for selected lookback.</div>' : '')}
    </div>
  `;
  bindNewsDrawerControls();
}

function bindNewsDrawerControls() {
  const lookback = document.getElementById('news-lookback-select');
  if (lookback) {
    lookback.addEventListener('change', event => {
      state.newsLookbackHours = Number(event.target.value) || 24;
      loadNewsCenter();
    });
  }
  const refresh = document.getElementById('news-refresh-button');
  if (refresh) refresh.addEventListener('click', refreshNewsArticles);
  const run = document.getElementById('news-run-button');
  if (run) run.addEventListener('click', runNewsAgent);
  ui.agentGrid.querySelectorAll('button[data-news-filter]').forEach(button => {
    button.addEventListener('click', () => {
      state.newsFilter = button.dataset.newsFilter;
      renderNewsDrawer();
    });
  });
}

function newsFilterButton(key, label) {
  return `<button class="filter-chip ${state.newsFilter === key ? 'active' : ''}" data-news-filter="${escapeHtml(key)}">${escapeHtml(label)}</button>`;
}

function renderNewsSummaryCard() {
  const report = state.newsAgentReport || getAgent('news_agent') || {};
  const sentiment = extractSentiment(report) || 'UNKNOWN';
  const confidence = formatPercent(report.confidence);
  const impact = extractImpactScore(report);
  const reason = firstValue(report, ['summary', 'top_reason', 'rationale', 'notes']) || firstArrayValue(report.major_drivers, 'driver') || firstArrayValue(report.reasons);
  const warnings = [...arrayValue(report.warnings), ...arrayValue(report.blockers)];
  return `
    <article class="news-summary-card">
      <div class="news-summary-head">
        <div>
          <div class="panel-kicker">News Agent Summary</div>
          <div class="news-summary-title">${escapeHtml(sentiment)}</div>
        </div>
        <span class="sentiment-badge ${sentimentClass(sentiment)}">${escapeHtml(sentiment)}</span>
      </div>
      <div class="summary-grid">
        <div><span>Confidence</span><strong>${escapeHtml(confidence)}</strong></div>
        <div><span>Freshness</span><strong>${escapeHtml(freshnessLabel(report))}</strong></div>
        <div><span>Impact</span><strong>${impact == null ? '--' : Math.round(impact)}</strong></div>
        <div><span>Lookback</span><strong>${escapeHtml(String(report.lookback_hours || state.newsLookbackHours))}h</strong></div>
      </div>
      <div class="impact-bar"><span style="width:${Math.max(0, Math.min(100, Number(impact || 0)))}%"></span></div>
      <p>${escapeHtml(reason || 'No News Agent report available yet.')}</p>
      ${warnings.length ? `<div class="agent-warning">${escapeHtml(String(warnings[0]))}</div>` : ''}
    </article>
  `;
}

function newsArticleCard(article) {
  const title = firstValue(article, ['title', 'headline', 'article_title']) || 'Untitled news item';
  const source = firstValue(article, ['source', 'publisher', 'provider']) || 'Unknown source';
  const published = firstValue(article, ['published_at', 'publishedAt', 'published', 'date', 'ts']);
  const sentiment = extractSentiment(article) || 'NEUTRAL';
  const impact = extractImpactScore(article);
  const summary = firstValue(article, ['ai_summary', 'summary', 'short_summary', 'description']) || '';
  const link = firstValue(article, ['url', 'link', 'article_url']);
  const tags = articleTags(article);
  const hasAiSummary = Boolean(firstValue(article, ['ai_summary', 'summary']));
  return `
    <article class="news-article-card">
      <div class="article-headline-row">
        <h3>${escapeHtml(title)}</h3>
        <button class="bookmark-button" title="Bookmark placeholder" aria-label="Bookmark placeholder">☆</button>
      </div>
      <div class="article-meta">
        <span>${escapeHtml(source)}</span>
        <span>${escapeHtml(timeAgoValue(published))}</span>
        <span class="sentiment-badge ${sentimentClass(sentiment)}">${escapeHtml(sentiment)}</span>
        <span class="impact-badge">${impact == null ? 'Impact --' : `Impact ${Math.round(impact)}`}</span>
        ${hasAiSummary ? '<span class="ai-summary-badge">AI Summary</span>' : ''}
      </div>
      ${tags.length ? `<div class="article-tags">${tags.map(tag => `<span>${escapeHtml(tag)}</span>`).join('')}</div>` : ''}
      ${summary ? `<p>${escapeHtml(summary)}</p>` : ''}
      ${link ? `<a class="article-link" href="${escapeHtml(link)}" target="_blank" rel="noopener noreferrer">Open ↗</a>` : ''}
    </article>
  `;
}

function agentCard(title, report) {
  const stateText = agentState(report);
  const confidence = Number(report.confidence);
  const warnings = Array.isArray(report.warnings) ? report.warnings : Array.isArray(report.blockers) ? report.blockers : [];
  const summary = report.summary || report.notes || report.rationale || agentSummary(report);
  return `
    <article class="agent-card">
      <div class="agent-head">
        <div class="agent-name">${escapeHtml(title)}</div>
        <div class="agent-state">${escapeHtml(stateText)}</div>
      </div>
      <div class="agent-meta">
        <span>Confidence ${Number.isFinite(confidence) ? Math.round(confidence * 100) + '%' : '--'}</span>
        <span>${freshnessLabel(report)}</span>
      </div>
      <div class="agent-summary">${escapeHtml(summary || 'No latest report available.')}</div>
      ${warnings.length ? `<div class="agent-warning">${escapeHtml(String(warnings[0]))}</div>` : ''}
    </article>
  `;
}

function renderTerminal() {
  const suggestions = (state.summary && state.summary.strategy_suggestions) || [];
  const agents = (state.summary && state.summary.agents) || {};
  if (state.terminalTab === 'pnl') {
    ui.terminalBody.innerHTML = `
      <div class="terminal-grid">
        ${terminalCell('Today P&L', 'Rs0.00', 'neutral')}
        ${terminalCell('Realized', 'Rs0.00', 'neutral')}
        ${terminalCell('Unrealized', 'Rs0.00', 'neutral')}
        ${terminalCell('Win Rate', '--', 'neutral')}
      </div>
    `;
  } else if (state.terminalTab === 'positions') {
    ui.terminalBody.innerHTML = `<div class="empty-state">No open positions. Paper trading is not implemented yet.</div>`;
  } else if (state.terminalTab === 'strategies') {
    ui.terminalBody.innerHTML = `<div class="strategy-list">${suggestions.map(strategyRow).join('') || strategyRow(defaultStrategy())}</div>`;
  } else if (state.terminalTab === 'signals') {
    ui.terminalBody.innerHTML = `<div class="signal-table">${Object.entries(agents).map(([key, report]) => signalRow(key, report)).join('')}</div>`;
  } else if (state.terminalTab === 'audit') {
    ui.terminalBody.innerHTML = `<div class="empty-state">Audit log feed is not wired into this workspace yet. Latest agent reports are persisted in backend storage.</div>`;
  } else {
    ui.terminalBody.innerHTML = `<div class="empty-state">Orders are not implemented. No live broker execution is available in this phase.</div>`;
  }
}

function terminalCell(label, value, tone) {
  return `
    <div class="terminal-cell">
      <div class="terminal-label">${escapeHtml(label)}</div>
      <div class="terminal-value ${tone}">${escapeHtml(value)}</div>
    </div>
  `;
}

function strategyRow(item) {
  return `
    <div class="strategy-row">
      <div>
        <div class="strategy-title">${escapeHtml(item.title || 'Wait / No Trade')}</div>
        <div class="strategy-copy">${escapeHtml(item.rationale || 'Strategy Engine not implemented yet.')}</div>
      </div>
      <div class="strategy-copy">Confidence ${Math.round(Number(item.confidence || 0) * 100)}%</div>
      <button class="disabled-action" disabled>Manual Approval Required</button>
    </div>
  `;
}

function signalRow(key, report) {
  return `
    <div class="signal-row">
      <div class="signal-name">${escapeHtml(prettyAgentName(key))}</div>
      <div class="signal-copy">${escapeHtml(agentState(report || {}))} · ${escapeHtml(agentSummary(report || {}))}</div>
    </div>
  `;
}

function setChartStatus(message, hide) {
  ui.chartStatus.textContent = message || '';
  ui.chartStatus.classList.toggle('show', Boolean(message) && !hide);
}

function updateMockButton() {
  ui.mockToggle.textContent = `Mock: ${state.useMock ? 'On' : 'Off'}`;
}

function getAgent(key) {
  return ((state.summary || {}).agents || {})[key] || {};
}

function agentState(report) {
  return report.overall_health || report.primary_regime || report.bias || report.macro_bias || report.overall_sentiment || report.status || 'UNKNOWN';
}

function agentSummary(report) {
  if (report.trade_filter) return `Trade filter: ${report.trade_filter}`;
  if (report.pcr_state) return `PCR state: ${report.pcr_state}`;
  if (report.fresh_trade_blocked !== undefined) return report.fresh_trade_blocked ? 'Fresh proposals blocked.' : 'Fresh proposals allowed.';
  return 'Awaiting latest agent report.';
}

function freshnessLabel(report) {
  const generated = report.generated_at || report.generatedAt;
  if (!generated) return 'Freshness --';
  const ts = Date.parse(generated);
  if (!Number.isFinite(ts)) return 'Freshness --';
  const minutes = Math.max(0, Math.round((Date.now() - ts) / 60000));
  return `${minutes}m ago`;
}

function normalizeNewsArticles(payload) {
  if (Array.isArray(payload)) return payload;
  if (!payload || typeof payload !== 'object') return [];
  if (Array.isArray(payload.articles)) return payload.articles;
  if (Array.isArray(payload.items)) return payload.items;
  if (Array.isArray(payload.news)) return payload.news;
  return [];
}

function shouldUseMockNewsReport(report) {
  if (!state.useMock) return false;
  if (!report || !Object.keys(report).length) return true;
  const summary = String(report.summary || report.notes || '');
  return /no analyzed news/i.test(summary);
}

function filteredNewsArticles() {
  return state.newsArticles.filter(article => {
    const sentiment = extractSentiment(article).toLowerCase();
    const impact = extractImpactScore(article);
    if (state.newsFilter === 'bullish') return sentiment.includes('bull');
    if (state.newsFilter === 'bearish') return sentiment.includes('bear');
    if (state.newsFilter === 'neutral') return sentiment.includes('neutral') || sentiment.includes('mixed');
    if (state.newsFilter === 'high-impact') return Number(impact || 0) >= 70;
    if (state.newsFilter === 'bookmarked') return Boolean(article.bookmarked || article.is_bookmarked);
    return true;
  });
}

function extractSentiment(item) {
  const raw = firstValue(item || {}, ['overall_sentiment', 'market_sentiment', 'sentiment', 'bias', 'status']) || 'UNKNOWN';
  const value = typeof raw === 'object' && raw ? raw.label || raw.value || raw.name : raw;
  return String(value || 'UNKNOWN').replaceAll('_', ' ').toUpperCase();
}

function extractImpactScore(item) {
  const raw = firstValue(item || {}, ['impact_score', 'impactScore', 'impact', 'score']);
  if (raw == null || raw === '') return null;
  if (typeof raw === 'string') {
    const lowered = raw.toLowerCase();
    if (lowered.includes('high')) return 80;
    if (lowered.includes('medium')) return 55;
    if (lowered.includes('low')) return 25;
  }
  const number = Number(raw);
  if (!Number.isFinite(number)) return null;
  return number <= 10 ? number * 10 : number;
}

function articleTags(article) {
  const tags = [];
  ['sector', 'category', 'index'].forEach(key => {
    if (article[key]) tags.push(article[key]);
  });
  ['affected_indices', 'affected_sectors', 'macro_tags', 'tags'].forEach(key => {
    arrayValue(article[key]).forEach(value => tags.push(value));
  });
  return Array.from(new Set(tags.map(tag => String(tag)).filter(Boolean))).slice(0, 5);
}

function firstValue(object, keys) {
  for (const key of keys) {
    if (object && object[key] !== undefined && object[key] !== null && object[key] !== '') {
      return object[key];
    }
  }
  return null;
}

function firstArrayValue(value, key) {
  const items = arrayValue(value);
  if (!items.length) return '';
  const first = items[0];
  if (key && first && typeof first === 'object') return first[key] || '';
  return first;
}

function arrayValue(value) {
  if (Array.isArray(value)) return value;
  return value ? [value] : [];
}

function sentimentClass(sentiment) {
  const value = String(sentiment || '').toLowerCase();
  if (value.includes('bull')) return 'bullish';
  if (value.includes('bear')) return 'bearish';
  if (value.includes('mixed')) return 'mixed';
  return 'neutral';
}

function formatPercent(value) {
  const number = Number(value);
  if (!Number.isFinite(number)) return '--';
  return `${Math.round(number <= 1 ? number * 100 : number)}%`;
}

function timeAgoValue(value) {
  const ts = timestampMs(value);
  if (!Number.isFinite(ts)) return value ? String(value) : 'Time --';
  const minutes = Math.max(0, Math.round((Date.now() - ts) / 60000));
  if (minutes < 1) return 'Just now';
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.round(minutes / 60);
  if (hours < 48) return `${hours}h ago`;
  return new Date(ts).toLocaleDateString('en-IN', { day: '2-digit', month: 'short' });
}

function timestampMs(value) {
  if (value == null || value === '') return NaN;
  if (typeof value === 'number') return value < 1e12 ? value * 1000 : value;
  const text = String(value).trim();
  if (/^\d+(\.\d+)?$/.test(text)) {
    const number = Number(text);
    return number < 1e12 ? number * 1000 : number;
  }
  return Date.parse(text);
}

function mockNewsReport() {
  return {
    index: state.symbol,
    overall_sentiment: 'MIXED_BULLISH',
    confidence: 0.72,
    impact_score: 72,
    lookback_hours: state.newsLookbackHours,
    generated_at: new Date().toISOString(),
    summary: 'Large-cap banks and IT cues are supportive, while crude and global yields keep fresh trade conviction measured.',
    warnings: [],
  };
}

function mockNewsArticles() {
  const now = Date.now();
  return [
    {
      title: 'Banks firm as credit growth and asset quality remain supportive',
      source: 'Mock Market Desk',
      published_at: new Date(now - 18 * 60000).toISOString(),
      sentiment: 'bullish',
      impact_score: 8,
      affected_indices: [state.symbol],
      affected_sectors: ['Banking'],
      macro_tags: ['RBI_POLICY'],
      summary: 'Financials are contributing positive breadth, helping index sentiment stay constructive.',
      url: '#',
    },
    {
      title: 'Crude holds elevated after supply-risk headlines',
      source: 'Mock Macro Wire',
      published_at: new Date(now - 42 * 60000).toISOString(),
      sentiment: 'bearish',
      impact_score: 7,
      affected_indices: ['NIFTY'],
      affected_sectors: ['Oil & Gas'],
      macro_tags: ['CRUDE', 'USDINR'],
      summary: 'Higher crude can weigh on India macro inputs and reduce long-side confidence.',
      url: '#',
    },
    {
      title: 'IT services steady ahead of global technology earnings',
      source: 'Mock Equity Desk',
      published_at: new Date(now - 76 * 60000).toISOString(),
      sentiment: 'neutral',
      impact_score: 5,
      affected_indices: [state.symbol],
      affected_sectors: ['IT'],
      summary: 'The setup is balanced, with stock-specific reactions likely until broader guidance improves.',
      url: '#',
    },
  ];
}

function prettyAgentName(key) {
  return String(key || '').replaceAll('_', ' ').replace(/\b\w/g, char => char.toUpperCase());
}

function defaultStrategy() {
  return {
    title: 'Wait / No Trade',
    confidence: 0.58,
    rationale: 'Strategy Engine not implemented yet.',
    manual_approval_required: true,
  };
}

function formatNumber(value) {
  const number = Number(value);
  if (!Number.isFinite(number)) return escapeHtml(value ?? '--');
  return number.toLocaleString('en-IN', { maximumFractionDigits: 2 });
}

function signed(value) {
  const number = Number(value);
  if (!Number.isFinite(number)) return '--';
  return `${number >= 0 ? '+' : ''}${number.toFixed(2)}%`;
}

function escapeHtml(value) {
  return String(value ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}
