'use strict';
let allArticles = [];
let sortMode = 'newest';
let sentFilter = null;
let bookmarksOnly = false;
let aiSummaryOnly = false;
let sectorFilter = null;
let scopeFilter = 'all';
let timeFilterHours = null;
let feedStatus = {};
let currentTickers = {};
let priceHistory = {};
let analyticsPayload = { overviewCards: [], alerts: [], sectorBoard: [], keyLevels: [], watchlistSignals: [], symbolMap: {}, sectorMap: {} };
let derivativesPayload = {
  overviewCards: [],
  predictionCards: [],
  contextNotes: [],
  riskFlags: [],
  crossAssetRows: [],
  relativeValueRows: [],
  scoreBreakdown: [],
  tradeScenarios: [],
  signalMatrix: [],
  triggerMap: []
};
let symbolAnalytics = {};
let customTickerQuotes = {};
let customTickerHistory = {};
let marketStatus = null;
let dataProvider = null;
let lastSnapshotAt = null;
let refreshInterval = 300;
let allowedRefreshWindows = [60, 120, 300, 600, 900];
let aiSummaryProgress = null;
let aiSummaryPollTimer;
const AI_SUMMARY_POLL_INTERVAL_MS = 5000;
const DEFAULT_TRACKED_TICKERS = ['INFY', 'HCLTECH', 'WIPRO', 'TCS', 'RELIANCE'];
const DEFAULT_WATCHLIST = ['INFY', 'HCLTECH', 'WIPRO', 'RELIANCE'];
const FIXED_TICKER_LABELS = ['Nifty 50', 'Nifty Bank', 'Nifty Midcap', 'Nifty Smallcap', 'VIX', 'Nifty IT', 'Gold', 'USD/INR', 'Crude Oil', 'Brent Crude'];
const suggestTimers = {};
const suggestHideTimers = {};

const SYMBOL_ALIASES = { INFOSYS: 'INFY', HCL: 'HCLTECH' };
const SYM_TO_LABEL = { INFY: 'Infosys', INFOSYS: 'Infosys', HCLTECH: 'HCL Tech', HCL: 'HCL Tech', WIPRO: 'Wipro', TCS: 'TCS', RELIANCE: 'Reliance' };
const SEC_BG = { IT: '#07234a', Banking: '#063321', Pharma: '#210f3a', Auto: '#2d1400', Energy: '#1c1200', FMCG: '#122000', Metals: '#1a1200', Infra: '#0f1e2d', General: '#101422' };
const SEC_TC = { IT: '#60a5fa', Banking: '#34d399', Pharma: '#c084fc', Auto: '#fb923c', Energy: '#fbbf24', FMCG: '#a3e635', Metals: '#f97316', Infra: '#38bdf8', General: '#94a3b8' };

function normalizeSymbol(sym) {
  const clean = (sym || '').trim().toUpperCase().replace(/[^A-Z0-9&.-]/g, '');
  return SYMBOL_ALIASES[clean] || clean;
}

function readStorageJson(key, fallback) {
  try {
    const raw = localStorage.getItem(key);
    return raw ? JSON.parse(raw) : fallback;
  } catch (_) {
    return fallback;
  }
}

let tickerSelections = new Set((readStorageJson('ticker_sel_v1', DEFAULT_TRACKED_TICKERS) || []).map(normalizeSymbol));
let watchlist = new Set((readStorageJson('wl_v2', DEFAULT_WATCHLIST) || []).map(normalizeSymbol));
let bookmarks = new Set(readStorageJson('bm_v1', []));
let portfolio = Object.fromEntries(Object.entries(readStorageJson('port_v1', {})).map(([sym, val]) => [normalizeSymbol(sym), val]));
let notifEnabled = false;
let seenHighImpact;
try {
  seenHighImpact = new Set(JSON.parse(sessionStorage.getItem('seen_hi') || '[]'));
} catch (_) {
  seenHighImpact = new Set();
}
let stateSyncTimer;
let snapshotTimer;

function cacheAppStateLocally() {
  localStorage.setItem('ticker_sel_v1', JSON.stringify([...tickerSelections]));
  localStorage.setItem('wl_v2', JSON.stringify([...watchlist]));
  localStorage.setItem('bm_v1', JSON.stringify([...bookmarks]));
  localStorage.setItem('port_v1', JSON.stringify(portfolio));
}

function currentAppState() {
  return {
    tickerSelections: [...tickerSelections],
    watchlist: [...watchlist],
    bookmarks: [...bookmarks],
    portfolio,
  };
}

function scheduleAppStateSync() {
  clearTimeout(stateSyncTimer);
  stateSyncTimer = setTimeout(persistAppState, 180);
}

function refreshSnapshotSoon(delay = 400) {
  clearTimeout(snapshotTimer);
  snapshotTimer = setTimeout(() => fetchMarketSnapshot(false), delay);
}

function saveTickers() { cacheAppStateLocally(); scheduleAppStateSync(); }
function saveWL() { cacheAppStateLocally(); scheduleAppStateSync(); }
function saveBM() { cacheAppStateLocally(); scheduleAppStateSync(); }
function savePort() { cacheAppStateLocally(); scheduleAppStateSync(); }
function num(val, digits = 2) { return typeof val === 'number' && Number.isFinite(val) ? val.toFixed(digits) : '—'; }
function signed(val, digits = 2, suffix = '%') { return typeof val === 'number' && Number.isFinite(val) ? `${val >= 0 ? '+' : ''}${val.toFixed(digits)}${suffix}` : '—'; }
function rupees(val) { return typeof val === 'number' && Number.isFinite(val) ? `Rs${val.toLocaleString('en-IN', { maximumFractionDigits: 2 })}` : '—'; }
function toneClass(tone) { return tone === 'bull' ? 'tone-bull' : tone === 'bear' ? 'tone-bear' : 'tone-neutral'; }
function chipClass(label) {
  const t = (label || '').toLowerCase();
  if (t.includes('uptrend') || t.includes('breakout') || t.includes('momentum') || t.includes('positive')) return 'chip-bull';
  if (t.includes('downtrend') || t.includes('breakdown') || t.includes('weak') || t.includes('negative')) return 'chip-bear';
  return 'chip-neutral';
}

function escapeHtml(value) {
  return String(value ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}

function cleanFeedLabel(label) {
  return String(label || '')
    .replace(/^Google News India\s+/i, '')
    .replace(/^Google News Global\s+/i, 'Global ')
    .replace(/\s+Markets$/i, '')
    .replace(/\s+Companies$/i, '')
    .trim();
}

let toastTimer;
function showToast(msg, ms = 3000) {
  const t = document.getElementById('toast');
  t.textContent = msg;
  t.classList.add('show');
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => t.classList.remove('show'), ms);
}

function inputIdFor(kind) {
  return kind === 'ticker' ? 'ticker-inp' : 'wl-inp';
}

function menuIdFor(kind) {
  return kind === 'ticker' ? 'ticker-suggest' : 'watchlist-suggest';
}

function formatRefreshWindow(seconds) {
  if (seconds < 60) return `${seconds}s`;
  if (seconds % 60 === 0) return `${seconds / 60}m`;
  return `${Math.floor(seconds / 60)}m ${seconds % 60}s`;
}

function syncRefreshControl() {
  const select = document.getElementById('refresh-window');
  if (!select) return;
  const optionsHtml = allowedRefreshWindows.map(seconds => `<option value="${seconds}">${formatRefreshWindow(seconds)}</option>`).join('');
  if (select.innerHTML !== optionsHtml) select.innerHTML = optionsHtml;
  select.value = String(refreshInterval);
}

function applyAppState(state) {
  if (!state || typeof state !== 'object') return;
  tickerSelections = new Set((state.tickerSelections || DEFAULT_TRACKED_TICKERS).map(normalizeSymbol));
  watchlist = new Set((state.watchlist || DEFAULT_WATCHLIST).map(normalizeSymbol));
  bookmarks = new Set(state.bookmarks || []);
  portfolio = Object.fromEntries(Object.entries(state.portfolio || {}).map(([sym, val]) => [normalizeSymbol(sym), val]));
  cacheAppStateLocally();
  renderTickerManager();
  renderWL();
  renderPortfolio();
  renderAnalytics();
}

async function persistAppState() {
  try {
    await fetch('/api/app-state', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(currentAppState())
    });
  } catch (e) {
    console.error('State sync error', e);
  }
}

async function fetchAppState() {
  try {
    const r = await fetch('/api/app-state');
    if (!r.ok) throw new Error('Failed to load app state');
    const d = await r.json();
    if (d.hasStoredState) {
      applyAppState(d.state || {});
    } else {
      await persistAppState();
    }
  } catch (e) {
    console.error('App state load error', e);
  }
}

function aiSummaryProgressLabel(progress = aiSummaryProgress) {
  if (!progress || !progress.total) return null;
  const complete = Number(progress.analysisComplete ?? progress.complete ?? 0);
  const total = Number(progress.total || 0);
  const queued = Number(progress.queued || 0);
  const inflight = Number(progress.inflight || 0);
  const activeWork = queued + inflight;
  const done = total > 0 && complete >= total;
  return {
    className: done ? 'sys-ok' : activeWork > 0 ? 'sys-warn' : 'sys-muted',
    text: `AI analysis ${complete}/${total}${done ? '' : activeWork > 0 ? ` · ${activeWork} queued` : ''}`,
  };
}

function patchAiSummaryProgressPill(progress = aiSummaryProgress) {
  const bar = document.getElementById('system-status-bar');
  if (!bar) return;
  const label = aiSummaryProgressLabel(progress);
  const existing = bar.querySelector('[data-ai-summary-progress="1"]');
  if (!label) {
    if (existing) existing.remove();
    return;
  }
  const pill = existing || document.createElement('span');
  pill.dataset.aiSummaryProgress = '1';
  pill.className = `sys-pill ${label.className}`;
  pill.textContent = label.text;
  if (progress && (progress.provider || progress.model)) {
    pill.title = `AI provider: ${progress.provider || 'unknown'} · model: ${progress.model || 'unknown'}`;
  }
  if (!existing) bar.appendChild(pill);
}

function renderMarketStatus() {
  const tickerStatus = document.getElementById('mkt-status');
  const bar = document.getElementById('system-status-bar');
  if (!tickerStatus || !bar || !marketStatus) return;

  const label = marketStatus.sessionLabel || (marketStatus.isMarketOpen ? 'Market open' : 'Market closed');
  const tickerChip = marketStatus.isMarketOpen
    ? '&#x1F7E2; Market Open (IST)'
    : marketStatus.session === 'holiday'
      ? '&#9208; Exchange Holiday'
      : '&#x1F534; Market Closed';
  tickerStatus.innerHTML = tickerChip;

  const pills = [`<span class="sys-pill">${escapeHtml(label)}</span>`];
  if (marketStatus.reason) {
    pills.push(`<span class="sys-pill sys-muted">${escapeHtml(marketStatus.reason)}</span>`);
  }
  if (dataProvider && dataProvider.active) {
    const providerLabel = dataProvider.active === 'upstox' ? 'Upstox live feed' : 'NSE fallback';
    const providerClass = dataProvider.active === 'upstox' ? 'sys-ok' : 'sys-muted';
    pills.push(`<span class="sys-pill ${providerClass}">${escapeHtml(providerLabel)}</span>`);
  }
  const feedEntries = Object.entries(feedStatus || {});
  if (feedEntries.length) {
    const healthy = feedEntries.filter(([, s]) => s.ok);
    const failed = feedEntries.filter(([, s]) => !s.ok);
    const healthyTitle = healthy.map(([src, s]) => `${cleanFeedLabel(src)} (${(s.scope || 'local').toUpperCase()})`).join('\n');
    const failedTitle = failed.map(([src, s]) => `${cleanFeedLabel(src)}: ${s.error || 'Feed unavailable'}`).join('\n');
    const sourceLabel = failed.length ? `Sources ${healthy.length}/${feedEntries.length}` : `${healthy.length} sources active`;
    pills.push(`<span class="sys-pill ${failed.length ? 'sys-warn' : 'sys-ok'}" title="${escapeHtml(failed.length ? failedTitle : healthyTitle)}">${escapeHtml(sourceLabel)}</span>`);
  }
  if (aiSummaryProgress && aiSummaryProgress.total) {
    const label = aiSummaryProgressLabel();
    if (label) {
      pills.push(`<span class="sys-pill ${label.className}" data-ai-summary-progress="1">${escapeHtml(label.text)}</span>`);
    }
  }
  if (marketStatus.tickerAgeSeconds !== null && marketStatus.tickerAgeSeconds !== undefined) {
    const tickClass = marketStatus.tickersStale ? 'sys-warn' : 'sys-ok';
    pills.push(`<span class="sys-pill ${tickClass}">Ticks ${escapeHtml(ageLabel(marketStatus.tickerAgeSeconds))}</span>`);
  }
  if (marketStatus.tickIntervalSeconds) {
    pills.push(`<span class="sys-pill sys-muted">Cadence ${escapeHtml(marketStatus.tickIntervalSeconds)}s</span>`);
  }
  if (marketStatus.newsAgeSeconds !== null && marketStatus.newsAgeSeconds !== undefined) {
    pills.push(`<span class="sys-pill sys-muted">News ${escapeHtml(ageLabel(marketStatus.newsAgeSeconds))}</span>`);
  }
  if (lastSnapshotAt) {
    pills.push(`<span class="sys-pill sys-muted">Snapshot ${escapeHtml(ageLabel((Date.now() - lastSnapshotAt) / 1000))}</span>`);
  }
  if (marketStatus.staleData) {
    pills.push('<span class="sys-pill sys-warn">Stale data</span>');
  }
  bar.innerHTML = pills.join('');
}

function switchTab(name, btn) {
  document.querySelectorAll('.tab-pane').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
  document.getElementById('tab-' + name).classList.add('active');
  if (btn) btn.classList.add('active');
}

function setSort(s, btn) {
  sortMode = s;
  document.querySelectorAll('.sort-btn').forEach(b => b.classList.remove('active'));
  if (btn) btn.classList.add('active');
  render();
}

function toggleSentFilter(s) {
  sentFilter = sentFilter === s ? null : s;
  document.getElementById('bull-filt').className = 'filt-btn' + (sentFilter === 'bullish' ? ' bull-on' : '');
  document.getElementById('bear-filt').className = 'filt-btn' + (sentFilter === 'bearish' ? ' bear-on' : '');
  render();
}

function toggleBookmarksOnly() {
  bookmarksOnly = !bookmarksOnly;
  document.getElementById('bm-filt').className = 'filt-btn' + (bookmarksOnly ? ' bm-on' : '');
  render();
}

function toggleAiSummaryOnly() {
  aiSummaryOnly = !aiSummaryOnly;
  document.getElementById('ai-filt').className = 'filt-btn' + (aiSummaryOnly ? ' ai-on' : '');
  render();
}

function setScopeFilter(scope, btn) {
  scopeFilter = scope || 'all';
  document.querySelectorAll('.scope-btn').forEach(b => b.classList.remove('scope-on'));
  if (btn) btn.classList.add('scope-on');
  updateHeatmap();
  render();
}

function setTimeFilter(hours, btn) {
  timeFilterHours = timeFilterHours === hours ? null : hours;
  document.querySelectorAll('.time-btn').forEach(b => b.classList.remove('time-on'));
  const activeBtn = timeFilterHours === null ? document.querySelector('.time-btn') : btn;
  if (activeBtn) activeBtn.classList.add('time-on');
  updateHeatmap();
  render();
}

async function setRefreshWindowSelect(value) {
  const seconds = parseInt(value, 10);
  if (!Number.isFinite(seconds)) return;
  try {
    const r = await fetch('/api/settings/refresh', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ seconds })
    });
    if (!r.ok) throw new Error('Failed to update refresh interval');
    const d = await r.json();
    refreshInterval = d.refreshInterval || refreshInterval;
    allowedRefreshWindows = d.allowedRefreshWindows || allowedRefreshWindows;
    syncRefreshControl();
    startCountdown();
    showToast(`News refresh set to ${formatRefreshWindow(refreshInterval)}`);
    setTimeout(fetchNews, 1200);
  } catch (e) {
    console.error('Refresh settings error', e);
    showToast('Could not update refresh window');
    syncRefreshControl();
  }
}

function isWL(a) {
  const t = (a.title + ' ' + a.summary).toUpperCase();
  for (const sym of watchlist) if (t.includes(sym) || t.includes(SYM_TO_LABEL[sym] || '')) return true;
  return false;
}

function articleHasAiSummary(a) {
  return a.summarySource === 'ai' && String(a.summary || '').trim().length > 0;
}

function applySort(arr) {
  const a = [...arr];
  if (sortMode === 'newest') return a.sort((x, y) => y.ts - x.ts);
  if (sortMode === 'impact') return a.sort((x, y) => y.impact - x.impact || y.ts - x.ts);
  if (sortMode === 'watchlist') return a.sort((x, y) => {
    const wx = isWL(x), wy = isWL(y);
    return wx !== wy ? (wx ? -1 : 1) : y.ts - x.ts;
  });
  return a;
}

function filteredArticles(source, opts = {}) {
  const {
    includeSector = true,
    includeScope = true,
    includeSent = true,
    includeBookmarks = true,
    includeAiSummary = true,
    includeSearch = true,
    includeTime = true,
  } = opts;
  const q = document.getElementById('search').value.toLowerCase().trim();
  let arts = [...source];
  if (includeTime && timeFilterHours !== null) {
    const cutoff = (Date.now() / 1000) - (timeFilterHours * 3600);
    arts = arts.filter(a => Number.isFinite(a.ts) && a.ts >= cutoff);
  }
  if (includeSector && sectorFilter) arts = arts.filter(a => a.sector === sectorFilter);
  if (includeScope && scopeFilter !== 'all') arts = arts.filter(a => (a.scope || 'local') === scopeFilter);
  if (includeSent && sentFilter) arts = arts.filter(a => a.sentiment.label === sentFilter);
  if (includeBookmarks && bookmarksOnly) arts = arts.filter(a => bookmarks.has(a.id));
  if (includeAiSummary && aiSummaryOnly) arts = arts.filter(articleHasAiSummary);
  if (includeSearch && q) arts = arts.filter(a =>
    a.title.toLowerCase().includes(q) ||
    a.summary.toLowerCase().includes(q) ||
    a.sector.toLowerCase().includes(q) ||
    a.source.toLowerCase().includes(q) ||
    (a.feed || '').toLowerCase().includes(q) ||
    (a.scope || '').toLowerCase().includes(q)
  );
  return arts;
}

function relativeTime(ts) {
  if (!Number.isFinite(ts)) return '';
  const seconds = Math.max(0, Math.floor((Date.now() / 1000) - ts));
  if (seconds < 3600) return `${Math.max(1, Math.floor(seconds / 60) || 1)}m ago`;
  if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ago`;
  return `${Math.floor(seconds / 86400)}d ago`;
}

function ageLabel(seconds) {
  if (!Number.isFinite(seconds)) return '—';
  if (seconds < 60) return `${Math.max(0, Math.round(seconds))}s ago`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
  if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ago`;
  return `${Math.floor(seconds / 86400)}d ago`;
}

function setFilter(sec) {
  if (sectorFilter === sec) { clearFilter(); return; }
  sectorFilter = sec;
  document.getElementById('filter-badge').classList.add('show');
  document.getElementById('f-name').textContent = sec;
  updateHeatmap();
  render();
}

function clearFilter() {
  sectorFilter = null;
  document.getElementById('filter-badge').classList.remove('show');
  updateHeatmap();
  render();
}

function renderWL() {
  const c = document.getElementById('wl-chips');
  c.innerHTML = '';
  for (const sym of watchlist) {
    const d = document.createElement('div');
    d.className = 'wl-chip';
    d.innerHTML = `${sym} <span class="wl-rm" onclick="removeWL('${sym}')">&#215;</span>`;
    c.appendChild(d);
  }
}

function renderTickerManager() {
  const c = document.getElementById('ticker-chips');
  c.innerHTML = '';
  for (const sym of tickerSelections) {
    const d = document.createElement('div');
    d.className = 'wl-chip';
    d.innerHTML = `${sym} <span class="wl-rm" onclick="removeTicker('${sym}')">&#215;</span>`;
    c.appendChild(d);
  }
}

function trackedTickerSymbols() {
  return [...tickerSelections].filter(Boolean).slice(0, 12);
}

function analyticsSymbols() {
  return [...new Set([...watchlist, ...Object.keys(portfolio)])].filter(Boolean).slice(0, 12);
}

function clearSuggestMenu(kind) {
  const menu = document.getElementById(menuIdFor(kind));
  if (!menu) return;
  menu.classList.remove('show');
  menu.innerHTML = '';
}

function hideSuggestMenu(kind, delay = 0) {
  clearTimeout(suggestHideTimers[kind]);
  suggestHideTimers[kind] = setTimeout(() => clearSuggestMenu(kind), delay);
}

function renderSuggestMenu(kind, results) {
  const menu = document.getElementById(menuIdFor(kind));
  if (!menu) return;
  menu.innerHTML = '';
  if (!results.length) {
    menu.classList.remove('show');
    return;
  }
  for (const item of results) {
    const btn = document.createElement('button');
    btn.type = 'button';
    btn.className = 'suggest-item';
    btn.onmousedown = event => event.preventDefault();
    btn.onclick = () => pickSuggestion(kind, item.symbol);

    const left = document.createElement('div');
    left.className = 'suggest-main';

    const sym = document.createElement('span');
    sym.className = 'suggest-symbol';
    sym.textContent = item.symbol;

    const name = document.createElement('span');
    name.className = 'suggest-name';
    name.textContent = item.name;

    const sector = document.createElement('span');
    sector.className = 'suggest-sector';
    sector.textContent = item.sector || '';

    left.appendChild(sym);
    left.appendChild(name);
    btn.appendChild(left);
    btn.appendChild(sector);
    menu.appendChild(btn);
  }
  menu.classList.add('show');
}

async function loadSuggestions(kind, query) {
  try {
    const r = await fetch('/api/symbols/search?q=' + encodeURIComponent(query || ''));
    const d = await r.json();
    renderSuggestMenu(kind, d.results || []);
  } catch (e) {
    console.error('Suggestion error', e);
  }
}

function handleSuggestInput(kind) {
  clearTimeout(suggestTimers[kind]);
  clearTimeout(suggestHideTimers[kind]);
  const inp = document.getElementById(inputIdFor(kind));
  if (!inp) return;
  suggestTimers[kind] = setTimeout(() => loadSuggestions(kind, inp.value.trim()), 120);
}

function pickSuggestion(kind, symbol) {
  if (kind === 'ticker') addTicker(symbol);
  else addWL(symbol);
  clearSuggestMenu(kind);
}

function handleSuggestKey(event, kind) {
  if (event.key === 'Enter') {
    event.preventDefault();
    if (kind === 'ticker') addTicker();
    else addWL();
  }
  if (event.key === 'Escape') hideSuggestMenu(kind);
}

function addWL(symArg = null) {
  const inp = document.getElementById('wl-inp');
  const sym = normalizeSymbol(symArg || inp.value);
  if (sym && !watchlist.has(sym)) {
    watchlist.add(sym);
    saveWL();
    renderWL();
    render();
    refreshSnapshotSoon();
    showToast(`Added ${sym} to watchlist`);
  }
  inp.value = '';
  clearSuggestMenu('watchlist');
}

function removeWL(sym) {
  watchlist.delete(sym);
  saveWL();
  renderWL();
  render();
  refreshSnapshotSoon();
}

function addTicker(symArg = null) {
  const inp = document.getElementById('ticker-inp');
  const sym = normalizeSymbol(symArg || inp.value);
  if (!sym) return;
  if (trackedTickerSymbols().length >= 12 && !tickerSelections.has(sym)) {
    showToast('Ticker strip is limited to 12 symbols');
    return;
  }
  if (!tickerSelections.has(sym)) {
    tickerSelections.add(sym);
    saveTickers();
    renderTickerManager();
    fetchTickerQuotes();
    refreshSnapshotSoon();
    showToast(`Added ${sym} to ticker strip`);
  }
  inp.value = '';
  clearSuggestMenu('ticker');
}

function removeTicker(sym) {
  tickerSelections.delete(sym);
  saveTickers();
  delete customTickerQuotes[sym];
  delete customTickerHistory[sym];
  renderTickerManager();
  if (Object.keys(currentTickers).length) renderTickers(currentTickers);
  refreshSnapshotSoon();
}

function toggleBookmark(id) {
  if (bookmarks.has(id)) bookmarks.delete(id); else bookmarks.add(id);
  saveBM();
  render();
}

async function fetchTickerQuotes() {
  const symbols = trackedTickerSymbols();
  if (!symbols.length) {
    customTickerQuotes = {};
    if (Object.keys(currentTickers).length) renderTickers(currentTickers);
    return;
  }
  try {
    const r = await fetch('/api/quotes?symbols=' + encodeURIComponent(symbols.join(',')));
    const data = await r.json();
    mergeCustomQuotes(data || {});
    if (Object.keys(currentTickers).length) renderTickers(currentTickers);
  } catch (e) {
    console.error('Ticker quote error', e);
  }
}

function pushHistoryPoint(store, key, price) {
  if (typeof price !== 'number' || !Number.isFinite(price)) return;
  store[key] = store[key] || [];
  if (store[key][store[key].length - 1] !== price) {
    store[key].push(price);
  }
  if (store[key].length > 40) store[key] = store[key].slice(-40);
}

function mergeCustomQuotes(quotes, recordHistory = true) {
  customTickerQuotes = quotes || {};
  if (!recordHistory) return;
  for (const [sym, quote] of Object.entries(customTickerQuotes)) {
    pushHistoryPoint(customTickerHistory, sym, quote.price);
  }
}

function appendPriceHistoryFromTicks(ticks) {
  for (const [label, data] of Object.entries(ticks || {})) {
    pushHistoryPoint(priceHistory, label, data.price);
  }
}

function applyMarketSnapshot(payload) {
  if (!payload || typeof payload !== 'object') return;
  lastSnapshotAt = Date.now();
  if (payload.marketStatus) marketStatus = payload.marketStatus;
  if (payload.dataProvider) dataProvider = payload.dataProvider;
  if (payload.history) priceHistory = payload.history || {};
  if (payload.trackedQuotes !== undefined) mergeCustomQuotes(payload.trackedQuotes || {});
  if (payload.analytics) {
    analyticsPayload = payload.analytics;
    renderAnalytics();
  }
  if (payload.derivatives) {
    derivativesPayload = payload.derivatives;
    renderDerivativesAnalysis();
  }
  if (payload.ticks) {
    appendPriceHistoryFromTicks(payload.ticks);
    renderTickers(payload.ticks);
  } else if (Object.keys(currentTickers).length) {
    renderTickers(currentTickers);
  } else {
    renderMarketStatus();
  }
}

function getLivePrice(sym) {
  const clean = normalizeSymbol(sym);
  if (customTickerQuotes[clean] && typeof customTickerQuotes[clean].price === 'number') return customTickerQuotes[clean].price;
  if (symbolAnalytics[clean] && typeof symbolAnalytics[clean].price === 'number') return symbolAnalytics[clean].price;
  const label = SYM_TO_LABEL[clean] || clean;
  const t = currentTickers[label] || currentTickers[clean];
  return t ? t.price : null;
}

function renderPortfolio() {
  const list = document.getElementById('port-list');
  const total = document.getElementById('port-total');
  const entries = Object.entries(portfolio);
  if (!entries.length) {
    list.innerHTML = '<div style="color:var(--muted);font-size:11px;text-align:center;padding:16px 0">No holdings yet.</div>';
    total.innerHTML = '';
    renderPortfolioSummary();
    return;
  }
  let totalCost = 0, totalCur = 0, hasLive = false;
  const rows = entries.map(([sym, { qty, buyPrice }]) => {
    const cur = getLivePrice(sym);
    const cost = qty * buyPrice;
    totalCost += cost;
    if (cur !== null) {
      hasLive = true;
      const val = qty * cur;
      totalCur += val;
      const pnl = val - cost;
      const pnlP = cost ? (pnl / cost * 100) : 0;
      const cls = pnl >= 0 ? 'up' : 'dn';
      return `<div class="port-item">
        <span class="port-sym">${sym}</span>
        <span class="port-detail">${qty} @ ${rupees(buyPrice)}</span>
        <span class="port-cur ${cls}">${rupees(cur)}</span>
        <span class="port-pnl ${cls}">${pnl >= 0 ? '+' : '-'}${rupees(Math.abs(pnl))} (${pnl >= 0 ? '+' : ''}${pnlP.toFixed(2)}%)</span>
        <span class="port-rm" onclick="removePortfolio('${sym}')">&#215;</span>
      </div>`;
    }
    totalCur += cost;
    return `<div class="port-item">
      <span class="port-sym">${sym}</span>
      <span class="port-detail">${qty} @ ${rupees(buyPrice)}</span>
      <span class="port-cur" style="color:var(--muted)">N/A</span>
      <span class="port-pnl" style="color:var(--muted)">—</span>
      <span class="port-rm" onclick="removePortfolio('${sym}')">&#215;</span>
    </div>`;
  }).join('');
  list.innerHTML = rows;
  if (hasLive) {
    const pnl = totalCur - totalCost;
    const pnlP = totalCost ? (pnl / totalCost * 100) : 0;
    total.innerHTML = `<div style="display:flex;justify-content:space-between;padding:7px 0 0;border-top:1px solid var(--border);margin-top:4px;">
      <span style="font-size:10px;color:var(--muted)">TOTAL P&amp;L</span>
      <span class="${pnl >= 0 ? 'up' : 'dn'}" style="font-size:11px;font-weight:700;">${pnl >= 0 ? '+' : '-'}${rupees(Math.abs(pnl))} (${pnl >= 0 ? '+' : ''}${pnlP.toFixed(2)}%)</span>
    </div>`;
  } else {
    total.innerHTML = '';
  }
  renderPortfolioSummary();
}

function addPortfolio() {
  const sym = normalizeSymbol(document.getElementById('port-sym').value);
  const qty = parseFloat(document.getElementById('port-qty').value);
  const buy = parseFloat(document.getElementById('port-buy').value);
  if (!sym || !qty || !buy || qty <= 0 || buy <= 0) { showToast('Enter symbol, quantity and buy price'); return; }
  portfolio[sym] = { qty, buyPrice: buy };
  savePort();
  renderPortfolio();
  refreshSnapshotSoon();
  document.getElementById('port-sym').value = '';
  document.getElementById('port-qty').value = '';
  document.getElementById('port-buy').value = '';
  showToast(`Added ${sym} to portfolio`);
}

function removePortfolio(sym) {
  delete portfolio[sym];
  savePort();
  renderPortfolio();
  refreshSnapshotSoon();
}

function hmData() {
  const m = {};
  for (const a of filteredArticles(allArticles, { includeSector: false, includeSent: false, includeBookmarks: false, includeAiSummary: false, includeSearch: false })) {
    const s = a.sector;
    if (!m[s]) m[s] = { count: 0, bull: 0, bear: 0 };
    m[s].count++;
    if (a.sentiment.label === 'bullish') m[s].bull++;
    if (a.sentiment.label === 'bearish') m[s].bear++;
  }
  return m;
}

function updateHeatmap() {
  const data = hmData();
  const sectorMap = analyticsPayload.sectorMap || {};
  const counts = Object.values(data).map(d => d.count);
  const maxCnt = counts.length ? Math.max(...counts) : 1;
  const hm = document.getElementById('heatmap');
  hm.innerHTML = '';
  let totalBull = 0, totalBear = 0;
  for (const d of Object.values(data)) { totalBull += d.bull; totalBear += d.bear; }
  const bp = document.getElementById('breadth-pill');
  if (totalBull > totalBear) { bp.className = 'breadth-pill breadth-bull'; bp.textContent = `Bullish ${totalBull} vs ${totalBear}`; }
  else if (totalBear > totalBull) { bp.className = 'breadth-pill breadth-bear'; bp.textContent = `Bearish ${totalBear} vs ${totalBull}`; }
  else { bp.className = 'breadth-pill breadth-neut'; bp.textContent = 'Neutral'; }
  for (const sec of Object.keys(SEC_BG)) {
    const d = data[sec] || { count: 0, bull: 0, bear: 0 };
    const snap = sectorMap[sec] || {};
    const isActive = sectorFilter === sec;
    const pct = maxCnt ? d.count / maxCnt * 100 : 0;
    const sent = d.bull > d.bear ? { icon: '▲', color: '#4ade80', label: 'bullish' } : d.bear > d.bull ? { icon: '▼', color: '#f87171', label: 'bearish' } : { icon: '—', color: '#94a3b8', label: 'neutral' };
    const opacity = d.count ? 0.45 + d.count / maxCnt * 0.55 : 0.3;
    const move = typeof snap.pct === 'number' ? `${snap.pct >= 0 ? '+' : ''}${snap.pct.toFixed(2)}%` : 'No price';
    const moveTone = typeof snap.pct === 'number' ? (snap.pct >= 0 ? 'up' : 'dn') : '';
    const cell = document.createElement('div');
    cell.className = 'hm-cell' + (isActive ? ' active-filter' : '');
    cell.style.cssText = `background:${SEC_BG[sec]};opacity:${opacity};`;
    cell.title = `${sec}: ${d.count} articles | ${d.bull} bullish | ${d.bear} bearish`;
    cell.onclick = () => setFilter(sec);
    cell.innerHTML = `<div class="hm-name" style="color:${SEC_TC[sec]}">${sec}</div>
      <div class="hm-cnt" style="color:${sent.color}">${sent.icon} ${sent.label}</div>
      <div class="hm-move ${moveTone}">${move}</div>
      <div class="hm-bar-wrap"><div class="hm-bar-fill" style="width:${pct}%"></div></div>`;
    hm.appendChild(cell);
  }
}

function renderOverview() {
  const el = document.getElementById('overview-grid');
  const cards = analyticsPayload.overviewCards || [];
  if (!cards.length) {
    el.innerHTML = '<div class="overview-card tone-neutral"><div class="ov-label">Loading</div><div class="ov-value">Analytics</div><div class="ov-detail">Waiting for market context.</div></div>';
    return;
  }
  el.innerHTML = cards.map(card => `<div class="overview-card ${toneClass(card.tone)}">
    <div class="ov-label">${card.label}</div>
    <div class="ov-value">${card.value}</div>
    <div class="ov-detail">${card.detail}</div>
  </div>`).join('');
}

function renderDeskAlerts() {
  const el = document.getElementById('desk-alerts');
  const alerts = analyticsPayload.alerts || [];
  if (!alerts.length) {
    el.innerHTML = '<div class="desk-item tone-neutral"><span class="desk-icon">&#8226;</span><div class="desk-text">No live desk notes yet.</div></div>';
    return;
  }
  el.innerHTML = alerts.map(text => `<div class="desk-item tone-neutral"><span class="desk-icon">&#8226;</span><div class="desk-text">${text}</div></div>`).join('');
}

function renderSectorBoard() {
  const el = document.getElementById('sector-board');
  const rows = (analyticsPayload.sectorBoard || []).slice(0, 8);
  if (!rows.length) {
    el.innerHTML = '<div class="desk-item tone-neutral"><span class="desk-icon">&#8226;</span><div class="desk-text">Waiting for sector rotation data.</div></div>';
    return;
  }
  el.innerHTML = rows.map(row => `<div class="board-row">
      <div class="board-name">
        <strong>${row.sector}</strong>
        <span class="board-meta">${row.label} | ${row.newsBias}</span>
      </div>
    <div class="board-pct ${row.pct >= 0 ? 'up' : 'dn'}">${signed(row.pct)}</div>
    <div class="board-bias">${row.newsBias}</div>
  </div>`).join('');
}

function renderKeyLevels() {
  const el = document.getElementById('key-levels');
  const rows = analyticsPayload.keyLevels || [];
  if (!rows.length) {
    el.innerHTML = '<div class="desk-item tone-neutral"><span class="desk-icon">&#8226;</span><div class="desk-text">Key levels will appear when price history is available.</div></div>';
    return;
  }
  el.innerHTML = rows.map(row => {
    const signalTone = chipClass(row.signal);
    return `<div class="key-card">
      <div class="key-top">
        <div>
          <div class="key-name">${row.label}</div>
          <div class="watch-note">${row.trend || 'Live snapshot'}</div>
        </div>
        <div class="key-price ${typeof row.pct === 'number' ? (row.pct >= 0 ? 'up' : 'dn') : ''}">${rupees(row.price).replace('Rs', row.label === 'India VIX' ? '' : 'Rs')}</div>
      </div>
      <div><span class="signal-chip ${signalTone}">${row.signal || 'Live snapshot'}</span></div>
      <div class="key-meta">
        <div class="mini-stat"><span class="stat-label">Day</span><span class="stat-value ${typeof row.pct === 'number' ? (row.pct >= 0 ? 'up' : 'dn') : ''}">${signed(row.pct)}</span></div>
        <div class="mini-stat"><span class="stat-label">RSI 14</span><span class="stat-value">${num(row.rsi14, 1)}</span></div>
        <div class="mini-stat"><span class="stat-label">Support</span><span class="stat-value">${row.support !== null && row.support !== undefined ? num(row.support) : '—'}</span></div>
        <div class="mini-stat"><span class="stat-label">Resistance</span><span class="stat-value">${row.resistance !== null && row.resistance !== undefined ? num(row.resistance) : '—'}</span></div>
      </div>
    </div>`;
  }).join('');
}

function renderWatchlistSignals() {
  const el = document.getElementById('watchlist-signals');
  const bySymbol = Object.fromEntries((analyticsPayload.watchlistSignals || []).map(item => [item.symbol, item]));
  const syms = [...watchlist];
  if (!syms.length) {
    el.innerHTML = '<div class="desk-item tone-neutral"><span class="desk-icon">&#8226;</span><div class="desk-text">Add symbols to start building your trade radar.</div></div>';
    return;
  }
  el.innerHTML = syms.map(sym => {
    const row = bySymbol[sym];
    if (!row) {
      return `<div class="watch-card">
        <div class="watch-top"><div><div class="watch-symbol">${sym}</div><div class="watch-name">Waiting for data</div></div></div>
        <div class="watch-note">Live analytics is not available for this symbol yet.</div>
      </div>`;
    }
    return `<div class="watch-card">
      <div class="watch-top">
        <div>
          <div class="watch-symbol">${row.symbol}</div>
          <div class="watch-name">${row.name || (SYM_TO_LABEL[row.symbol] || row.symbol)}</div>
        </div>
        <div class="watch-price">
          <div class="${row.pct >= 0 ? 'up' : 'dn'}" style="font-size:14px;font-weight:800;">${rupees(row.price)}</div>
          <div class="${row.pct >= 0 ? 'up' : 'dn'}" style="font-size:10px;">${signed(row.pct)}</div>
        </div>
      </div>
      <div style="display:flex;gap:6px;flex-wrap:wrap;">
        <span class="signal-chip ${chipClass(row.trend)}">${row.trend}</span>
        <span class="signal-chip ${chipClass(row.signal)}">${row.signal}</span>
      </div>
      <div class="watch-metrics">
        <div class="mini-stat"><span class="stat-label">RSI 14</span><span class="stat-value">${num(row.rsi14, 1)}</span></div>
        <div class="mini-stat"><span class="stat-label">5D</span><span class="stat-value ${typeof row.ret5 === 'number' ? (row.ret5 >= 0 ? 'up' : 'dn') : ''}">${signed(row.ret5)}</span></div>
        <div class="mini-stat"><span class="stat-label">20D</span><span class="stat-value ${typeof row.ret20 === 'number' ? (row.ret20 >= 0 ? 'up' : 'dn') : ''}">${signed(row.ret20)}</span></div>
        <div class="mini-stat"><span class="stat-label">Support</span><span class="stat-value">${num(row.support)}</span></div>
        <div class="mini-stat"><span class="stat-label">Resistance</span><span class="stat-value">${num(row.resistance)}</span></div>
        <div class="mini-stat"><span class="stat-label">20D High Gap</span><span class="stat-value ${typeof row.breakoutGap === 'number' ? (row.breakoutGap >= 0 ? 'up' : 'dn') : ''}">${signed(row.breakoutGap)}</span></div>
      </div>
      <div class="watch-note">Volatility ${num(row.vol20, 1)} annualized | Volume ratio ${num(row.volumeRatio, 2)} | Drawdown from 6M high ${signed(row.drawdownFromHigh)}</div>
    </div>`;
  }).join('');
}

function renderPortfolioSummary() {
  const el = document.getElementById('portfolio-summary');
  const entries = Object.entries(portfolio);
  if (!entries.length) {
    el.innerHTML = '<div class="summary-card"><div class="ov-label">No Portfolio</div><div class="ov-value">Add holdings</div><div class="ov-detail">You will get live P&amp;L and risk context here.</div></div>';
    return;
  }
  let cost = 0, live = 0;
  let best = null, worst = null;
  for (const [sym, { qty, buyPrice }] of entries) {
    cost += qty * buyPrice;
    const cur = getLivePrice(sym);
    const ret = cur ? ((cur - buyPrice) / buyPrice * 100) : null;
    live += cur ? qty * cur : qty * buyPrice;
    if (ret !== null) {
      if (!best || ret > best.ret) best = { sym, ret };
      if (!worst || ret < worst.ret) worst = { sym, ret };
    }
  }
  const pnl = live - cost;
  el.innerHTML = `
    <div class="summary-card ${pnl >= 0 ? 'tone-bull' : 'tone-bear'}"><div class="ov-label">Unrealized P&amp;L</div><div class="ov-value">${pnl >= 0 ? '+' : '-'}${rupees(Math.abs(pnl))}</div><div class="ov-detail">${cost ? ((pnl / cost) * 100).toFixed(2) : '0.00'}% on marked positions.</div></div>
    <div class="summary-card tone-neutral"><div class="ov-label">Live Value</div><div class="ov-value">${rupees(live)}</div><div class="ov-detail">Cost basis ${rupees(cost)}</div></div>
    <div class="summary-card ${best && best.ret >= 0 ? 'tone-bull' : 'tone-neutral'}"><div class="ov-label">Best Holding</div><div class="ov-value">${best ? best.sym : '—'}</div><div class="ov-detail">${best ? signed(best.ret) : 'Waiting for live price'}</div></div>
    <div class="summary-card ${worst && worst.ret < 0 ? 'tone-bear' : 'tone-neutral'}"><div class="ov-label">Weakest Holding</div><div class="ov-value">${worst ? worst.sym : '—'}</div><div class="ov-detail">${worst ? signed(worst.ret) : 'Waiting for live price'}</div></div>`;
}

function renderDerivativesPrediction() {
  const el = document.getElementById('deriv-prediction-grid');
  const cards = derivativesPayload.predictionCards || [];
  if (!cards.length) {
    el.innerHTML = '<div class="overview-card tone-neutral"><div class="ov-label">Loading</div><div class="ov-value">Prediction</div><div class="ov-detail">Composite derivatives bias will appear when enough live context is available.</div></div>';
    return;
  }
  el.innerHTML = cards.map(card => `<div class="overview-card ${toneClass(card.tone)}">
    <div class="ov-label">${card.label}</div>
    <div class="ov-value">${card.value}</div>
    <div class="ov-detail">${card.detail}</div>
  </div>`).join('');
}

function renderDerivativesRiskFlags() {
  const el = document.getElementById('deriv-risk-list');
  const flags = derivativesPayload.riskFlags || [];
  if (!flags.length) {
    el.innerHTML = '<div class="desk-item tone-neutral"><span class="desk-icon">&#8226;</span><div class="desk-text">No acute derivatives risk flags right now.</div></div>';
    return;
  }
  el.innerHTML = flags.map(flag => `<div class="desk-item ${toneClass(flag.tone)}"><span class="desk-icon">&#9888;</span><div class="desk-text"><strong>${flag.label}</strong> ${flag.detail}</div></div>`).join('');
}

function renderDerivativesScoreBoard() {
  const el = document.getElementById('deriv-score-board');
  const rows = derivativesPayload.scoreBreakdown || [];
  if (!rows.length) {
    el.innerHTML = '<div class="desk-item tone-neutral"><span class="desk-icon">&#8226;</span><div class="desk-text">Score breakdown is loading.</div></div>';
    return;
  }
  el.innerHTML = rows.map(row => {
    const tone = row.score > 0 ? 'up' : row.score < 0 ? 'dn' : '';
    const bias = row.score > 0 ? 'Bullish' : row.score < 0 ? 'Bearish' : 'Neutral';
    return `<div class="board-row">
      <div class="board-name">
        <strong>${row.label}</strong>
        <span class="board-meta">${row.detail}</span>
      </div>
      <div class="board-pct ${tone}">${row.score >= 0 ? '+' : ''}${row.score}</div>
      <div class="board-bias">${bias}</div>
    </div>`;
  }).join('');
}

function renderDerivativesScenarios() {
  const el = document.getElementById('deriv-scenario-list');
  const rows = derivativesPayload.tradeScenarios || [];
  if (!rows.length) {
    el.innerHTML = '<div class="desk-item tone-neutral"><span class="desk-icon">&#8226;</span><div class="desk-text">Scenario playbook will appear when index structure is available.</div></div>';
    return;
  }
  el.innerHTML = rows.map(row => {
    const chip = row.tone === 'bull' ? 'chip-bull' : row.tone === 'bear' ? 'chip-bear' : 'chip-neutral';
    const label = row.tone === 'bull' ? 'Bullish' : row.tone === 'bear' ? 'Bearish' : 'Neutral';
    return `<div class="key-card">
      <div class="key-top">
        <div>
          <div class="key-name">${row.label}</div>
          <div class="watch-note">${row.note}</div>
        </div>
        <div><span class="signal-chip ${chip}">${label}</span></div>
      </div>
      <div class="key-meta">
        <div class="mini-stat"><span class="stat-label">Trigger</span><span class="stat-value">${row.trigger || '—'}</span></div>
        <div class="mini-stat"><span class="stat-label">Target</span><span class="stat-value">${row.target || '—'}</span></div>
        <div class="mini-stat"><span class="stat-label">Invalidation</span><span class="stat-value">${row.invalidation || '—'}</span></div>
        <div class="mini-stat"><span class="stat-label">Setup</span><span class="stat-value">${row.label}</span></div>
      </div>
    </div>`;
  }).join('');
}

function renderDerivativesContext() {
  const el = document.getElementById('deriv-context-list');
  const notes = derivativesPayload.contextNotes || [];
  if (!notes.length) {
    el.innerHTML = '<div class="desk-item tone-neutral"><span class="desk-icon">&#8226;</span><div class="desk-text">Context notes will appear when live market relationships are available.</div></div>';
    return;
  }
  el.innerHTML = notes.map(text => `<div class="desk-item tone-neutral"><span class="desk-icon">&#8226;</span><div class="desk-text">${text}</div></div>`).join('');
}

function renderDerivativesAnalysis() {
  renderDerivativesPrediction();
  renderDerivativesRiskFlags();
  renderDerivativesScoreBoard();
  renderDerivativesScenarios();
  renderDerivativesContext();
}

function renderAnalytics() {
  symbolAnalytics = analyticsPayload.symbolMap || {};
  renderOverview();
  renderDeskAlerts();
  renderSectorBoard();
  renderKeyLevels();
  renderWatchlistSignals();
  renderPortfolioSummary();
  updateHeatmap();
  renderPortfolio();
}

function impCls(n) { return n >= 7 ? 'imp-hi' : n >= 4 ? 'imp-md' : 'imp-lo'; }
function sentBadge(s) { if (s === 'bullish') return ['b-bull', '▲ Bullish']; if (s === 'bearish') return ['b-bear', '▼ Bearish']; return ['b-neut', '— Neutral']; }

function aiSummarySourceBadge(a) {
  if (!articleHasAiSummary(a)) return '';
  const analysis = a.aiAnalysis || {};
  const textSource = String(analysis.textSource || '').toLowerCase();
  const inputChars = Number(analysis.inputChars || 0);
  const title = inputChars > 0 ? `AI used ${inputChars.toLocaleString()} characters from ${textSource || 'unknown source'}` : 'AI summary source';
  if (textSource === 'article-page') {
    return `<span class="badge b-ai-full" title="${escapeHtml(title)}">Full Body</span>`;
  }
  if (textSource === 'rss-feed') {
    return `<span class="badge b-ai-rss" title="${escapeHtml(title)}">RSS Feed</span>`;
  }
  return '<span class="badge b-ai-src" title="AI summary source unknown">AI Source?</span>';
}

function cardBadgesHTML(a) {
  const hasAiSummary = articleHasAiSummary(a);
  const [bCls, bTxt] = sentBadge(a.sentiment && a.sentiment.label);
  const scope = a.scope === 'global' ? 'Global' : 'Local';
  const scopeBadgeClass = a.scope === 'global' ? 'b-global' : 'b-local';
  return `<span class="badge ${scopeBadgeClass}">${scope}</span><span class="badge b-sector">${escapeHtml(a.sector || 'General')}</span>${hasAiSummary ? '<span class="badge b-ai">AI Summary</span>' : ''}${aiSummarySourceBadge(a)}<span class="badge ${bCls}">${bTxt}</span>`;
}

function summaryHTML(a, hasAiSummary = false) {
  const text = articleSummaryText(a);
  if (!text) return '<div class="card-summary"><em style="opacity:.5">No summary.</em></div>';
  return `<div class="summary-block"><div class="card-summary${hasAiSummary ? ' card-summary-ai' : ''}">${escapeHtml(text)}</div></div>`;
}

function articleSummaryText(a) {
  return String(a.summary || '').replace(/\s+/g, ' ').trim();
}

function articleCardStableSignature(a) {
  return JSON.stringify([
    a.title,
    a.source,
    a.feed,
    a.published,
    a.ts,
    a.scope,
    a.sector,
    a.impact,
    a.link,
    a.sentiment && a.sentiment.label,
    bookmarks.has(a.id),
    isWL(a),
  ]);
}

function applyCardMetadata(node, article) {
  if (!node) return;
  node.dataset.articleStableSignature = articleCardStableSignature(article);
  node.dataset.summaryText = articleSummaryText(article);
  node.dataset.summarySource = article.summarySource || '';
}

function cardHTML(a) {
  const wl = isWL(a);
  const bm = bookmarks.has(a.id);
  const hasAiSummary = articleHasAiSummary(a);
  const ic = impCls(a.impact);
  const relTime = relativeTime(a.ts);
  const impactReasons = a.impactMeta && Array.isArray(a.impactMeta.reasons) ? a.impactMeta.reasons : [];
  const impactTitle = impactReasons.length ? ` title="${escapeHtml(impactReasons.join(' | '))}"` : '';
  const readBtn = a.link && a.link !== '#' ? `<a class="read-btn" href="${a.link}" target="_blank" rel="noopener noreferrer">Read &#10138;</a>` : '<span style="font-size:10px;color:var(--muted)">No link</span>';
  return `<div class="card" data-article-id="${escapeHtml(a.id)}">
    <div class="card-top">
      <div class="card-title">${wl ? '<span class="wl-tag">&#9733; WL</span>' : ''}${a.title}</div>
      <div class="card-badges">${cardBadgesHTML(a)}</div>
    </div>
    ${summaryHTML(a, hasAiSummary)}
    <div class="card-footer">
      <div class="meta"><span class="meta-src" title="${a.feed || a.source}">${a.source}</span><span class="meta-time">${a.published}${relTime ? ` · ${relTime}` : ''}</span></div>
      <div style="display:flex;align-items:center;gap:8px;">
        <div class="impact-row ${ic}"${impactTitle}><span class="imp-lbl">Impact</span><div class="imp-track"><div class="imp-fill" style="width:${a.impact * 10}%"></div></div><span class="imp-lbl">${a.impact}/10</span></div>
        <div class="card-actions">
          <button class="bm-btn${bm ? ' bm-on' : ''}" onclick="toggleBookmark('${a.id}')" title="${bm ? 'Remove bookmark' : 'Bookmark'}">${bm ? '&#9733;' : '&#9734;'}</button>
          ${readBtn}
        </div>
      </div>
    </div>
  </div>`;
}

function render() {
  let arts = filteredArticles(allArticles);
  arts = applySort(arts);
  const feed = document.getElementById('feed');
  const noRes = document.getElementById('no-results');
  if (!arts.length) {
    feed.replaceChildren();
    noRes.style.display = 'block';
    return;
  }
  noRes.style.display = 'none';
  syncFeedCards(arts);
}

function sparklineSVG(pts, w, h) {
  if (!pts || pts.length < 2) return '';
  const mn = Math.min(...pts), mx = Math.max(...pts);
  const rng = mx - mn || 1;
  const coords = pts.map((v, i) => {
    const x = (i / (pts.length - 1)) * w;
    const y = h - ((v - mn) / rng) * (h - 2) - 1;
    return `${x.toFixed(1)},${y.toFixed(1)}`;
  }).join(' ');
  const color = pts[pts.length - 1] >= pts[0] ? '#22c55e' : '#ef4444';
  return `<svg width="${w}" height="${h}" viewBox="0 0 ${w} ${h}" style="overflow:visible;display:block;"><polyline points="${coords}" fill="none" stroke="${color}" stroke-width="1.5" stroke-linejoin="round"/></svg>`;
}

function renderTickers(data) {
  currentTickers = data || {};
  const wrap = document.getElementById('tick-wrap');
  if (!Object.keys(currentTickers).length) {
    renderMarketStatus();
    return;
  }
  wrap.innerHTML = '';
  const rows = [];
  for (const label of FIXED_TICKER_LABELS) {
    if (currentTickers[label]) rows.push({ key: label, label, data: currentTickers[label], history: priceHistory[label] || [] });
  }
  for (const sym of trackedTickerSymbols()) {
    const custom = customTickerQuotes[sym];
    const fallbackLabel = SYM_TO_LABEL[sym] || sym;
    const fallback = currentTickers[fallbackLabel] || currentTickers[sym];
    const quote = custom || fallback;
    if (!quote) continue;
    rows.push({
      key: sym,
      label: custom ? (custom.label || sym) : fallbackLabel,
      data: quote,
      history: customTickerHistory[sym] || priceHistory[fallbackLabel] || [],
    });
  }
  for (const row of rows) {
    const label = row.label;
    const d = row.data;
    const dir = d.change >= 0 ? 'up' : 'dn';
    const sign = d.change >= 0 ? '+' : '';
    const sym = d.sym !== undefined ? d.sym : (label === 'Brent' || label === 'Gold' ? '$' : '');
    const arrow = d.change >= 0 ? '▲' : '▼';
    const age = typeof d.ageSeconds === 'number'
      ? d.ageSeconds
      : typeof d.fetchedAt === 'number'
        ? Math.max(0, (Date.now() / 1000) - d.fetchedAt)
        : null;
    const stale = Boolean(d.stale || (d.live && marketStatus && marketStatus.tickersStale));
    const liveDot = d.live ? `<span class="live-dot${stale ? ' stale' : ''}"></span>` : '';
    const freshness = age !== null ? ` | ${ageLabel(age)}` : '';
    const hist = row.history;
    const spark = hist && hist.length > 1 ? sparklineSVG(hist, 60, 16) : '';
    wrap.innerHTML += `<div class="ticker-item" title="${escapeHtml((d.source || 'Market feed') + freshness)}">
      <span class="t-label">${liveDot}${label}</span>
      <span class="t-price ${dir}">${sym ? sym : ''}${d.price.toLocaleString('en-IN', { maximumFractionDigits: 2 })}</span>
      <span class="t-chg ${dir}">${arrow} ${sign}${d.pct.toFixed(2)}%</span>
      ${spark ? `<div class="t-spark">${spark}</div>` : ''}
    </div>`;
  }
  renderMarketStatus();
  renderPortfolio();
}

function connectTickerSSE() {
  const es = new EventSource('/api/tickers/stream');
  es.onmessage = e => {
    try {
      const payload = JSON.parse(e.data);
      applyMarketSnapshot(payload);
    } catch (_) {}
  };
  es.onerror = () => { es.close(); setTimeout(connectTickerSSE, 5000); };
}

async function enableNotifications() {
  if (!('Notification' in window)) { showToast('Notifications not supported'); return; }
  const perm = await Notification.requestPermission();
  notifEnabled = perm === 'granted';
  document.getElementById('notif-btn').classList.toggle('on', notifEnabled);
  showToast(notifEnabled ? 'Alerts enabled for impact >= 8 news' : 'Notification permission denied');
}

function checkNotifications(articles) {
  if (!notifEnabled) return;
  for (const a of articles) {
    if (a.impact >= 8 && !seenHighImpact.has(a.id)) {
      seenHighImpact.add(a.id);
      try { new Notification(`Impact ${a.impact}/10 | ${a.source}`, { body: a.title, tag: a.id }); } catch (_) {}
    }
  }
  sessionStorage.setItem('seen_hi', JSON.stringify([...seenHighImpact]));
}

async function fetchHistory() {
  try {
    const r = await fetch('/api/history');
    priceHistory = await r.json();
    if (Object.keys(currentTickers).length) renderTickers(currentTickers);
  } catch (_) {}
}

async function fetchAnalytics() {
  try {
    const r = await fetch('/api/analytics');
    analyticsPayload = await r.json();
    renderAnalytics();
  } catch (e) {
    console.error('Analytics error', e);
  }
}

async function fetchDerivativesAnalysis() {
  try {
    const r = await fetch('/api/derivatives/overview');
    derivativesPayload = await r.json();
    renderDerivativesAnalysis();
  } catch (e) {
    console.error('Derivatives analysis error', e);
  }
}

async function fetchMarketSnapshot(includeHistory = false) {
  try {
    const r = await fetch('/api/snapshot?history=' + (includeHistory ? '1' : '0'));
    if (!r.ok) throw new Error('Snapshot request failed');
    const payload = await r.json();
    applyMarketSnapshot(payload);
  } catch (e) {
    console.error('Market snapshot error', e);
  }
}

let countdownTimer;
let firstLoad = true;

function mergeAiSummaryUpdates(updates) {
  if (!Array.isArray(updates) || !updates.length) return [];
  const byId = new Map(allArticles.map(article => [article.id, article]));
  const changed = [];
  for (const update of updates) {
    const article = byId.get(update.id);
    if (!article || update.summarySource !== 'ai' || !String(update.summary || '').trim()) continue;
    const before = JSON.stringify({
      summary: article.summary || '',
      summarySource: article.summarySource || '',
      analysisSource: article.analysisSource || '',
      sentiment: article.sentiment || {},
      impact: article.impact,
      impactMeta: article.impactMeta || {},
      sector: article.sector || '',
      aiAnalysis: article.aiAnalysis || {},
    });
    if (update.sentiment && typeof update.sentiment === 'object') article.sentiment = update.sentiment;
    if (typeof update.impact === 'number') article.impact = update.impact;
    if (update.impactMeta && typeof update.impactMeta === 'object') article.impactMeta = update.impactMeta;
    if (update.sector) article.sector = update.sector;
    if (update.aiAnalysis && typeof update.aiAnalysis === 'object') article.aiAnalysis = update.aiAnalysis;
    if (update.analysisSource) article.analysisSource = update.analysisSource;
    article.summary = update.summary;
    article.summarySource = 'ai';
    const after = JSON.stringify({
      summary: article.summary || '',
      summarySource: article.summarySource || '',
      analysisSource: article.analysisSource || '',
      sentiment: article.sentiment || {},
      impact: article.impact,
      impactMeta: article.impactMeta || {},
      sector: article.sector || '',
      aiAnalysis: article.aiAnalysis || {},
    });
    if (before !== after) {
      changed.push(article);
    }
  }
  return changed;
}

function articleCardSelector(articleId) {
  if (window.CSS && typeof window.CSS.escape === 'function') {
    return `.card[data-article-id="${window.CSS.escape(String(articleId))}"]`;
  }
  return `.card[data-article-id="${String(articleId).replace(/["\\]/g, '\\$&')}"]`;
}

function createCardNode(article) {
  const template = document.createElement('template');
  template.innerHTML = cardHTML(article).trim();
  const node = template.content.firstElementChild;
  applyCardMetadata(node, article);
  return node;
}

function syncCardAiBadge(card, hasAiSummary) {
  const badges = card.querySelector('.card-badges');
  if (!badges) return;
  const existing = badges.querySelector('.b-ai');
  if (!hasAiSummary) {
    if (existing) existing.remove();
    return;
  }
  if (existing) return;
  const badge = document.createElement('span');
  badge.className = 'badge b-ai';
  badge.textContent = 'AI Summary';
  const sentimentBadge = badges.querySelector('.b-bull, .b-bear, .b-neut');
  badges.insertBefore(badge, sentimentBadge || null);
}

function updateCardBadgeNode(card, article) {
  const badges = card.querySelector('.card-badges');
  if (!badges) return;
  badges.innerHTML = cardBadgesHTML(article);
}

function updateCardImpactNode(card, article) {
  const row = card.querySelector('.impact-row');
  if (!row) return;
  const impact = Number(article.impact || 0);
  const impactReasons = article.impactMeta && Array.isArray(article.impactMeta.reasons) ? article.impactMeta.reasons : [];
  row.className = `impact-row ${impCls(impact)}`;
  if (impactReasons.length) row.title = impactReasons.join(' | '); else row.removeAttribute('title');
  const fill = row.querySelector('.imp-fill');
  if (fill) fill.style.width = `${Math.max(0, Math.min(10, impact)) * 10}%`;
  const labels = row.querySelectorAll('.imp-lbl');
  if (labels.length > 1) labels[1].textContent = `${impact}/10`;
}

function updateCardSummaryNode(card, article) {
  const text = articleSummaryText(article);
  const summary = card.querySelector('.card-summary');
  if (!summary || !text) return false;
  if (summary.textContent !== text) {
    summary.textContent = text;
  }
  const hasAiSummary = articleHasAiSummary(article);
  summary.classList.toggle('card-summary-ai', hasAiSummary);
  updateCardBadgeNode(card, article);
  card.dataset.summaryText = text;
  card.dataset.summarySource = article.summarySource || '';
  return true;
}

function syncExistingCardNode(card, article) {
  const nextStableSignature = articleCardStableSignature(article);
  if (card.dataset.articleStableSignature !== nextStableSignature) {
    return createCardNode(article) || card;
  }
  updateCardSummaryNode(card, article);
  applyCardMetadata(card, article);
  return card;
}

function syncFeedCards(articles) {
  const feed = document.getElementById('feed');
  feed.querySelectorAll(':scope > :not(.card)').forEach(node => node.remove());
  const existingById = new Map();
  feed.querySelectorAll('.card[data-article-id]').forEach(card => {
    existingById.set(card.dataset.articleId, card);
  });

  const expectedIds = new Set(articles.map(article => String(article.id)));
  let cursor = feed.firstElementChild;
  for (const article of articles) {
    const existingNode = existingById.get(String(article.id));
    let node = existingNode ? syncExistingCardNode(existingNode, article) : createCardNode(article);
    if (!node) continue;
    if (existingNode && node !== existingNode) {
      feed.insertBefore(node, cursor === existingNode ? existingNode : cursor);
      existingNode.remove();
    } else if (node !== cursor) {
      feed.insertBefore(node, cursor);
    }
    cursor = node.nextElementSibling;
  }

  feed.querySelectorAll('.card[data-article-id]').forEach(card => {
    if (!expectedIds.has(card.dataset.articleId)) card.remove();
  });
}

function insertArticleCardIfVisible(article) {
  const sorted = applySort(filteredArticles(allArticles));
  const index = sorted.findIndex(item => item.id === article.id);
  if (index < 0) return false;
  const feed = document.getElementById('feed');
  const noRes = document.getElementById('no-results');
  const node = createCardNode(article);
  if (!node) return false;
  for (let i = index + 1; i < sorted.length; i++) {
    const nextCard = feed.querySelector(articleCardSelector(sorted[i].id));
    if (nextCard) {
      feed.insertBefore(node, nextCard);
      noRes.style.display = 'none';
      return true;
    }
  }
  feed.appendChild(node);
  noRes.style.display = 'none';
  return true;
}

function patchVisibleAiSummaryCards(changedArticles) {
  let patched = 0;
  for (const article of changedArticles) {
    const card = document.querySelector(articleCardSelector(article.id));
    if (!card) {
      if (insertArticleCardIfVisible(article)) patched++;
      continue;
    }
    const text = String(article.summary || '').replace(/\s+/g, ' ').trim();
    if (!text) continue;

    updateCardSummaryNode(card, article);
    updateCardBadgeNode(card, article);
    updateCardImpactNode(card, article);
    applyCardMetadata(card, article);

    patched++;
  }
  return patched;
}

function scheduleAiSummaryPolling(progress = aiSummaryProgress) {
  clearTimeout(aiSummaryPollTimer);
  if (!progress || !progress.total) return;
  const queued = Number(progress.queued || 0);
  const inflight = Number(progress.inflight || 0);
  if (progress.enabled === false && inflight <= 0 && queued <= 0) return;
  if ((progress.pending || 0) <= 0 && inflight <= 0 && queued <= 0) return;
  aiSummaryPollTimer = setTimeout(fetchAiSummaryUpdates, AI_SUMMARY_POLL_INTERVAL_MS);
}

async function fetchAiSummaryUpdates() {
  try {
    const r = await fetch('/api/news/ai-summaries');
    if (!r.ok) throw new Error('AI summary update request failed');
    const d = await r.json();
    const changedArticles = mergeAiSummaryUpdates(d.updates || []);
    aiSummaryProgress = d.progress || aiSummaryProgress;
    if (changedArticles.length) {
      patchVisibleAiSummaryCards(changedArticles);
      fetchMarketSnapshot(false);
    }
    patchAiSummaryProgressPill(aiSummaryProgress);
    scheduleAiSummaryPolling(aiSummaryProgress);
  } catch (e) {
    console.error('AI summary update error', e);
    aiSummaryPollTimer = setTimeout(fetchAiSummaryUpdates, AI_SUMMARY_POLL_INTERVAL_MS * 2);
  }
}

function startCountdown() {
  let secs = refreshInterval;
  clearInterval(countdownTimer);
  countdownTimer = setInterval(() => {
    secs--;
    const m = Math.floor(secs / 60), s = secs % 60;
    document.getElementById('next-refresh').textContent = `(next ${m > 0 ? m + 'm ' : ''}${s}s)`;
    if (secs <= 0) { clearInterval(countdownTimer); fetchNews(); }
  }, 1000);
}

async function fetchNews() {
  try {
    const r = await fetch('/api/news');
    const d = await r.json();
    const prev = allArticles.length;
    allArticles = d.articles || [];
    feedStatus = d.feedStatus || {};
    refreshInterval = d.refreshInterval || refreshInterval;
    allowedRefreshWindows = d.allowedRefreshWindows || allowedRefreshWindows;
    marketStatus = d.marketStatus || marketStatus;
    aiSummaryProgress = d.aiSummaryProgress || aiSummaryProgress;
    syncRefreshControl();
    document.getElementById('updated').textContent = 'Updated ' + d.updated;
    const pulse = document.getElementById('refresh-pulse');
    pulse.classList.add('active');
    setTimeout(() => pulse.classList.remove('active'), 2000);
    if (!firstLoad && allArticles.length > prev) showToast(`${allArticles.length - prev} new article${allArticles.length - prev > 1 ? 's' : ''} loaded`);
    if (firstLoad) firstLoad = false;
    checkNotifications(allArticles);
    renderMarketStatus();
    render();
    updateHeatmap();
    startCountdown();
    scheduleAiSummaryPolling(aiSummaryProgress);
  } catch (e) {
    console.error('News error', e);
    if (firstLoad) {
      document.getElementById('feed').innerHTML = '<div class="loading"><div style="color:#ef4444">Failed to load. Is the server running?</div></div>';
    }
  }
}

document.addEventListener('keydown', e => {
  if (e.key === '/' && document.activeElement !== document.getElementById('search')) {
    e.preventDefault();
    document.getElementById('search').focus();
  }
  if (e.key === 'Escape') {
    const s = document.getElementById('search');
    s.value = '';
    s.blur();
    render();
  }
});

renderTickerManager();
renderWL();
renderPortfolio();
renderAnalytics();
renderDerivativesAnalysis();
fetchNews();
fetchMarketSnapshot(true);
connectTickerSSE();
fetchAppState().finally(() => {
  fetchTickerQuotes();
  fetchMarketSnapshot(true);
});
setInterval(() => fetchMarketSnapshot(false), 120000);
