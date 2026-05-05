# Indian Market Agent

This repo contains a Flask backend plus a static frontend for an India-focused market agent platform with news intelligence, analytics, derivatives context, and optional Upstox-backed market data.

## Project Layout

- `backend/app.py`: Flask API routes, polling loops, runtime orchestration, and provider fallback wiring
- `backend/core/`: runtime settings and SQLite persistence
- `backend/agents/news/`: RSS sources, article extraction, news scoring, AI prompts, AI summary queueing, and News Intelligence Agent reports
- `backend/news/`: temporary compatibility bridges for older imports
- `backend/shared/`: shared constants and helpers for platform modules
- `backend/routes/`: future Flask route modules
- `backend/market/`: symbol catalogs, NSE/Upstox mappings, and reusable market math
- `backend/providers/upstox/`: Upstox quote parsing, Market Data Feed V3 helpers, live feed client, and option-chain summarization
- `backend/worker.py`: production worker entrypoint for news polling, market refresh, Upstox streaming, and AI queue work
- `frontend/`: static HTML, CSS, and vanilla JavaScript dashboard
- `docs/production_improvement_plan.md`: phased roadmap for AI quality, worker separation, UI cleanup, and production hardening
- `docs/aws_lightsail_deployment.md`: step-by-step production runbook for `stockterminal.in`
- `deploy/lightsail/`: example systemd, Nginx, and environment files for AWS Lightsail
- `tests/`: regression tests for scoring, persistence, derivatives, and Upstox integration helpers

## Run Locally

```bash
cd /Users/yashkumar/Documents/claude-projects/indian-market-agent
pip install -r requirements.txt
python backend/app.py
```

The backend serves the frontend automatically from `frontend/`.

## Upstox Integration

The app uses the Upstox Analytics Token only. This is a long-lived, read-only token for market-data APIs such as full quotes, Market Data Feed V3, option chain, option contracts, greeks, and instrument search.

Quick local run:

```bash
MARKET_DATA_PROVIDER=upstox UPSTOX_ANALYTICS_TOKEN=your_token UPSTOX_HTTP_TRANSPORT=curl python backend/app.py
```

Useful Upstox settings:

- `MARKET_DATA_PROVIDER=upstox` prefers Upstox quotes and option-chain APIs
- `UPSTOX_HTTP_TRANSPORT=curl` uses the system curl transport when Upstox's edge blocks Python requests
- `UPSTOX_FALLBACK_TO_NSE=1` keeps NSE fallback enabled if Upstox is unavailable for a symbol
- `UPSTOX_INSTRUMENT_KEYS='{"SBIN":"NSE_EQ|INE062A01020"}'` overrides or extends symbol mappings
- `UPSTOX_OPTION_EXPIRY=YYYY-MM-DD` sets a default expiry for `/api/derivatives/option-chain`

Production-friendly Upstox endpoints:

- `GET /api/integrations/upstox/status`

## Macro Context Agent

The Macro Context Agent is a deterministic, rule-based layer that turns macro variables into a structured market-context report for India-focused index workflows. It does not produce buy/sell calls or place orders. Instead, it classifies whether the macro backdrop is supportive, mixed, bearish, or event-risk heavy, then maps that into trade filters and strategy-engine guidance such as reducing long confidence, reducing short confidence, waiting for event risk, or blocking fresh trades during extreme macro shock.

Source stack:

- FMP for `USD/INR`, gold, crude, US indices/global cues, the economic calendar, and optional basic macro snapshots
- Upstox/current app quote state for India VIX through a small optional wrapper
- Future India-specific provider stubs for RBI, MOSPI, NSDL/NSE, and CCIL

Default scheduled times in `Asia/Kolkata`:

- `08:35` pre-market macro report
- `09:25` post-open macro confirmation
- `12:30` mid-day macro check
- `15:00` pre-close macro check
- `15:45` post-market macro summary

Important env vars:

- `FMP_ENABLED=false`
- `FMP_API_KEY=`
- `FMP_TIMEOUT_SECONDS=8`
- `FMP_CACHE_TTL_SECONDS=3600`
- `MACRO_AGENT_ENABLED=true`
- `MACRO_AGENT_REFRESH_MODE=scheduled`
- `MACRO_AGENT_PREMARKET_TIME=08:35`
- `MACRO_AGENT_OPEN_CHECK_TIME=09:25`
- `MACRO_AGENT_MIDDAY_TIME=12:30`
- `MACRO_AGENT_PRE_CLOSE_TIME=15:00`
- `MACRO_AGENT_POSTMARKET_TIME=15:45`
- `MACRO_AGENT_TIMEZONE=Asia/Kolkata`

API endpoint:

- `GET /api/agents/macro-context`
- Query params: `force_refresh=true|false`, `mock=true|false`

Run tests:

```bash
/Library/Frameworks/Python.framework/Versions/3.11/bin/python3 -m pytest
```

Strategy Engine integration:

- The Macro Context Agent writes its latest JSON-serializable report into `backend/agents/agent_output_store.py`.
- A future Strategy Engine can consume the report as a context/filter layer for long confidence, short confidence, position sizing, event-risk waiting, and extreme-risk trade blocking.

## F&O Structure Agent

The F&O Structure Agent is a read-only option-chain analysis layer for future strategy-engine context. It supports only NIFTY 50 and SENSEX options in v1. BANKNIFTY is intentionally out of scope and is returned as an unsupported neutral report rather than being analyzed.

Source stack:

- Upstox Analytics Token for read-only market-data APIs
- Option Chain API for strike-wise CE/PE OI, volume, bid/ask, IV, and Greeks
- Option Contracts API for expiries and contract metadata
- Option Greeks API for optional selected-strike refreshes later

The agent computes PCR, support/resistance zones, put/call writing, call/put unwinding, max pain, expiry risk, preferred option zones, and strategy-engine guidance such as reducing size or avoiding directional trades. It does not place orders, modify orders, cancel orders, or produce buy/sell recommendations.

API endpoint:

- `GET /api/agents/fo-structure`
- Query params: `symbol=NIFTY|SENSEX`, `expiry=YYYY-MM-DD`, `mock=true|false`

Useful local checks:

```bash
python3 -m pytest -q tests/agents/test_fo_pcr.py tests/agents/test_fo_zones.py tests/agents/test_fo_max_pain.py tests/agents/test_fo_structure_agent.py tests/providers/test_upstox_options_provider.py
curl "http://127.0.0.1:9090/api/agents/fo-structure?symbol=NIFTY&mock=true"
curl "http://127.0.0.1:9090/api/agents/fo-structure?symbol=SENSEX&mock=true"
```

## Market Regime Agent

The Market Regime Agent is a read-only candle-analysis layer for future strategy-engine context. It supports only `NIFTY` and `SENSEX` in v1; `BANKNIFTY` is intentionally out of scope and returns a safe `UNCLEAR` report instead of being analyzed.

Source stack:

- Upstox Intraday Candle Data V3 for current-day 5-minute candles
- Upstox Historical Candle Data V3 for previous-day high/low/close context
- Upstox OHLC Quotes V3 for optional quick quote and India VIX context

The agent computes VWAP, EMA 9, EMA 21, RSI 14, ATR 14, opening range, previous-day levels, day high/low, trend/range/chop/volatility scores, and strategy-engine guidance such as preferring breakout filters, waiting, avoiding directional trades, or reducing size. It does not use websockets in v1; polling 5-minute candles is enough. It does not place orders, modify orders, cancel orders, or produce buy/sell recommendations.

API endpoint:

- `GET /api/agents/market-regime`
- Query params: `symbol=NIFTY|SENSEX`, `timeframe=5`, `mock=true|false`, `regime_hint=bullish|bearish|range|choppy|high_vol`

Useful local checks:

```bash
python3 -m pytest -q tests/agents/test_market_regime_indicators.py tests/agents/test_market_regime_scoring.py tests/agents/test_market_regime_agent.py tests/providers/test_upstox_market_data_provider.py tests/services/test_market_regime_runtime.py
curl "http://127.0.0.1:9090/api/agents/market-regime?symbol=NIFTY&mock=true"
curl "http://127.0.0.1:9090/api/agents/market-regime?symbol=SENSEX&mock=true"
```

## Execution Health Agent

The Execution Health Agent is a deterministic, system-level gate for future strategy evaluation and trade proposal layers. It checks latest agent report freshness, market data freshness, provider/runtime health, Upstox status, F&O/option-chain freshness signals, and market session context when available.

It does not predict direction, generate trades, approve risk, place orders, or execute broker actions. In v1, `allow_live_execution` is always `false`.

The report includes `overall_health`, `trade_allowed`, `fresh_trade_blocked`, `health_score`, blockers, warnings, provider status, agent freshness, runtime status, and strategy-engine guidance. Critical stale F&O or Market Regime data blocks fresh trade proposals; optional provider failures degrade confidence without automatically blocking.

API endpoint:

- `GET /api/agents/execution-health`
- Query params: `mock=true|false`, `scenario=healthy|degraded|unhealthy|startup`

Useful local check:

```bash
curl "http://127.0.0.1:9090/api/agents/execution-health?mock=true"
```

## Agent Output Storage

Agent reports keep the existing latest-report cache in `app_state` using keys such as `agent_output:market_regime_agent:NIFTY:MARKET_REGIME_REPORT`, so dashboard/runtime lookups remain compatible. Each structured agent report is also inserted into the generic `agent_outputs` table for historical debugging, audit trails, backtesting, outcome tracking, and future learning.

The history layer currently covers News, Macro Context, F&O Structure, Market Regime, Execution Health, and future agents that call `save_agent_report()`. Schema scaffolding is also present for later learning phases: `agent_outcomes`, `error_analysis`, `tuning_suggestions`, `ruleset_versions`, and `audit_logs`.

## Lightsail Deployment

The repo now includes a Lightsail-oriented deployment path built around:

- `gunicorn` with a single worker so the in-process refresh loops run once
- optional split-process mode with `gunicorn` for web routes and `backend.worker` for background loops
- `systemd` for service supervision
- `nginx` as the reverse proxy in front of the local `gunicorn` port
- Upstox market data through `UPSTOX_ANALYTICS_TOKEN`, with NSE fallback intact

Included files:

- [deploy/lightsail/market-desk.service](/Users/yashkumar/Documents/claude-projects/indian-market-agent/deploy/lightsail/market-desk.service)
- [deploy/lightsail/market-desk-worker.service](/Users/yashkumar/Documents/claude-projects/indian-market-agent/deploy/lightsail/market-desk-worker.service)
- [deploy/lightsail/market-desk.env.example](/Users/yashkumar/Documents/claude-projects/indian-market-agent/deploy/lightsail/market-desk.env.example)
- [deploy/lightsail/nginx-market-desk.conf](/Users/yashkumar/Documents/claude-projects/indian-market-agent/deploy/lightsail/nginx-market-desk.conf)

Recommended flow:

1. Launch an Ubuntu Lightsail instance and attach a static IP.
2. Point a domain or subdomain to that static IP.
3. Clone this repo to `/srv/indian-news-bot/indian-market-agent`.
4. Create a virtualenv and install dependencies:

```bash
cd /srv/indian-news-bot/indian-market-agent
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
```

5. Copy the example env file and fill it in:

```bash
cp deploy/lightsail/market-desk.env.example deploy/lightsail/market-desk.env
```

Important values:

```text
MARKET_DESK_DATA_DIR=/srv/indian-news-bot/indian-market-agent/backend/data
MARKET_DESK_DISABLE_THREADS=1
MARKET_DATA_PROVIDER=upstox
UPSTOX_ANALYTICS_TOKEN=<your_upstox_analytics_token>
UPSTOX_HTTP_TRANSPORT=auto
UPSTOX_USER_AGENT=curl/8.7.1

AI_PROVIDER=bedrock
BEDROCK_REGION=ap-south-1
BEDROCK_MODEL_ID=qwen.qwen3-next-80b-a3b

# Optional right-panel AI Chat terminal. This is separate from news summaries.
# It sends live dashboard context plus fresh Google News RSS results to Bedrock.
AI_CHAT_PROVIDER=bedrock-api-key
BEDROCK_API_KEY=<your_bedrock_api_key>
BEDROCK_OPENAI_BASE_URL=https://bedrock-mantle.ap-south-1.api.aws/v1
BEDROCK_OPENAI_API=chat_completions
AI_CHAT_MODEL=qwen.qwen3-next-80b-a3b-instruct
```

6. Install the systemd service:

```bash
sudo cp deploy/lightsail/market-desk.service /etc/systemd/system/market-desk.service
sudo cp deploy/lightsail/market-desk-worker.service /etc/systemd/system/market-desk-worker.service
sudo systemctl daemon-reload
sudo systemctl enable --now market-desk market-desk-worker
```

7. Install and enable Nginx using the included site config:

```bash
sudo cp deploy/lightsail/nginx-market-desk.conf /etc/nginx/sites-available/market-desk
sudo ln -s /etc/nginx/sites-available/market-desk /etc/nginx/sites-enabled/market-desk
sudo nginx -t
sudo systemctl reload nginx
```

8. Add TLS with Certbot so the dashboard is served over HTTPS.
9. Generate an Analytics Token in the Upstox Developer Apps page and place it in `UPSTOX_ANALYTICS_TOKEN`.

Notes:

- With `MARKET_DESK_DISABLE_THREADS=1`, Gunicorn serves HTTP only and `market-desk-worker` owns polling, AI queueing, and live feed loops.
- Runtime news/market snapshots are shared through SQLite so the web process can stay fast while the worker refreshes data.
- The Analytics Token is read-only, so this app does not use Upstox order APIs, OAuth callbacks, or static-IP sync.

For the full production upgrade path for the existing `stockterminal.in` site, follow:

- [docs/aws_lightsail_deployment.md](/Users/yashkumar/Documents/claude-projects/indian-market-agent/docs/aws_lightsail_deployment.md)

## Validation

```bash
/Library/Frameworks/Python.framework/Versions/3.11/bin/python3 -m py_compile backend/app.py tests/test_app.py
node --check frontend/assets/app.js
/Library/Frameworks/Python.framework/Versions/3.11/bin/python3 -m unittest discover -s tests -v
```
