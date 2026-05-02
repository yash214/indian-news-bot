# Indian Market Agent

This repo contains a Flask backend plus a static frontend for an India-focused market agent platform with news intelligence, analytics, derivatives context, and optional Upstox-backed market data.

## Project Layout

- `backend/app.py`: Flask API routes, polling loops, runtime orchestration, and provider fallback wiring
- `backend/core/`: runtime settings and SQLite persistence
- `backend/agents/news/`: RSS sources, article extraction, news scoring, AI prompts, AI summary queueing, and News Intelligence Agent reports
- `backend/news/`: temporary compatibility bridges for older imports
- `backend/shared/`: shared constants and helpers for platform modules
- `backend/routes/`: future Flask route modules
- `backend/market/`: symbol catalogs, NSE/Yahoo/Upstox mappings, and reusable market math
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
