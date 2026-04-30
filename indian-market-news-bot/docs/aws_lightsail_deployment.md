# AWS Lightsail Deployment Runbook

This guide upgrades or installs Stock Terminal on an Ubuntu Lightsail instance for:

- Domain: `stockterminal.in`
- Web process: Gunicorn behind Nginx
- Background process: separate worker for news, tickers, Upstox V3 stream, analytics, derivatives, and AI queue
- AI summaries: Amazon Bedrock in production, local Ollama optional for dev/fallback
- Market data: Upstox first, NSE fallback intact

## 1. Instance Sizing

Choose the instance based on where AI runs.

- Web + NSE fallback only: 2 GB RAM minimum, 4 GB preferred.
- Web + Upstox + background worker + Bedrock AI: 2 GB RAM minimum, 4 GB preferred.
- Web + Upstox + background worker + small local Ollama model such as `qwen2.5:3b`: 4 GB RAM minimum.
- Web + Upstox + background worker + local `qwen2.5:7b`: 8 GB RAM recommended.

For production, prefer Bedrock so article extraction and AI tagging do not overload the Lightsail CPU.

## 2. DNS And Static IP

Lightsail must have a static IPv4 attached.

In Lightsail:

1. Attach a static IP to the instance.
2. Open firewall ports `22`, `80`, and `443`.
3. Confirm DNS points to that static IP:

```bash
dig +short stockterminal.in
dig +short www.stockterminal.in
```

Both should return the Lightsail static IP.

In Upstox app settings:

- Redirect URL: `https://stockterminal.in/api/auth/upstox/callback`
- Primary IP: your Lightsail static IPv4
- Secondary IP: blank unless you add a second failover server

## 3. Server Bootstrap

SSH into the Lightsail instance:

```bash
ssh ubuntu@<LIGHTSAIL_STATIC_IP>
```

Install OS packages:

```bash
sudo apt update
sudo apt install -y git python3-venv python3-pip nginx certbot python3-certbot-nginx sqlite3 curl
```

Install Ollama only if AI summaries will run locally on the server. Skip this section when `AI_PROVIDER=bedrock`:

```bash
curl -fsSL https://ollama.com/install.sh | sh
sudo systemctl enable --now ollama
ollama pull qwen2.5:7b
```

For a smaller instance, use:

```bash
ollama pull qwen2.5:3b
```

## 4. Code Deploy Or Upgrade

Use `/srv/indian-news-bot/indian-market-news-bot` as the production directory.

If this is a fresh install:

```bash
sudo mkdir -p /srv/indian-news-bot
sudo chown -R ubuntu:ubuntu /srv/indian-news-bot
cd /srv/indian-news-bot
git clone https://github.com/<YOUR_GITHUB_USER>/<YOUR_REPO>.git indian-market-news-bot
cd /srv/indian-news-bot/indian-market-news-bot
```

If the old version already exists:

```bash
cd /srv/indian-news-bot/indian-market-news-bot
git status --short
```

Back up the current database before upgrading:

```bash
mkdir -p /srv/backups/stockterminal
cp backend/data/market_desk.db /srv/backups/stockterminal/market_desk.$(date +%Y%m%d-%H%M%S).db
```

Update the code:

```bash
git pull
```

Create or refresh the virtualenv:

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

## 5. Environment File

Create the production env file:

```bash
cp deploy/lightsail/market-desk.env.example deploy/lightsail/market-desk.env
vim deploy/lightsail/market-desk.env
```

Minimum production values:

```text
MARKET_DESK_DATA_DIR=/srv/indian-news-bot/indian-market-news-bot/backend/data
MARKET_DESK_DISABLE_THREADS=1

MARKET_DATA_PROVIDER=upstox
UPSTOX_FALLBACK_TO_NSE=1
UPSTOX_CLIENT_ID=<UPSTOX_API_KEY>
UPSTOX_CLIENT_SECRET=<UPSTOX_API_SECRET>
UPSTOX_REDIRECT_URI=https://stockterminal.in/api/auth/upstox/callback
UPSTOX_PRIMARY_IP=<LIGHTSAIL_STATIC_IP>
UPSTOX_SECONDARY_IP=

ENABLE_AI_NEWS_SUMMARIES=1
ENABLE_ARTICLE_EXTRACTION=1
AI_PROVIDER=bedrock
BEDROCK_REGION=ap-south-1
BEDROCK_MODEL_ID=qwen.qwen3-next-80b-a3b
BEDROCK_TIMEOUT_SECONDS=120
AWS_ACCESS_KEY_ID=<AWS_ACCESS_KEY_WITH_BEDROCK_INVOKE_ACCESS>
AWS_SECRET_ACCESS_KEY=<AWS_SECRET_ACCESS_KEY>
AWS_DEFAULT_REGION=ap-south-1

MAX_AI_SUMMARY_WORKERS=1
AI_SUMMARY_WINDOW_HOURS=24
```

For local Ollama instead of Bedrock:

```text
AI_PROVIDER=ollama
OLLAMA_API_BASE=http://127.0.0.1:11434/api
OLLAMA_NEWS_SUMMARY_MODEL=qwen2.5:7b
MAX_AI_SUMMARY_WORKERS=1
AI_SUMMARY_WINDOW_HOURS=24
```

If the instance is smaller, change:

```text
OLLAMA_NEWS_SUMMARY_MODEL=qwen2.5:3b
```

For a quick Bedrock API-key test instead of IAM credentials:

```text
AI_PROVIDER=bedrock-api-key
BEDROCK_REGION=ap-south-1
BEDROCK_MANTLE_MODEL_ID=qwen.qwen3-next-80b-a3b-instruct
BEDROCK_API_KEY=<BEDROCK_API_KEY_FROM_QUICKSTART>
BEDROCK_OPENAI_BASE_URL=https://bedrock-mantle.ap-south-1.api.aws/v1
BEDROCK_OPENAI_API=chat_completions
```

Use the API-key mode only for quick validation. For the production service, prefer `AI_PROVIDER=bedrock` with tightly scoped AWS credentials.

## 6. Install Services

```bash
sudo cp deploy/lightsail/market-desk.service /etc/systemd/system/market-desk.service
sudo cp deploy/lightsail/market-desk-worker.service /etc/systemd/system/market-desk-worker.service
sudo systemctl daemon-reload
sudo systemctl enable --now market-desk market-desk-worker
```

Check status:

```bash
sudo systemctl status market-desk --no-pager -l
sudo systemctl status market-desk-worker --no-pager -l
```

Logs:

```bash
sudo journalctl -u market-desk -f
sudo journalctl -u market-desk-worker -f
```

## 7. Nginx And HTTPS

Install the Nginx site:

```bash
sudo cp deploy/lightsail/nginx-market-desk.conf /etc/nginx/sites-available/market-desk
sudo ln -sf /etc/nginx/sites-available/market-desk /etc/nginx/sites-enabled/market-desk
sudo rm -f /etc/nginx/sites-enabled/default
sudo nginx -t
sudo systemctl reload nginx
```

Issue TLS certificate:

```bash
sudo certbot --nginx -d stockterminal.in -d www.stockterminal.in
```

Reload:

```bash
sudo nginx -t
sudo systemctl reload nginx
```

## 8. Health Checks

Local server checks:

```bash
curl -s http://127.0.0.1:8000/api/health
curl -I http://127.0.0.1:8000/
```

Public checks:

```bash
curl -s https://stockterminal.in/api/health
curl -I https://stockterminal.in/
```

Expected:

- `/api/health` returns JSON.
- `dataProvider.active` is `upstox` after OAuth succeeds, otherwise `nse`.
- `newsCount` and `tickerCount` should be greater than zero after the worker refreshes.

## 9. Complete Upstox OAuth

Open this in your browser:

```text
https://stockterminal.in/api/auth/upstox/login
```

After login, check:

```bash
curl -s https://stockterminal.in/api/integrations/upstox/status
```

The app should show Upstox configured and connected. If the token fails after changing static IPs, complete OAuth again.

## 10. Upgrade Checklist

For future deployments:

```bash
cd /srv/indian-news-bot/indian-market-news-bot
cp backend/data/market_desk.db /srv/backups/stockterminal/market_desk.$(date +%Y%m%d-%H%M%S).db
git pull
. .venv/bin/activate
pip install -r requirements.txt
sudo cp deploy/lightsail/market-desk.service /etc/systemd/system/market-desk.service
sudo cp deploy/lightsail/market-desk-worker.service /etc/systemd/system/market-desk-worker.service
sudo cp deploy/lightsail/nginx-market-desk.conf /etc/nginx/sites-available/market-desk
sudo systemctl daemon-reload
sudo nginx -t
sudo systemctl restart market-desk market-desk-worker
sudo systemctl reload nginx
curl -s https://stockterminal.in/api/health
```

## 11. Rollback

If the new version has problems:

```bash
cd /srv/indian-news-bot/indian-market-news-bot
git log --oneline -5
git checkout <previous_commit>
. .venv/bin/activate
pip install -r requirements.txt
sudo systemctl restart market-desk market-desk-worker
```

If the database needs rollback:

```bash
sudo systemctl stop market-desk market-desk-worker
cp /srv/backups/stockterminal/<backup-file>.db backend/data/market_desk.db
sudo systemctl start market-desk market-desk-worker
```
