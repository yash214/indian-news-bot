# Indian Market News Bot

This repo contains a Flask backend plus a static frontend for an India-focused market dashboard with news scoring, analytics, derivatives context, and optional Upstox-backed market data.

## Project Layout

- `backend/`: Flask API, polling loops, persistence, market/news analytics
- `frontend/`: static HTML, CSS, and vanilla JavaScript dashboard
- `deploy/lightsail/`: example systemd, Nginx, and environment files for AWS Lightsail
- `tests/`: regression tests for scoring, persistence, derivatives, and Upstox integration helpers

## Run Locally

```bash
cd /Users/yashkumar/Documents/claude-projects/indian-market-news-bot
pip install -r requirements.txt
python backend/app.py
```

The backend serves the frontend automatically from `frontend/`.

## Upstox Integration

The app supports two Upstox modes:

- `UPSTOX_ACCESS_TOKEN`: quick manual token for local testing
- OAuth callback flow: better for production hosting

Quick local run with a manual token:

```bash
MARKET_DATA_PROVIDER=upstox UPSTOX_ACCESS_TOKEN=your_token python backend/app.py
```

Useful Upstox settings:

- `MARKET_DATA_PROVIDER=upstox` prefers Upstox quotes and option-chain APIs
- `UPSTOX_FALLBACK_TO_NSE=1` keeps NSE fallback enabled if Upstox is unavailable for a symbol
- `UPSTOX_INSTRUMENT_KEYS='{"SBIN":"NSE_EQ|INE062A01020"}'` overrides or extends symbol mappings
- `UPSTOX_OPTION_EXPIRY=YYYY-MM-DD` sets a default expiry for `/api/derivatives/option-chain`
- `UPSTOX_CLIENT_ID`, `UPSTOX_CLIENT_SECRET`, `UPSTOX_REDIRECT_URI` enable OAuth login
- `UPSTOX_PRIMARY_IP`, `UPSTOX_SECONDARY_IP` store the static IPs to sync with Upstox

Production-friendly Upstox endpoints:

- `GET /api/integrations/upstox/status`
- `GET /api/auth/upstox/login`
- `GET /api/auth/upstox/callback`
- `POST /api/auth/upstox/disconnect`
- `POST /api/auth/upstox/static-ips/sync`

## Lightsail Deployment

The repo now includes a Lightsail-oriented deployment path built around:

- `gunicorn` with a single worker so the in-process refresh loops run once
- `systemd` for service supervision
- `nginx` as the reverse proxy in front of the local `gunicorn` port
- a Lightsail static public IP used directly for Upstox IP registration

Included files:

- [deploy/lightsail/market-desk.service](/Users/yashkumar/Documents/claude-projects/indian-market-news-bot/deploy/lightsail/market-desk.service)
- [deploy/lightsail/market-desk.env.example](/Users/yashkumar/Documents/claude-projects/indian-market-news-bot/deploy/lightsail/market-desk.env.example)
- [deploy/lightsail/nginx-market-desk.conf](/Users/yashkumar/Documents/claude-projects/indian-market-news-bot/deploy/lightsail/nginx-market-desk.conf)

Recommended flow:

1. Launch an Ubuntu Lightsail instance and attach a static IP.
2. Point a domain or subdomain to that static IP.
3. Clone this repo to `/srv/indian-market-news-bot`.
4. Create a virtualenv and install dependencies:

```bash
cd /srv/indian-market-news-bot
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
MARKET_DESK_DATA_DIR=/srv/indian-market-news-bot/backend/data
MARKET_DATA_PROVIDER=upstox
UPSTOX_CLIENT_ID=<your_upstox_api_key>
UPSTOX_CLIENT_SECRET=<your_upstox_api_secret>
UPSTOX_REDIRECT_URI=https://<your-domain>/api/auth/upstox/callback
UPSTOX_PRIMARY_IP=<your_lightsail_static_ip>
UPSTOX_SECONDARY_IP=
```

6. Install the systemd service:

```bash
sudo cp deploy/lightsail/market-desk.service /etc/systemd/system/market-desk.service
sudo systemctl daemon-reload
sudo systemctl enable --now market-desk
```

7. Install and enable Nginx using the included site config:

```bash
sudo cp deploy/lightsail/nginx-market-desk.conf /etc/nginx/sites-available/market-desk
sudo ln -s /etc/nginx/sites-available/market-desk /etc/nginx/sites-enabled/market-desk
sudo nginx -t
sudo systemctl reload nginx
```

8. Add TLS with Certbot so the Upstox callback URL is HTTPS.
9. In Upstox, use the same callback URL and set:
   `Primary IP = your Lightsail static IP`
   `Secondary IP = blank for now or a second failover server later`
10. Visit `https://<your-domain>/api/auth/upstox/login` to complete OAuth.

Notes:

- Keep `gunicorn` at `--workers 1` unless we later move background loops out of process.
- Lightsail already gives you the fixed public IP you need, so no outbound proxy layer is required.
- If you update static IPs in Upstox, existing tokens can be invalidated and you may need to complete OAuth again.

## Validation

```bash
/Library/Frameworks/Python.framework/Versions/3.11/bin/python3 -m py_compile backend/app.py tests/test_app.py
node --check frontend/assets/app.js
/Library/Frameworks/Python.framework/Versions/3.11/bin/python3 -m unittest discover -s tests -v
```
