# Indian Market News Bot

This project is now organized into separate backend and frontend folders:

- `backend/`: Flask API and market/news polling logic
- `frontend/`: static HTML, CSS, and JavaScript for the dashboard UI

## Run locally

```bash
cd /Users/yashkumar/Documents/claude-projects/indian-market-news-bot
pip install -r requirements.txt
python backend/app.py
```

The Flask app serves the frontend from the `frontend/` folder, so you only need to run the backend process.

## Optional Upstox market data

By default the app uses NSE public endpoints with Yahoo fallbacks. To prefer Upstox REST market quotes, start the backend with:

```bash
MARKET_DATA_PROVIDER=upstox UPSTOX_ACCESS_TOKEN=your_token python backend/app.py
```

Useful optional settings:

- `UPSTOX_FALLBACK_TO_NSE=1` keeps NSE fallback enabled when an Upstox symbol is unavailable.
- `UPSTOX_INSTRUMENT_KEYS='{"SBIN":"NSE_EQ|INE062A01020"}'` adds or overrides instrument keys.
- `UPSTOX_OPTION_EXPIRY=YYYY-MM-DD` provides a default expiry for `/api/derivatives/option-chain`.
