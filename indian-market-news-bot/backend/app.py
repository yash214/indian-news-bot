#!/usr/bin/env python3
"""
Indian Market News Bot backend.

Project layout:
    indian-market-news-bot/
      backend/
        app.py
        requirements.txt
      frontend/
        index.html
        assets/
          styles.css
          app.js

Run from the project root:
    pip install -r requirements.txt
    python backend/app.py
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import queue
import re
import sqlite3
import sys
import threading
import time
from pathlib import Path
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, time as dt_time, timezone, timedelta
from statistics import mean, pstdev
from urllib.parse import quote_plus

from flask import Flask, Response, jsonify, request, stream_with_context

try:
    import certifi
    import requests
except ImportError:
    sys.exit("Missing: pip install requests certifi")

try:
    import feedparser
except ImportError:
    sys.exit("Missing: pip install feedparser")

try:
    import yfinance as yf
except ImportError:
    yf = None

# ── Config ─────────────────────────────────────────────────────────────────
IST = timezone(timedelta(hours=5, minutes=30))

LOCAL_SCOPE = "local"
GLOBAL_SCOPE = "global"

LOCAL_NEWS_PARAMS = {"hl": "en-IN", "gl": "IN", "ceid": "IN:en"}
GLOBAL_NEWS_PARAMS = {"hl": "en-US", "gl": "US", "ceid": "US:en"}


def news_feed(name: str, url: str, scope: str) -> dict[str, str]:
    return {"name": name, "url": url, "scope": scope}


def google_news_search_rss(query: str, scope: str) -> str:
    params = LOCAL_NEWS_PARAMS if scope == LOCAL_SCOPE else GLOBAL_NEWS_PARAMS
    return (
        "https://news.google.com/rss/search?"
        f"q={quote_plus(query)}&hl={params['hl']}&gl={params['gl']}&ceid={params['ceid']}"
    )


RSS_FEEDS = [
    news_feed("Moneycontrol Markets", "https://www.moneycontrol.com/rss/latestnews.xml", LOCAL_SCOPE),
    news_feed("LiveMint Markets", "https://www.livemint.com/rss/markets", LOCAL_SCOPE),
    news_feed("LiveMint Companies", "https://www.livemint.com/rss/companies", LOCAL_SCOPE),
    news_feed("ET Markets", "https://economictimes.indiatimes.com/markets/rss.cms", LOCAL_SCOPE),
    news_feed(
        "Business Standard Markets",
        google_news_search_rss("(indian stock market OR sensex OR nifty OR rbi OR rupee) site:business-standard.com", LOCAL_SCOPE),
        LOCAL_SCOPE,
    ),
    news_feed(
        "Financial Express Markets",
        google_news_search_rss("(indian stock market OR sensex OR nifty OR ipo OR rupee) site:financialexpress.com", LOCAL_SCOPE),
        LOCAL_SCOPE,
    ),
    news_feed(
        "CNBC TV18 Markets",
        google_news_search_rss("(indian stock market OR sensex OR nifty OR rupee OR rbi) site:cnbctv18.com", LOCAL_SCOPE),
        LOCAL_SCOPE,
    ),
    news_feed(
        "Business Today Markets",
        google_news_search_rss("(indian stock market OR sensex OR nifty OR ipo OR results) site:businesstoday.in", LOCAL_SCOPE),
        LOCAL_SCOPE,
    ),
    news_feed(
        "BusinessLine Markets",
        google_news_search_rss("(indian stock market OR sensex OR nifty OR rupee OR rbi) site:thehindubusinessline.com", LOCAL_SCOPE),
        LOCAL_SCOPE,
    ),
    news_feed(
        "NDTV Profit Markets",
        google_news_search_rss("(indian stock market OR sensex OR nifty OR rupee OR crude) site:ndtvprofit.com", LOCAL_SCOPE),
        LOCAL_SCOPE,
    ),
    news_feed("Google News India Markets", google_news_search_rss("indian stock market nifty sensex", LOCAL_SCOPE), LOCAL_SCOPE),
    news_feed("Google News India IT", google_news_search_rss("nifty IT infosys wipro hcltech tcs", LOCAL_SCOPE), LOCAL_SCOPE),
    news_feed(
        "Google News India Macro",
        google_news_search_rss("RBI SEBI india economy inflation rupee bond yields", LOCAL_SCOPE),
        LOCAL_SCOPE,
    ),
    news_feed(
        "Google News India Banking",
        google_news_search_rss("hdfc icici sbi nifty bank RBI banking india", LOCAL_SCOPE),
        LOCAL_SCOPE,
    ),
    news_feed(
        "Google News India Crude",
        google_news_search_rss("crude oil brent wti india OMC fuel inflation", LOCAL_SCOPE),
        LOCAL_SCOPE,
    ),
    news_feed(
        "Reuters Markets",
        google_news_search_rss("(markets OR stocks OR bonds OR forex OR oil OR central bank) site:reuters.com", GLOBAL_SCOPE),
        GLOBAL_SCOPE,
    ),
    news_feed(
        "Bloomberg Markets",
        google_news_search_rss("(markets OR stocks OR bonds OR oil OR rates) site:bloomberg.com", GLOBAL_SCOPE),
        GLOBAL_SCOPE,
    ),
    news_feed(
        "CNBC Global Markets",
        google_news_search_rss("(markets OR stocks OR bonds OR oil OR currencies) site:cnbc.com", GLOBAL_SCOPE),
        GLOBAL_SCOPE,
    ),
    news_feed(
        "MarketWatch Markets",
        google_news_search_rss("(markets OR stocks OR bonds OR oil OR fed) site:marketwatch.com", GLOBAL_SCOPE),
        GLOBAL_SCOPE,
    ),
    news_feed(
        "Financial Times Markets",
        google_news_search_rss("(markets OR stocks OR bonds OR oil OR central bank) site:ft.com", GLOBAL_SCOPE),
        GLOBAL_SCOPE,
    ),
    news_feed(
        "WSJ Markets",
        google_news_search_rss("(markets OR stocks OR bonds OR oil OR fed) site:wsj.com", GLOBAL_SCOPE),
        GLOBAL_SCOPE,
    ),
    news_feed(
        "Barrons Markets",
        google_news_search_rss("(markets OR stocks OR bonds OR oil OR fed) site:barrons.com", GLOBAL_SCOPE),
        GLOBAL_SCOPE,
    ),
    news_feed(
        "Yahoo Finance Markets",
        google_news_search_rss("(markets OR stocks OR bonds OR oil OR forex) site:finance.yahoo.com", GLOBAL_SCOPE),
        GLOBAL_SCOPE,
    ),
    news_feed(
        "Google News Global Macro",
        google_news_search_rss("global stock market federal reserve ECB treasury yields oil prices currency markets", GLOBAL_SCOPE),
        GLOBAL_SCOPE,
    ),
]

NSE_INDICES_WANTED = {
    "NIFTY 50":         "Nifty 50",
    "NIFTY IT":         "Nifty IT",
    "NIFTY BANK":       "Nifty Bank",
    "NIFTY MIDCAP 100": "Nifty Midcap",
    "NIFTY SMLCAP 100": "Nifty Smallcap",
    "INDIA VIX":        "VIX",
}

NSE_STOCKS = {
    "Infosys":  "INFY",
    "HCL Tech": "HCLTECH",
    "Wipro":    "WIPRO",
    "TCS":      "TCS",
    "Reliance": "RELIANCE",
}

SYMBOL_SUGGESTIONS = [
    {"symbol": "RELIANCE", "name": "Reliance Industries", "sector": "Energy"},
    {"symbol": "TCS", "name": "Tata Consultancy Services", "sector": "IT"},
    {"symbol": "INFY", "name": "Infosys", "sector": "IT"},
    {"symbol": "HCLTECH", "name": "HCL Technologies", "sector": "IT"},
    {"symbol": "WIPRO", "name": "Wipro", "sector": "IT"},
    {"symbol": "TECHM", "name": "Tech Mahindra", "sector": "IT"},
    {"symbol": "LTIM", "name": "LTIMindtree", "sector": "IT"},
    {"symbol": "PERSISTENT", "name": "Persistent Systems", "sector": "IT"},
    {"symbol": "COFORGE", "name": "Coforge", "sector": "IT"},
    {"symbol": "TATAELXSI", "name": "Tata Elxsi", "sector": "IT"},
    {"symbol": "HDFCBANK", "name": "HDFC Bank", "sector": "Banking"},
    {"symbol": "ICICIBANK", "name": "ICICI Bank", "sector": "Banking"},
    {"symbol": "SBIN", "name": "State Bank of India", "sector": "Banking"},
    {"symbol": "AXISBANK", "name": "Axis Bank", "sector": "Banking"},
    {"symbol": "KOTAKBANK", "name": "Kotak Mahindra Bank", "sector": "Banking"},
    {"symbol": "INDUSINDBK", "name": "IndusInd Bank", "sector": "Banking"},
    {"symbol": "BANKBARODA", "name": "Bank of Baroda", "sector": "Banking"},
    {"symbol": "PNB", "name": "Punjab National Bank", "sector": "Banking"},
    {"symbol": "AUBANK", "name": "AU Small Finance Bank", "sector": "Banking"},
    {"symbol": "IDFCFIRSTB", "name": "IDFC First Bank", "sector": "Banking"},
    {"symbol": "ITC", "name": "ITC", "sector": "FMCG"},
    {"symbol": "HINDUNILVR", "name": "Hindustan Unilever", "sector": "FMCG"},
    {"symbol": "NESTLEIND", "name": "Nestle India", "sector": "FMCG"},
    {"symbol": "BRITANNIA", "name": "Britannia Industries", "sector": "FMCG"},
    {"symbol": "DABUR", "name": "Dabur India", "sector": "FMCG"},
    {"symbol": "MARICO", "name": "Marico", "sector": "FMCG"},
    {"symbol": "SUNPHARMA", "name": "Sun Pharma", "sector": "Pharma"},
    {"symbol": "CIPLA", "name": "Cipla", "sector": "Pharma"},
    {"symbol": "DRREDDY", "name": "Dr Reddy's Laboratories", "sector": "Pharma"},
    {"symbol": "DIVISLAB", "name": "Divi's Laboratories", "sector": "Pharma"},
    {"symbol": "LUPIN", "name": "Lupin", "sector": "Pharma"},
    {"symbol": "TORNTPHARM", "name": "Torrent Pharmaceuticals", "sector": "Pharma"},
    {"symbol": "MANKIND", "name": "Mankind Pharma", "sector": "Pharma"},
    {"symbol": "MARUTI", "name": "Maruti Suzuki", "sector": "Auto"},
    {"symbol": "TATAMOTORS", "name": "Tata Motors", "sector": "Auto"},
    {"symbol": "M&M", "name": "Mahindra & Mahindra", "sector": "Auto"},
    {"symbol": "BAJAJ-AUTO", "name": "Bajaj Auto", "sector": "Auto"},
    {"symbol": "EICHERMOT", "name": "Eicher Motors", "sector": "Auto"},
    {"symbol": "HEROMOTOCO", "name": "Hero MotoCorp", "sector": "Auto"},
    {"symbol": "TVSMOTOR", "name": "TVS Motor", "sector": "Auto"},
    {"symbol": "LT", "name": "Larsen & Toubro", "sector": "Infra"},
    {"symbol": "ULTRACEMCO", "name": "UltraTech Cement", "sector": "Infra"},
    {"symbol": "ADANIPORTS", "name": "Adani Ports", "sector": "Infra"},
    {"symbol": "SIEMENS", "name": "Siemens India", "sector": "Infra"},
    {"symbol": "ABB", "name": "ABB India", "sector": "Infra"},
    {"symbol": "NTPC", "name": "NTPC", "sector": "Energy"},
    {"symbol": "POWERGRID", "name": "Power Grid Corporation", "sector": "Energy"},
    {"symbol": "ONGC", "name": "ONGC", "sector": "Energy"},
    {"symbol": "BPCL", "name": "Bharat Petroleum", "sector": "Energy"},
    {"symbol": "COALINDIA", "name": "Coal India", "sector": "Energy"},
    {"symbol": "JSWSTEEL", "name": "JSW Steel", "sector": "Metals"},
    {"symbol": "TATASTEEL", "name": "Tata Steel", "sector": "Metals"},
    {"symbol": "HINDALCO", "name": "Hindalco", "sector": "Metals"},
    {"symbol": "VEDL", "name": "Vedanta", "sector": "Metals"},
    {"symbol": "ADANIENT", "name": "Adani Enterprises", "sector": "General"},
    {"symbol": "BAJFINANCE", "name": "Bajaj Finance", "sector": "Financials"},
    {"symbol": "BAJAJFINSV", "name": "Bajaj Finserv", "sector": "Financials"},
    {"symbol": "JIOFIN", "name": "Jio Financial Services", "sector": "Financials"},
    {"symbol": "ASIANPAINT", "name": "Asian Paints", "sector": "General"},
    {"symbol": "TITAN", "name": "Titan Company", "sector": "General"},
    {"symbol": "BHARTIARTL", "name": "Bharti Airtel", "sector": "Telecom"},
]

YAHOO_EXTRAS = {
    "Gold":        ("GC=F",     "$"),
    "USD/INR":     ("USDINR=X", ""),
    "Crude Oil":   ("CL=F",     "$"),
    "Brent Crude": ("BZ=F",     "$"),
}

ANALYTICS_INDEX_NAMES = {
    "NIFTY 50": "Nifty 50",
    "NIFTY BANK": "Nifty Bank",
    "NIFTY IT": "Nifty IT",
    "NIFTY AUTO": "Nifty Auto",
    "NIFTY FMCG": "Nifty FMCG",
    "NIFTY PHARMA": "Nifty Pharma",
    "NIFTY METAL": "Nifty Metal",
    "NIFTY REALTY": "Nifty Realty",
    "NIFTY PSU BANK": "Nifty PSU Bank",
    "NIFTY FIN SERVICE": "Nifty Financial",
    "NIFTY FINANCIAL SERVICES": "Nifty Financial",
    "NIFTY MIDCAP 100": "Nifty Midcap",
    "NIFTY SMLCAP 100": "Nifty Smallcap",
    "NIFTY OIL & GAS": "Nifty Oil & Gas",
    "INDIA VIX": "India VIX",
}

PRIMARY_LEVEL_LABELS = ["Nifty 50", "Nifty Bank", "Nifty IT", "India VIX"]

INDEX_HISTORY_SYMBOLS = {
    "Nifty 50": ["^NSEI"],
    "Nifty Bank": ["^NSEBANK", "^CNXBANK"],
    "Nifty IT": ["^CNXIT"],
    "India VIX": ["^INDIAVIX"],
}

SECTOR_TO_INDEX = {
    "IT": "Nifty IT",
    "Banking": "Nifty Bank",
    "Pharma": "Nifty Pharma",
    "Auto": "Nifty Auto",
    "Energy": "Nifty Oil & Gas",
    "FMCG": "Nifty FMCG",
    "Metals": "Nifty Metal",
    "Infra": "Nifty Realty",
    "General": "Nifty 50",
}

WATCHLIST_SYMBOL_LIMIT = 12
TRACKED_QUOTE_LIMIT = 20

DEFAULT_TRACKED_TICKERS = ["INFY", "HCLTECH", "WIPRO", "TCS", "RELIANCE"]
DEFAULT_WATCHLIST = ["INFY", "HCLTECH", "WIPRO", "RELIANCE"]
DEFAULT_APP_STATE = {
    "tickerSelections": DEFAULT_TRACKED_TICKERS,
    "watchlist": DEFAULT_WATCHLIST,
    "bookmarks": [],
    "portfolio": {},
}

BACKEND_DIR = Path(__file__).resolve().parent
DATA_DIR = BACKEND_DIR / "data"
STATE_DB_PATH = DATA_DIR / "market_desk.db"
HOLIDAY_CALENDAR_PATH = DATA_DIR / "nse_holidays.json"
MARKET_OPEN_TIME = dt_time(hour=9, minute=15)
MARKET_CLOSE_TIME = dt_time(hour=15, minute=30)
PREOPEN_TICK_INTERVAL_SECONDS = 10
INTRADAY_TICK_INTERVAL_SECONDS = 10
AFTER_HOURS_TICK_INTERVAL_SECONDS = 60
INTRADAY_TICK_STALE_SECONDS = 30
AFTER_HOURS_TICK_STALE_SECONDS = 180
MIN_NEWS_STALE_SECONDS = 600
LIVE_NSE_QUOTE_CACHE_TTL = 8.0
CLOSED_NSE_QUOTE_CACHE_TTL = 45.0
NSE_SESSION_REFRESH_SECONDS = 900
MAX_QUOTE_WORKERS = 8
MAX_NEWS_WORKERS = 8
NSE_PROVIDER_NAME = "nse"
UPSTOX_PROVIDER_NAME = "upstox"
UPSTOX_DEFAULT_API_BASE = "https://api.upstox.com/v2"
UPSTOX_QUOTE_BATCH_LIMIT = 500

UPSTOX_DEFAULT_INSTRUMENT_KEYS = {
    # Upstox quotes use stable instrument keys. These cover the app defaults
    # without downloading the full BOD instrument file on every startup.
    "INFY": "NSE_EQ|INE009A01021",
    "HCLTECH": "NSE_EQ|INE860A01027",
    "WIPRO": "NSE_EQ|INE075A01022",
    "TCS": "NSE_EQ|INE467B01029",
    "RELIANCE": "NSE_EQ|INE002A01018",
}

UPSTOX_INDEX_INSTRUMENT_KEYS = {
    "Nifty 50": "NSE_INDEX|Nifty 50",
    "Nifty Bank": "NSE_INDEX|Nifty Bank",
    "Nifty IT": "NSE_INDEX|Nifty IT",
    "India VIX": "NSE_INDEX|India VIX",
}

UPSTOX_OPTION_UNDERLYINGS = {
    "NIFTY": "NSE_INDEX|Nifty 50",
    "NIFTY50": "NSE_INDEX|Nifty 50",
    "NIFTY 50": "NSE_INDEX|Nifty 50",
    "BANKNIFTY": "NSE_INDEX|Nifty Bank",
    "NIFTYBANK": "NSE_INDEX|Nifty Bank",
    "NIFTY BANK": "NSE_INDEX|Nifty Bank",
    "FINNIFTY": "NSE_INDEX|Nifty Fin Service",
    "FIN NIFTY": "NSE_INDEX|Nifty Fin Service",
}

EMPTY_ANALYTICS_PAYLOAD = {
    "generatedAt": None,
    "overviewCards": [],
    "alerts": [],
    "sectorBoard": [],
    "sectorMap": {},
    "keyLevels": [],
    "watchlistSignals": [],
    "symbolMap": {},
    "regime": None,
    "primary": [],
}

EMPTY_DERIVATIVES_PAYLOAD = {
    "generatedAt": None,
    "overviewCards": [],
    "predictionCards": [],
    "contextNotes": [],
    "riskFlags": [],
    "crossAssetRows": [],
    "relativeValueRows": [],
    "scoreBreakdown": [],
    "tradeScenarios": [],
    "signalMatrix": [],
    "triggerMap": [],
}

NSE_SESSION = requests.Session()
NSE_HEADERS = {
    "User-Agent":       "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124 Safari/537.36",
    "Accept":           "application/json,text/html,*/*",
    "Accept-Language":  "en-IN,en;q=0.9",
    "Referer":          "https://www.nseindia.com/",
    "X-Requested-With": "XMLHttpRequest",
}

_sess = requests.Session()
_sess.verify = certifi.where()
_sess.headers.update({
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 Chrome/124 Safari/537.36",
    "Accept": "application/json,text/html,*/*",
    "Accept-Language": "en-IN,en;q=0.9",
})
NSE_SESSION.verify = certifi.where()
NSE_SESSION.headers.update(NSE_HEADERS)

SECTOR_KW = {
    "IT":      ["infosys", "tcs", "wipro", "hcl", "tech mahindra", "coforge", "mphasis",
                "ltimindtree", "hexaware", "software", "it sector", "nifty it", "nasdaq", "accenture"],
    "Banking": ["hdfc bank", "icici bank", "sbi", "kotak", "axis bank", "rbi", "banking",
                "npa", "credit", "loan", "nifty bank", "bandhan", "indusind", "yes bank"],
    "Pharma":  ["sun pharma", "cipla", "dr reddy", "aurobindo", "divi", "lupin", "alkem",
                "pharmaceutical", "drug", "fda", "usfda", "biocon", "glenmark"],
    "Auto":    ["maruti", "tata motors", "m&m", "bajaj auto", "hero motocorp", "eicher",
                "automobile", "ev", "electric vehicle", "auto sector", "tvs", "ola electric"],
    "Energy":  ["reliance", "ongc", "ntpc", "power grid", "adani energy", "torrent power",
                "oil", "gas", "crude", "crude oil", "brent", "wti", "opec", "solar",
                "renewable", "bpcl", "ioc", "coal"],
    "FMCG":    ["hindustan unilever", "hul", "itc", "nestle", "dabur", "emami",
                "britannia", "fmcg", "consumer goods", "marico", "colgate", "godrej"],
    "Metals":  ["tata steel", "jsw", "hindalco", "vedanta", "coal india", "jindal",
                "steel", "aluminium", "copper", "metal", "nmdc", "sail", "moil"],
    "Infra":   ["l&t", "larsen", "adani ports", "delhivery", "ircon", "rvnl",
                "infrastructure", "construction", "cement", "road", "nhpc", "abb"],
}

BULLISH = {
    "surge": 3, "soar": 3, "rally": 2, "record high": 3, "all-time high": 3, "52-week high": 2,
    "outperform": 2, "upgrade": 2, "beat estimates": 3, "breakout": 2, "top gainer": 2,
    "gain": 1, "rise": 1, "growth": 1, "positive": 1, "strong": 1, "robust": 2,
    "bullish": 3, "buy": 2, "recovery": 2, "rebound": 2, "jump": 2,
    "boost": 2, "higher": 1, "profit": 1, "earnings beat": 3, "outperformed": 2,
    "above expectations": 2, "overweight": 2, "accumulate": 2, "target raised": 3,
    "dividend": 1, "buyback": 2, "record profit": 3, "margin expansion": 2,
}

BEARISH = {
    "crash": 3, "plunge": 3, "collapse": 3, "sell-off": 3, "selloff": 3,
    "bearish": 3, "downgrade": 2, "miss": 2, "fall": 2, "drop": 2,
    "decline": 2, "weak": 1, "loss": 2, "risk": 1, "concern": 1,
    "pressure": 1, "correction": 2, "slump": 2, "negative": 1, "warning": 2,
    "below expectations": 2, "cut": 1, "layoffs": 2, "slowdown": 2,
    "recession": 3, "underweight": 2, "underperform": 2, "target cut": 3,
    "margin squeeze": 2, "write-off": 3, "fraud": 3, "default": 3, "penalty": 2,
    "52-week low": 2, "circuit breaker": 2, "suspension": 2, "ban": 2,
}

IMPACT_EVENTS = {
    "rbi": 2.0, "sebi": 1.8, "budget": 1.8, "gdp": 1.4, "inflation": 1.5,
    "fed": 1.4, "tariff": 1.4, "sanction": 1.5, "crash": 2.4,
    "surge": 1.2, "record": 1.1, "ban": 2.0, "merger": 1.6,
    "acquisition": 1.6, "takeover": 1.8, "quarterly results": 1.4,
    "earnings": 1.2, "dividend": 0.7, "rate cut": 2.0, "rate hike": 2.0,
    "repo rate": 1.8, "policy": 1.2, "war": 1.8, "election": 1.2,
    "ipo": 0.8, "block deal": 1.2, "bulk deal": 1.2, "fii": 1.0,
    "dii": 0.9, "circuit": 1.8, "halt": 1.8, "usfda": 1.5,
    "crude oil": 1.5, "brent": 1.2, "wti": 1.1, "opec": 1.2,
    "q1": 0.6, "q2": 0.6, "q3": 0.6, "q4": 0.6,
}
IMPACT_KW = list(IMPACT_EVENTS)

MARKET_CONTEXT = {
    "nifty": 1.0, "sensex": 0.9, "bank nifty": 1.0, "rupee": 0.8,
    "usd/inr": 0.8, "india vix": 0.8, "f&o": 0.8, "derivatives": 0.7,
    "options": 0.6, "futures": 0.6, "nse": 0.6, "bse": 0.5,
    "infosys": 0.5, "tcs": 0.5, "reliance": 0.5, "hdfc bank": 0.5,
    "icici bank": 0.5, "sbi": 0.5,
}

TITLE_SHOCK = {
    "crash": 1.5, "plunge": 1.4, "collapse": 1.4, "surge": 1.1,
    "soar": 1.1, "record": 0.9, "ban": 1.2, "fraud": 1.5,
    "default": 1.5, "penalty": 1.0, "rate cut": 1.1, "rate hike": 1.1,
}

LOW_SIGNAL_HEADLINES = {
    "stocks to watch": 1.2, "market live": 1.0, "live updates": 1.0,
    "top news": 0.8, "opening bell": 0.7, "closing bell": 0.7,
    "trade setup": 0.8, "day trading guide": 0.8,
}

SOURCE_IMPACT = {
    "reuters": 1.0, "bloomberg": 1.0, "cnbc tv18": 0.9, "cnbc-tv18": 0.9,
    "moneycontrol": 0.9, "ndtv profit": 0.85, "business standard": 0.8,
    "economic times": 0.8, "et markets": 0.8, "businessline": 0.75,
    "livemint": 0.75, "financial express": 0.7, "financial times": 0.8,
    "wall street journal": 0.75, "wsj": 0.75, "barrons": 0.65,
    "marketwatch": 0.55, "yahoo finance": 0.5, "google news": 0.3,
}

_KEYWORD_RE_CACHE: dict[str, re.Pattern] = {}

# ── State ──────────────────────────────────────────────────────────────────
_lock = threading.Lock()
_sse_lock = threading.Lock()
_sse_queues: list[queue.Queue[str]] = []

_arts: list[dict] = []
_ticks: dict = {}
_index_snapshot: dict = {}
_feed_status: dict = {}
_updated: str = "loading..."
_price_history: dict[str, list[float]] = {}
MAX_HIST = 40
ALLOWED_REFRESH_WINDOWS = [60, 120, 300, 600, 900]
NSE_HOLIDAY_CALENDAR: dict[str, dict[str, str]] = {}
_app_state: dict = {
    "tickerSelections": list(DEFAULT_TRACKED_TICKERS),
    "watchlist": list(DEFAULT_WATCHLIST),
    "bookmarks": [],
    "portfolio": {},
}
_has_persisted_state = False
_analytics_payload: dict = dict(EMPTY_ANALYTICS_PAYLOAD)
_derivatives_payload: dict = dict(EMPTY_DERIVATIVES_PAYLOAD)
_tracked_symbol_quotes: dict[str, dict] = {}
_last_news_refresh_ts: float | None = None
_last_tick_refresh_ts: float | None = None
_last_analytics_refresh_ts: float | None = None
_last_derivatives_refresh_ts: float | None = None

_yahoo_cache: dict[str, tuple[float, float, float, float]] = {}
# Commodity/FX extras do not need sub-minute polling. A longer TTL keeps the
# dashboard responsive while avoiding aggressive rate limits from Yahoo.
_yahoo_cache_ttl = 300.0
_nse_quote_cache: dict[str, tuple[dict, float]] = {}
_upstox_quote_cache: dict[str, tuple[dict, float]] = {}
_chart_cache: dict[tuple[str, str, str], tuple[dict, float]] = {}
_chart_cache_ttl = 1800.0
_news_refresh_seconds = 300
_refresh_wakeup = threading.Event()
_thread_local = threading.local()


# ── Helpers ────────────────────────────────────────────────────────────────
def ist_now() -> datetime:
    return datetime.now(IST)


def is_market_open() -> bool:
    return bool(get_market_status()["isMarketOpen"])


def _build_http_session() -> requests.Session:
    sess = requests.Session()
    sess.verify = certifi.where()
    sess.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                      "AppleWebKit/537.36 Chrome/124 Safari/537.36",
        "Accept": "application/json,text/html,*/*",
        "Accept-Language": "en-IN,en;q=0.9",
    })
    return sess


def http_session() -> requests.Session:
    sess = getattr(_thread_local, "http_session", None)
    if sess is None:
        sess = _build_http_session()
        _thread_local.http_session = sess
    return sess


def _build_nse_session() -> requests.Session:
    sess = requests.Session()
    sess.verify = certifi.where()
    sess.headers.update(NSE_HEADERS)
    sess._market_desk_ready_at = 0.0
    return sess


def nse_session() -> requests.Session:
    sess = getattr(_thread_local, "nse_session", None)
    if sess is None:
        sess = _build_nse_session()
        _thread_local.nse_session = sess
    return sess


def requested_market_data_provider() -> str:
    provider = os.environ.get("MARKET_DATA_PROVIDER", NSE_PROVIDER_NAME).strip().lower()
    return provider if provider in {NSE_PROVIDER_NAME, UPSTOX_PROVIDER_NAME} else NSE_PROVIDER_NAME


def upstox_access_token() -> str:
    return os.environ.get("UPSTOX_ACCESS_TOKEN", "").strip()


def upstox_api_base() -> str:
    return os.environ.get("UPSTOX_API_BASE", UPSTOX_DEFAULT_API_BASE).strip().rstrip("/")


def upstox_fallback_enabled() -> bool:
    raw = os.environ.get("UPSTOX_FALLBACK_TO_NSE", "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def active_market_data_provider() -> str:
    if requested_market_data_provider() == UPSTOX_PROVIDER_NAME and upstox_access_token():
        return UPSTOX_PROVIDER_NAME
    return NSE_PROVIDER_NAME


def market_data_provider_status() -> dict:
    requested = requested_market_data_provider()
    configured = bool(upstox_access_token())
    active = active_market_data_provider()
    return {
        "requested": requested,
        "active": active,
        "upstoxConfigured": configured,
        "fallbackToNse": upstox_fallback_enabled(),
        "reason": (
            "Upstox access token missing; using NSE fallback"
            if requested == UPSTOX_PROVIDER_NAME and not configured
            else "Upstox REST quotes enabled"
            if active == UPSTOX_PROVIDER_NAME
            else "NSE public endpoints enabled"
        ),
    }


def ticker_refresh_interval(status: dict | None = None) -> int:
    status = status or get_market_status()
    if status.get("session") == "preopen":
        return PREOPEN_TICK_INTERVAL_SECONDS
    if status.get("isMarketOpen"):
        return INTRADAY_TICK_INTERVAL_SECONDS
    return AFTER_HOURS_TICK_INTERVAL_SECONDS


def nse_quote_cache_ttl(status: dict | None = None) -> float:
    status = status or get_market_status()
    return LIVE_NSE_QUOTE_CACHE_TTL if status.get("session") in {"preopen", "open"} else CLOSED_NSE_QUOTE_CACHE_TTL


def quote_age_seconds(quote: dict | None, now_ts: float | None = None) -> float | None:
    if not quote or quote.get("fetchedAt") is None:
        return None
    now_ts = time.time() if now_ts is None else now_ts
    return round(max(now_ts - float(quote["fetchedAt"]), 0), 1)


def strip_html(s: str) -> str:
    return re.sub(r"<[^>]+>", "", s or "").strip()


def url_hash(url: str) -> str:
    return hashlib.md5(url.encode()).hexdigest()[:12]


def clean_headline(title: str, publisher: str = "") -> str:
    headline = strip_html(title)
    source = strip_html(publisher)
    suffix = f" - {source}"
    if source and headline.lower().endswith(suffix.lower()):
        headline = headline[:-len(suffix)].strip()
    return headline


def clean_summary(summary: str, publisher: str = "") -> str:
    text = strip_html(summary)
    source = strip_html(publisher)
    if source:
        text = re.sub(rf"(?:\s+[-|:]\s+|\s{{2,}}){re.escape(source)}$", "", text, flags=re.IGNORECASE).strip()
    return text


def feed_publisher_label(feed_name: str) -> str:
    for suffix in (" Markets", " Companies"):
        if feed_name.endswith(suffix):
            return feed_name[:-len(suffix)]
    return feed_name


def normalized_headline(title: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", (title or "").lower())).strip()


def keyword_re(keyword: str) -> re.Pattern:
    key = (keyword or "").strip().lower()
    cached = _KEYWORD_RE_CACHE.get(key)
    if cached:
        return cached
    escaped = r"\s+".join(re.escape(part) for part in key.split())
    prefix = r"(?<![a-z0-9])" if key and key[0].isalnum() else ""
    suffix = r"(?![a-z0-9])" if key and key[-1].isalnum() else ""
    pattern = re.compile(prefix + escaped + suffix, re.IGNORECASE)
    _KEYWORD_RE_CACHE[key] = pattern
    return pattern


def keyword_found(text: str, keyword: str) -> bool:
    return bool(keyword_re(keyword).search(text or ""))


def weighted_keyword_hits(
    title: str,
    body: str,
    weights: dict[str, float],
    title_multiplier: float = 1.35,
    body_multiplier: float = 1.0,
) -> tuple[float, list[str]]:
    score = 0.0
    hits: list[str] = []
    title = title or ""
    body = body or ""
    for keyword, weight in weights.items():
        if keyword_found(title, keyword):
            score += weight * title_multiplier
            hits.append(keyword)
        elif body and keyword_found(body, keyword):
            score += weight * body_multiplier
            hits.append(keyword)
    return score, hits


def classify(text: str) -> str:
    t = (text or "").lower()
    scores = {sec: sum(1 for k in kws if keyword_found(t, k)) for sec, kws in SECTOR_KW.items()}
    best = max(scores, key=scores.get)
    return best if scores[best] > 0 else "General"


def sentiment(title: str, body: str) -> dict:
    bull, _ = weighted_keyword_hits(title, body, BULLISH, title_multiplier=1.6)
    bear, _ = weighted_keyword_hits(title, body, BEARISH, title_multiplier=1.6)
    total = bull + bear
    edge = abs(bull - bear)
    if total <= 0 or edge < 0.75 or edge / total < 0.15:
        return {"label": "neutral", "score": 0.0}
    score = round(min(max(bull, bear) / (total + 2.0), 1.0), 2)
    if bull > bear:
        return {"label": "bullish", "score": score}
    if bear > bull:
        return {"label": "bearish", "score": score}
    return {"label": "neutral", "score": 0.0}


def source_impact_score(source: str) -> float:
    source_l = (source or "").lower()
    for key, score in SOURCE_IMPACT.items():
        if key in source_l:
            return score
    return 0.35 if source_l else 0.0


def recency_impact_score(published_dt: datetime | None, now: datetime | None = None) -> float:
    if published_dt is None:
        return 0.0
    now = now or ist_now()
    if published_dt.tzinfo is None:
        published_dt = published_dt.replace(tzinfo=IST)
    age_hours = max((now - published_dt.astimezone(IST)).total_seconds() / 3600, 0)
    if age_hours <= 0.5:
        return 1.0
    if age_hours <= 2:
        return 0.8
    if age_hours <= 6:
        return 0.5
    if age_hours <= 24:
        return 0.2
    if age_hours <= 72:
        return -0.3
    return -0.8


def low_signal_penalty(title: str, body: str, core_score: float) -> tuple[float, list[str]]:
    score, hits = weighted_keyword_hits(title, body, LOW_SIGNAL_HEADLINES, title_multiplier=1.0)
    if not hits:
        return 0.0, []
    penalty = min(score, 1.4)
    if core_score >= 4:
        penalty *= 0.45
    return round(penalty, 2), hits


def impact_details(
    title: str,
    body: str,
    sent: dict,
    source: str = "",
    published_dt: datetime | None = None,
    scope: str = LOCAL_SCOPE,
    now: datetime | None = None,
) -> tuple[int, dict]:
    event_raw, event_hits = weighted_keyword_hits(title, body, IMPACT_EVENTS, title_multiplier=1.35)
    context_raw, context_hits = weighted_keyword_hits(title, body, MARKET_CONTEXT, title_multiplier=1.2)
    shock_raw, shock_hits = weighted_keyword_hits(title, "", TITLE_SHOCK, title_multiplier=1.0)

    sentiment_component = min(float(sent.get("score", 0.0)) * 4.2, 3.6)
    event_component = min(event_raw, 4.2)
    context_component = min(context_raw, 1.2)
    title_component = min(shock_raw, 1.4)
    core_score = sentiment_component + event_component + context_component + title_component

    source_component = source_impact_score(source)
    freshness_component = recency_impact_score(published_dt, now)
    if core_score < 2:
        source_component *= 0.35
        freshness_component *= 0.35
    if scope == LOCAL_SCOPE and context_hits:
        context_component = min(context_component + 0.25, 1.3)

    penalty, low_signal_hits = low_signal_penalty(title, body, core_score)
    raw_score = core_score + source_component + freshness_component - penalty
    if core_score < 1:
        raw_score = min(raw_score, 2.0)

    score = int(clamp(round(raw_score), 0, 10))
    matched = sorted(set(event_hits + context_hits + shock_hits))
    reasons = []
    if sent.get("label") != "neutral":
        reasons.append(f"{sent.get('label')} sentiment")
    if event_hits:
        reasons.append("event: " + ", ".join(event_hits[:4]))
    if context_hits:
        reasons.append("market context: " + ", ".join(context_hits[:3]))
    if source_component >= 0.6:
        reasons.append(f"source: {source}")
    if freshness_component >= 0.6:
        reasons.append("fresh headline")
    if penalty:
        reasons.append("generic headline penalty")

    return score, {
        "raw": round(raw_score, 2),
        "components": {
            "sentiment": round(sentiment_component, 2),
            "event": round(event_component, 2),
            "context": round(context_component, 2),
            "title": round(title_component, 2),
            "source": round(source_component, 2),
            "freshness": round(freshness_component, 2),
            "penalty": round(penalty, 2),
        },
        "matchedKeywords": matched[:12],
        "lowSignalKeywords": low_signal_hits,
        "reasons": reasons[:6],
    }


def impact(
    title: str,
    body: str,
    sent: dict,
    source: str = "",
    published_dt: datetime | None = None,
    scope: str = LOCAL_SCOPE,
) -> int:
    score, _ = impact_details(title, body, sent, source=source, published_dt=published_dt, scope=scope)
    return score


# ── Analytics helpers ─────────────────────────────────────────────────────
def safe_float(value, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        if isinstance(value, str):
            value = value.replace(",", "").strip()
        return float(value)
    except Exception:
        return default


def round_or_none(value: float | None, digits: int = 2) -> float | None:
    if value is None:
        return None
    return round(value, digits)


def sma(values: list[float], length: int) -> float | None:
    if len(values) < length:
        return None
    return mean(values[-length:])


def pct_return(values: list[float], periods: int) -> float | None:
    if len(values) <= periods:
        return None
    prev = values[-periods - 1]
    if not prev:
        return None
    return (values[-1] - prev) / prev * 100


def realized_vol(values: list[float], periods: int = 20) -> float | None:
    if len(values) <= periods:
        return None
    rets = []
    window = values[-(periods + 1):]
    for prev, cur in zip(window, window[1:]):
        if prev:
            rets.append((cur - prev) / prev)
    if len(rets) < 2:
        return None
    return pstdev(rets) * math.sqrt(252) * 100


def rsi(values: list[float], period: int = 14) -> float | None:
    if len(values) <= period:
        return None
    gains, losses = [], []
    for prev, cur in zip(values[-(period + 1):], values[-period:]):
        delta = cur - prev
        gains.append(max(delta, 0.0))
        losses.append(abs(min(delta, 0.0)))
    avg_gain = mean(gains)
    avg_loss = mean(losses)
    if avg_loss == 0:
        return 100.0 if avg_gain > 0 else 50.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def trend_label(price: float, sma20_val: float | None, sma50_val: float | None, rsi_val: float | None) -> str:
    if not sma20_val or not sma50_val or rsi_val is None:
        return "Developing"
    if price > sma20_val > sma50_val and rsi_val >= 55:
        return "Uptrend"
    if price < sma20_val < sma50_val and rsi_val <= 45:
        return "Downtrend"
    if price > sma20_val and rsi_val >= 50:
        return "Accumulation"
    if price < sma20_val and rsi_val <= 50:
        return "Distribution"
    return "Range"


def setup_label(
    price: float,
    high20: float | None,
    low20: float | None,
    sma20_val: float | None,
    rsi_val: float | None,
    ret5: float | None,
) -> str:
    if high20 and price >= high20 * 0.995 and (rsi_val or 0) >= 58:
        return "Breakout watch"
    if low20 and price <= low20 * 1.01 and (rsi_val or 100) <= 42:
        return "Breakdown risk"
    if sma20_val and price > sma20_val and (ret5 or 0) > 1:
        return "Momentum long"
    if sma20_val and price < sma20_val and (ret5 or 0) < -1:
        return "Trend weak"
    return "Wait for setup"


def relative_gap(left: float | None, right: float | None) -> float | None:
    if left is None or right is None:
        return None
    return round(left - right, 2)


def clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def score_band(value: float | None, positive_strong: float, positive_mild: float, negative_mild: float, negative_strong: float) -> int:
    if value is None:
        return 0
    if value >= positive_strong:
        return 2
    if value >= positive_mild:
        return 1
    if value <= negative_strong:
        return -2
    if value <= negative_mild:
        return -1
    return 0


def intraday_return(values: list[float], periods: int = 3) -> float | None:
    if len(values) <= periods:
        return None
    prev = values[-periods - 1]
    if not prev:
        return None
    return round((values[-1] - prev) / prev * 100, 2)


def intraday_range_pct(values: list[float], periods: int = 12) -> float | None:
    if len(values) < 2:
        return None
    window = values[-periods:] if len(values) >= periods else values
    low = min(window)
    high = max(window)
    if not low:
        return None
    return round((high - low) / low * 100, 2)


def implied_move_points(price: float | None, vix_price: float | None) -> tuple[float | None, float | None]:
    if price is None or vix_price is None:
        return None, None
    move_pct = (vix_price / 100) / math.sqrt(252) * 100
    move_points = price * move_pct / 100
    return round(move_points, 2), round(move_pct, 2)


def bias_from_score(score: int) -> tuple[str, str]:
    if score >= 5:
        return "Strong Long Bias", "bull"
    if score >= 2:
        return "Long Bias", "bull"
    if score <= -5:
        return "Strong Short Bias", "bear"
    if score <= -2:
        return "Short Bias", "bear"
    return "Two-Way / Mean Reversion", "neutral"


def day_type_from_context(score: int, vix_price: float | None, short_momentum: float | None, intraday_range: float | None) -> tuple[str, str]:
    if abs(score) >= 5 and (vix_price or 0) < 16:
        return "Trend Day", "Directional conditions are aligned and volatility is still controlled."
    if abs(score) >= 4 and (vix_price or 0) >= 16:
        return "Volatile Trend", "Directional edge exists, but option premium and reversals can be sharper."
    if (vix_price or 0) >= 17 and (intraday_range or 0) >= 0.8:
        return "High Gamma Two-Way", "Expect wider intraday swings and faster invalidation if momentum stalls."
    if abs(short_momentum or 0) < 0.2 and (intraday_range or 0) < 0.5:
        return "Range / Fade Day", "Momentum is not broad enough yet, so breakout follow-through is less reliable."
    return "Rotation Day", "Leadership is shifting, so relative strength and confirmation matter more than raw direction."


def conviction_from_score(score: int, data_points: int) -> int:
    base = 42 + abs(score) * 8 + min(data_points, 8) * 2
    return int(clamp(base, 35, 88))


def format_level(value: float | None, prefix: str = "") -> str:
    if value is None:
        return "Unavailable"
    return f"{prefix}{value:,.2f}"


def sanitize_symbol_list(raw: str) -> list[str]:
    seen, out = set(), []
    for piece in (raw or "").split(","):
        sym = re.sub(r"[^A-Z0-9&.-]", "", piece.upper().strip())
        if sym and sym not in seen:
            seen.add(sym)
            out.append(sym)
        if len(out) >= WATCHLIST_SYMBOL_LIMIT:
            break
    return out


def symbol_directory_entry(symbol: str) -> dict | None:
    clean = re.sub(r"[^A-Z0-9&.-]", "", (symbol or "").upper())
    for item in SYMBOL_SUGGESTIONS:
        if item["symbol"] == clean:
            return item
    return None


def search_symbols(query: str, limit: int = 8) -> list[dict]:
    q = (query or "").strip().upper()
    if not q:
        return SYMBOL_SUGGESTIONS[:limit]

    ranked: list[tuple[int, dict]] = []
    for item in SYMBOL_SUGGESTIONS:
        symbol = item["symbol"]
        name = item["name"].upper()
        score = 0
        if symbol.startswith(q):
            score += 12
        elif q in symbol:
            score += 8
        if name.startswith(q):
            score += 10
        elif q in name:
            score += 6
        if score:
            ranked.append((score, item))
    ranked.sort(key=lambda row: (-row[0], row[1]["symbol"]))
    return [item for _, item in ranked[:limit]]


def build_sector_news_scores(articles: list[dict]) -> dict[str, dict]:
    scores: dict[str, dict] = defaultdict(lambda: {"score": 0.0, "count": 0, "bull": 0, "bear": 0})
    now_ts = time.time()
    for art in articles:
        sector = art.get("sector") or "General"
        sent = art.get("sentiment", {}).get("label", "neutral")
        sign = 1 if sent == "bullish" else -1 if sent == "bearish" else 0
        age_hours = max((now_ts - art.get("ts", now_ts)) / 3600, 0)
        recency = max(0.35, 1.35 - (age_hours / 24))
        weight = art.get("impact", 0) * recency
        scores[sector]["score"] += sign * weight
        scores[sector]["count"] += 1
        if sent == "bullish":
            scores[sector]["bull"] += 1
        elif sent == "bearish":
            scores[sector]["bear"] += 1
    return {k: {"score": round(v["score"], 2), **v} for k, v in scores.items()}


def sector_bias_label(score: float) -> tuple[str, str]:
    if score >= 6:
        return "Positive", "bull"
    if score <= -6:
        return "Negative", "bear"
    return "Mixed", "neutral"


def default_app_state() -> dict:
    return {
        "tickerSelections": list(DEFAULT_APP_STATE["tickerSelections"]),
        "watchlist": list(DEFAULT_APP_STATE["watchlist"]),
        "bookmarks": list(DEFAULT_APP_STATE["bookmarks"]),
        "portfolio": dict(DEFAULT_APP_STATE["portfolio"]),
    }


def ensure_data_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def db_connect(path: Path = STATE_DB_PATH) -> sqlite3.Connection:
    ensure_data_dir()
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def init_state_db(path: Path = STATE_DB_PATH) -> None:
    with db_connect(path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS app_state (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.commit()


def _db_get_record(key: str, path: Path = STATE_DB_PATH) -> tuple[bool, dict | list | str | int | float | None]:
    init_state_db(path)
    with db_connect(path) as conn:
        row = conn.execute("SELECT value FROM app_state WHERE key = ?", (key,)).fetchone()
    if row is None:
        return False, None
    return True, json.loads(row["value"])


def db_get_json(key: str, default=None, path: Path = STATE_DB_PATH):
    exists, value = _db_get_record(key, path)
    return value if exists else default


def db_set_json(key: str, value, path: Path = STATE_DB_PATH) -> None:
    init_state_db(path)
    with db_connect(path) as conn:
        conn.execute(
            """
            INSERT INTO app_state(key, value, updated_at)
            VALUES(?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
                value = excluded.value,
                updated_at = excluded.updated_at
            """,
            (key, json.dumps(value), datetime.now(timezone.utc).isoformat()),
        )
        conn.commit()


def sanitize_symbols(value, limit: int = WATCHLIST_SYMBOL_LIMIT) -> list[str]:
    if isinstance(value, list):
        pieces = value
    else:
        pieces = str(value or "").split(",")

    seen, out = set(), []
    for piece in pieces:
        sym = re.sub(r"[^A-Z0-9&.-]", "", str(piece).upper().strip())
        if sym and sym not in seen:
            seen.add(sym)
            out.append(sym)
        if len(out) >= limit:
            break
    return out


def sanitize_bookmarks(value) -> list[str]:
    if not isinstance(value, list):
        return []
    seen, out = set(), []
    for piece in value:
        cleaned = re.sub(r"[^A-Za-z0-9_-]", "", str(piece).strip())[:64]
        if cleaned and cleaned not in seen:
            seen.add(cleaned)
            out.append(cleaned)
        if len(out) >= 500:
            break
    return out


def sanitize_portfolio(value) -> dict[str, dict[str, float]]:
    if not isinstance(value, dict):
        return {}
    out = {}
    for raw_symbol, raw_entry in value.items():
        symbol = sanitize_symbols([raw_symbol], limit=1)
        if not symbol or not isinstance(raw_entry, dict):
            continue
        qty = safe_float(raw_entry.get("qty"), default=0.0)
        buy_price = safe_float(raw_entry.get("buyPrice"), default=0.0)
        if qty <= 0 or buy_price <= 0:
            continue
        out[symbol[0]] = {
            "qty": round(qty, 4),
            "buyPrice": round(buy_price, 2),
        }
        if len(out) >= WATCHLIST_SYMBOL_LIMIT:
            break
    return out


def sanitize_state_patch(payload: dict | None) -> dict:
    payload = payload or {}
    clean = {}
    if "tickerSelections" in payload:
        clean["tickerSelections"] = sanitize_symbols(payload.get("tickerSelections"), WATCHLIST_SYMBOL_LIMIT)
    if "watchlist" in payload:
        clean["watchlist"] = sanitize_symbols(payload.get("watchlist"), WATCHLIST_SYMBOL_LIMIT)
    if "bookmarks" in payload:
        clean["bookmarks"] = sanitize_bookmarks(payload.get("bookmarks"))
    if "portfolio" in payload:
        clean["portfolio"] = sanitize_portfolio(payload.get("portfolio"))
    return clean


def load_persisted_app_state(path: Path = STATE_DB_PATH) -> tuple[dict, bool]:
    state = default_app_state()
    has_stored_state = False
    for key in DEFAULT_APP_STATE:
        exists, value = _db_get_record(key, path)
        if not exists:
            continue
        has_stored_state = True
        clean = sanitize_state_patch({key: value})
        if key in clean:
            state[key] = clean[key]
    return state, has_stored_state


def load_refresh_settings(path: Path = STATE_DB_PATH) -> int:
    settings = db_get_json("settings", default={}, path=path)
    if not isinstance(settings, dict):
        return 300
    seconds = int(settings.get("refreshInterval", 300) or 300)
    return seconds if seconds in ALLOWED_REFRESH_WINDOWS else 300


def persist_refresh_settings(seconds: int, path: Path = STATE_DB_PATH) -> None:
    db_set_json("settings", {"refreshInterval": seconds}, path=path)


def load_holiday_calendar(path: Path = HOLIDAY_CALENDAR_PATH) -> dict[str, dict[str, str]]:
    if not path.exists():
        return {}
    data = json.loads(path.read_text())
    calendar: dict[str, dict[str, str]] = {}
    for year, items in data.items():
        year_key = str(year)
        calendar[year_key] = {}
        for item in items:
            if not isinstance(item, dict):
                continue
            date_str = str(item.get("date", "")).strip()
            name = str(item.get("name") or item.get("description") or "").strip()
            if date_str and name:
                calendar[year_key][date_str] = name
    return calendar


def holiday_name_on(day: date) -> str | None:
    return NSE_HOLIDAY_CALENDAR.get(str(day.year), {}).get(day.isoformat())


def is_trading_day(day: date) -> tuple[bool, str | None]:
    if day.weekday() >= 5:
        return False, None
    holiday_name = holiday_name_on(day)
    if holiday_name:
        return False, holiday_name
    return True, None


def combine_ist(day: date, value: dt_time) -> datetime:
    return datetime.combine(day, value, tzinfo=IST)


def next_trading_open(now: datetime) -> datetime | None:
    for offset in range(0, 370):
        candidate_day = now.date() + timedelta(days=offset)
        is_open_day, _ = is_trading_day(candidate_day)
        if not is_open_day:
            continue
        session_open = combine_ist(candidate_day, MARKET_OPEN_TIME)
        if offset == 0 and now >= session_open:
            continue
        return session_open
    return None


def get_market_status(now: datetime | None = None) -> dict:
    now = now or ist_now()
    today = now.date()
    is_open_day, holiday_name = is_trading_day(today)
    session_open = combine_ist(today, MARKET_OPEN_TIME)
    session_close = combine_ist(today, MARKET_CLOSE_TIME)

    if not is_open_day:
        if holiday_name:
            session = "holiday"
            reason = f"Exchange holiday: {holiday_name}"
        else:
            session = "weekend"
            reason = f"Weekend closure ({now.strftime('%A')})"
        is_market_open_now = False
    elif now < session_open:
        session = "preopen"
        reason = f"Pre-open. Regular trading starts at {MARKET_OPEN_TIME.strftime('%H:%M')} IST."
        is_market_open_now = False
    elif now <= session_close:
        session = "open"
        reason = "Regular cash-market trading session."
        is_market_open_now = True
    else:
        session = "postclose"
        reason = f"Post-close. Regular trading ended at {MARKET_CLOSE_TIME.strftime('%H:%M')} IST."
        is_market_open_now = False

    with _lock:
        news_ts = _last_news_refresh_ts
        tick_ts = _last_tick_refresh_ts

    news_age = round(now.timestamp() - news_ts, 1) if news_ts else None
    ticker_age = round(now.timestamp() - tick_ts, 1) if tick_ts else None
    news_stale_after = max(get_news_refresh_seconds() * 2, MIN_NEWS_STALE_SECONDS)
    tickers_stale_after = INTRADAY_TICK_STALE_SECONDS if is_market_open_now else AFTER_HOURS_TICK_STALE_SECONDS
    news_stale = news_age is None or news_age > news_stale_after
    tickers_stale = ticker_age is None or ticker_age > tickers_stale_after

    session_labels = {
        "holiday": "Exchange holiday",
        "weekend": "Weekend",
        "preopen": "Pre-open",
        "open": "Market open",
        "postclose": "Post-close",
    }
    next_open = next_trading_open(now)
    return {
        "asOf": now.isoformat(),
        "isTradingDay": bool(is_open_day),
        "isMarketOpen": bool(is_market_open_now),
        "session": session,
        "sessionLabel": session_labels[session],
        "reason": reason,
        "holiday": holiday_name,
        "nextOpen": next_open.isoformat() if next_open else None,
        "newsAgeSeconds": news_age,
        "tickerAgeSeconds": ticker_age,
        "newsStale": news_stale,
        "tickersStale": tickers_stale,
        "staleData": news_stale or tickers_stale,
        "tickIntervalSeconds": ticker_refresh_interval({"session": session, "isMarketOpen": is_market_open_now}),
        "tickStaleAfterSeconds": tickers_stale_after,
    }


def tracked_symbols_for_state(state: dict) -> list[str]:
    symbols = []
    for sym in state.get("tickerSelections", []):
        if sym not in symbols:
            symbols.append(sym)
    for sym in state.get("watchlist", []):
        if sym not in symbols:
            symbols.append(sym)
    for sym in state.get("portfolio", {}).keys():
        if sym not in symbols:
            symbols.append(sym)
        if len(symbols) >= TRACKED_QUOTE_LIMIT:
            break
    return symbols[:TRACKED_QUOTE_LIMIT]


def analytics_symbols_for_state(state: dict) -> list[str]:
    symbols = []
    for sym in state.get("watchlist", []):
        if sym not in symbols:
            symbols.append(sym)
    for sym in state.get("portfolio", {}).keys():
        if sym not in symbols:
            symbols.append(sym)
        if len(symbols) >= WATCHLIST_SYMBOL_LIMIT:
            break
    return symbols[:WATCHLIST_SYMBOL_LIMIT]


def get_news_refresh_seconds() -> int:
    with _lock:
        return _news_refresh_seconds


def set_news_refresh_seconds(seconds: int) -> int:
    global _news_refresh_seconds
    if seconds not in ALLOWED_REFRESH_WINDOWS:
        raise ValueError("Unsupported refresh interval")
    with _lock:
        _news_refresh_seconds = seconds
    persist_refresh_settings(seconds)
    _refresh_wakeup.set()
    return seconds


def get_app_state_copy() -> dict:
    with _lock:
        return {
            "tickerSelections": list(_app_state["tickerSelections"]),
            "watchlist": list(_app_state["watchlist"]),
            "bookmarks": list(_app_state["bookmarks"]),
            "portfolio": dict(_app_state["portfolio"]),
        }


def persist_app_state(state: dict) -> None:
    for key in DEFAULT_APP_STATE:
        db_set_json(key, state[key])


def update_app_state(payload: dict | None) -> dict:
    global _app_state, _has_persisted_state
    patch = sanitize_state_patch(payload)
    if not patch:
        return get_app_state_copy()

    with _lock:
        merged = {
            "tickerSelections": list(_app_state["tickerSelections"]),
            "watchlist": list(_app_state["watchlist"]),
            "bookmarks": list(_app_state["bookmarks"]),
            "portfolio": dict(_app_state["portfolio"]),
        }
        merged.update(patch)
        _app_state = merged
        _has_persisted_state = True

    persist_app_state(merged)
    try:
        refresh_tracked_symbol_quotes(merged)
        rebuild_computed_payloads()
    except Exception:
        pass
    return get_app_state_copy()


def format_quote_for_client(sym: str, quote: dict, status: dict | None = None) -> dict:
    status = status or get_market_status()
    entry = symbol_directory_entry(sym)
    age = quote_age_seconds(quote)
    stale_after = nse_quote_cache_ttl(status) * 2
    name = (entry or {}).get("name") or quote.get("name") or sym
    return {
        "symbol": sym,
        "label": sym,
        "name": name,
        "price": quote["price"],
        "change": quote["change"],
        "pct": quote["pct"],
        "live": True,
        "sym": "Rs",
        "fetchedAt": quote.get("fetchedAt"),
        "ageSeconds": age,
        "stale": age is None or age > stale_after,
        "source": quote.get("source", "NSE"),
    }


def format_quotes_for_client(quotes: dict[str, dict], status: dict | None = None) -> dict[str, dict]:
    status = status or get_market_status()
    return {sym: format_quote_for_client(sym, quote, status=status) for sym, quote in quotes.items() if quote}


def refresh_quote_cache_for_symbols(symbols: list[str]) -> dict[str, dict]:
    quotes = {}
    if not symbols:
        return quotes

    def quote_worker(sym: str) -> tuple[str, dict | None]:
        try:
            return sym, fetch_live_quote(sym)
        except Exception:
            return sym, None

    with ThreadPoolExecutor(max_workers=min(MAX_QUOTE_WORKERS, len(symbols))) as executor:
        futures = [executor.submit(quote_worker, sym) for sym in symbols]
        for future in as_completed(futures):
            sym, quote = future.result()
            if quote:
                quotes[sym] = quote
    return quotes


def refresh_tracked_symbol_quotes(state: dict | None = None) -> dict[str, dict]:
    global _tracked_symbol_quotes
    state = state or get_app_state_copy()
    symbols = tracked_symbols_for_state(state)
    quotes = refresh_quote_cache_for_symbols(symbols)
    with _lock:
        previous = dict(_tracked_symbol_quotes)
        for sym in symbols:
            if sym in quotes:
                previous[sym] = quotes[sym]
        _tracked_symbol_quotes = {sym: previous[sym] for sym in symbols if sym in previous}
        return dict(_tracked_symbol_quotes)


def rebuild_computed_payloads() -> None:
    global _analytics_payload, _derivatives_payload, _last_analytics_refresh_ts, _last_derivatives_refresh_ts
    state = get_app_state_copy()
    with _lock:
        arts = list(_arts)
        ticks = dict(_ticks)
        index_snapshot = dict(_index_snapshot)
        tracked_quotes = dict(_tracked_symbol_quotes)
        history = dict(_price_history)

    analytics_payload = build_market_analytics_payload(
        arts,
        ticks,
        index_snapshot,
        analytics_symbols_for_state(state),
        quote_map=tracked_quotes,
    )
    derivatives_payload = build_derivatives_analysis_payload(
        arts,
        ticks,
        index_snapshot,
        price_history=history,
        market_status=get_market_status(),
    )
    refreshed_at = time.time()
    with _lock:
        _analytics_payload = analytics_payload
        _derivatives_payload = derivatives_payload
        _last_analytics_refresh_ts = refreshed_at
        _last_derivatives_refresh_ts = refreshed_at


def market_data_snapshot(include_history: bool = False) -> dict:
    with _lock:
        ticks = dict(_ticks)
        tracked_quotes = dict(_tracked_symbol_quotes)
        analytics_payload = dict(_analytics_payload)
        derivatives_payload = dict(_derivatives_payload)
        last_tick_ts = _last_tick_refresh_ts
        last_analytics_ts = _last_analytics_refresh_ts
        last_derivatives_ts = _last_derivatives_refresh_ts
        history = {key: list(values) for key, values in _price_history.items()} if include_history else None

    status = get_market_status()
    payload = {
        "serverTime": ist_now().isoformat(),
        "lastTickAt": last_tick_ts,
        "lastAnalyticsAt": last_analytics_ts,
        "lastDerivativesAt": last_derivatives_ts,
        "marketStatus": status,
        "dataProvider": market_data_provider_status(),
        "ticks": ticks,
        "trackedQuotes": format_quotes_for_client(tracked_quotes, status=status),
        "analytics": analytics_payload,
        "derivatives": derivatives_payload,
    }
    if include_history:
        payload["history"] = history or {}
    return payload


def initialize_runtime_state() -> None:
    global NSE_HOLIDAY_CALENDAR, _app_state, _has_persisted_state, _news_refresh_seconds
    NSE_HOLIDAY_CALENDAR = load_holiday_calendar()
    state, has_stored_state = load_persisted_app_state()
    refresh_seconds = load_refresh_settings()
    with _lock:
        _app_state = state
        _has_persisted_state = has_stored_state
        _news_refresh_seconds = refresh_seconds


# ── Network helpers ────────────────────────────────────────────────────────
def _get_feed(url: str) -> bytes:
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/rss+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://www.google.com/",
    }
    r = http_session().get(url, headers=headers, timeout=8)
    r.raise_for_status()
    return r.content


def _nse_init_session(force: bool = False) -> requests.Session:
    sess = nse_session()
    now = time.time()
    ready_at = float(getattr(sess, "_market_desk_ready_at", 0.0) or 0.0)
    if not force and (now - ready_at) < NSE_SESSION_REFRESH_SECONDS:
        return sess
    try:
        sess.get("https://www.nseindia.com", headers=NSE_HEADERS, timeout=6, verify=certifi.where())
        sess._market_desk_ready_at = now
    except Exception as e:
        print(f"[!] NSE session init: {e}")
    return sess


def _is_missing_number(value) -> bool:
    return value is None or value != value


def _yahoo_chart(sym: str, range_: str = "6mo", interval: str = "1d") -> dict:
    now = time.time()
    key = (sym, range_, interval)
    cached = _chart_cache.get(key)
    if cached and (now - cached[1] < _chart_cache_ttl):
        return cached[0]

    if yf is not None:
        try:
            ticker = yf.Ticker(sym)
            hist = ticker.history(period=range_, interval=interval, auto_adjust=False, actions=False)
            rows = []
            if hist is not None and not hist.empty:
                for row in hist.itertuples():
                    close = getattr(row, "Close", None)
                    if _is_missing_number(close):
                        continue
                    c = float(close)
                    high = getattr(row, "High", None)
                    low = getattr(row, "Low", None)
                    volume = getattr(row, "Volume", None)
                    rows.append({
                        "close": c,
                        "high": c if _is_missing_number(high) else float(high),
                        "low": c if _is_missing_number(low) else float(low),
                        "volume": 0 if _is_missing_number(volume) else int(volume),
                    })
            if rows:
                meta = ticker.history_metadata or {}
                previous_close = (
                    rows[-2]["close"]
                    if len(rows) > 1
                    else safe_float(meta.get("chartPreviousClose", meta.get("previousClose")), rows[-1]["close"])
                )
                data = {
                    "symbol": meta.get("symbol", sym),
                    "currency": meta.get("currency", ""),
                    "previous_close": previous_close,
                    "rows": rows,
                }
                _chart_cache[key] = (data, now)
                return data
        except Exception:
            pass

    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}"
    r = http_session().get(url, params={"range": range_, "interval": interval}, timeout=8)
    r.raise_for_status()
    payload = r.json()["chart"]["result"][0]
    quote = payload["indicators"]["quote"][0]
    rows = []
    closes = quote.get("close", []) or []
    highs = quote.get("high", []) or []
    lows = quote.get("low", []) or []
    volumes = quote.get("volume", []) or []
    for close, high, low, volume in zip(closes, highs, lows, volumes):
        if _is_missing_number(close):
            continue
        c = float(close)
        rows.append({
            "close": c,
            "high": c if _is_missing_number(high) else float(high),
            "low": c if _is_missing_number(low) else float(low),
            "volume": 0 if _is_missing_number(volume) else int(volume),
        })
    if len(rows) < 1:
        raise ValueError(f"Insufficient history for {sym}")
    meta = payload.get("meta", {})
    data = {
        "symbol": meta.get("symbol", sym),
        "currency": meta.get("currency", ""),
        "previous_close": (
            rows[-2]["close"]
            if len(rows) > 1
            else safe_float(meta.get("chartPreviousClose"), rows[-1]["close"])
        ),
        "rows": rows,
    }
    _chart_cache[key] = (data, now)
    return data


def _yahoo_price(sym: str) -> tuple[float, float, float]:
    now = time.time()
    cached = _yahoo_cache.get(sym)
    if cached and (now - cached[3] < _yahoo_cache_ttl):
        return cached[0], cached[1], cached[2]

    try:
        data = _yahoo_chart(sym, range_="2d", interval="1d")
        closes = [row["close"] for row in data["rows"]]
        p = float(closes[-1])
        prev = float(closes[-2]) if len(closes) > 1 else p
        ch = round(p - prev, 2)
        pct = round((ch / prev * 100) if prev else 0, 2)
        _yahoo_cache[sym] = (round(p, 2), ch, pct, now)
        return round(p, 2), ch, pct
    except Exception:
        if cached:
            return cached[0], cached[1], cached[2]
        raise


def parse_upstox_instrument_overrides(raw: str | None = None) -> dict[str, str]:
    raw = os.environ.get("UPSTOX_INSTRUMENT_KEYS", "") if raw is None else raw
    raw = (raw or "").strip()
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, dict):
            return {
                re.sub(r"[^A-Z0-9&. -]", "", str(symbol).upper()).strip(): str(key).strip()
                for symbol, key in parsed.items()
                if str(symbol).strip() and str(key).strip()
            }
    except Exception:
        pass

    out = {}
    for piece in re.split(r"[;,]", raw):
        if "=" not in piece:
            continue
        symbol, key = piece.split("=", 1)
        symbol = re.sub(r"[^A-Z0-9&. -]", "", symbol.upper()).strip()
        key = key.strip()
        if symbol and key:
            out[symbol] = key
    return out


def upstox_instrument_key_for_symbol(symbol: str) -> str | None:
    clean = re.sub(r"[^A-Z0-9&.-]", "", (symbol or "").upper())
    overrides = parse_upstox_instrument_overrides()
    return overrides.get(clean) or UPSTOX_DEFAULT_INSTRUMENT_KEYS.get(clean)


def upstox_headers() -> dict[str, str]:
    token = upstox_access_token()
    if not token:
        raise RuntimeError("UPSTOX_ACCESS_TOKEN is not configured")
    return {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}",
    }


def parse_upstox_timestamp(value, default_ts: float) -> float:
    if value is None or value == "":
        return default_ts
    try:
        if isinstance(value, (int, float)):
            number = float(value)
            return number / 1000 if number > 10_000_000_000 else number
        text = str(value).strip()
        if text.isdigit():
            number = float(text)
            return number / 1000 if number > 10_000_000_000 else number
        return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
    except Exception:
        return default_ts


def upstox_quote_from_payload(label: str, payload: dict, received_at: float) -> dict | None:
    ohlc = payload.get("ohlc") or {}
    last = safe_float(payload.get("last_price", payload.get("ltp")))
    if not last:
        return None
    previous_close = safe_float(ohlc.get("close", payload.get("close_price")), last)
    net_change = payload.get("net_change")
    change = safe_float(net_change, last - previous_close)
    pct = round((change / previous_close * 100) if previous_close else 0, 2)
    fetched_at = parse_upstox_timestamp(
        payload.get("timestamp") or payload.get("last_trade_time"),
        received_at,
    )
    directory_entry = symbol_directory_entry(label)
    name = (
        payload.get("symbol")
        or payload.get("trading_symbol")
        or (directory_entry or {}).get("name")
        or label
    )
    depth = payload.get("depth") or {}
    best_bid = (depth.get("buy") or [{}])[0] if isinstance(depth.get("buy"), list) else {}
    best_ask = (depth.get("sell") or [{}])[0] if isinstance(depth.get("sell"), list) else {}
    quote = {
        "symbol": label,
        "name": name,
        "price": round(last, 2),
        "previous_close": round(previous_close, 2),
        "change": round(change, 2),
        "pct": pct,
        "day_high": round(safe_float(ohlc.get("high"), last), 2),
        "day_low": round(safe_float(ohlc.get("low"), last), 2),
        "open": round(safe_float(ohlc.get("open"), last), 2),
        "volume": safe_float(payload.get("volume")),
        "oi": safe_float(payload.get("oi")),
        "bid": safe_float(best_bid.get("price")),
        "ask": safe_float(best_ask.get("price")),
        "fetchedAt": fetched_at,
        "receivedAt": received_at,
        "source": "Upstox",
        "instrumentKey": payload.get("instrument_token") or payload.get("instrument_key"),
    }
    return quote


def fetch_upstox_quotes_by_label(label_to_key: dict[str, str]) -> dict[str, dict]:
    if not label_to_key or not upstox_access_token():
        return {}

    now = time.time()
    ttl = nse_quote_cache_ttl()
    out: dict[str, dict] = {}
    pending: dict[str, str] = {}
    for label, key in label_to_key.items():
        if not key:
            continue
        cache_key = f"{label}|{key}"
        cached = _upstox_quote_cache.get(cache_key)
        if cached and (now - cached[1] < ttl):
            out[label] = cached[0]
        else:
            pending[label] = key

    if not pending:
        return out

    labels = list(pending.keys())[:UPSTOX_QUOTE_BATCH_LIMIT]
    keys = [pending[label] for label in labels]
    key_to_label = {pending[label]: label for label in labels}
    response = http_session().get(
        f"{upstox_api_base()}/market-quote/quotes",
        params={"instrument_key": ",".join(keys)},
        headers=upstox_headers(),
        timeout=8,
    )
    response.raise_for_status()
    payload = response.json()
    if payload.get("status") not in {None, "success"}:
        raise RuntimeError(f"Upstox quote request failed: {payload.get('status')}")

    for quote_payload in (payload.get("data") or {}).values():
        instrument_key = quote_payload.get("instrument_token") or quote_payload.get("instrument_key")
        label = key_to_label.get(instrument_key)
        if not label:
            symbol = re.sub(r"[^A-Z0-9&.-]", "", str(quote_payload.get("symbol", "")).upper())
            label = symbol if symbol in pending else None
        if not label:
            continue
        quote = upstox_quote_from_payload(label, quote_payload, now)
        if not quote:
            continue
        cache_key = f"{label}|{pending[label]}"
        _upstox_quote_cache[cache_key] = (quote, now)
        out[label] = quote
    return out


def _fetch_upstox_quote(symbol: str) -> dict | None:
    clean = re.sub(r"[^A-Z0-9&.-]", "", (symbol or "").upper())
    key = upstox_instrument_key_for_symbol(clean)
    if not clean or not key:
        return None
    return fetch_upstox_quotes_by_label({clean: key}).get(clean)


def fetch_upstox_index_quotes() -> dict[str, dict]:
    return fetch_upstox_quotes_by_label(dict(UPSTOX_INDEX_INSTRUMENT_KEYS))


def fetch_live_quote(symbol: str) -> dict | None:
    if active_market_data_provider() == UPSTOX_PROVIDER_NAME:
        try:
            quote = _fetch_upstox_quote(symbol)
            if quote:
                return quote
        except Exception as exc:
            print(f"[!] Upstox {symbol}: {exc}")
        if not upstox_fallback_enabled():
            return None
    return _fetch_nse_quote(symbol)


def option_underlying_key(underlying: str) -> str | None:
    clean = re.sub(r"[^A-Z0-9 ]", "", (underlying or "").upper()).strip()
    overrides = parse_upstox_instrument_overrides()
    return overrides.get(clean) or UPSTOX_OPTION_UNDERLYINGS.get(clean)


def summarize_upstox_option_chain(rows: list[dict], underlying: str, expiry_date: str, max_rows: int = 80) -> dict:
    spot = None
    compact_rows = []
    total_call_oi = total_put_oi = 0.0
    total_call_prev_oi = total_put_prev_oi = 0.0
    max_call = {"strike": None, "oi": -1.0}
    max_put = {"strike": None, "oi": -1.0}

    for row in rows or []:
        strike = safe_float(row.get("strike_price"))
        spot = safe_float(row.get("underlying_spot_price"), spot or 0.0) or spot
        call_md = (row.get("call_options") or {}).get("market_data") or {}
        put_md = (row.get("put_options") or {}).get("market_data") or {}
        call_greeks = (row.get("call_options") or {}).get("option_greeks") or {}
        put_greeks = (row.get("put_options") or {}).get("option_greeks") or {}
        call_oi = safe_float(call_md.get("oi"))
        put_oi = safe_float(put_md.get("oi"))
        call_prev_oi = safe_float(call_md.get("prev_oi"))
        put_prev_oi = safe_float(put_md.get("prev_oi"))
        total_call_oi += call_oi
        total_put_oi += put_oi
        total_call_prev_oi += call_prev_oi
        total_put_prev_oi += put_prev_oi
        if call_oi > max_call["oi"]:
            max_call = {"strike": strike, "oi": call_oi}
        if put_oi > max_put["oi"]:
            max_put = {"strike": strike, "oi": put_oi}
        compact_rows.append({
            "strike": strike,
            "call": {
                "ltp": safe_float(call_md.get("ltp")),
                "oi": call_oi,
                "changeInOi": call_oi - call_prev_oi,
                "volume": safe_float(call_md.get("volume")),
                "bid": safe_float(call_md.get("bid_price")),
                "ask": safe_float(call_md.get("ask_price")),
                "iv": safe_float(call_greeks.get("iv")),
                "delta": safe_float(call_greeks.get("delta")),
            },
            "put": {
                "ltp": safe_float(put_md.get("ltp")),
                "oi": put_oi,
                "changeInOi": put_oi - put_prev_oi,
                "volume": safe_float(put_md.get("volume")),
                "bid": safe_float(put_md.get("bid_price")),
                "ask": safe_float(put_md.get("ask_price")),
                "iv": safe_float(put_greeks.get("iv")),
                "delta": safe_float(put_greeks.get("delta")),
            },
        })

    if spot:
        compact_rows.sort(key=lambda row: abs(row["strike"] - spot))
    else:
        compact_rows.sort(key=lambda row: row["strike"])
    limited_rows = sorted(compact_rows[:max_rows], key=lambda row: row["strike"])
    call_change = total_call_oi - total_call_prev_oi
    put_change = total_put_oi - total_put_prev_oi
    pcr = round(total_put_oi / total_call_oi, 2) if total_call_oi else None
    return {
        "provider": "Upstox",
        "underlying": underlying,
        "expiry": expiry_date,
        "generatedAt": ist_now().isoformat(),
        "summary": {
            "spot": round_or_none(spot),
            "pcr": pcr,
            "totalCallOi": round(total_call_oi),
            "totalPutOi": round(total_put_oi),
            "callOiChange": round(call_change),
            "putOiChange": round(put_change),
            "maxCallOiStrike": max_call["strike"],
            "maxPutOiStrike": max_put["strike"],
            "flowBias": "Put writing support" if put_change > call_change else "Call writing pressure" if call_change > put_change else "Balanced",
        },
        "rows": limited_rows,
    }


def fetch_upstox_option_chain(underlying: str, expiry_date: str, max_rows: int = 80) -> dict:
    if not upstox_access_token():
        raise RuntimeError("UPSTOX_ACCESS_TOKEN is not configured")
    underlying_key = option_underlying_key(underlying)
    if not underlying_key:
        raise ValueError(f"Unsupported Upstox option underlying: {underlying}")
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", expiry_date or ""):
        raise ValueError("expiry must be provided in YYYY-MM-DD format")
    response = http_session().get(
        f"{upstox_api_base()}/option/chain",
        params={"instrument_key": underlying_key, "expiry_date": expiry_date},
        headers=upstox_headers(),
        timeout=10,
    )
    response.raise_for_status()
    payload = response.json()
    if payload.get("status") not in {None, "success"}:
        raise RuntimeError(f"Upstox option-chain request failed: {payload.get('status')}")
    return summarize_upstox_option_chain(payload.get("data") or [], underlying, expiry_date, max_rows=max_rows)


def _fetch_nse_quote(symbol: str) -> dict | None:
    clean = re.sub(r"[^A-Z0-9&.-]", "", (symbol or "").upper())
    if not clean:
        return None
    now = time.time()
    cached = _nse_quote_cache.get(clean)
    if cached and (now - cached[1] < nse_quote_cache_ttl()):
        return cached[0]

    session = _nse_init_session()
    r = session.get(
        "https://www.nseindia.com/api/quote-equity",
        params={"symbol": clean},
        headers=NSE_HEADERS,
        timeout=6,
        verify=certifi.where(),
    )
    if r.status_code in {401, 403}:
        session = _nse_init_session(force=True)
        r = session.get(
            "https://www.nseindia.com/api/quote-equity",
            params={"symbol": clean},
            headers=NSE_HEADERS,
            timeout=6,
            verify=certifi.where(),
        )
    r.raise_for_status()
    payload = r.json()
    pi = payload.get("priceInfo", {})
    last = safe_float(pi.get("lastPrice"))
    prev = safe_float(pi.get("previousClose"), last)
    if not last:
        return None
    quote = {
        "symbol": clean,
        "name": payload.get("info", {}).get("companyName") or clean,
        "price": round(last, 2),
        "previous_close": round(prev, 2),
        "change": round(last - prev, 2),
        "pct": round(((last - prev) / prev * 100) if prev else 0, 2),
        "day_high": round(safe_float(pi.get("intraDayHighLow", {}).get("max"), last), 2),
        "day_low": round(safe_float(pi.get("intraDayHighLow", {}).get("min"), last), 2),
        "fetchedAt": now,
        "source": "NSE",
    }
    _nse_quote_cache[clean] = (quote, now)
    return quote


def _history_candidates(label_or_symbol: str, is_index: bool = False) -> list[str]:
    if is_index:
        return INDEX_HISTORY_SYMBOLS.get(label_or_symbol, [])
    sym = re.sub(r"[^A-Z0-9.-]", "", label_or_symbol.upper())
    if not sym:
        return []
    if sym.startswith("^") or "." in sym:
        return [sym]
    return [f"{sym}.NS", sym]


def build_live_only_signal(symbol: str, live_quote: dict) -> dict:
    price = live_quote["price"]
    day_high = live_quote.get("day_high", price)
    day_low = live_quote.get("day_low", price)
    pct = live_quote.get("pct", 0.0)
    if pct >= 1.25:
        trend = "Intraday strength"
    elif pct <= -1.25:
        trend = "Intraday weakness"
    else:
        trend = "Range"

    if day_high and price >= day_high * 0.9975:
        signal = "Near day high"
    elif day_low and price <= day_low * 1.0025:
        signal = "Near day low"
    elif pct >= 0.5:
        signal = "Buyer support"
    elif pct <= -0.5:
        signal = "Seller pressure"
    else:
        signal = "Wait for setup"

    breakout_gap = ((price / day_high) - 1) * 100 if day_high else None
    return {
        "symbol": re.sub(r"[^A-Z0-9&.-]", "", symbol.upper()),
        "name": live_quote.get("name") or symbol,
        "price": round(price, 2),
        "change": round(live_quote.get("change", 0.0), 2),
        "pct": round(pct, 2),
        "trend": trend,
        "signal": signal,
        "rsi14": None,
        "ret5": None,
        "ret20": None,
        "vol20": None,
        "sma20": None,
        "sma50": None,
        "high20": round_or_none(day_high),
        "low20": round_or_none(day_low),
        "support": round_or_none(day_low),
        "resistance": round_or_none(day_high),
        "volumeRatio": None,
        "breakoutGap": round_or_none(breakout_gap),
        "drawdownFromHigh": round_or_none(breakout_gap),
    }


def build_symbol_signal(symbol: str, live_quote: dict | None = None, is_index: bool = False) -> dict | None:
    candidates = _history_candidates(symbol, is_index=is_index)
    hist = None
    for candidate in candidates:
        try:
            hist = _yahoo_chart(candidate, range_="6mo", interval="1d")
            break
        except Exception:
            continue
    if hist is None:
        return build_live_only_signal(symbol, live_quote) if live_quote else None

    rows = hist["rows"]
    closes = [row["close"] for row in rows]
    highs = [row["high"] for row in rows]
    lows = [row["low"] for row in rows]
    volumes = [row["volume"] for row in rows]

    market_price = live_quote["price"] if live_quote and live_quote.get("price") else closes[-1]
    prev_close = live_quote["previous_close"] if live_quote and live_quote.get("previous_close") else closes[-2]
    day_change = live_quote["change"] if live_quote and live_quote.get("change") is not None else market_price - prev_close
    day_pct = live_quote["pct"] if live_quote and live_quote.get("pct") is not None else ((market_price - prev_close) / prev_close * 100 if prev_close else 0)

    sma20_val = sma(closes, 20)
    sma50_val = sma(closes, 50)
    rsi_val = rsi(closes, 14)
    ret5 = pct_return(closes, 5)
    ret20 = pct_return(closes, 20)
    vol20 = realized_vol(closes, 20)
    high20 = max(highs[-20:]) if len(highs) >= 20 else max(highs)
    low20 = min(lows[-20:]) if len(lows) >= 20 else min(lows)
    avg_vol20 = mean(volumes[-20:]) if len(volumes) >= 20 else mean(volumes)
    volume_ratio = (volumes[-1] / avg_vol20) if avg_vol20 else None

    support = None
    resistance = None
    candidates_support = [lvl for lvl in [low20, sma20_val, sma50_val] if lvl is not None and lvl <= market_price]
    candidates_resistance = [lvl for lvl in [high20, sma20_val, sma50_val] if lvl is not None and lvl >= market_price]
    if candidates_support:
        support = max(candidates_support)
    if candidates_resistance:
        resistance = min(candidates_resistance)

    trend = trend_label(market_price, sma20_val, sma50_val, rsi_val)
    signal = setup_label(market_price, high20, low20, sma20_val, rsi_val, ret5)
    breakout_gap = ((market_price / high20) - 1) * 100 if high20 else None
    drawdown_from_high = ((market_price / max(highs)) - 1) * 100 if highs else None

    return {
        "symbol": re.sub(r"[^A-Z0-9&.-]", "", symbol.upper()),
        "name": (live_quote or {}).get("name") or symbol,
        "price": round(market_price, 2),
        "change": round(day_change, 2),
        "pct": round(day_pct, 2),
        "trend": trend,
        "signal": signal,
        "rsi14": round_or_none(rsi_val),
        "ret5": round_or_none(ret5),
        "ret20": round_or_none(ret20),
        "vol20": round_or_none(vol20),
        "sma20": round_or_none(sma20_val),
        "sma50": round_or_none(sma50_val),
        "high20": round_or_none(high20),
        "low20": round_or_none(low20),
        "support": round_or_none(support),
        "resistance": round_or_none(resistance),
        "volumeRatio": round_or_none(volume_ratio),
        "breakoutGap": round_or_none(breakout_gap),
        "drawdownFromHigh": round_or_none(drawdown_from_high),
    }


# ── Data fetchers ──────────────────────────────────────────────────────────
def fetch_feed_articles(feed_cfg: dict[str, str]) -> tuple[str, dict, list[dict]]:
    src = feed_cfg["name"]
    url = feed_cfg["url"]
    scope = feed_cfg["scope"]
    articles = []
    try:
        data = _get_feed(url)
        feed = feedparser.parse(data)
        for e in feed.entries[:18]:
            source_meta = e.get("source") or {}
            publisher = strip_html(source_meta.get("title", "")) if hasattr(source_meta, "get") else ""
            publisher = publisher or feed_publisher_label(src)
            title = clean_headline(e.get("title", ""), publisher)
            summary = clean_summary(e.get("summary", e.get("description", "")), publisher)[:480]
            link = e.get("link", "#")
            h = url_hash(link) if link != "#" else url_hash(title[:60])
            if not title:
                continue

            try:
                pp = e.get("published_parsed") or e.get("updated_parsed")
                dt = datetime(*pp[:6], tzinfo=timezone.utc).astimezone(IST) if pp else ist_now()
            except Exception:
                dt = ist_now()

            sent = sentiment(title, summary)
            impact_score, impact_meta = impact_details(
                title,
                summary,
                sent,
                source=publisher,
                published_dt=dt,
                scope=scope,
            )
            articles.append({
                "id": h,
                "title": title,
                "titleKey": normalized_headline(title),
                "summary": summary,
                "link": link,
                "source": publisher,
                "feed": src,
                "scope": scope,
                "sector": classify(title + " " + summary),
                "sentiment": sent,
                "impact": impact_score,
                "impactMeta": impact_meta,
                "published": dt.strftime("%d %b %H:%M"),
                "ts": dt.timestamp(),
            })
        return src, {"ok": True, "count": len(articles), "scope": scope}, articles
    except Exception as ex:
        print(f"[!] Feed {src}: {ex}")
        return src, {"ok": False, "error": str(ex)[:120], "scope": scope}, []


def fetch_news() -> tuple[list[dict], dict]:
    seen_hashes, seen_titles, out = set(), set(), []
    status = {}

    with ThreadPoolExecutor(max_workers=min(MAX_NEWS_WORKERS, len(RSS_FEEDS))) as executor:
        futures = [executor.submit(fetch_feed_articles, feed_cfg) for feed_cfg in RSS_FEEDS]
        for future in as_completed(futures):
            src, feed_status, articles = future.result()
            status[src] = feed_status
            for article in articles:
                title_key = article.pop("titleKey", "")
                if article["id"] in seen_hashes or title_key in seen_titles:
                    continue
                seen_hashes.add(article["id"])
                seen_titles.add(title_key)
                out.append(article)

    out.sort(key=lambda x: -x["ts"])
    return out, status


def fetch_tickers() -> tuple[dict, dict]:
    out = {}
    analytics_indices = {}
    fetched_at = time.time()

    if active_market_data_provider() == UPSTOX_PROVIDER_NAME:
        try:
            for label, quote in fetch_upstox_index_quotes().items():
                client_quote = {
                    "price": quote["price"],
                    "change": quote["change"],
                    "pct": quote["pct"],
                    "live": True,
                    "sym": "",
                    "fetchedAt": quote.get("fetchedAt", fetched_at),
                    "source": quote.get("source", "Upstox"),
                }
                analytics_indices[label] = dict(client_quote)
                ticker_label = "VIX" if label == "India VIX" else label
                out[ticker_label] = dict(client_quote)
        except Exception as e:
            print(f"[!] Upstox indices: {e}")

    # NSE indices
    try:
        r = _nse_init_session().get(
            "https://www.nseindia.com/api/allIndices",
            headers=NSE_HEADERS, timeout=6, verify=certifi.where()
        )
        if r.status_code in {401, 403}:
            r = _nse_init_session(force=True).get(
                "https://www.nseindia.com/api/allIndices",
                headers=NSE_HEADERS, timeout=6, verify=certifi.where()
            )
        r.raise_for_status()
        for idx in r.json().get("data", []):
            name = idx.get("indexSymbol", "")
            if name in ANALYTICS_INDEX_NAMES:
                label = ANALYTICS_INDEX_NAMES[name]
                last = safe_float(idx.get("last"))
                prev = safe_float(idx.get("previousClose"), last)
                if last:
                    ch = round(last - prev, 2)
                    pct = round((ch / prev * 100) if prev else 0, 2)
                    if label not in analytics_indices:
                        analytics_indices[label] = {
                            "price": round(last, 2),
                            "change": ch,
                            "pct": pct,
                            "live": True,
                            "sym": "",
                            "fetchedAt": fetched_at,
                            "source": "NSE",
                        }
            if name in NSE_INDICES_WANTED:
                label = NSE_INDICES_WANTED[name]
                last = safe_float(idx.get("last"))
                prev = safe_float(idx.get("previousClose"), last)
                ch = round(last - prev, 2)
                pct = round((ch / prev * 100) if prev else 0, 2)
                if label not in out:
                    out[label] = {"price": round(last, 2), "change": ch, "pct": pct, "live": True, "sym": "", "fetchedAt": fetched_at, "source": "NSE"}
    except Exception as e:
        print(f"[!] NSE allIndices: {e}")

    # NSE individual stocks
    def stock_worker(item: tuple[str, str]) -> tuple[str, str, dict | None, Exception | None]:
        label, sym = item
        try:
            return label, sym, fetch_live_quote(sym), None
        except Exception as exc:
            return label, sym, None, exc

    with ThreadPoolExecutor(max_workers=min(MAX_QUOTE_WORKERS, len(NSE_STOCKS))) as executor:
        futures = [executor.submit(stock_worker, item) for item in NSE_STOCKS.items()]
        for future in as_completed(futures):
            label, sym, quote, error = future.result()
            if error:
                print(f"[!] NSE {sym}: {error}")
                continue
            if quote:
                out[label] = {
                    "price": quote["price"],
                    "change": quote["change"],
                    "pct": quote["pct"],
                    "live": True,
                    "sym": "Rs",
                    "fetchedAt": quote.get("fetchedAt", fetched_at),
                    "source": quote.get("source", "NSE"),
                }

    # Yahoo extras with cache
    def yahoo_worker(item: tuple[str, tuple[str, str]]) -> tuple[str, str, str, tuple[float, float, float] | None, Exception | None]:
        label, (sym, csym) = item
        try:
            return label, sym, csym, _yahoo_price(sym), None
        except Exception as exc:
            return label, sym, csym, None, exc

    with ThreadPoolExecutor(max_workers=min(MAX_QUOTE_WORKERS, len(YAHOO_EXTRAS))) as executor:
        futures = [executor.submit(yahoo_worker, item) for item in YAHOO_EXTRAS.items()]
        for future in as_completed(futures):
            label, sym, csym, result, error = future.result()
            if error:
                print(f"[!] Yahoo {label}: {error}")
                continue
            if result:
                p, ch, pct = result
                out[label] = {"price": p, "change": ch, "pct": pct, "live": False, "sym": csym, "fetchedAt": fetched_at, "source": "Yahoo"}

    return out, analytics_indices


def build_market_analytics_payload(
    articles: list[dict],
    ticks: dict,
    index_snapshot: dict,
    symbols: list[str],
    quote_map: dict[str, dict] | None = None,
) -> dict:
    sector_news = build_sector_news_scores(articles)
    sector_rows = []
    sector_map = {}

    for sector, label in SECTOR_TO_INDEX.items():
        snap = index_snapshot.get(label)
        news = sector_news.get(sector, {"score": 0.0, "count": 0, "bull": 0, "bear": 0})
        bias, tone = sector_bias_label(news["score"])
        row = {
            "sector": sector,
            "label": label,
            "pct": round_or_none(snap.get("pct") if snap else None),
            "price": round_or_none(snap.get("price") if snap else None),
            "count": news["count"],
            "bull": news["bull"],
            "bear": news["bear"],
            "newsScore": news["score"],
            "newsBias": bias,
            "tone": tone,
        }
        sector_rows.append(row)
        sector_map[sector] = row

    tradable_rows = [row for row in sector_rows if row["pct"] is not None and row["sector"] != "General"]
    leaders = sorted(tradable_rows, key=lambda row: row["pct"], reverse=True)
    positive = sum(1 for row in tradable_rows if row["pct"] > 0)
    negative = sum(1 for row in tradable_rows if row["pct"] < 0)

    primary_signals = []
    primary_map = {}
    if ticks or index_snapshot:
        for label in PRIMARY_LEVEL_LABELS:
            live_quote = index_snapshot.get(label) or ticks.get(label)
            signal = build_symbol_signal(label, live_quote=live_quote, is_index=True)
            if signal:
                primary_signals.append({"label": label, **signal})
                primary_map[label] = signal
            elif live_quote:
                primary_signals.append({
                    "label": label,
                    "symbol": label.upper().replace(" ", ""),
                    "name": label,
                    "price": live_quote["price"],
                    "change": live_quote["change"],
                    "pct": live_quote["pct"],
                    "trend": "Live",
                    "signal": "Live snapshot",
                })

    nifty = (index_snapshot.get("Nifty 50") or ticks.get("Nifty 50") or {})
    bank = (index_snapshot.get("Nifty Bank") or ticks.get("Nifty Bank") or {})
    it_idx = (index_snapshot.get("Nifty IT") or ticks.get("Nifty IT") or {})
    midcap = (index_snapshot.get("Nifty Midcap") or ticks.get("Nifty Midcap") or {})
    smallcap = (index_snapshot.get("Nifty Smallcap") or ticks.get("Nifty Smallcap") or {})
    vix = (index_snapshot.get("India VIX") or ticks.get("VIX") or {})
    crude = ticks.get("Crude Oil") or {}

    risk_score = 0
    nifty_pct = nifty.get("pct")
    bank_pct = bank.get("pct")
    vix_price = vix.get("price")
    vix_chg = vix.get("pct")
    crude_price = crude.get("price")
    crude_pct = crude.get("pct")
    if nifty_pct is not None:
        risk_score += 1 if nifty_pct > 0 else -1
    if bank_pct is not None and nifty_pct is not None:
        risk_score += 1 if bank_pct >= nifty_pct else -1
    if midcap.get("pct") is not None and nifty_pct is not None and midcap["pct"] > nifty_pct:
        risk_score += 1
    if smallcap.get("pct") is not None and nifty_pct is not None and smallcap["pct"] > nifty_pct:
        risk_score += 1
    if positive > negative:
        risk_score += 1
    elif negative > positive:
        risk_score -= 1
    if vix_price:
        risk_score += 1 if vix_price < 15 else -1
    if vix_chg is not None:
        risk_score += 1 if vix_chg < 0 else -1 if vix_chg > 1.5 else 0

    if risk_score >= 4:
        regime = {"label": "Risk-On Trend", "tone": "bull", "detail": "Breadth is supportive and volatility is contained."}
    elif risk_score <= -3:
        regime = {"label": "Risk-Off Tape", "tone": "bear", "detail": "Size down and respect headline risk while VIX is elevated."}
    else:
        regime = {"label": "Rotation Market", "tone": "neutral", "detail": "Leadership is selective, so sector selection matters more than headline index direction."}

    news_score = round(sum(item["score"] for item in sector_news.values()), 2)
    news_tone = "bull" if news_score > 5 else "bear" if news_score < -5 else "neutral"
    breadth_label = f"{positive} sectors up / {negative} down"
    leadership = leaders[0]["sector"] if leaders else "Mixed"
    laggard = leaders[-1]["sector"] if leaders else "Mixed"
    breadth_spread = positive - negative
    smid_gap = relative_gap(smallcap.get("pct"), nifty_pct)
    bank_vs_it = relative_gap(bank_pct, it_idx.get("pct"))
    crude_tone = "bull" if crude_pct and crude_pct >= 1 else "bear" if crude_pct and crude_pct <= -1 else "neutral"
    if crude_price is not None and crude_pct is not None:
        crude_detail = f"{crude_pct:+.2f}% on the day. Useful for tracking energy names, OMCs, and inflation-sensitive moves."
    elif crude_price is not None:
        crude_detail = "Tracking front-month crude oil futures for energy-sensitive setups."
    else:
        crude_detail = "Tracking front-month crude oil futures for energy-sensitive setups."

    overview_cards = [
        {"label": "Regime", "value": regime["label"], "detail": regime["detail"], "tone": regime["tone"]},
        {"label": "Breadth", "value": breadth_label, "detail": f"Spread {breadth_spread:+d} across key sectors", "tone": "bull" if breadth_spread > 0 else "bear" if breadth_spread < 0 else "neutral"},
        {"label": "Volatility", "value": f"{vix_price:.2f} VIX" if vix_price is not None else "Unavailable", "detail": "Calm tape" if vix_price and vix_price < 14 else "Higher hedging demand" if vix_price and vix_price >= 16 else "Middle of the range", "tone": "bull" if vix_price and vix_price < 14 else "bear" if vix_price and vix_price >= 16 else "neutral"},
        {"label": "Crude Oil", "value": f"${crude_price:.2f}" if crude_price is not None else "Unavailable", "detail": crude_detail, "tone": crude_tone},
        {"label": "Leadership", "value": leadership, "detail": f"Weakest pocket: {laggard}", "tone": "bull" if leaders and leaders[0]["pct"] and leaders[0]["pct"] > 0 else "neutral"},
        {"label": "SMID vs Nifty", "value": f"{smid_gap:+.2f}%" if smid_gap is not None else "Unavailable", "detail": "Positive means broader risk appetite is expanding beyond the headline index.", "tone": "bull" if smid_gap and smid_gap > 0 else "bear" if smid_gap and smid_gap < 0 else "neutral"},
        {"label": "News Pulse", "value": f"{news_score:+.1f}", "detail": "Weighted from recent high-impact headlines by sector.", "tone": news_tone},
    ]

    alerts = []
    if leaders:
        leader_row = leaders[0]
        alerts.append(f"{leader_row['sector']} is leading today at {leader_row['pct']:+.2f}% with {leader_row['newsBias'].lower()} news flow.")
    if vix_price is not None:
        if vix_price >= 16:
            alerts.append(f"India VIX is at {vix_price:.2f}. Expect wider intraday swings and demand cleaner entries.")
        elif vix_price <= 13.5:
            alerts.append(f"India VIX is muted at {vix_price:.2f}, which usually favors trend-following over panic hedging.")
    if crude_pct is not None and abs(crude_pct) >= 1.5:
        move = "spiking" if crude_pct > 0 else "sliding"
        alerts.append(f"Crude oil is {move} {abs(crude_pct):.2f}% today. Watch energy names, OMCs, and inflation-sensitive sectors for spillover.")
    if bank_vs_it is not None and abs(bank_vs_it) >= 0.75:
        lead = "Banks" if bank_vs_it > 0 else "IT"
        alerts.append(f"{lead} is outperforming the other leadership pocket by {abs(bank_vs_it):.2f}%, a useful clue for intraday sector rotation.")
    if smid_gap is not None and smid_gap >= 0.75:
        alerts.append(f"Smallcaps are beating Nifty 50 by {smid_gap:.2f}%. That usually signals broader participation and better breakout follow-through.")
    elif smid_gap is not None and smid_gap <= -0.75:
        alerts.append(f"Smallcaps are lagging Nifty 50 by {abs(smid_gap):.2f}%, which is often a warning that risk appetite is narrowing.")

    symbol_signals = []
    symbol_map = {}
    for sym in symbols:
        live_quote = (quote_map or {}).get(sym)
        if live_quote is None:
            try:
                live_quote = fetch_live_quote(sym)
            except Exception:
                live_quote = None
        try:
            signal = build_symbol_signal(sym, live_quote=live_quote, is_index=False)
        except Exception:
            signal = None
        if signal:
            symbol_signals.append(signal)
            symbol_map[signal["symbol"]] = signal

    key_levels = []
    for item in primary_signals:
        key_levels.append({
            "label": item["label"],
            "price": item.get("price"),
            "pct": item.get("pct"),
            "trend": item.get("trend"),
            "rsi14": item.get("rsi14"),
            "support": item.get("support"),
            "resistance": item.get("resistance"),
            "signal": item.get("signal"),
        })

    return {
        "generatedAt": ist_now().strftime("%H:%M:%S"),
        "overviewCards": overview_cards,
        "alerts": alerts[:5],
        "sectorBoard": leaders,
        "sectorMap": sector_map,
        "keyLevels": key_levels,
        "watchlistSignals": symbol_signals,
        "symbolMap": symbol_map,
        "regime": regime,
        "primary": primary_signals,
    }


def build_derivatives_analysis_payload(
    articles: list[dict],
    ticks: dict,
    index_snapshot: dict,
    price_history: dict[str, list[float]] | None = None,
    market_status: dict | None = None,
) -> dict:
    price_history = price_history or {}
    market_status = market_status or get_market_status()
    sector_news = build_sector_news_scores(articles)

    nifty = (index_snapshot.get("Nifty 50") or ticks.get("Nifty 50") or {})
    bank = (index_snapshot.get("Nifty Bank") or ticks.get("Nifty Bank") or {})
    it_idx = (index_snapshot.get("Nifty IT") or ticks.get("Nifty IT") or {})
    midcap = (index_snapshot.get("Nifty Midcap") or ticks.get("Nifty Midcap") or {})
    smallcap = (index_snapshot.get("Nifty Smallcap") or ticks.get("Nifty Smallcap") or {})
    vix = (index_snapshot.get("India VIX") or ticks.get("VIX") or {})
    crude = ticks.get("Crude Oil") or {}
    brent = ticks.get("Brent Crude") or {}
    usd_inr = ticks.get("USD/INR") or {}
    gold = ticks.get("Gold") or {}

    nifty_pct = nifty.get("pct")
    bank_pct = bank.get("pct")
    it_pct = it_idx.get("pct")
    midcap_pct = midcap.get("pct")
    smallcap_pct = smallcap.get("pct")
    vix_price = vix.get("price")
    vix_pct = vix.get("pct")
    crude_price = crude.get("price")
    crude_pct = crude.get("pct")
    usd_pct = usd_inr.get("pct")

    bank_vs_nifty = relative_gap(bank_pct, nifty_pct)
    it_vs_nifty = relative_gap(it_pct, nifty_pct)
    midcap_vs_nifty = relative_gap(midcap_pct, nifty_pct)
    smallcap_vs_nifty = relative_gap(smallcap_pct, nifty_pct)

    banking_news = sector_news.get("Banking", {"score": 0.0})
    it_news = sector_news.get("IT", {"score": 0.0})
    energy_news = sector_news.get("Energy", {"score": 0.0})
    general_news = sector_news.get("General", {"score": 0.0})
    headline_news_score = round(
        banking_news.get("score", 0.0) * 0.35
        + it_news.get("score", 0.0) * 0.2
        + energy_news.get("score", 0.0) * 0.15
        + general_news.get("score", 0.0) * 0.3,
        2,
    )

    nifty_hist = price_history.get("Nifty 50", [])
    bank_hist = price_history.get("Nifty Bank", [])
    vix_hist = price_history.get("VIX", [])
    nifty_flow_3 = intraday_return(nifty_hist, 3)
    nifty_flow_8 = intraday_return(nifty_hist, 8)
    bank_flow_3 = intraday_return(bank_hist, 3)
    bank_flow_8 = intraday_return(bank_hist, 8)
    vix_flow_3 = intraday_return(vix_hist, 3)
    focus_label = "Nifty Bank" if (bank_vs_nifty or 0) > 0.35 or abs(bank_flow_8 or 0) > abs(nifty_flow_8 or 0) else "Nifty 50"
    focus_display = "Bank Nifty" if focus_label == "Nifty Bank" else focus_label
    focus_hist = bank_hist if focus_label == "Nifty Bank" else nifty_hist
    focus_flow_3 = bank_flow_3 if focus_label == "Nifty Bank" else nifty_flow_3
    focus_flow_8 = bank_flow_8 if focus_label == "Nifty Bank" else nifty_flow_8
    focus_intraday_range = intraday_range_pct(focus_hist, 12)

    trend_component = score_band(nifty_pct, 0.75, 0.2, -0.2, -0.75) + score_band(nifty_flow_8, 0.45, 0.12, -0.12, -0.45)
    leadership_component = score_band(bank_vs_nifty, 0.6, 0.2, -0.2, -0.6) + score_band(smallcap_vs_nifty, 0.5, 0.15, -0.15, -0.5)

    vol_component = 0
    if vix_price is not None:
        vol_component += 1 if vix_price <= 14 else -1 if vix_price >= 16.5 else 0
    if vix_pct is not None:
        vol_component += 1 if vix_pct <= -1.5 else -1 if vix_pct >= 1.5 else 0

    macro_component = 0
    if crude_pct is not None:
        macro_component += 1 if crude_pct <= -0.8 else -1 if crude_pct >= 1 else 0
    if usd_pct is not None:
        macro_component += 1 if usd_pct <= -0.15 else -1 if usd_pct >= 0.15 else 0

    news_component = score_band(headline_news_score, 10, 2.5, -2.5, -10)
    flow_component = score_band(focus_flow_3, 0.35, 0.1, -0.1, -0.35) + score_band(focus_flow_8, 0.55, 0.15, -0.15, -0.55)
    composite_score = trend_component + leadership_component + vol_component + macro_component + news_component + flow_component

    data_points = sum(
        value is not None
        for value in [
            nifty_pct, bank_pct, bank_vs_nifty, smallcap_vs_nifty, vix_price, vix_pct, crude_pct, usd_pct,
            headline_news_score, nifty_flow_3, nifty_flow_8, bank_flow_3, bank_flow_8,
        ]
    )
    bias_label, bias_tone = bias_from_score(composite_score)
    conviction = conviction_from_score(composite_score, data_points)
    bull_prob = int(clamp(50 + composite_score * 5, 18, 82))
    bear_prob = 100 - bull_prob
    day_type, day_type_detail = day_type_from_context(composite_score, vix_price, focus_flow_3, focus_intraday_range)

    primary_signal_map = {}
    if ticks or index_snapshot:
        for label in PRIMARY_LEVEL_LABELS:
            live_quote = index_snapshot.get(label) or ticks.get(label) or (ticks.get("VIX") if label == "India VIX" else None)
            try:
                primary_signal_map[label] = build_symbol_signal(label, live_quote=live_quote, is_index=True)
            except Exception:
                primary_signal_map[label] = None

    focus_live_quote = bank if focus_label == "Nifty Bank" else nifty
    focus_signal = primary_signal_map.get(focus_label)
    focus_price = focus_signal.get("price") if focus_signal else focus_live_quote.get("price")
    expected_move_points, expected_move_pct = implied_move_points(focus_price, vix_price)

    overview_cards = [
        {
            "label": "Index Leader",
            "value": focus_display if focus_price is not None else "Waiting",
            "detail": "This is where the cleaner short-term derivatives expression is currently clustering.",
            "tone": bias_tone,
        },
        {
            "label": "Volatility Regime",
            "value": f"{vix_price:.2f} VIX" if vix_price is not None else "Unavailable",
            "detail": "Higher VIX usually means wider option premiums and faster sentiment flips."
            if vix_price is not None
            else "Waiting for volatility data.",
            "tone": "bear" if vix_price is not None and vix_price >= 16 else "bull" if vix_price is not None and vix_price <= 13.5 else "neutral",
        },
        {
            "label": "Bank vs Nifty",
            "value": f"{bank_vs_nifty:+.2f}%" if bank_vs_nifty is not None else "Unavailable",
            "detail": "Positive means Bank Nifty is outperforming the headline index.",
            "tone": "bull" if bank_vs_nifty is not None and bank_vs_nifty > 0 else "bear" if bank_vs_nifty is not None and bank_vs_nifty < 0 else "neutral",
        },
        {
            "label": "Short-Term Flow",
            "value": f"{focus_flow_8:+.2f}%" if focus_flow_8 is not None else "Unavailable",
            "detail": f"Recent {focus_display} tape over the latest dashboard ticks.",
            "tone": "bull" if focus_flow_8 is not None and focus_flow_8 > 0 else "bear" if focus_flow_8 is not None and focus_flow_8 < 0 else "neutral",
        },
        {
            "label": "Crude Impulse",
            "value": f"${crude_price:.2f}" if crude_price is not None else "Unavailable",
            "detail": "Crude matters for OMCs, inflation expectations, and broad risk tone.",
            "tone": "bull" if crude_pct is not None and crude_pct < 0 else "bear" if crude_pct is not None and crude_pct > 1 else "neutral",
        },
        {
            "label": "Rupee Pulse",
            "value": f"{usd_inr.get('price', 0):.2f}" if usd_inr.get("price") is not None else "Unavailable",
            "detail": "USD/INR pressure often feeds into imported inflation and foreign-flow sentiment.",
            "tone": "bear" if usd_pct is not None and usd_pct > 0.3 else "bull" if usd_pct is not None and usd_pct < -0.3 else "neutral",
        },
    ]

    prediction_cards = [
        {
            "label": "Model Bias",
            "value": bias_label,
            "detail": "Composite directional read from index trend, breadth, intraday tape, volatility, macro, and news.",
            "tone": bias_tone,
        },
        {
            "label": "Conviction",
            "value": f"{conviction} / 100",
            "detail": "Higher means more factors are aligned in the same direction. It is still context, not certainty.",
            "tone": "bull" if conviction >= 68 and bias_tone == "bull" else "bear" if conviction >= 68 and bias_tone == "bear" else "neutral",
        },
        {
            "label": "Bull Path",
            "value": f"{bull_prob}%",
            "detail": "Probability-weighted leaning toward upside continuation from the current composite score.",
            "tone": "bull" if bull_prob > 55 else "neutral",
        },
        {
            "label": "Bear Path",
            "value": f"{bear_prob}%",
            "detail": "Probability-weighted leaning toward downside continuation from the current composite score.",
            "tone": "bear" if bear_prob > 55 else "neutral",
        },
        {
            "label": "Day Type",
            "value": day_type,
            "detail": day_type_detail,
            "tone": "bull" if "Trend" in day_type and bias_tone == "bull" else "bear" if "Trend" in day_type and bias_tone == "bear" else "neutral",
        },
        {
            "label": "Expected Move",
            "value": f"{expected_move_points:,.0f} pts" if expected_move_points is not None else "Unavailable",
            "detail": f"Approx {expected_move_pct:.2f}% 1-day move from India VIX on {focus_display}."
            if expected_move_pct is not None
            else "Waiting for a valid price and VIX snapshot to estimate range.",
            "tone": "neutral",
        },
    ]

    context_notes = []
    context_notes.append(
        f"{focus_display} is the cleaner derivatives focus right now, with composite score {composite_score:+d} and {conviction}/100 conviction."
    )
    if bank_vs_nifty is not None:
        lead = "Banks" if bank_vs_nifty > 0 else "The headline index"
        context_notes.append(f"{lead} are leading by {abs(bank_vs_nifty):.2f}% versus Nifty 50, which matters for where the next clean impulse is most likely to show up.")
    if focus_flow_3 is not None and focus_flow_8 is not None:
        context_notes.append(f"Short-term tape check: {focus_display} is {focus_flow_3:+.2f}% over the last 3 ticks and {focus_flow_8:+.2f}% over the last 8 ticks.")
    if smallcap_vs_nifty is not None and abs(smallcap_vs_nifty) >= 0.5:
        breadth_mood = "broader participation is expanding" if smallcap_vs_nifty > 0 else "risk appetite is narrowing into larger names"
        context_notes.append(f"Smallcaps are {abs(smallcap_vs_nifty):.2f}% {'ahead of' if smallcap_vs_nifty > 0 else 'behind'} Nifty 50, suggesting {breadth_mood}.")
    if vix_price is not None:
        if vix_price >= 16:
            context_notes.append(f"India VIX at {vix_price:.2f} means intraday trend calls need more room and faster invalidation discipline.")
        elif vix_price <= 13.5:
            context_notes.append(f"India VIX at {vix_price:.2f} supports cleaner premium decay and better trend follow-through if price confirms.")
    if headline_news_score:
        context_notes.append(
            f"News pulse snapshot: Banking {banking_news['score']:+.1f}, IT {it_news['score']:+.1f}, Energy {energy_news['score']:+.1f}, Headline market {general_news['score']:+.1f}."
        )

    risk_flags = []
    if market_status.get("staleData"):
        risk_flags.append({"label": "Stale data", "detail": "One or more live feeds are stale. Reduce trust in short-term calls until the tape refreshes.", "tone": "bear"})
    if vix_price is not None and vix_price >= 16:
        risk_flags.append({"label": "Elevated volatility", "detail": "Option premiums are richer and reversals can be sharper than the raw index move suggests.", "tone": "bear"})
    if vix_pct is not None and vix_pct >= 2:
        risk_flags.append({"label": "Volatility repricing", "detail": "VIX is rising fast intraday, which can punish late directional entries.", "tone": "bear"})
    if bank_vs_nifty is not None and abs(bank_vs_nifty) >= 1:
        risk_flags.append({"label": "Leadership narrow", "detail": "A big Bank-vs-Nifty gap can be powerful, but it also means the move is less broad than it looks.", "tone": "neutral"})
    if smallcap_vs_nifty is not None and smallcap_vs_nifty <= -0.75:
        risk_flags.append({"label": "Breadth weak", "detail": "Smallcaps are lagging hard, which often reduces breakout durability.", "tone": "bear"})
    if crude_pct is not None and crude_pct >= 1:
        risk_flags.append({"label": "Crude pressure", "detail": "A sharp crude rise raises inflation sensitivity and can cap upside in rate-sensitive pockets.", "tone": "bear"})
    if usd_pct is not None and usd_pct >= 0.2:
        risk_flags.append({"label": "Rupee weakness", "detail": "A firm USD/INR move can add macro pressure and make equity upside less forgiving.", "tone": "bear"})
    if focus_intraday_range is not None and focus_intraday_range >= 1:
        risk_flags.append({"label": "Range already expanded", "detail": "A large early range means continuation entries need much cleaner structure than usual.", "tone": "neutral"})
    if len(focus_hist) < 6:
        risk_flags.append({"label": "Tape model warming up", "detail": "Short-term momentum signals are based on limited live history so far.", "tone": "neutral"})

    score_breakdown = [
        {"label": "Trend", "score": trend_component, "detail": "Daily index direction plus short-term tape follow-through."},
        {"label": "Leadership", "score": leadership_component, "detail": "Banking leadership and breadth expansion versus the headline index."},
        {"label": "Volatility", "score": vol_component, "detail": "India VIX level and whether volatility is being bid or offered."},
        {"label": "Macro", "score": macro_component, "detail": "Crude and USD/INR pressure or relief."},
        {"label": "News", "score": news_component, "detail": "Weighted pulse from market-sensitive news buckets."},
        {"label": "Flow", "score": flow_component, "detail": "Very short-term live tape direction from recent snapshots."},
    ]

    cross_asset_rows = [
        {
            "label": "Crude Oil",
            "price": crude.get("price"),
            "pct": crude.get("pct"),
            "unit": "$",
            "detail": "Energy sensitivity, inflation expectations, OMC context.",
        },
        {
            "label": "Brent Crude",
            "price": brent.get("price"),
            "pct": brent.get("pct"),
            "unit": "$",
            "detail": "Global oil benchmark and geopolitical risk barometer.",
        },
        {
            "label": "USD/INR",
            "price": usd_inr.get("price"),
            "pct": usd_inr.get("pct"),
            "unit": "",
            "detail": "Rupee pressure, imported inflation, and foreign-flow context.",
        },
        {
            "label": "Gold",
            "price": gold.get("price"),
            "pct": gold.get("pct"),
            "unit": "$",
            "detail": "Safe-haven tone and risk-aversion cross-check.",
        },
    ]

    relative_value_rows = [
        {
            "label": "Bank Nifty vs Nifty",
            "pct": bank_vs_nifty,
            "detail": "Tracks financial leadership against the headline index.",
        },
        {
            "label": "Nifty IT vs Nifty",
            "pct": it_vs_nifty,
            "detail": "Checks whether growth-sensitive tech is confirming or diverging.",
        },
        {
            "label": "Midcap vs Nifty",
            "pct": midcap_vs_nifty,
            "detail": "Useful for judging whether participation is broadening.",
        },
        {
            "label": "Smallcap vs Nifty",
            "pct": smallcap_vs_nifty,
            "detail": "A quick breadth read for risk appetite beyond the headline index.",
        },
    ]

    signal_matrix = []
    if ticks or index_snapshot:
        for label, live_quote in [
            ("Nifty 50", nifty),
            ("Nifty Bank", bank),
            ("Nifty IT", it_idx),
            ("India VIX", vix),
        ]:
            signal = primary_signal_map.get(label)
            short_3 = intraday_return(price_history.get("VIX" if label == "India VIX" else label, []), 3)
            short_8 = intraday_return(price_history.get("VIX" if label == "India VIX" else label, []), 8)
            signal_matrix.append({
                "label": "Bank Nifty" if label == "Nifty Bank" else label,
                "price": (signal or {}).get("price", live_quote.get("price")),
                "pct": (signal or {}).get("pct", live_quote.get("pct")),
                "short3": short_3,
                "short8": short_8,
                "trend": (signal or {}).get("trend", "Live"),
                "signal": (signal or {}).get("signal", "Live snapshot"),
            })

    trigger_map = []
    if ticks or index_snapshot:
        for label in PRIMARY_LEVEL_LABELS:
            signal = primary_signal_map.get(label)
            if not signal:
                continue
            trigger_map.append({
                "label": "Bank Nifty" if label == "Nifty Bank" else label,
                "price": signal.get("price"),
                "pct": signal.get("pct"),
                "trend": signal.get("trend"),
                "signal": signal.get("signal"),
                "support": signal.get("support"),
                "resistance": signal.get("resistance"),
                "rsi14": signal.get("rsi14"),
                "ret5": signal.get("ret5"),
            })

    trade_scenarios = []
    focus_support = (focus_signal or {}).get("support")
    focus_resistance = (focus_signal or {}).get("resistance")
    if focus_price is not None:
        bull_target = round_or_none((focus_resistance or focus_price) + ((expected_move_points or 0) * 0.7))
        bear_target = round_or_none((focus_support or focus_price) - ((expected_move_points or 0) * 0.7))
        fade_anchor = round_or_none(focus_price + ((expected_move_points or 0) * 0.35))
        trade_scenarios = [
            {
                "label": "Bull continuation",
                "tone": "bull",
                "trigger": f"Acceptance above {format_level(focus_resistance)}" if focus_resistance is not None else f"Strength above {format_level(focus_price)}",
                "target": format_level(bull_target),
                "invalidation": format_level(focus_support),
                "note": f"Works best when {focus_display} holds leadership and VIX stops expanding.",
            },
            {
                "label": "Bear continuation",
                "tone": "bear",
                "trigger": f"Break below {format_level(focus_support)}" if focus_support is not None else f"Weakness below {format_level(focus_price)}",
                "target": format_level(bear_target),
                "invalidation": format_level(focus_resistance),
                "note": "Cleaner if breadth narrows, USD/INR stays firm, or VIX keeps getting bid.",
            },
            {
                "label": "Fade / reversal",
                "tone": "neutral",
                "trigger": f"Failed move back inside {format_level(focus_support)} - {format_level(focus_resistance)}" if focus_support is not None and focus_resistance is not None else f"Failed extension around {format_level(fade_anchor)}",
                "target": format_level(focus_price),
                "invalidation": format_level(bull_target if composite_score >= 0 else bear_target),
                "note": "Most relevant when conviction is middling and the day type is rotation or high-gamma two-way.",
            },
        ]

    return {
        "generatedAt": ist_now().strftime("%H:%M:%S"),
        "overviewCards": overview_cards,
        "predictionCards": prediction_cards,
        "contextNotes": context_notes[:6],
        "riskFlags": risk_flags[:6],
        "crossAssetRows": cross_asset_rows,
        "relativeValueRows": relative_value_rows,
        "scoreBreakdown": score_breakdown,
        "tradeScenarios": trade_scenarios,
        "signalMatrix": signal_matrix,
        "triggerMap": trigger_map,
    }


# ── Background loops ───────────────────────────────────────────────────────
def broadcast_tickers(data: dict) -> None:
    payload = "data:" + json.dumps(market_data_snapshot(include_history=False)) + "\n\n"
    with _sse_lock:
        dead = []
        for q in _sse_queues:
            try:
                q.put_nowait(payload)
            except queue.Full:
                dead.append(q)
        for q in dead:
            try:
                _sse_queues.remove(q)
            except ValueError:
                pass


def _update_price_history(ticks: dict) -> None:
    with _lock:
        for label, data in ticks.items():
            price = data.get("price")
            if price is None:
                continue
            _price_history.setdefault(label, []).append(price)
            if len(_price_history[label]) > MAX_HIST:
                _price_history[label] = _price_history[label][-MAX_HIST:]


def refresh_loop() -> None:
    global _arts, _feed_status, _updated, _last_news_refresh_ts
    _nse_init_session()
    while True:
        try:
            print("[*] Refreshing news...")
            arts, fstatus = fetch_news()
            refreshed_at = time.time()
            with _lock:
                _arts = arts
                _feed_status = fstatus
                _updated = ist_now().strftime("%H:%M:%S")
                _last_news_refresh_ts = refreshed_at
            rebuild_computed_payloads()
            print(f"[+] {len(arts)} articles | {_updated}")
        except Exception as e:
            print(f"[!] refresh_loop error: {e}")
        wait_seconds = get_news_refresh_seconds()
        _refresh_wakeup.wait(timeout=wait_seconds)
        _refresh_wakeup.clear()


def ticker_loop() -> None:
    global _ticks, _index_snapshot, _last_tick_refresh_ts
    while True:
        try:
            _nse_init_session()
            ticks, analytics_indices = fetch_tickers()
            refreshed_at = time.time()
            with _lock:
                _ticks = ticks
                _index_snapshot = analytics_indices
                _last_tick_refresh_ts = refreshed_at
            _update_price_history(ticks)
            refresh_tracked_symbol_quotes()
            rebuild_computed_payloads()
            broadcast_tickers(ticks)
            print(f"[~] Tickers: {list(ticks.keys())}")
        except Exception as e:
            print(f"[!] ticker_loop error: {e}")
        time.sleep(ticker_refresh_interval())


# ── Flask ──────────────────────────────────────────────────────────────────
FRONTEND_DIR = Path(__file__).resolve().parent.parent / "frontend"

app = Flask(__name__, static_folder=str(FRONTEND_DIR), static_url_path="")


@app.route("/api/news")
def api_news():
    with _lock:
        payload = {
            "articles": list(_arts),
            "updated": _updated,
            "feedStatus": dict(_feed_status),
            "refreshInterval": _news_refresh_seconds,
            "allowedRefreshWindows": ALLOWED_REFRESH_WINDOWS,
        }
    payload["marketStatus"] = get_market_status()
    return jsonify(payload)


@app.route("/api/tickers")
def api_tickers():
    with _lock:
        return jsonify(_ticks)


@app.route("/api/snapshot")
def api_snapshot():
    include_history = request.args.get("history", "0") in {"1", "true", "yes"}
    return jsonify(market_data_snapshot(include_history=include_history))


@app.route("/api/symbols/search")
def api_symbol_search():
    query = request.args.get("q", "")
    return jsonify({"results": search_symbols(query)})


@app.route("/api/app-state", methods=["GET", "POST"])
def api_app_state():
    if request.method == "POST":
        payload = request.get_json(silent=True) or {}
        state = update_app_state(payload)
        with _lock:
            has_stored_state = _has_persisted_state
        return jsonify({"state": state, "hasStoredState": has_stored_state})

    with _lock:
        has_stored_state = _has_persisted_state
    return jsonify({
        "state": get_app_state_copy(),
        "hasStoredState": has_stored_state,
    })


@app.route("/api/quotes")
def api_quotes():
    symbols = sanitize_symbol_list(request.args.get("symbols", ""))
    status = get_market_status()
    stale_after = nse_quote_cache_ttl(status) * 2
    with _lock:
        cached_quotes = {sym: _tracked_symbol_quotes.get(sym) for sym in symbols}
    refresh_symbols = [
        sym for sym, quote in cached_quotes.items()
        if quote is None or (quote_age_seconds(quote) is not None and quote_age_seconds(quote) > stale_after)
    ]
    fresh_quotes = refresh_quote_cache_for_symbols(refresh_symbols)
    merged = {sym: fresh_quotes.get(sym) or cached_quotes.get(sym) for sym in symbols}
    out = format_quotes_for_client({sym: quote for sym, quote in merged.items() if quote}, status=status)
    return jsonify(out)


@app.route("/api/history")
def api_history():
    with _lock:
        return jsonify(_price_history)


@app.route("/api/analytics")
def api_analytics():
    with _lock:
        payload = dict(_analytics_payload)
    return jsonify(payload)


@app.route("/api/derivatives/overview")
def api_derivatives_overview():
    with _lock:
        payload = dict(_derivatives_payload)
    return jsonify(payload)


@app.route("/api/derivatives/option-chain")
def api_derivatives_option_chain():
    underlying = request.args.get("underlying", "NIFTY")
    expiry = request.args.get("expiry") or os.environ.get("UPSTOX_OPTION_EXPIRY", "")
    max_rows = int(request.args.get("maxRows", "80") or 80)
    try:
        payload = fetch_upstox_option_chain(
            underlying=underlying,
            expiry_date=expiry,
            max_rows=max(10, min(max_rows, 200)),
        )
    except ValueError as exc:
        return jsonify({"error": str(exc), "provider": "Upstox"}), 400
    except RuntimeError as exc:
        return jsonify({"error": str(exc), "provider": "Upstox"}), 400
    except requests.HTTPError as exc:
        status_code = exc.response.status_code if exc.response is not None else 502
        return jsonify({"error": "Upstox option-chain request failed", "provider": "Upstox"}), status_code
    except Exception as exc:
        return jsonify({"error": str(exc), "provider": "Upstox"}), 502
    return jsonify(payload)


@app.route("/api/settings/refresh", methods=["GET", "POST"])
def api_settings_refresh():
    if request.method == "POST":
        data = request.get_json(silent=True) or {}
        seconds = int(data.get("seconds", 0) or 0)
        try:
            current = set_news_refresh_seconds(seconds)
        except ValueError:
            return jsonify({"error": "Unsupported refresh interval", "allowed": ALLOWED_REFRESH_WINDOWS}), 400
        return jsonify({"refreshInterval": current, "allowedRefreshWindows": ALLOWED_REFRESH_WINDOWS})

    return jsonify({
        "refreshInterval": get_news_refresh_seconds(),
        "allowedRefreshWindows": ALLOWED_REFRESH_WINDOWS,
    })


@app.route("/api/health")
def api_health():
    market_status = get_market_status()
    with _lock:
        news_count = len(_arts)
        ticker_count = len(_ticks)
        analytics_ready = bool(_analytics_payload.get("generatedAt"))
        derivatives_ready = bool(_derivatives_payload.get("generatedAt"))
    status = "ok" if not market_status["staleData"] else "degraded"
    return jsonify({
        "status": status,
        "dataProvider": market_data_provider_status(),
        "marketStatus": market_status,
        "newsCount": news_count,
        "tickerCount": ticker_count,
        "analyticsReady": analytics_ready,
        "derivativesReady": derivatives_ready,
        "refreshInterval": get_news_refresh_seconds(),
    })


@app.route("/api/tickers/stream")
def api_tickers_stream():
    q: queue.Queue[str] = queue.Queue(maxsize=20)
    with _sse_lock:
        _sse_queues.append(q)
    initial = market_data_snapshot(include_history=True)

    def generate():
        try:
            yield "data:" + json.dumps(initial) + "\n\n"
            while True:
                try:
                    msg = q.get(timeout=30)
                    yield msg
                except queue.Empty:
                    yield ": keepalive\n\n"
        finally:
            with _sse_lock:
                try:
                    _sse_queues.remove(q)
                except ValueError:
                    pass

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.route("/")
def index():
    return app.send_static_file("index.html")


# ── Frontend entrypoint ────────────────────────────────────────────────────
initialize_runtime_state()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "9090"))
    t1 = threading.Thread(target=refresh_loop, daemon=True)
    t1.start()
    t2 = threading.Thread(target=ticker_loop, daemon=True)
    t2.start()

    print("=" * 60)
    print("  India Market Desk")
    print(f"  http://127.0.0.1:{port}")
    print()
    provider = market_data_provider_status()
    print(f"  Live {provider['active'].upper()} prices via SSE ({INTRADAY_TICK_INTERVAL_SECONDS}s intraday, {AFTER_HOURS_TICK_INTERVAL_SECONDS}s after-hours)")
    if provider["requested"] == UPSTOX_PROVIDER_NAME and provider["active"] != UPSTOX_PROVIDER_NAME:
        print("  Upstox requested but UPSTOX_ACCESS_TOKEN is missing; using NSE fallback")
    print("  Ctrl+C to stop")
    print("=" * 60)

    # Do not auto-open a browser here; macOS may block it with Permission denied.
    app.run(host="127.0.0.1", port=port, debug=False, use_reloader=False, threaded=True)
