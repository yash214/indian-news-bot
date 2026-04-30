"""Static market symbols, index mappings, and provider catalog data."""

from __future__ import annotations

import re

try:
    from backend.core.settings import WATCHLIST_SYMBOL_LIMIT
except ModuleNotFoundError:
    from core.settings import WATCHLIST_SYMBOL_LIMIT


NSE_INDICES_WANTED = {
    "NIFTY 50": "Nifty 50",
    "NIFTY IT": "Nifty IT",
    "NIFTY BANK": "Nifty Bank",
    "NIFTY MIDCAP 100": "Nifty Midcap",
    "NIFTY SMLCAP 100": "Nifty Smallcap",
    "INDIA VIX": "VIX",
}

NSE_STOCKS = {
    "Infosys": "INFY",
    "HCL Tech": "HCLTECH",
    "Wipro": "WIPRO",
    "TCS": "TCS",
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
    "Gold": ("GC=F", "$"),
    "USD/INR": ("USDINR=X", ""),
    "Crude Oil": ("CL=F", "$"),
    "Brent Crude": ("BZ=F", "$"),
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

UPSTOX_DEFAULT_INSTRUMENT_KEYS = {
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
