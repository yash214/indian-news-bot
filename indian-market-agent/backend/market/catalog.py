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

INDEX_SYMBOL_ALIASES = {
    "NIFTY": {"name": "Nifty 50", "sector": "Index", "instrumentKey": "NSE_INDEX|Nifty 50"},
    "NIFTY50": {"name": "Nifty 50", "sector": "Index", "instrumentKey": "NSE_INDEX|Nifty 50"},
    "NIFTYBANK": {"name": "Nifty Bank", "sector": "Index", "instrumentKey": "NSE_INDEX|Nifty Bank"},
    "BANKNIFTY": {"name": "Nifty Bank", "sector": "Index", "instrumentKey": "NSE_INDEX|Nifty Bank"},
    "FINNIFTY": {"name": "Nifty Financial Services", "sector": "Index", "instrumentKey": "NSE_INDEX|Nifty Fin Service"},
    "MIDCPNIFTY": {"name": "Nifty Midcap Select", "sector": "Index", "instrumentKey": "NSE_INDEX|Nifty Midcap Select"},
    "NIFTYIT": {"name": "Nifty IT", "sector": "Index", "instrumentKey": "NSE_INDEX|Nifty IT"},
    "NIFTYMIDCAP100": {"name": "Nifty Midcap 100", "sector": "Index", "instrumentKey": "NSE_INDEX|Nifty Midcap 100"},
    "NIFTYSMLCAP100": {"name": "Nifty Smallcap 100", "sector": "Index", "instrumentKey": "NSE_INDEX|Nifty Smallcap 100"},
    "INDIAVIX": {"name": "India VIX", "sector": "Index", "instrumentKey": "NSE_INDEX|India VIX"},
    "NIFTYAUTO": {"name": "Nifty Auto", "sector": "Index", "instrumentKey": "NSE_INDEX|Nifty Auto"},
    "NIFTYFMCG": {"name": "Nifty FMCG", "sector": "Index", "instrumentKey": "NSE_INDEX|Nifty FMCG"},
    "NIFTYPHARMA": {"name": "Nifty Pharma", "sector": "Index", "instrumentKey": "NSE_INDEX|Nifty Pharma"},
    "NIFTYMETAL": {"name": "Nifty Metal", "sector": "Index", "instrumentKey": "NSE_INDEX|Nifty Metal"},
    "NIFTYREALTY": {"name": "Nifty Realty", "sector": "Index", "instrumentKey": "NSE_INDEX|Nifty Realty"},
    "NIFTYPSUBANK": {"name": "Nifty PSU Bank", "sector": "Index", "instrumentKey": "NSE_INDEX|Nifty PSU Bank"},
    "NIFTYOILGAS": {"name": "Nifty Oil & Gas", "sector": "Index", "instrumentKey": "NSE_INDEX|Nifty Oil & Gas"},
}

STOOQ_GLOBAL_SYMBOLS = {
    "^SPX": {"name": "S&P 500", "sector": "Global Index", "stooqSymbol": "^SPX", "sym": ""},
    "^NDQ": {"name": "Nasdaq 100", "sector": "Global Index", "stooqSymbol": "^NDQ", "sym": ""},
    "^DJI": {"name": "Dow Jones Industrial Average", "sector": "Global Index", "stooqSymbol": "^DJI", "sym": ""},
    "^RUT": {"name": "Russell 2000", "sector": "Global Index", "stooqSymbol": "^RUT", "sym": ""},
    "^DAX": {"name": "DAX", "sector": "Global Index", "stooqSymbol": "^DAX", "sym": ""},
    "^FTSE": {"name": "FTSE 100", "sector": "Global Index", "stooqSymbol": "^FTSE", "sym": ""},
    "^NKX": {"name": "Nikkei 225", "sector": "Global Index", "stooqSymbol": "^NKX", "sym": ""},
    "^HSI": {"name": "Hang Seng Index", "sector": "Global Index", "stooqSymbol": "^HSI", "sym": ""},
    "AAPL.US": {"name": "Apple", "sector": "Global Stock", "stooqSymbol": "AAPL.US", "sym": "$"},
    "MSFT.US": {"name": "Microsoft", "sector": "Global Stock", "stooqSymbol": "MSFT.US", "sym": "$"},
    "NVDA.US": {"name": "Nvidia", "sector": "Global Stock", "stooqSymbol": "NVDA.US", "sym": "$"},
    "AMZN.US": {"name": "Amazon", "sector": "Global Stock", "stooqSymbol": "AMZN.US", "sym": "$"},
    "GOOGL.US": {"name": "Alphabet", "sector": "Global Stock", "stooqSymbol": "GOOGL.US", "sym": "$"},
    "META.US": {"name": "Meta Platforms", "sector": "Global Stock", "stooqSymbol": "META.US", "sym": "$"},
    "TSLA.US": {"name": "Tesla", "sector": "Global Stock", "stooqSymbol": "TSLA.US", "sym": "$"},
    "JPM.US": {"name": "JPMorgan Chase", "sector": "Global Stock", "stooqSymbol": "JPM.US", "sym": "$"},
    "XOM.US": {"name": "Exxon Mobil", "sector": "Global Stock", "stooqSymbol": "XOM.US", "sym": "$"},
    "LLY.US": {"name": "Eli Lilly", "sector": "Global Stock", "stooqSymbol": "LLY.US", "sym": "$"},
    "AVGO.US": {"name": "Broadcom", "sector": "Global Stock", "stooqSymbol": "AVGO.US", "sym": "$"},
    "BRK.B.US": {"name": "Berkshire Hathaway B", "sector": "Global Stock", "stooqSymbol": "BRK.B.US", "sym": "$"},
    "USDINR": {"name": "USD/INR", "sector": "Currency", "stooqSymbol": "USDINR", "sym": ""},
    "CL.F": {"name": "WTI Crude Oil Futures", "sector": "Commodity", "stooqSymbol": "CL.F", "sym": "$"},
    "CB.F": {"name": "Brent Crude Oil Futures", "sector": "Commodity", "stooqSymbol": "CB.F", "sym": "$"},
    "GC.F": {"name": "Gold Futures", "sector": "Commodity", "stooqSymbol": "GC.F", "sym": "$"},
}

STOOQ_SYMBOL_ALIASES = {
    "SPX": "^SPX",
    "S&P500": "^SPX",
    "S&P": "^SPX",
    "NASDAQ": "^NDQ",
    "NDX": "^NDQ",
    "NAS100": "^NDQ",
    "DOW": "^DJI",
    "DJIA": "^DJI",
    "RUSSELL2000": "^RUT",
    "RUT": "^RUT",
    "DAX": "^DAX",
    "FTSE": "^FTSE",
    "NIKKEI": "^NKX",
    "NIKKEI225": "^NKX",
    "HANGSENG": "^HSI",
    "HSI": "^HSI",
    "AAPL": "AAPL.US",
    "MSFT": "MSFT.US",
    "NVDA": "NVDA.US",
    "AMZN": "AMZN.US",
    "GOOGL": "GOOGL.US",
    "GOOG": "GOOGL.US",
    "META": "META.US",
    "TSLA": "TSLA.US",
    "JPM": "JPM.US",
    "XOM": "XOM.US",
    "LLY": "LLY.US",
    "AVGO": "AVGO.US",
    "BRK.B": "BRK.B.US",
    "BRKB": "BRK.B.US",
    "USD/INR": "USDINR",
    "USDINR": "USDINR",
    "CRUDE": "CL.F",
    "CRUDEOIL": "CL.F",
    "WTI": "CL.F",
    "BRENT": "CB.F",
    "BRENTCRUDE": "CB.F",
    "BENT": "CB.F",
    "BENTCRUDE": "CB.F",
    "GOLD": "GC.F",
    "XAU": "GC.F",
}

STOOQ_CROSS_ASSETS = {
    "Gold": "GC.F",
    "USD/INR": "USDINR",
    "Crude Oil": "CL.F",
    "Brent Crude": "CB.F",
}


def _dedupe_symbol_items(items: list[dict]) -> list[dict]:
    seen, out = set(), []
    for item in items:
        symbol = item.get("symbol")
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        out.append(item)
    return out


SYMBOL_SUGGESTIONS = _dedupe_symbol_items([
    *[
        {"symbol": symbol, "name": meta["name"], "sector": meta["sector"], "instrumentKey": meta["instrumentKey"]}
        for symbol, meta in INDEX_SYMBOL_ALIASES.items()
    ],
    *[
        {"symbol": symbol, "name": meta["name"], "sector": meta["sector"], "stooqSymbol": meta["stooqSymbol"]}
        for symbol, meta in STOOQ_GLOBAL_SYMBOLS.items()
    ],
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
    {"symbol": "ADANIGREEN", "name": "Adani Green Energy", "sector": "Energy"},
    {"symbol": "ADANIPOWER", "name": "Adani Power", "sector": "Energy"},
    {"symbol": "AMBUJACEM", "name": "Ambuja Cements", "sector": "Infra"},
    {"symbol": "APOLLOHOSP", "name": "Apollo Hospitals", "sector": "Healthcare"},
    {"symbol": "BAJAJHLDNG", "name": "Bajaj Holdings", "sector": "Financials"},
    {"symbol": "BEL", "name": "Bharat Electronics", "sector": "Defence"},
    {"symbol": "BHEL", "name": "Bharat Heavy Electricals", "sector": "Infra"},
    {"symbol": "BOSCHLTD", "name": "Bosch", "sector": "Auto"},
    {"symbol": "CANBK", "name": "Canara Bank", "sector": "Banking"},
    {"symbol": "CHOLAFIN", "name": "Cholamandalam Investment", "sector": "Financials"},
    {"symbol": "DLF", "name": "DLF", "sector": "Realty"},
    {"symbol": "DMART", "name": "Avenue Supermarts", "sector": "Retail"},
    {"symbol": "GAIL", "name": "GAIL India", "sector": "Energy"},
    {"symbol": "GODREJCP", "name": "Godrej Consumer Products", "sector": "FMCG"},
    {"symbol": "GRASIM", "name": "Grasim Industries", "sector": "Infra"},
    {"symbol": "HAL", "name": "Hindustan Aeronautics", "sector": "Defence"},
    {"symbol": "HDFCLIFE", "name": "HDFC Life Insurance", "sector": "Financials"},
    {"symbol": "HINDPETRO", "name": "Hindustan Petroleum", "sector": "Energy"},
    {"symbol": "ICICIGI", "name": "ICICI Lombard General Insurance", "sector": "Financials"},
    {"symbol": "ICICIPRULI", "name": "ICICI Prudential Life Insurance", "sector": "Financials"},
    {"symbol": "INDIGO", "name": "InterGlobe Aviation", "sector": "Aviation"},
    {"symbol": "IOC", "name": "Indian Oil Corporation", "sector": "Energy"},
    {"symbol": "IRCTC", "name": "IRCTC", "sector": "Railways"},
    {"symbol": "IRFC", "name": "Indian Railway Finance Corporation", "sector": "Financials"},
    {"symbol": "JSWENERGY", "name": "JSW Energy", "sector": "Energy"},
    {"symbol": "LODHA", "name": "Macrotech Developers", "sector": "Realty"},
    {"symbol": "MOTHERSON", "name": "Samvardhana Motherson", "sector": "Auto"},
    {"symbol": "NAUKRI", "name": "Info Edge India", "sector": "Internet"},
    {"symbol": "NHPC", "name": "NHPC", "sector": "Energy"},
    {"symbol": "PIDILITIND", "name": "Pidilite Industries", "sector": "General"},
    {"symbol": "POLYCAB", "name": "Polycab India", "sector": "Infra"},
    {"symbol": "RECLTD", "name": "REC", "sector": "Financials"},
    {"symbol": "SBICARD", "name": "SBI Cards", "sector": "Financials"},
    {"symbol": "SBILIFE", "name": "SBI Life Insurance", "sector": "Financials"},
    {"symbol": "SHREECEM", "name": "Shree Cement", "sector": "Infra"},
    {"symbol": "SHRIRAMFIN", "name": "Shriram Finance", "sector": "Financials"},
    {"symbol": "SOLARINDS", "name": "Solar Industries India", "sector": "Defence"},
    {"symbol": "TATACONSUM", "name": "Tata Consumer Products", "sector": "FMCG"},
    {"symbol": "TATAPOWER", "name": "Tata Power", "sector": "Energy"},
    {"symbol": "TORNTPOWER", "name": "Torrent Power", "sector": "Energy"},
    {"symbol": "TRENT", "name": "Trent", "sector": "Retail"},
    {"symbol": "UNIONBANK", "name": "Union Bank of India", "sector": "Banking"},
    {"symbol": "UNITDSPR", "name": "United Spirits", "sector": "FMCG"},
    {"symbol": "VBL", "name": "Varun Beverages", "sector": "FMCG"},
    {"symbol": "ZYDUSLIFE", "name": "Zydus Lifesciences", "sector": "Pharma"},
])

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
    **{symbol: meta["instrumentKey"] for symbol, meta in INDEX_SYMBOL_ALIASES.items()},
}

UPSTOX_INDEX_INSTRUMENT_KEYS = {
    "Nifty 50": "NSE_INDEX|Nifty 50",
    "Nifty Bank": "NSE_INDEX|Nifty Bank",
    "Nifty IT": "NSE_INDEX|Nifty IT",
    "Nifty Midcap": "NSE_INDEX|Nifty Midcap 100",
    "Nifty Smallcap": "NSE_INDEX|Nifty Smallcap 100",
    "Nifty Auto": "NSE_INDEX|Nifty Auto",
    "Nifty FMCG": "NSE_INDEX|Nifty FMCG",
    "Nifty Pharma": "NSE_INDEX|Nifty Pharma",
    "Nifty Metal": "NSE_INDEX|Nifty Metal",
    "Nifty Realty": "NSE_INDEX|Nifty Realty",
    "Nifty PSU Bank": "NSE_INDEX|Nifty PSU Bank",
    "Nifty Financial": "NSE_INDEX|Nifty Fin Service",
    "Nifty Oil & Gas": "NSE_INDEX|Nifty Oil & Gas",
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
        sym = re.sub(r"[^A-Z0-9&.^-]", "", piece.upper().strip())
        if sym and sym not in seen:
            seen.add(sym)
            out.append(sym)
        if len(out) >= WATCHLIST_SYMBOL_LIMIT:
            break
    return out


def symbol_directory_entry(symbol: str) -> dict | None:
    clean = re.sub(r"[^A-Z0-9&.^-]", "", (symbol or "").upper())
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
