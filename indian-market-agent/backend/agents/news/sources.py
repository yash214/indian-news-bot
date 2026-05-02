"""News feed source definitions for India Market Desk."""

from __future__ import annotations

from urllib.parse import quote_plus


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
