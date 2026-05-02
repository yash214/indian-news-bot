"""Shared enum strings for platform-wide payloads.

Modules can migrate to these constants gradually; this file is intentionally
non-invasive so the refactor does not alter runtime behavior.
"""

SENTIMENT_BULLISH = "bullish"
SENTIMENT_BEARISH = "bearish"
SENTIMENT_NEUTRAL = "neutral"

INDEX_NIFTY = "NIFTY"
INDEX_BANKNIFTY = "BANKNIFTY"

TRADE_FILTER_NO_FILTER = "NO_FILTER"
TRADE_FILTER_REDUCE_LONG_CONFIDENCE = "REDUCE_LONG_CONFIDENCE"
TRADE_FILTER_REDUCE_SHORT_CONFIDENCE = "REDUCE_SHORT_CONFIDENCE"
TRADE_FILTER_EVENT_RISK_WAIT = "EVENT_RISK_WAIT"
TRADE_FILTER_BLOCK_FRESH_TRADES = "BLOCK_FRESH_TRADES"

