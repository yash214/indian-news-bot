import importlib.util
import tempfile
import unittest
from unittest import mock
from pathlib import Path
import struct


PROJECT_ROOT = Path(__file__).resolve().parents[1]
APP_PATH = PROJECT_ROOT / "backend" / "app.py"


spec = importlib.util.spec_from_file_location("market_desk_app", APP_PATH)
app = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(app)

from backend.agents.news.url_resolver import resolve_google_news_url


def pb_varint(value: int) -> bytes:
    out = bytearray()
    number = int(value)
    while True:
        to_write = number & 0x7F
        number >>= 7
        if number:
            out.append(to_write | 0x80)
        else:
            out.append(to_write)
            return bytes(out)


def pb_key(field_number: int, wire_type: int) -> bytes:
    return pb_varint((field_number << 3) | wire_type)


def pb_len(field_number: int, payload: bytes) -> bytes:
    return pb_key(field_number, 2) + pb_varint(len(payload)) + payload


def pb_double(field_number: int, value: float) -> bytes:
    return pb_key(field_number, 1) + struct.pack("<d", value)


def pb_int(field_number: int, value: int) -> bytes:
    return pb_key(field_number, 0) + pb_varint(value)


class MarketStatusTests(unittest.TestCase):
    def test_weekend_is_closed(self):
        now = app.datetime(2026, 4, 25, 10, 0, tzinfo=app.IST)
        status = app.get_market_status(now)
        self.assertFalse(status["isTradingDay"])
        self.assertFalse(status["isMarketOpen"])
        self.assertEqual(status["session"], "weekend")

    def test_holiday_is_closed(self):
        now = app.datetime(2026, 10, 2, 10, 0, tzinfo=app.IST)
        status = app.get_market_status(now)
        self.assertFalse(status["isTradingDay"])
        self.assertFalse(status["isMarketOpen"])
        self.assertEqual(status["session"], "holiday")
        self.assertIn("Mahatma Gandhi Jayanti", status["reason"])

    def test_regular_session_is_open(self):
        now = app.datetime(2026, 4, 27, 10, 0, tzinfo=app.IST)
        status = app.get_market_status(now)
        self.assertTrue(status["isTradingDay"])
        self.assertTrue(status["isMarketOpen"])
        self.assertEqual(status["session"], "open")

    def test_live_quote_cache_ttl_is_shorter_during_market_hours(self):
        self.assertEqual(
            app.nse_quote_cache_ttl({"session": "open", "isMarketOpen": True}),
            app.LIVE_NSE_QUOTE_CACHE_TTL,
        )
        self.assertEqual(
            app.nse_quote_cache_ttl({"session": "postclose", "isMarketOpen": False}),
            app.CLOSED_NSE_QUOTE_CACHE_TTL,
        )


class NewsScoringTests(unittest.TestCase):
    def test_macro_policy_news_scores_high_even_when_direction_is_mixed(self):
        now = app.datetime(2026, 4, 27, 10, 0, tzinfo=app.IST)
        title = "RBI keeps repo rate unchanged in policy decision"
        body = "Governor says inflation remains a key risk for Indian markets."
        sent = app.sentiment(title, body)
        score, meta = app.impact_details(title, body, sent, source="Reuters", published_dt=now, now=now)

        self.assertGreaterEqual(score, 7)
        self.assertIn("rbi", meta["matchedKeywords"])
        self.assertIn("repo rate", meta["matchedKeywords"])

    def test_generic_live_update_headline_stays_low_impact(self):
        now = app.datetime(2026, 4, 27, 10, 0, tzinfo=app.IST)
        title = "Stocks to watch today: market live updates ahead of opening bell"
        body = "A quick list of names in focus for traders."
        sent = app.sentiment(title, body)
        score, meta = app.impact_details(title, body, sent, source="Reuters", published_dt=now, now=now)

        self.assertLessEqual(score, 2)
        self.assertIn("stocks to watch", meta["lowSignalKeywords"])

    def test_keyword_matching_avoids_substring_false_positives(self):
        score, meta = app.impact_details(
            "Soil testing demand improves for farm services",
            "Analysts discuss agriculture services without any oil market event.",
            {"label": "neutral", "score": 0.0},
        )

        self.assertNotIn("oil", meta["matchedKeywords"])
        self.assertLessEqual(score, 2)

    def test_results_and_earnings_news_scores_as_high_impact(self):
        now = app.datetime(2026, 4, 27, 10, 0, tzinfo=app.IST)
        title = "Infosys Q4 earnings beat estimates; dividend announced"
        body = "Revenue growth is strong and margin expansion boosts profit."
        sent = app.sentiment(title, body)
        score = app.impact(title, body, sent, source="Moneycontrol", published_dt=now)

        self.assertGreaterEqual(score, 8)

    def test_article_preview_builds_cleaner_multi_sentence_summary(self):
        preview = app.build_article_preview(
            "Reliance raises capex for telecom expansion",
            (
                "Reliance raises capex for telecom expansion. "
                "The company plans to accelerate network rollout across key circles. "
                "Management said the spending will support subscriber growth and data usage. "
                "Brokerages expect the move to pressure near-term cash flow but improve long-term positioning. "
                "Analysts will watch execution timelines and tariff support."
            ),
            "Reuters",
        )

        self.assertNotIn("Reliance raises capex for telecom expansion. Reliance raises capex", preview)
        self.assertIn("accelerate network rollout", preview)
        self.assertLessEqual(len(preview), 680)

    def test_summary_enrichment_detects_headline_like_snippets(self):
        self.assertTrue(
            app.summary_needs_ai(
                "TARIL shares jump 10% on order win",
                "TARIL shares jump 10% on order win Business Standard",
            )
        )

    def test_ai_summary_prompt_includes_trading_context(self):
        prompt = app.build_news_summary_prompt(
            {
                "title": "TARIL shares jump 10% on order win",
                "summary": "The company received a large project order and traders are watching for follow-through.",
                "articleText": "Full article body says the company received a large transformer order and expects execution over the next two quarters.",
                "articleTextSource": "article-page",
                "source": "Business Standard",
                "sector": "General",
                "sentiment": {"label": "bullish"},
                "impact": 4,
            }
        )

        self.assertIn("Write one dense, useful plain-text market brief in 5-6 concise sentences", prompt)
        self.assertIn("Business Standard", prompt)
        self.assertIn("TARIL shares jump 10% on order win", prompt)
        self.assertIn("article-page", prompt)
        self.assertIn("large transformer order", prompt)

    def test_ai_article_analysis_normalizer_sanitizes_market_tags(self):
        normalized = app.normalize_article_analysis(
            {
                "summary": "The company won a large order that may support revenue visibility. Traders should still watch execution timelines and broader index participation before assuming follow-through.",
                "sentiment": "BULLISH",
                "impactScore": 7.7,
                "confidence": 0.82,
                "sector": "Infra",
                "indexImpact": {
                    "nifty": "neutral",
                    "bankNifty": "limited",
                    "sectorIndex": "bullish",
                    "timeframe": "intraday",
                },
                "reasons": ["Order win improves visibility", "Sector read-through is stronger than index impact"],
            },
            fallback_article={"sector": "General", "impact": 3, "sentiment": {"label": "neutral"}},
        )

        self.assertEqual(normalized["sentiment"], "bullish")
        self.assertEqual(normalized["impactScore"], 8)
        self.assertEqual(normalized["confidence"], 0.82)
        self.assertEqual(normalized["sector"], "Infra")
        self.assertEqual(normalized["indexImpact"]["sectorIndex"], "bullish")

    def test_extract_article_text_reads_accessible_page_body(self):
        html = """
        <html><head><title>Story</title></head><body>
        <nav>Subscribe to our newsletter</nav>
        <article>
          <h1>TARIL shares jump 10% on order win</h1>
          <p>TARIL shares rallied after the company announced a major transformer order worth Rs 150 crore from a domestic customer.</p>
          <p>The order is expected to support revenue visibility over the next two quarters, according to the company statement.</p>
          <p>Traders are watching whether volume sustains after the initial gap-up move and whether management gives more detail on margins.</p>
        </article>
        </body></html>
        """
        text = app.extract_article_text(html, title="TARIL shares jump 10% on order win", max_chars=1000)
        self.assertIn("Rs 150 crore", text)
        self.assertIn("revenue visibility", text)
        self.assertNotIn("Subscribe to our newsletter", text)

    def test_article_text_usefulness_requires_more_than_feed_snippet(self):
        feed = "Short market update."
        article_text = " ".join(["Detailed article sentence about markets and earnings."] * 25)
        self.assertTrue(app.article_text_is_useful(article_text, feed_text=feed, min_chars=300))
        self.assertFalse(app.article_text_is_useful("Too short.", feed_text=feed, min_chars=300))

    def test_article_extraction_skips_google_news_wrappers(self):
        self.assertFalse(
            app.article_link_supports_direct_extraction(
                "https://news.google.com/rss/articles/example?oc=5"
            )
        )
        self.assertTrue(
            app.article_link_supports_direct_extraction(
                "https://www.livemint.com/market/stock-market-news/example.html"
            )
        )

    def test_google_news_resolver_decodes_publisher_url(self):
        publisher_url = "https://publisher.example.com/markets/story.html"
        batch_payload = (
            ")]}'\n\n"
            + app.json.dumps(
                [
                    [
                        "wrb.fr",
                        "Fbv4je",
                        app.json.dumps(["garturlres", publisher_url, 1, publisher_url + "/amp/"]),
                        None,
                        None,
                        None,
                        "generic",
                    ]
                ]
            )
        )

        class FakeResponse:
            def __init__(self, text):
                self.text = text

            def raise_for_status(self):
                return None

        class FakeSession:
            def __init__(self):
                self.post_payload = None

            def get(self, url, **kwargs):
                return FakeResponse('<div data-n-a-id="abc123" data-n-a-ts="1777400764" data-n-a-sg="sig"></div>')

            def post(self, url, **kwargs):
                self.post_payload = kwargs.get("data") or {}
                return FakeResponse(batch_payload)

        session = FakeSession()
        resolved = resolve_google_news_url("https://news.google.com/rss/articles/abc123?oc=5", session)

        self.assertEqual(resolved, publisher_url)
        self.assertIn("garturlreq", session.post_payload["f.req"])

    def test_ai_summary_normalizer_limits_to_five_sentences(self):
        normalized = app.normalize_ai_summary("Line 1\nLine 2\nLine 3\nLine 4\nLine 5")
        self.assertEqual(normalized, "Line 1 Line 2 Line 3 Line 4 Line 5")

    def test_ai_summary_normalizer_keeps_brief_paragraph(self):
        normalized = app.normalize_ai_summary(
            "Line 1. Line 2. Line 3. Line 4. Line 5. Line 6."
        )
        self.assertEqual(normalized, "Line 1. Line 2. Line 3. Line 4. Line 5. Line 6.")

    def test_extract_ollama_response_text_reads_response_field(self):
        text = app.extract_ollama_response_text({"response": "Four line summary"})
        self.assertEqual(text, "Four line summary")


class UpstoxProviderTests(unittest.TestCase):
    def setUp(self):
        app._stooq_quote_cache.clear()
        app._stooq_status.update({
            "lastError": None,
            "lastErrorAt": None,
            "lastOkAt": None,
            "lastLatencyMs": None,
            "failedSymbols": [],
            "blockedUntil": None,
        })
        app._yahoo_cache.clear()
        app._yahoo_status.update({
            "lastError": None,
            "lastErrorAt": None,
            "lastOkAt": None,
            "failedSymbols": [],
            "blockedUntil": None,
        })

    def test_decode_feed_response_parses_live_feed_ltpc_message(self):
        ltpc_payload = (
            pb_double(1, 1500.5)
            + pb_int(2, 1_745_729_552_723)
            + pb_int(3, 25)
            + pb_double(4, 1490.0)
        )
        feed_payload = pb_len(1, ltpc_payload) + pb_int(4, 0)
        feed_entry = pb_len(1, b"NSE_EQ|INE009A01021") + pb_len(2, feed_payload)
        response_payload = pb_int(1, 1) + pb_len(2, feed_entry) + pb_int(3, 1_745_729_566_039)

        decoded = app.decode_feed_response(response_payload)

        self.assertEqual(decoded["type"], "live_feed")
        self.assertEqual(decoded["currentTs"], 1_745_729_566_039)
        self.assertEqual(decoded["feeds"]["NSE_EQ|INE009A01021"]["ltpc"]["ltp"], 1500.5)
        self.assertEqual(decoded["feeds"]["NSE_EQ|INE009A01021"]["ltpc"]["cp"], 1490.0)

    def test_upstox_uses_analytics_token_only(self):
        with mock.patch.dict(app.os.environ, {"UPSTOX_ANALYTICS_TOKEN": "analytics-token"}, clear=False):
            self.assertEqual(app.upstox_analytics_token(), "analytics-token")
            self.assertTrue(app.upstox_configured())
            self.assertEqual(app.upstox_token_source(), "analytics_env")

    def test_upstox_headers_match_working_curl_shape(self):
        with mock.patch.dict(app.os.environ, {"UPSTOX_ANALYTICS_TOKEN": "analytics-token"}, clear=False):
            headers = app.upstox_headers()

        self.assertEqual(headers["Accept"], "application/json")
        self.assertEqual(headers["Authorization"], "Bearer analytics-token")
        self.assertEqual(headers["User-Agent"], "curl/8.7.1")
        self.assertNotIn("Content-Type", headers)

    def test_symbol_search_includes_major_index_aliases(self):
        results = app.search_symbols("nifty bank", limit=5)
        symbols = {item["symbol"] for item in results}

        self.assertIn("NIFTYBANK", symbols)

    def test_symbol_search_includes_stooq_global_symbols(self):
        results = app.search_symbols("aapl", limit=5)
        symbols = {item["symbol"] for item in results}

        self.assertIn("AAPL.US", symbols)

    def test_brent_crude_aliases_use_stooq_cb_contract(self):
        self.assertEqual(app.STOOQ_CROSS_ASSETS["Brent Crude"], "CB.F")
        self.assertEqual(app.stooq_symbol_meta("BRENT")[0], "CB.F")
        self.assertEqual(app.stooq_symbol_meta("BENT")[0], "CB.F")

    def test_stooq_csv_quote_parser(self):
        csv_text = (
            "Symbol,Date,Time,Open,High,Low,Close,Volume,Change,%Change\n"
            "AAPL.US,2026-04-30,19:19:21,270.425,273.76,268.14,272.36,13968659,3.36,1.25\n"
        )

        quote = app.stooq_quote_from_csv("AAPL.US", "AAPL.US", csv_text, name="Apple", currency_symbol="$", received_at=1000)

        self.assertEqual(quote["price"], 272.36)
        self.assertEqual(quote["change"], 3.36)
        self.assertEqual(quote["pct"], 1.25)
        self.assertEqual(quote["previous_close"], 269.0)
        self.assertEqual(quote["source"], "Stooq")
        self.assertEqual(quote["sym"], "$")
        self.assertEqual(quote["fetchedAt"], 1000)

    def test_stooq_quote_url_requests_change_fields(self):
        url = app.stooq_quote_url("CB.F")

        self.assertIn("c1p2", url)

    def test_stooq_html_quote_parser_matches_visible_page_values(self):
        html_text = """
        <span id=aq_cb.f_c2|3>111.61</span>
        <span id=aq_cb.f_d2>2026-05-01</span>
        <span id=aq_cb.f_t1>10:54:09</span>
        <span id=aq_cb.f_m2>-2.40</span>
        <span id=aq_cb.f_m3>(-2.11%)</span>
        <span id=aq_cb.f_h>112.43</span>
        <span id=aq_cb.f_l>110.35</span>
        <span id=aq_cb.f_o>111.47</span>
        <span id=aq_cb.f_p>114.01</span>
        """

        quote = app.stooq_quote_from_html("Brent Crude", "CB.F", html_text, name="Brent", currency_symbol="$", received_at=1000)

        self.assertEqual(quote["price"], 111.61)
        self.assertEqual(quote["change"], -2.4)
        self.assertEqual(quote["pct"], -2.11)
        self.assertEqual(quote["previous_close"], 114.01)
        self.assertEqual(quote["day_high"], 112.43)
        self.assertEqual(quote["day_low"], 110.35)
        self.assertEqual(quote["providerTimestamp"], "2026-05-01 10:54:09")
        self.assertEqual(quote["source"], "Stooq")
        self.assertEqual(quote["sourceDetail"], "Stooq page")

    def test_stooq_html_quote_parser_handles_compact_header_values(self):
        html_text = """
        CRUDE OIL WTI (CL.F)
        1 May, 10:56 105.52 +0.45 (+0.43%)
        """

        quote = app.stooq_quote_from_html("Crude Oil", "CL.F", html_text, name="WTI", currency_symbol="$", received_at=1000)

        self.assertEqual(quote["price"], 105.52)
        self.assertEqual(quote["change"], 0.45)
        self.assertEqual(quote["pct"], 0.43)
        self.assertEqual(quote["previous_close"], 105.07)
        self.assertEqual(quote["providerTimestamp"], "1 May 10:56")
        self.assertEqual(quote["source"], "Stooq")

    def test_stooq_html_quote_parser_handles_global_stock_header_values(self):
        html_text = "APPLE (AAPL.US) 30 Apr, 22:00 271.350 +1.180 (+0.44%)"

        quote = app.stooq_quote_from_html("AAPL.US", "AAPL.US", html_text, name="Apple", currency_symbol="$", received_at=1000)

        self.assertEqual(quote["price"], 271.35)
        self.assertEqual(quote["change"], 1.18)
        self.assertEqual(quote["pct"], 0.44)
        self.assertEqual(quote["previous_close"], 270.17)

    def test_cross_asset_quotes_request_brent_from_stooq(self):
        expected = {
            label: {"price": 100.0, "source": "Stooq", "stooqSymbol": symbol}
            for label, symbol in app.STOOQ_CROSS_ASSETS.items()
        }
        with mock.patch.object(app, "fetch_stooq_quotes_by_label", return_value=expected) as stooq_mock:
            with mock.patch.object(app, "_yahoo_price", side_effect=AssertionError("Yahoo should only be fallback")):
                quotes = app.fetch_cross_asset_quotes()

        self.assertEqual(quotes["Brent Crude"]["stooqSymbol"], "CB.F")
        self.assertEqual(quotes["Crude Oil"]["source"], "Stooq")
        self.assertEqual(quotes["USD/INR"]["source"], "Stooq")
        stooq_mock.assert_called_once_with(dict(app.STOOQ_CROSS_ASSETS), prefer_page=True)

    def test_cross_asset_quotes_use_yahoo_only_for_missing_stooq_quotes(self):
        stooq_quotes = {
            "Gold": {"price": 100.0, "source": "Stooq", "stooqSymbol": "GC.F"},
            "Brent Crude": {"price": 111.2, "source": "Stooq", "stooqSymbol": "CB.F"},
        }
        with mock.patch.object(app, "fetch_stooq_quotes_by_label", return_value=stooq_quotes):
            with mock.patch.object(app, "_yahoo_price", return_value=(94.9, 0.1, 0.11)) as yahoo_mock:
                quotes = app.fetch_cross_asset_quotes()

        self.assertEqual(quotes["Gold"]["source"], "Stooq")
        self.assertEqual(quotes["Brent Crude"]["source"], "Stooq")
        self.assertEqual(quotes["Crude Oil"]["source"], "Yahoo")
        self.assertEqual(quotes["USD/INR"]["source"], "Yahoo")
        yahoo_mock.assert_has_calls([mock.call("USDINR=X"), mock.call("CL=F")], any_order=True)

    def test_stooq_quotes_fall_back_to_page_before_yahoo(self):
        csv_response = mock.Mock()
        csv_response.text = "Exceeded the daily hits limit"
        csv_response.raise_for_status.return_value = None
        page_response = mock.Mock()
        page_response.text = """
        <span id=aq_cb.f_c2|3>111.61</span>
        <span id=aq_cb.f_d2>2026-05-01</span>
        <span id=aq_cb.f_t1>10:54:09</span>
        <span id=aq_cb.f_m2>-2.40</span>
        <span id=aq_cb.f_m3>(-2.11%)</span>
        <span id=aq_cb.f_h>112.43</span>
        <span id=aq_cb.f_l>110.35</span>
        <span id=aq_cb.f_o>111.47</span>
        <span id=aq_cb.f_p>114.01</span>
        """
        page_response.raise_for_status.return_value = None
        session = mock.Mock()
        session.get.side_effect = [csv_response, page_response]

        with mock.patch.object(app, "http_session", return_value=session):
            quotes = app.fetch_stooq_quotes_by_label({"Brent Crude": "CB.F"})

        self.assertEqual(quotes["Brent Crude"]["price"], 111.61)
        self.assertEqual(quotes["Brent Crude"]["sourceDetail"], "Stooq page")
        self.assertEqual(session.get.call_count, 2)

    def test_stooq_backoff_serves_cached_quote_without_network(self):
        quote = {
            "symbol": "Brent Crude",
            "price": 111.61,
            "change": -2.4,
            "pct": -2.11,
            "fetchedAt": app.time.time() - 30,
            "source": "Stooq",
            "stooqSymbol": "CB.F",
            "sym": "$",
        }
        app._stooq_quote_cache["Brent Crude|CB.F"] = (quote, app.time.time() - 30)
        app._stooq_status["blockedUntil"] = app.time.time() + 60

        session = mock.Mock()
        with mock.patch.object(app, "http_session", return_value=session):
            quotes = app.fetch_stooq_quotes_by_label({"Brent Crude": "CB.F"})

        self.assertEqual(quotes["Brent Crude"]["price"], 111.61)
        self.assertTrue(quotes["Brent Crude"]["stale"])
        self.assertEqual(session.get.call_count, 0)

    def test_stooq_symbol_uses_yahoo_fallback_when_stooq_misses(self):
        with mock.patch.object(app, "fetch_stooq_quotes_by_label", return_value={}):
            with mock.patch.object(app, "_fetch_nse_quote", side_effect=AssertionError("NSE should not be called")):
                with mock.patch.object(app, "_yahoo_price", return_value=(271.35, 1.18, 0.44)) as yahoo_mock:
                    quotes = app.refresh_quote_cache_for_symbols(["AAPL"])

        self.assertEqual(quotes["AAPL"]["source"], "Yahoo")
        self.assertEqual(quotes["AAPL"]["sourceDetail"], "Yahoo fallback after Stooq miss")
        self.assertEqual(quotes["AAPL"]["yahooSymbol"], "AAPL")
        yahoo_mock.assert_called_once_with("AAPL")

    def test_yahoo_backoff_reuses_cached_quote_without_network(self):
        app._yahoo_cache["AAPL"] = (271.35, 1.18, 0.44, app.time.time() - 500)
        app._yahoo_status["blockedUntil"] = app.time.time() + 60

        with mock.patch.object(app, "_yahoo_chart", side_effect=AssertionError("Yahoo network should not be called")):
            quote = app._yahoo_price("AAPL")

        self.assertEqual(quote, (271.35, 1.18, 0.44))

    def test_global_symbols_use_stooq_before_upstox_or_nse(self):
        expected = {
            "^SPX": {
                "price": 7184.2,
                "change": 13.9,
                "pct": 0.19,
                "fetchedAt": 1000,
                "source": "Stooq",
                "sym": "",
            }
        }
        with mock.patch.object(app, "fetch_stooq_quotes_by_label", return_value=expected) as stooq_mock:
            with mock.patch.object(app, "_fetch_nse_quote", side_effect=AssertionError("NSE should not be called")):
                quotes = app.refresh_quote_cache_for_symbols(["^SPX"])

        self.assertEqual(quotes["^SPX"]["source"], "Stooq")
        stooq_mock.assert_called_once_with({"^SPX": "^SPX"})

    def test_resolve_upstox_instrument_key_uses_search_for_new_symbols(self):
        app._upstox_instrument_search_cache.clear()
        row = {
            "name": "PERSISTENT SYSTEMS LTD",
            "segment": "NSE_EQ",
            "exchange": "NSE",
            "instrument_key": "NSE_EQ|INE262H01021",
            "trading_symbol": "PERSISTENT",
            "instrument_type": "EQ",
        }
        with mock.patch.dict(app.os.environ, {"UPSTOX_ANALYTICS_TOKEN": "analytics-token"}, clear=False):
            with mock.patch.object(app, "upstox_search_instruments", return_value=[row]) as search_mock:
                key = app.resolve_upstox_instrument_key("PERSISTENT")

        self.assertEqual(key, "NSE_EQ|INE262H01021")
        search_mock.assert_called_once()

    def test_upstox_html_403_retries_with_curl_transport(self):
        app._upstox_quote_cache.clear()
        app._upstox_stream_quote_cache.clear()
        app._upstox_curl_preferred_until = 0.0
        blocked_response = mock.Mock()
        blocked_response.status_code = 403
        blocked_response.headers = {"content-type": "text/html; charset=UTF-8"}
        blocked_response.text = "<!DOCTYPE html><html><title>Forbidden</title></html>"
        blocked_response.json.side_effect = ValueError("not json")
        session = mock.Mock()
        session.get.return_value = blocked_response
        curl_payload = (
            '{"status":"success","data":{"NSE_INDEX:Nifty 50":{'
            '"instrument_token":"NSE_INDEX|Nifty 50",'
            '"symbol":"NA",'
            '"last_price":24000.0,'
            '"net_change":100.0,'
            '"ohlc":{"open":23900.0,"high":24100.0,"low":23850.0,"close":23900.0}'
            '}}}'
            "__MARKET_DESK_HTTP_STATUS__200"
        )
        completed = mock.Mock(returncode=0, stdout=curl_payload, stderr="")

        with mock.patch.dict(
            app.os.environ,
            {
                "MARKET_DATA_PROVIDER": "upstox",
                "UPSTOX_ANALYTICS_TOKEN": "analytics-token",
                "UPSTOX_HTTP_TRANSPORT": "auto",
            },
            clear=False,
        ):
            with mock.patch.object(app, "http_session", return_value=session):
                with mock.patch.object(app.subprocess, "run", return_value=completed) as curl_mock:
                    quotes = app.fetch_upstox_quotes_by_label({"Nifty 50": "NSE_INDEX|Nifty 50"})

        self.assertEqual(quotes["Nifty 50"]["source"], "Upstox")
        self.assertEqual(quotes["Nifty 50"]["price"], 24000.0)
        session.get.assert_called_once()
        curl_args = curl_mock.call_args.args[0]
        curl_input = curl_mock.call_args.kwargs["input"]
        self.assertEqual(curl_args, ["curl", "--config", "-"])
        self.assertNotIn("analytics-token", " ".join(curl_args))
        self.assertIn("Authorization: Bearer analytics-token", curl_input)
        app._upstox_curl_preferred_until = 0.0

    def test_upstox_forced_curl_transport_skips_requests(self):
        app._upstox_quote_cache.clear()
        app._upstox_stream_quote_cache.clear()
        app._upstox_curl_preferred_until = 0.0
        curl_payload = (
            '{"status":"success","data":{"NSE_EQ:INFY":{'
            '"instrument_token":"NSE_EQ|INE009A01021",'
            '"symbol":"INFY",'
            '"last_price":1500.0,'
            '"net_change":10.0,'
            '"ohlc":{"open":1490.0,"high":1510.0,"low":1488.0,"close":1490.0}'
            '}}}'
            "__MARKET_DESK_HTTP_STATUS__200"
        )
        completed = mock.Mock(returncode=0, stdout=curl_payload, stderr="")
        session = mock.Mock()

        with mock.patch.dict(
            app.os.environ,
            {
                "MARKET_DATA_PROVIDER": "upstox",
                "UPSTOX_ANALYTICS_TOKEN": "analytics-token",
                "UPSTOX_HTTP_TRANSPORT": "curl",
            },
            clear=False,
        ):
            with mock.patch.object(app, "http_session", return_value=session):
                with mock.patch.object(app.subprocess, "run", return_value=completed):
                    quotes = app.fetch_upstox_quotes_by_label({"INFY": "NSE_EQ|INE009A01021"})

        self.assertEqual(quotes["INFY"]["source"], "Upstox")
        session.get.assert_not_called()

    def test_upstox_provider_falls_back_without_token(self):
        with mock.patch.dict(app.os.environ, {"MARKET_DATA_PROVIDER": "upstox", "UPSTOX_ANALYTICS_TOKEN": ""}, clear=False):
            status = app.market_data_provider_status()
        self.assertEqual(status["requested"], "upstox")
        self.assertEqual(status["active"], "nse")
        self.assertFalse(status["upstoxConfigured"])

    def test_upstox_provider_uses_analytics_token(self):
        with mock.patch.dict(app.os.environ, {"MARKET_DATA_PROVIDER": "upstox", "UPSTOX_ANALYTICS_TOKEN": "analytics-token"}, clear=False):
            status = app.market_data_provider_status()
        self.assertEqual(status["requested"], "upstox")
        self.assertEqual(status["active"], "upstox")
        self.assertTrue(status["upstoxConfigured"])
        self.assertEqual(status["upstoxTokenSource"], "analytics_env")
        self.assertEqual(status["upstoxTokenMode"], "analytics")

    def test_fetch_live_quote_uses_upstox_when_configured(self):
        app._upstox_quote_cache.clear()
        response = mock.Mock()
        response.status_code = 200
        response.raise_for_status.return_value = None
        response.json.return_value = {
            "status": "success",
            "data": {
                "NSE_EQ:INFY": {
                    "instrument_token": "NSE_EQ|INE009A01021",
                    "symbol": "INFY",
                    "last_price": 1500.0,
                    "net_change": 12.0,
                    "timestamp": "2026-04-27T10:00:00+05:30",
                    "ohlc": {"open": 1490.0, "high": 1510.0, "low": 1488.0, "close": 1488.0},
                    "depth": {"buy": [{"price": 1499.5}], "sell": [{"price": 1500.5}]},
                    "volume": 1000,
                    "oi": 0,
                },
            },
        }
        session = mock.Mock()
        session.get.return_value = response
        with mock.patch.dict(app.os.environ, {"MARKET_DATA_PROVIDER": "upstox", "UPSTOX_ANALYTICS_TOKEN": "token"}, clear=False):
            with mock.patch.object(app, "http_session", return_value=session):
                quote = app.fetch_live_quote("INFY")

        self.assertIsNotNone(quote)
        self.assertEqual(quote["source"], "Upstox")
        self.assertEqual(quote["price"], 1500.0)
        self.assertEqual(quote["pct"], 0.81)
        self.assertEqual(quote["instrumentKey"], "NSE_EQ|INE009A01021")
        session.get.assert_called_once()

    def test_fetch_upstox_quotes_prefers_stream_cache_before_rest(self):
        key = "NSE_EQ|INE009A01021"
        app._upstox_stream_quote_cache.clear()
        app._upstox_stream_quote_cache[key] = ({
            "symbol": "INFY",
            "name": "Infosys",
            "price": 1501.0,
            "previous_close": 1490.0,
            "change": 11.0,
            "pct": 0.74,
            "day_high": 1504.0,
            "day_low": 1488.0,
            "open": 1492.0,
            "volume": 1000.0,
            "oi": 0.0,
            "bid": 1500.5,
            "ask": 1501.5,
            "fetchedAt": app.time.time(),
            "receivedAt": app.time.time(),
            "source": "Upstox V3",
            "instrumentKey": key,
        }, app.time.time())
        session = mock.Mock()
        with mock.patch.dict(app.os.environ, {"MARKET_DATA_PROVIDER": "upstox", "UPSTOX_ANALYTICS_TOKEN": "token"}, clear=False):
            with mock.patch.object(app, "http_session", return_value=session):
                quote_map = app.fetch_upstox_quotes_by_label({"INFY": key})

        self.assertEqual(quote_map["INFY"]["source"], "Upstox V3")
        session.get.assert_not_called()

    def test_upstox_quotes_url_matches_documented_separator_encoding(self):
        url = app.upstox_quotes_url(["NSE_INDEX|Nifty 50", "NSE_INDEX|Nifty Bank"])
        self.assertIn("instrument_key=NSE_INDEX%7CNifty%2050,NSE_INDEX%7CNifty%20Bank", url)
        self.assertNotIn("%2C", url)
        self.assertNotIn("+", url)

    def test_fetch_upstox_quotes_retries_individually_when_batch_is_rejected(self):
        app._upstox_quote_cache.clear()
        app._upstox_stream_quote_cache.clear()
        batch_response = mock.Mock()
        batch_response.status_code = 403
        batch_response.json.return_value = {
            "status": "error",
            "errors": [{"errorCode": "UDAPI100050", "message": "Forbidden"}],
        }
        infy_response = mock.Mock()
        infy_response.status_code = 200
        infy_response.json.return_value = {
            "status": "success",
            "data": {
                "NSE_EQ:INFY": {
                    "instrument_token": "NSE_EQ|INE009A01021",
                    "symbol": "INFY",
                    "last_price": 1500.0,
                    "net_change": 10.0,
                    "ohlc": {"open": 1490.0, "high": 1510.0, "low": 1488.0, "close": 1490.0},
                },
            },
        }
        tcs_response = mock.Mock()
        tcs_response.status_code = 403
        tcs_response.json.return_value = {
            "status": "error",
            "errors": [{"errorCode": "UDAPI100050", "message": "Forbidden"}],
        }
        session = mock.Mock()
        session.get.side_effect = [batch_response, infy_response, tcs_response]

        with mock.patch.dict(app.os.environ, {"MARKET_DATA_PROVIDER": "upstox", "UPSTOX_ANALYTICS_TOKEN": "token"}, clear=False):
            with mock.patch.object(app, "http_session", return_value=session):
                quotes = app.fetch_upstox_quotes_by_label({
                    "INFY": "NSE_EQ|INE009A01021",
                    "TCS": "NSE_EQ|INE467B01029",
                })

        self.assertEqual(quotes["INFY"]["source"], "Upstox")
        self.assertNotIn("TCS", quotes)
        self.assertEqual(session.get.call_count, 3)

    def test_refresh_quote_cache_for_symbols_keeps_nse_fallback_for_missing_upstox_quotes(self):
        upstox_quotes = {
            "INFY": {
                "symbol": "INFY",
                "name": "Infosys",
                "price": 1500.0,
                "change": 10.0,
                "pct": 0.67,
                "fetchedAt": app.time.time(),
                "source": "Upstox",
            },
        }
        nse_tcs = {
            "symbol": "TCS",
            "name": "TCS",
            "price": 4100.0,
            "previous_close": 4080.0,
            "change": 20.0,
            "pct": 0.49,
            "day_high": 4110.0,
            "day_low": 4068.0,
            "open": 4085.0,
            "volume": 200.0,
            "oi": 0.0,
            "bid": None,
            "ask": None,
            "fetchedAt": app.time.time(),
            "source": "NSE",
        }
        with mock.patch.dict(app.os.environ, {"MARKET_DATA_PROVIDER": "upstox", "UPSTOX_ANALYTICS_TOKEN": "token"}, clear=False):
            with mock.patch.object(app, "fetch_upstox_quotes_by_label", return_value=upstox_quotes):
                with mock.patch.object(app, "_fetch_nse_quote", side_effect=lambda sym: nse_tcs if sym == "TCS" else None):
                    quotes = app.refresh_quote_cache_for_symbols(["INFY", "TCS"])

        self.assertEqual(quotes["INFY"]["source"], "Upstox")
        self.assertEqual(quotes["TCS"]["source"], "NSE")

    def test_refresh_quote_cache_for_symbols_falls_back_when_upstox_errors(self):
        nse_infy = {
            "symbol": "INFY",
            "name": "Infosys",
            "price": 1500.0,
            "previous_close": 1490.0,
            "change": 10.0,
            "pct": 0.67,
            "day_high": 1510.0,
            "day_low": 1488.0,
            "open": 1492.0,
            "volume": 1000.0,
            "oi": 0.0,
            "bid": None,
            "ask": None,
            "fetchedAt": app.time.time(),
            "source": "NSE",
        }
        with mock.patch.dict(app.os.environ, {"MARKET_DATA_PROVIDER": "upstox", "UPSTOX_ANALYTICS_TOKEN": "token"}, clear=False):
            with mock.patch.object(app, "fetch_upstox_quotes_by_label", side_effect=RuntimeError("403 Forbidden")):
                with mock.patch.object(app, "_fetch_nse_quote", return_value=nse_infy):
                    quotes = app.refresh_quote_cache_for_symbols(["INFY"])

        self.assertEqual(quotes["INFY"]["source"], "NSE")

    def test_option_chain_summary_extracts_oi_and_flow(self):
        payload = app.summarize_upstox_option_chain(
            [
                {
                    "strike_price": 22000,
                    "underlying_spot_price": 22050,
                    "call_options": {"market_data": {"oi": 100, "prev_oi": 80}},
                    "put_options": {"market_data": {"oi": 300, "prev_oi": 200}},
                },
                {
                    "strike_price": 22100,
                    "underlying_spot_price": 22050,
                    "call_options": {"market_data": {"oi": 500, "prev_oi": 200}},
                    "put_options": {"market_data": {"oi": 100, "prev_oi": 100}},
                },
            ],
            underlying="NIFTY",
            expiry_date="2026-04-30",
        )
        self.assertEqual(payload["summary"]["pcr"], 0.67)
        self.assertEqual(payload["summary"]["maxCallOiStrike"], 22100.0)
        self.assertEqual(payload["summary"]["maxPutOiStrike"], 22000.0)
        self.assertEqual(payload["summary"]["flowBias"], "Call writing pressure")

    def test_upstox_integration_status_reports_analytics_mode(self):
        with mock.patch.dict(app.os.environ, {"UPSTOX_ANALYTICS_TOKEN": "analytics-token"}, clear=False):
            status = app.upstox_integration_status()

        self.assertTrue(status["connected"])
        self.assertTrue(status["readOnly"])
        self.assertEqual(status["credential"], "UPSTOX_ANALYTICS_TOKEN")
        self.assertEqual(status["tokenMode"], "analytics")

    def test_background_threads_are_disabled_during_unittest(self):
        self.assertFalse(app.background_threads_enabled())

    def test_start_background_workers_is_idempotent(self):
        refresh_thread = mock.Mock()
        ticker_thread = mock.Mock()
        stream_thread = mock.Mock()
        global_thread = mock.Mock()
        with mock.patch.object(app, "background_threads_enabled", return_value=True):
            with mock.patch.object(app, "_background_threads_started", False):
                with mock.patch.object(app.threading, "Thread", side_effect=[refresh_thread, ticker_thread, stream_thread, global_thread]) as thread_ctor:
                    self.assertTrue(app.start_background_workers())
                    self.assertFalse(app.start_background_workers())

        self.assertEqual(thread_ctor.call_count, 4)
        refresh_thread.start.assert_called_once()
        ticker_thread.start.assert_called_once()
        stream_thread.start.assert_called_once()
        global_thread.start.assert_called_once()


class AiChatTests(unittest.TestCase):
    def test_ai_chat_defaults_to_bedrock_api_key_when_key_exists(self):
        with mock.patch.dict(app.os.environ, {"BEDROCK_API_KEY": "bedrock-key", "AI_CHAT_PROVIDER": ""}, clear=False):
            self.assertEqual(app.ai_chat_provider_name(), "bedrock-api-key")

    def test_ai_chat_prompt_includes_internet_context(self):
        context = {
            "tickerTape": {"Brent Crude": {"price": 88.2, "pct": 1.4, "source": "Stooq"}},
            "topicAiSummaries": [{"title": "Brent rises on supply risk", "summary": "AI summary says supply risk lifted oil."}],
            "internetNews": [{"title": "Brent rises on supply risk", "source": "Reuters"}],
        }

        prompt = app.build_ai_chat_prompt("why is bent crude up?", [], context)

        self.assertIn("interpret it as \"Brent crude\"", prompt)
        self.assertIn("topicAiSummaries", prompt)
        self.assertIn("internetNews", prompt)
        self.assertIn("Reuters", prompt)

    def test_ai_chat_context_includes_matching_ai_summaries(self):
        old_articles = list(app._arts)
        old_ticks = dict(app._ticks)
        old_history = dict(app._price_history)
        try:
            app._arts = [
                {
                    "id": "oil-1",
                    "title": "Brent crude rises on supply risk",
                    "summary": "AI summary: Brent rose as supply risk and inventory concerns lifted oil prices.",
                    "summarySource": "ai",
                    "analysisSource": "ai",
                    "sector": "Energy",
                    "scope": "global",
                    "impact": 6,
                    "sentiment": {"label": "bearish"},
                    "aiAnalysis": {"reasons": ["Higher crude can pressure Indian import costs"], "indexImpact": {"Nifty": "negative"}},
                    "ts": app.time.time(),
                },
                {
                    "id": "bank-1",
                    "title": "Banks steady",
                    "summary": "AI summary: Banks were stable.",
                    "summarySource": "ai",
                    "sector": "Banking",
                    "impact": 2,
                    "sentiment": {"label": "neutral"},
                    "ts": app.time.time() - 100,
                },
            ]
            app._ticks = {
                "Brent Crude": {"price": 111.2, "change": -2.8, "pct": -2.45, "source": "Stooq", "fetchedAt": app.time.time()},
            }
            app._price_history = {"Brent Crude": [112.0, 111.6, 111.2]}

            context = app.build_ai_chat_context("why is Brent crude moving?")

            self.assertEqual(context["topicAiSummaries"][0]["title"], "Brent crude rises on supply risk")
            self.assertEqual(context["topicAiSummaries"][0]["summarySource"], "ai")
            self.assertIn("recentMomentum", context["tickerTape"]["Brent Crude"])
        finally:
            app._arts = old_articles
            app._ticks = old_ticks
            app._price_history = old_history

    def test_ai_chat_endpoint_uses_configured_provider(self):
        class FakeProvider:
            def __init__(self):
                self.prompt = ""

            def is_configured(self):
                return True

            def generate_text(self, **kwargs):
                self.prompt = kwargs["prompt"]
                return "Brent is up because fresh internet headlines point to supply risk while the live tape is positive."

        provider = FakeProvider()
        with mock.patch.dict(
            app.os.environ,
            {
                "AI_CHAT_PROVIDER": "bedrock-api-key",
                "BEDROCK_API_KEY": "bedrock-key",
                "AI_CHAT_MODEL": "qwen.qwen3-next-80b-a3b-instruct",
            },
            clear=False,
        ):
            with mock.patch.object(app, "create_ai_text_provider", return_value=provider):
                with mock.patch.object(
                    app,
                    "build_ai_chat_context",
                    return_value={
                        "tickerTape": {"Brent Crude": {"price": 88.2, "pct": 1.4, "source": "Stooq"}},
                        "internetNews": [{"title": "Brent rises on supply risk", "source": "Reuters"}],
                    },
                ):
                    with app.app.test_client() as client:
                        response = client.post("/api/ai-chat", json={"message": "Why is Brent crude up?", "history": []})

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["provider"], "bedrock-api-key")
        self.assertEqual(payload["model"], "qwen.qwen3-next-80b-a3b-instruct")
        self.assertIn("supply risk", payload["answer"])
        self.assertIn("Brent Crude", provider.prompt)

    def test_internet_context_fetches_google_news_rss(self):
        app._ai_chat_web_cache.clear()
        rss = b"""<?xml version="1.0"?>
        <rss><channel>
          <item>
            <title>Brent crude rises as supply risks return</title>
            <link>https://example.com/oil</link>
            <source url="https://example.com">Reuters</source>
            <description>Oil traders watched inventory and geopolitical headlines.</description>
            <pubDate>Thu, 30 Apr 2026 10:00:00 GMT</pubDate>
          </item>
        </channel></rss>
        """
        with mock.patch.object(app, "_get_feed", return_value=rss):
            results = app._internet_results_for_ai_chat("why is Brent crude up?")

        self.assertTrue(results)
        self.assertIn("Brent crude", results[0]["title"])
        self.assertEqual(results[0]["source"], "Reuters")


class PersistenceTests(unittest.TestCase):
    def test_app_state_round_trip_with_sqlite(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "state.db"
            expected = {
                "tickerSelections": ["TCS", "INFY"],
                "watchlist": ["INFY", "RELIANCE"],
                "bookmarks": ["abc123"],
                "portfolio": {
                    "INFY": {"qty": 2.0, "buyPrice": 1500.0},
                },
            }
            for key, value in expected.items():
                app.db_set_json(key, value, db_path)

            loaded, has_stored_state = app.load_persisted_app_state(db_path)
            self.assertTrue(has_stored_state)
            self.assertEqual(loaded["tickerSelections"], expected["tickerSelections"])
            self.assertEqual(loaded["watchlist"], expected["watchlist"])
            self.assertEqual(loaded["bookmarks"], expected["bookmarks"])
            self.assertEqual(loaded["portfolio"], expected["portfolio"])

    def test_state_sanitizer_removes_invalid_entries(self):
        cleaned = app.sanitize_state_patch({
            "watchlist": ["infy", "bad symbol!", "RELIANCE", "INFY"],
            "bookmarks": ["safe_1", "../../oops"],
            "portfolio": {
                "infy": {"qty": 2, "buyPrice": 1500},
                "BAD SYMBOL": {"qty": 1, "buyPrice": 100},
                "TCS": {"qty": 0, "buyPrice": 10},
            },
        })
        self.assertEqual(cleaned["watchlist"], ["INFY", "BADSYMBOL", "RELIANCE"])
        self.assertEqual(cleaned["bookmarks"], ["safe_1", "oops"])
        self.assertEqual(cleaned["portfolio"], {
            "INFY": {"qty": 2.0, "buyPrice": 1500.0},
            "BADSYMBOL": {"qty": 1.0, "buyPrice": 100.0},
        })

    def test_ai_news_summary_round_trip_with_sqlite(self):
        article = {
            "id": "abc123",
            "title": "Nifty gains as IT stocks rally",
            "link": "https://example.com/article",
            "source": "Example",
            "published": "27 Apr 12:00",
            "summary": "Feed summary",
            "sourceSummary": "Feed summary",
        }
        cache_key = app.ai_summary_cache_key(article)
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "state.db"
            app.persist_ai_news_summary(cache_key, article, "AI generated summary.", db_path)
            self.assertEqual(app.load_persisted_ai_news_summary(cache_key, db_path), "AI generated summary.")

    def test_ai_news_analysis_round_trip_with_sqlite(self):
        article = {
            "id": "abc123",
            "title": "Nifty gains as IT stocks rally",
            "link": "https://example.com/article",
            "source": "Example",
            "published": "27 Apr 12:00",
            "summary": "Feed summary",
            "sourceSummary": "Feed summary",
        }
        cache_key = app.ai_analysis_cache_key(article)
        analysis = {
            "summary": "IT stocks helped the market tone improve.",
            "sentiment": "bullish",
            "impactScore": 6,
            "confidence": 0.7,
            "sector": "IT",
            "indexImpact": {"nifty": "bullish", "bankNifty": "limited", "sectorIndex": "bullish", "timeframe": "intraday"},
            "reasons": ["IT leadership supported Nifty"],
        }
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "state.db"
            app.persist_ai_news_analysis(cache_key, article, analysis, db_path)
            self.assertEqual(app.load_persisted_ai_news_analysis(cache_key, db_path), analysis)

    def test_ai_summary_updates_endpoint_returns_only_ai_summaries(self):
        old_articles = list(app._arts)
        old_updated = app._updated
        now = app.time.time()
        with app._lock:
            app._arts = [
                {
                    "id": "ai-1",
                    "summary": "AI generated summary.",
                    "summarySource": "ai",
                    "ts": now,
                },
                {
                    "id": "plain-1",
                    "summary": "Plain feed summary.",
                    "ts": now,
                },
            ]
            app._updated = "12:00:00"
        try:
            with app.app.test_client() as client:
                response = client.get("/api/news/ai-summaries")
        finally:
            with app._lock:
                app._arts = old_articles
                app._updated = old_updated

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(len(payload["updates"]), 1)
        self.assertEqual(payload["updates"][0]["id"], "ai-1")
        self.assertEqual(payload["updates"][0]["summary"], "AI generated summary.")
        self.assertEqual(payload["updates"][0]["summarySource"], "ai")
        self.assertIn("impactMeta", payload["updates"][0])
        self.assertEqual(payload["progress"]["total"], 2)
        self.assertEqual(payload["progress"]["complete"], 1)


class MarketSnapshotTests(unittest.TestCase):
    def test_market_snapshot_includes_live_payloads_and_freshness(self):
        now_ts = app.time.time()
        with app._lock:
            old_state = {
                "ticks": dict(app._ticks),
                "tracked": dict(app._tracked_symbol_quotes),
                "history": {key: list(value) for key, value in app._price_history.items()},
                "analytics": dict(app._analytics_payload),
                "derivatives": dict(app._derivatives_payload),
                "last_news": app._last_news_refresh_ts,
                "last_tick": app._last_tick_refresh_ts,
                "last_analytics": app._last_analytics_refresh_ts,
                "last_derivatives": app._last_derivatives_refresh_ts,
            }
            app._ticks = {
                "Nifty 50": {"price": 22500.0, "change": 120.0, "pct": 0.54, "live": True, "fetchedAt": now_ts},
            }
            app._tracked_symbol_quotes = {
                "INFY": {
                    "symbol": "INFY",
                    "name": "Infosys",
                    "price": 1500.0,
                    "change": 12.0,
                    "pct": 0.81,
                    "fetchedAt": now_ts,
                    "source": "NSE",
                },
            }
            app._price_history = {"Nifty 50": [22400.0, 22500.0]}
            app._analytics_payload = {"generatedAt": "10:00:00", "overviewCards": []}
            app._derivatives_payload = {"generatedAt": "10:00:00", "predictionCards": []}
            app._last_news_refresh_ts = now_ts
            app._last_tick_refresh_ts = now_ts
            app._last_analytics_refresh_ts = now_ts
            app._last_derivatives_refresh_ts = now_ts
        try:
            snapshot = app.market_data_snapshot(include_history=True)
        finally:
            with app._lock:
                app._ticks = old_state["ticks"]
                app._tracked_symbol_quotes = old_state["tracked"]
                app._price_history = old_state["history"]
                app._analytics_payload = old_state["analytics"]
                app._derivatives_payload = old_state["derivatives"]
                app._last_news_refresh_ts = old_state["last_news"]
                app._last_tick_refresh_ts = old_state["last_tick"]
                app._last_analytics_refresh_ts = old_state["last_analytics"]
                app._last_derivatives_refresh_ts = old_state["last_derivatives"]

        self.assertIn("marketStatus", snapshot)
        self.assertIn("analytics", snapshot)
        self.assertIn("derivatives", snapshot)
        self.assertIn("history", snapshot)
        self.assertEqual(snapshot["ticks"]["Nifty 50"]["price"], 22500.0)
        self.assertEqual(snapshot["trackedQuotes"]["INFY"]["price"], 1500.0)
        self.assertFalse(snapshot["trackedQuotes"]["INFY"]["stale"])
        self.assertEqual(snapshot["history"]["Nifty 50"], [22400.0, 22500.0])


class DerivativesEngineTests(unittest.TestCase):
    def test_derivatives_payload_exposes_prediction_and_trade_plan(self):
        now_ts = app.time.time()
        articles = [
            {
                "sector": "Banking",
                "sentiment": {"label": "bullish"},
                "impact": 5,
                "ts": now_ts,
            },
            {
                "sector": "General",
                "sentiment": {"label": "bullish"},
                "impact": 4,
                "ts": now_ts,
            },
        ]
        ticks = {
            "Nifty 50": {"price": 22500.0, "change": 225.0, "pct": 1.01, "day_high": 22540.0, "day_low": 22310.0},
            "Nifty Bank": {"price": 49000.0, "change": 800.0, "pct": 1.66, "day_high": 49120.0, "day_low": 48520.0},
            "Nifty IT": {"price": 38000.0, "change": 304.0, "pct": 0.81, "day_high": 38110.0, "day_low": 37740.0},
            "Nifty Midcap": {"price": 52000.0, "change": 624.0, "pct": 1.21},
            "Nifty Smallcap": {"price": 16800.0, "change": 235.0, "pct": 1.40},
            "VIX": {"price": 13.2, "change": -0.3, "pct": -2.22, "day_high": 13.6, "day_low": 13.0},
            "Crude Oil": {"price": 81.2, "change": -0.9, "pct": -1.10},
            "Brent Crude": {"price": 84.6, "change": -0.8, "pct": -0.94},
            "USD/INR": {"price": 82.7, "change": -0.2, "pct": -0.24},
            "Gold": {"price": 2330.0, "change": 4.0, "pct": 0.17},
        }
        price_history = {
            "Nifty 50": [21920.0, 21980.0, 22060.0, 22120.0, 22200.0, 22310.0, 22390.0, 22450.0, 22500.0],
            "Nifty Bank": [47950.0, 48100.0, 48240.0, 48400.0, 48520.0, 48650.0, 48780.0, 48910.0, 49000.0],
            "VIX": [14.6, 14.4, 14.1, 13.9, 13.8, 13.6, 13.5, 13.3, 13.2],
        }
        market_status = {"staleData": False}

        with mock.patch.object(
            app,
            "build_symbol_signal",
            side_effect=lambda symbol, live_quote=None, is_index=False: (
                app.build_live_only_signal(symbol, live_quote) if live_quote else None
            ),
        ):
            payload = app.build_derivatives_analysis_payload(
                articles,
                ticks,
                index_snapshot={},
                price_history=price_history,
                market_status=market_status,
            )

        self.assertEqual(len(payload["predictionCards"]), 6)
        self.assertEqual(payload["predictionCards"][0]["label"], "Model Bias")
        self.assertEqual(payload["predictionCards"][0]["value"], "Strong Long Bias")
        self.assertEqual(payload["predictionCards"][4]["value"], "Trend Day")
        self.assertIn("pts", payload["predictionCards"][5]["value"])
        self.assertEqual(len(payload["scoreBreakdown"]), 6)
        self.assertEqual(len(payload["tradeScenarios"]), 3)
        self.assertEqual(len(payload["signalMatrix"]), 4)
        self.assertEqual(payload["signalMatrix"][1]["label"], "Bank Nifty")
        self.assertEqual(payload["tradeScenarios"][0]["label"], "Bull continuation")
        self.assertEqual(payload["overviewCards"][0]["value"], "Bank Nifty")
        self.assertNotIn("Stale data", [flag["label"] for flag in payload["riskFlags"]])


if __name__ == "__main__":
    unittest.main()
