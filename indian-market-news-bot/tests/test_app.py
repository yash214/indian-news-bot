import importlib.util
import tempfile
import unittest
from unittest import mock
from pathlib import Path
from urllib.parse import parse_qs, urlparse


PROJECT_ROOT = Path(__file__).resolve().parents[1]
APP_PATH = PROJECT_ROOT / "backend" / "app.py"


spec = importlib.util.spec_from_file_location("market_desk_app", APP_PATH)
app = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(app)


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


class UpstoxProviderTests(unittest.TestCase):
    def test_upstox_access_token_uses_db_fallback(self):
        with mock.patch.dict(app.os.environ, {"UPSTOX_ACCESS_TOKEN": ""}, clear=False):
            with mock.patch.object(app, "stored_upstox_token_record", return_value={"access_token": "db-token"}):
                self.assertEqual(app.upstox_access_token(), "db-token")

    def test_upstox_oauth_dialog_url_contains_expected_params(self):
        with mock.patch.dict(
            app.os.environ,
            {
                "UPSTOX_CLIENT_ID": "client-123",
                "UPSTOX_CLIENT_SECRET": "secret-456",
                "UPSTOX_REDIRECT_URI": "https://desk.example.com/api/auth/upstox/callback",
            },
            clear=False,
        ):
            url = app.upstox_oauth_dialog_url("state-xyz")

        parsed = urlparse(url)
        params = parse_qs(parsed.query)
        self.assertEqual(parsed.path, "/v2/login/authorization/dialog")
        self.assertEqual(params["response_type"], ["code"])
        self.assertEqual(params["client_id"], ["client-123"])
        self.assertEqual(params["redirect_uri"], ["https://desk.example.com/api/auth/upstox/callback"])
        self.assertEqual(params["state"], ["state-xyz"])

    def test_upstox_provider_falls_back_without_token(self):
        with mock.patch.dict(app.os.environ, {"MARKET_DATA_PROVIDER": "upstox", "UPSTOX_ACCESS_TOKEN": ""}, clear=False):
            status = app.market_data_provider_status()
        self.assertEqual(status["requested"], "upstox")
        self.assertEqual(status["active"], "nse")
        self.assertFalse(status["upstoxConfigured"])

    def test_fetch_live_quote_uses_upstox_when_configured(self):
        app._upstox_quote_cache.clear()
        response = mock.Mock()
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
        with mock.patch.dict(app.os.environ, {"MARKET_DATA_PROVIDER": "upstox", "UPSTOX_ACCESS_TOKEN": "token"}, clear=False):
            with mock.patch.object(app, "http_session", return_value=session):
                quote = app.fetch_live_quote("INFY")

        self.assertIsNotNone(quote)
        self.assertEqual(quote["source"], "Upstox")
        self.assertEqual(quote["price"], 1500.0)
        self.assertEqual(quote["pct"], 0.81)
        self.assertEqual(quote["instrumentKey"], "NSE_EQ|INE009A01021")
        session.get.assert_called_once()

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

    def test_upstox_callback_persists_token_record(self):
        client = app.app.test_client()
        with mock.patch.object(app, "load_upstox_oauth_state", return_value="state-xyz"):
            with mock.patch.object(app, "upstox_auth_token_request", return_value={"access_token": "fresh-token"}) as exchange_mock:
                with mock.patch.object(app, "persist_upstox_token_record") as persist_mock:
                    with mock.patch.object(app, "persist_upstox_oauth_state") as state_mock:
                        response = client.get("/api/auth/upstox/callback?code=auth-code&state=state-xyz")

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.headers["Location"], "/")
        exchange_mock.assert_called_once_with("auth-code")
        persist_mock.assert_called_once()
        state_mock.assert_called_once_with("")

    def test_upstox_integration_status_reports_static_ip_config(self):
        with mock.patch.dict(app.os.environ, {"UPSTOX_PRIMARY_IP": "1.2.3.4", "UPSTOX_SECONDARY_IP": ""}, clear=False):
            with mock.patch.object(app, "upstox_access_token", return_value="token"):
                status = app.upstox_integration_status()

        self.assertTrue(status["staticIpConfigured"])
        self.assertTrue(status["staticIpSyncReady"])
        self.assertEqual(status["primaryIp"], "1.2.3.4")

    def test_background_threads_are_disabled_during_unittest(self):
        self.assertFalse(app.background_threads_enabled())

    def test_start_background_workers_is_idempotent(self):
        refresh_thread = mock.Mock()
        ticker_thread = mock.Mock()
        with mock.patch.object(app, "background_threads_enabled", return_value=True):
            with mock.patch.object(app, "_background_threads_started", False):
                with mock.patch.object(app.threading, "Thread", side_effect=[refresh_thread, ticker_thread]) as thread_ctor:
                    self.assertTrue(app.start_background_workers())
                    self.assertFalse(app.start_background_workers())

        self.assertEqual(thread_ctor.call_count, 2)
        refresh_thread.start.assert_called_once()
        ticker_thread.start.assert_called_once()


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
