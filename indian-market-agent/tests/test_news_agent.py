import importlib.util
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

from backend.agents.news.agent import NewsIntelligenceAgent
from backend.agents.news.report_aggregator import NewsReportAggregator
from backend.agents.news.report_store import load_recent_article_ai_analyses, save_article_ai_analysis
from backend.agents.news.schemas import ArticleAIAnalysis, EventRisk, StrategyEngineGuidance


PROJECT_ROOT = Path(__file__).resolve().parents[1]
APP_PATH = PROJECT_ROOT / "backend" / "app.py"

spec = importlib.util.spec_from_file_location("market_desk_app_for_news_agent", APP_PATH)
app = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(app)


def article_analysis(
    article_id: str,
    sentiment: str,
    impact_score: int,
    confidence: float,
    *,
    index: str = "NIFTY",
    tags=None,
    trade_filter: str = "NO_FILTER",
    event_risk: EventRisk | None = None,
) -> ArticleAIAnalysis:
    return ArticleAIAnalysis(
        article_id=article_id,
        title=f"{article_id} headline",
        source="Example",
        url=f"https://example.com/{article_id}",
        published_at="2026-05-01T10:00:00+05:30",
        analyzed_at="2026-05-01T10:05:00+00:00",
        published_ts=time.time(),
        summary=f"{article_id} summary with a market relevant catalyst.",
        sentiment=sentiment,
        impact_score=impact_score,
        confidence=confidence,
        category="macro",
        affected_indices=[index],
        affected_sectors=["Banking"] if index == "BANKNIFTY" else ["Oil & Gas"],
        macro_tags=list(tags or []),
        event_risk=event_risk or EventRisk(),
        trade_filter=trade_filter,
        strategy_engine_guidance=StrategyEngineGuidance(notes="test guidance"),
        reasons=[f"{article_id} reason"],
        raw_llm_json={"sentiment": sentiment},
    )


class NewsAgentTests(unittest.TestCase):
    def test_article_analysis_normalization_with_valid_llm_json(self):
        article = {
            "id": "oil-1",
            "title": "Brent jumps as supply risk rises",
            "source": "Reuters",
            "link": "https://example.com/oil",
            "published": "01 May 10:00",
            "ts": time.time(),
            "sector": "Energy",
            "impact": 6,
            "sentiment": {"label": "bearish"},
            "summary": "Brent crude jumped on supply risk.",
        }
        llm_json = {
            "summary": "Brent crude rose after supply-risk headlines, increasing input-cost pressure for Indian oil-sensitive sectors. The move can weigh on market risk appetite if it keeps USD/INR firm and pressures OMC margins.",
            "sentiment": "bearish",
            "impact_score": 8,
            "confidence": 0.82,
            "category": "macro",
            "affected_indices": ["NIFTY"],
            "affected_sectors": ["Oil & Gas"],
            "macro_tags": ["CRUDE", "USDINR"],
            "event_risk": {"is_event_risk": True, "risk_level": "medium", "reason": "Crude supply risk"},
            "trade_filter": "EVENT_RISK_WAIT",
            "strategy_engine_guidance": {"long_confidence_adjustment": -8, "short_confidence_adjustment": 0, "block_fresh_trades": False, "notes": "Crude risk can reduce long conviction."},
            "reasons": ["Crude is a direct macro input for India"],
        }

        analysis = NewsIntelligenceAgent().normalize_llm_analysis(article, llm_json)

        self.assertEqual(analysis.sentiment, "bearish")
        self.assertEqual(analysis.impact_score, 8)
        self.assertEqual(analysis.confidence, 0.82)
        self.assertEqual(analysis.affected_indices, ["NIFTY"])
        self.assertIn("CRUDE", analysis.macro_tags)
        self.assertEqual(analysis.trade_filter, "EVENT_RISK_WAIT")
        self.assertTrue(analysis.raw_llm_json)

    def test_article_analysis_falls_back_when_llm_json_is_invalid(self):
        article = {
            "id": "fallback-1",
            "title": "RBI policy uncertainty keeps banks volatile",
            "source": "Example",
            "link": "https://example.com/rbi",
            "published": "01 May 11:00",
            "ts": time.time(),
            "sector": "Banking",
            "impact": 7,
            "sentiment": {"label": "bearish"},
            "impactMeta": {"reasons": ["event: rbi", "market context: bank nifty"]},
            "summary": "RBI policy uncertainty kept banking shares volatile.",
        }

        analysis = NewsIntelligenceAgent().normalize_llm_analysis(article, {})

        self.assertEqual(analysis.sentiment, "bearish")
        self.assertEqual(analysis.impact_score, 7)
        self.assertIn("BANKNIFTY", analysis.affected_indices)
        self.assertFalse(analysis.raw_llm_json)
        self.assertIn("event: rbi", analysis.reasons)

    def test_aggregator_produces_bearish_report_for_high_impact_crude_fii_news(self):
        analyses = [
            article_analysis("crude", "bearish", 9, 0.9, tags=["CRUDE"], trade_filter="REDUCE_LONG_CONFIDENCE"),
            article_analysis("fii", "bearish", 8, 0.8, tags=["FII_FLOWS"]),
            article_analysis("minor", "bullish", 2, 0.4, tags=["GLOBAL_CUES"]),
        ]

        report = NewsReportAggregator(analyses).build_report("NIFTY", lookback_hours=24)

        self.assertEqual(report.overall_sentiment, "BEARISH")
        self.assertGreaterEqual(report.impact_score, 7)
        self.assertEqual(report.trade_filter, "REDUCE_LONG_CONFIDENCE")
        self.assertEqual(report.market_regime_hint, "NEWS_NEGATIVE")
        self.assertTrue(any(driver["driver"] == "CRUDE" for driver in report.major_drivers))

    def test_aggregator_produces_mixed_bullish_when_bullish_articles_dominate(self):
        analyses = [
            article_analysis("it", "bullish", 7, 0.8, tags=["GLOBAL_CUES"]),
            article_analysis("banks", "bullish", 6, 0.75, tags=["RBI_POLICY"]),
            article_analysis("crude", "bearish", 3, 0.5, tags=["CRUDE"]),
        ]

        report = NewsReportAggregator(analyses).build_report("NIFTY", lookback_hours=24)

        self.assertIn(report.overall_sentiment, {"BULLISH", "MIXED_BULLISH"})
        self.assertEqual(report.market_regime_hint, "NEWS_SUPPORTIVE")
        self.assertLess(report.strategy_engine_guidance.short_confidence_adjustment, 0)

    def test_trade_filter_priority_block_fresh_trades_wins(self):
        analyses = [
            article_analysis("normal", "bullish", 7, 0.8, trade_filter="REDUCE_SHORT_CONFIDENCE"),
            article_analysis(
                "event",
                "bearish",
                9,
                0.9,
                trade_filter="EVENT_RISK_WAIT",
                event_risk=EventRisk(is_event_risk=True, risk_level="high", reason="Major policy event"),
            ),
        ]

        report = NewsReportAggregator(analyses).build_report("NIFTY", lookback_hours=24)

        self.assertEqual(report.trade_filter, "BLOCK_FRESH_TRADES")
        self.assertTrue(report.strategy_engine_guidance.block_fresh_trades)

    def test_empty_article_list_returns_neutral_report(self):
        report = NewsReportAggregator([]).build_report("BANKNIFTY", lookback_hours=24)

        self.assertEqual(report.index, "BANKNIFTY")
        self.assertEqual(report.overall_sentiment, "NEUTRAL")
        self.assertEqual(report.impact_score, 0)
        self.assertEqual(report.trade_filter, "NO_FILTER")
        self.assertIn("No analyzed news", report.summary)

    def test_article_analysis_round_trip_in_news_agent_store(self):
        analysis = article_analysis("stored", "bullish", 6, 0.7, tags=["GLOBAL_CUES"])
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "market.db"
            save_article_ai_analysis(analysis, db_path)
            loaded = load_recent_article_ai_analyses(lookback_hours=24, path=db_path)

        self.assertEqual(len(loaded), 1)
        self.assertEqual(loaded[0].article_id, "stored")
        self.assertEqual(loaded[0].macro_tags, ["GLOBAL_CUES"])

    def test_news_agent_report_endpoint_returns_valid_json(self):
        analysis = article_analysis("api", "bearish", 8, 0.8, tags=["CRUDE"])
        with mock.patch.object(app, "load_recent_article_ai_analyses", return_value=[analysis]):
            with mock.patch.object(app, "save_index_news_report") as save_report:
                with app.app.test_client() as client:
                    response = client.get("/api/news/agent/report?index=NIFTY&lookback_hours=24")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["report_type"], "ROLLING_24H_INDEX_NEWS_REPORT")
        self.assertEqual(payload["index"], "NIFTY")
        self.assertIn(payload["overall_sentiment"], {"BEARISH", "MIXED_BEARISH"})
        self.assertIn("strategy_engine_guidance", payload)
        save_report.assert_called_once()


if __name__ == "__main__":
    unittest.main()
