import json
import unittest
from datetime import datetime

from backend.agents.macro_context.agent import MacroContextAgent
from backend.agents.macro_context.schemas import MacroEvent, MacroFactorInput, MacroSnapshot
from backend.core.settings import IST


def factor(name: str, *, value=None, change_pct_1d=None, change_pct_5d=None, source="test", raw=None):
    return MacroFactorInput(
        name=name,
        symbol=name.upper(),
        value=value,
        change_pct_1d=change_pct_1d,
        change_pct_5d=change_pct_5d,
        source=source,
        raw=raw or {},
    )


def snapshot(*, factors=None, events=None):
    return MacroSnapshot(
        market="INDIA",
        timestamp=datetime(2026, 5, 3, 9, 25, tzinfo=IST),
        factors=factors or {},
        events=events or [],
        source_status={},
    )


class MacroContextAgentTests(unittest.TestCase):
    def setUp(self):
        self.agent = MacroContextAgent()

    def test_bearish_macro_setup(self):
        report = self.agent.analyze(snapshot(
            factors={
                "crude": factor("crude", value=84.0, change_pct_1d=4.0),
                "usd_inr": factor("usd_inr", value=83.5, change_pct_1d=0.6),
                "india_vix": factor("india_vix", value=21.0, change_pct_1d=7.0),
                "gold": factor("gold", value=2350.0, change_pct_1d=1.0),
                "global_cues": factor("global_cues", value=-0.4, change_pct_1d=-0.4),
            }
        ))

        self.assertIn(report.macro_bias, {"MIXED_BEARISH", "BEARISH", "EVENT_RISK"})
        self.assertIn(report.trade_filter, {"REDUCE_LONG_CONFIDENCE", "EVENT_RISK_WAIT"})
        self.assertLess(report.strategy_engine_guidance.long_confidence_adjustment, 0)
        self.assertTrue(report.strategy_engine_guidance.reduce_position_size)

    def test_bullish_macro_setup(self):
        report = self.agent.analyze(snapshot(
            factors={
                "crude": factor("crude", value=78.0, change_pct_1d=-3.0),
                "usd_inr": factor("usd_inr", value=82.2, change_pct_1d=-0.3),
                "india_vix": factor("india_vix", value=14.0, change_pct_1d=-5.0),
                "global_cues": factor("global_cues", value=0.7, change_pct_1d=0.7),
                "gold": factor("gold", value=2295.0, change_pct_1d=-0.4),
            }
        ))

        self.assertIn(report.macro_bias, {"MIXED_BULLISH", "BULLISH"})
        self.assertIn(report.trade_filter, {"REDUCE_SHORT_CONFIDENCE", "NO_FILTER"})
        self.assertLessEqual(report.strategy_engine_guidance.short_confidence_adjustment, 0)

    def test_extreme_vix(self):
        report = self.agent.analyze(snapshot(
            factors={
                "india_vix": factor("india_vix", value=30.0, change_pct_1d=12.0),
                "crude": factor("crude", value=82.0, change_pct_1d=0.5),
                "usd_inr": factor("usd_inr", value=83.1, change_pct_1d=0.2),
                "global_cues": factor("global_cues", value=-0.3, change_pct_1d=-0.3),
            }
        ))

        self.assertIn(report.macro_bias, {"EVENT_RISK", "BEARISH"})
        self.assertIn(report.trade_filter, {"EVENT_RISK_WAIT", "BLOCK_FRESH_TRADES"})
        self.assertTrue(report.strategy_engine_guidance.reduce_position_size)

    def test_vix_shock_blocks_fresh_trades(self):
        report = self.agent.analyze(snapshot(
            factors={
                "india_vix": factor("india_vix", value=24.0, change_pct_1d=16.0),
                "crude": factor("crude", value=82.0, change_pct_1d=0.3),
                "usd_inr": factor("usd_inr", value=83.0, change_pct_1d=0.1),
                "global_cues": factor("global_cues", value=0.2, change_pct_1d=0.2),
            }
        ))

        self.assertEqual(report.trade_filter, "BLOCK_FRESH_TRADES")
        self.assertTrue(report.strategy_engine_guidance.block_fresh_trades)

    def test_high_impact_calendar_event(self):
        event_time = datetime(2026, 5, 3, 18, 0, tzinfo=IST)
        report = self.agent.analyze(snapshot(
            factors={
                "crude": factor("crude", value=81.5, change_pct_1d=0.4),
                "usd_inr": factor("usd_inr", value=82.8, change_pct_1d=0.1),
                "india_vix": factor("india_vix", value=17.0, change_pct_1d=1.0),
                "global_cues": factor("global_cues", value=0.1, change_pct_1d=0.1),
            },
            events=[MacroEvent(country="United States", event="US CPI", importance="high", event_time=event_time, source="test")],
        ))

        self.assertEqual(report.trade_filter, "EVENT_RISK_WAIT")
        self.assertTrue(any("Event risk" in driver or "US CPI" in driver for driver in report.major_drivers + report.warnings))

    def test_missing_data_returns_safe_report_with_lower_confidence(self):
        sparse_report = self.agent.analyze(snapshot(
            factors={"crude": factor("crude", value=80.0, change_pct_1d=1.1)}
        ))
        full_report = self.agent.analyze(snapshot(
            factors={
                "crude": factor("crude", value=80.0, change_pct_1d=1.1),
                "usd_inr": factor("usd_inr", value=83.0, change_pct_1d=0.1),
                "india_vix": factor("india_vix", value=14.0, change_pct_1d=-1.0),
                "global_cues": factor("global_cues", value=0.2, change_pct_1d=0.2),
            }
        ))

        self.assertIsNotNone(sparse_report)
        self.assertTrue(any("Missing macro factor" in warning for warning in sparse_report.warnings))
        self.assertLess(sparse_report.confidence, full_report.confidence)

    def test_neutral_setup(self):
        report = self.agent.analyze(snapshot(
            factors={
                "crude": factor("crude", value=80.0, change_pct_1d=0.2),
                "usd_inr": factor("usd_inr", value=82.9, change_pct_1d=0.05),
                "india_vix": factor("india_vix", value=15.0, change_pct_1d=0.2),
                "global_cues": factor("global_cues", value=0.1, change_pct_1d=0.1),
                "gold": factor("gold", value=2302.0, change_pct_1d=0.3),
            }
        ))

        self.assertEqual(report.macro_bias, "NEUTRAL")
        self.assertEqual(report.trade_filter, "NO_FILTER")

    def test_report_json_serialization(self):
        report = self.agent.analyze(snapshot(
            factors={
                "crude": factor("crude", value=78.0, change_pct_1d=-2.4),
                "usd_inr": factor("usd_inr", value=82.4, change_pct_1d=-0.4),
                "india_vix": factor("india_vix", value=13.0, change_pct_1d=-2.0),
                "global_cues": factor("global_cues", value=0.6, change_pct_1d=0.6),
            }
        ))

        payload = report.to_dict()
        self.assertIsInstance(payload["generated_at"], str)
        self.assertIsInstance(payload["valid_until"], str)
        json.dumps(payload)


if __name__ == "__main__":
    unittest.main()
