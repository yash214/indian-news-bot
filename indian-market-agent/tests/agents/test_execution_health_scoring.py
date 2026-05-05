import unittest

from backend.agents.execution_health.scoring import score_execution_health
from backend.agents.execution_health.schemas import CRITICAL, FAIL, HEALTHY, INFO, PASS, UNHEALTHY, UNKNOWN, WARN, WARNING, DEGRADED, HealthCheck


class ExecutionHealthScoringTests(unittest.TestCase):
    def test_all_pass_checks_are_healthy(self):
        result = score_execution_health([
            HealthCheck("agent_freshness:market_regime_agent:NIFTY:MARKET_REGIME_REPORT", PASS, INFO, 10, "fresh", {"report_type": "MARKET_REGIME_REPORT"}),
            HealthCheck("agent_freshness:fo_structure_agent:NIFTY:FO_STRUCTURE_REPORT", PASS, INFO, 10, "fresh", {"report_type": "FO_STRUCTURE_REPORT"}),
            HealthCheck("market_data:freshness", PASS, INFO, 5, "fresh", {"category": "market_data"}),
        ])

        self.assertEqual(result["overall_health"], HEALTHY)
        self.assertTrue(result["strategy_engine_guidance"].allow_trade_proposal)
        self.assertFalse(result["strategy_engine_guidance"].allow_live_execution)

    def test_optional_warning_degrades_health(self):
        result = score_execution_health([
            HealthCheck("market_data:freshness", PASS, INFO, 5, "fresh", {"category": "market_data"}),
            HealthCheck("provider:fmp", WARN, WARNING, None, "FMP unavailable", {"category": "provider", "critical": False}),
        ])

        self.assertEqual(result["overall_health"], DEGRADED)
        self.assertTrue(result["strategy_engine_guidance"].reduce_confidence)
        self.assertTrue(result["strategy_engine_guidance"].allow_trade_proposal)

    def test_critical_fo_stale_fail_is_unhealthy(self):
        result = score_execution_health(
            [HealthCheck("agent_freshness:fo_structure_agent:NIFTY:FO_STRUCTURE_REPORT", FAIL, CRITICAL, 700, "stale", {"report_type": "FO_STRUCTURE_REPORT", "critical": True})],
            market_session={"is_open": True},
        )

        self.assertEqual(result["overall_health"], UNHEALTHY)
        self.assertFalse(result["strategy_engine_guidance"].allow_trade_proposal)

    def test_critical_market_regime_stale_fail_is_unhealthy(self):
        result = score_execution_health(
            [HealthCheck("agent_freshness:market_regime_agent:NIFTY:MARKET_REGIME_REPORT", FAIL, CRITICAL, 700, "stale", {"report_type": "MARKET_REGIME_REPORT", "critical": True})],
            market_session={"is_open": True},
        )

        self.assertEqual(result["overall_health"], UNHEALTHY)
        self.assertTrue(result["fresh_trade_blocked"])

    def test_no_checks_is_unknown(self):
        result = score_execution_health([])

        self.assertEqual(result["overall_health"], UNKNOWN)
        self.assertFalse(result["trade_allowed"])
        self.assertTrue(result["fresh_trade_blocked"])

    def test_confidence_is_clamped(self):
        result = score_execution_health([
            HealthCheck("market_data:freshness", FAIL, CRITICAL, 300, "stale", {"category": "market_data", "critical": True}),
            HealthCheck("agent_freshness:fo_structure_agent:NIFTY:FO_STRUCTURE_REPORT", FAIL, CRITICAL, 700, "stale", {"report_type": "FO_STRUCTURE_REPORT", "critical": True}),
            HealthCheck("agent_freshness:market_regime_agent:NIFTY:MARKET_REGIME_REPORT", FAIL, CRITICAL, 700, "stale", {"report_type": "MARKET_REGIME_REPORT", "critical": True}),
        ])

        self.assertEqual(result["overall_health"], UNHEALTHY)
        self.assertEqual(result["confidence"], 0.35)


if __name__ == "__main__":
    unittest.main()
