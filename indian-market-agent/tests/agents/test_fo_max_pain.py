import unittest
from datetime import datetime, timezone

from backend.agents.fo_structure.max_pain import calculate_max_pain
from backend.agents.fo_structure.schemas import OptionChainSnapshot, OptionStrike


def snapshot(strikes):
    return OptionChainSnapshot(
        symbol="NIFTY",
        underlying_key="NSE_INDEX|Nifty 50",
        spot=100,
        expiry="2026-05-07",
        timestamp=datetime.now(timezone.utc),
        strikes=strikes,
    )


class FOMaxPainTests(unittest.TestCase):
    def test_calculates_simple_max_pain(self):
        payload = snapshot([
            OptionStrike(strike=90, call_oi=10, put_oi=30),
            OptionStrike(strike=100, call_oi=20, put_oi=20),
            OptionStrike(strike=110, call_oi=30, put_oi=10),
        ])
        self.assertEqual(calculate_max_pain(payload), 100)

    def test_returns_none_for_insufficient_oi(self):
        payload = snapshot([
            OptionStrike(strike=90, call_oi=10, put_oi=0),
            OptionStrike(strike=100, call_oi=0, put_oi=0),
        ])
        self.assertIsNone(calculate_max_pain(payload))


if __name__ == "__main__":
    unittest.main()
