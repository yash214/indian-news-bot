import unittest
from datetime import datetime, timezone

from backend.agents.fo_structure.pcr import calculate_pcr, classify_pcr
from backend.agents.fo_structure.schemas import OptionChainSnapshot, OptionStrike


def snapshot(call_ois, put_ois):
    strikes = [
        OptionStrike(strike=100 + idx * 50, call_oi=call_oi, put_oi=put_oi)
        for idx, (call_oi, put_oi) in enumerate(zip(call_ois, put_ois))
    ]
    return OptionChainSnapshot(
        symbol="NIFTY",
        underlying_key="NSE_INDEX|Nifty 50",
        spot=100.0,
        expiry="2026-05-07",
        timestamp=datetime.now(timezone.utc),
        strikes=strikes,
    )


class FOPCRTests(unittest.TestCase):
    def test_pcr_bullish(self):
        pcr = calculate_pcr(snapshot([100, 100], [130, 100]))
        self.assertEqual(pcr, 1.15)
        self.assertEqual(classify_pcr(pcr), "BULLISH")

    def test_pcr_bearish(self):
        pcr = calculate_pcr(snapshot([100, 100], [80, 90]))
        self.assertEqual(pcr, 0.85)
        self.assertEqual(classify_pcr(pcr), "BEARISH")

    def test_pcr_neutral(self):
        pcr = calculate_pcr(snapshot([100, 100], [100, 105]))
        self.assertEqual(pcr, 1.02)
        self.assertEqual(classify_pcr(pcr), "NEUTRAL")

    def test_pcr_unknown_when_call_oi_zero(self):
        pcr = calculate_pcr(snapshot([0, None], [100, 100]))
        self.assertIsNone(pcr)
        self.assertEqual(classify_pcr(pcr), "UNKNOWN")

    def test_extreme_states(self):
        self.assertEqual(classify_pcr(1.6), "EXTREME_BULLISH")
        self.assertEqual(classify_pcr(0.65), "EXTREME_BEARISH")


if __name__ == "__main__":
    unittest.main()
