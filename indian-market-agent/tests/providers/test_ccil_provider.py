import unittest
from unittest import mock

from backend.providers.ccil_provider import CCILProvider


def response_with_text(text: str):
    response = mock.Mock()
    response.text = text
    response.raise_for_status.return_value = None
    return response


class CCILProviderTests(unittest.TestCase):
    def test_disabled_provider_returns_none(self):
        provider = CCILProvider(enabled=False)
        self.assertIsNone(provider.get_bond_market_snapshot())

    def test_mocked_bond_data_parses(self):
        html = """
        <html><body>
        <div>03-05-2026</div>
        <table>
          <tr><th>Instrument</th><th>Yield</th><th>Change bps</th></tr>
          <tr><td>India 10Y Yield</td><td>7.24</td><td>12</td></tr>
        </table>
        </body></html>
        """
        provider = CCILProvider(enabled=True)
        provider.session.get = mock.Mock(return_value=response_with_text(html))

        payload = provider.get_bond_market_snapshot()

        self.assertEqual(payload["india_10y_yield"], 7.24)
        self.assertEqual(payload["india_10y_change_bps"], 12.0)
        self.assertEqual(payload["yield_bias"], "BEARISH")

    def test_yield_spike_maps_to_bearish(self):
        provider = CCILProvider(enabled=True)
        payload = provider.normalize_bond_snapshot({
            "as_of_date": "2026-05-03",
            "india_10y_yield": 7.35,
            "india_10y_change_bps": 22,
        })
        self.assertEqual(payload["yield_bias"], "BEARISH")
        self.assertEqual(payload["impact"], 8)

    def test_malformed_response_returns_none(self):
        provider = CCILProvider(enabled=True)
        provider.session.get = mock.Mock(return_value=response_with_text("<html><body>unknown</body></html>"))
        self.assertIsNone(provider.get_bond_market_snapshot())


if __name__ == "__main__":
    unittest.main()
