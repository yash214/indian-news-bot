import unittest
from unittest import mock

from backend.providers.nsdl_provider import NSDLProvider


def response_with_text(text: str):
    response = mock.Mock()
    response.text = text
    response.raise_for_status.return_value = None
    return response


class NSDLProviderTests(unittest.TestCase):
    def test_disabled_provider_returns_none(self):
        provider = NSDLProvider(enabled=False)
        self.assertIsNone(provider.get_latest_fpi_flows())

    def test_mocked_html_parses_normalized_values(self):
        html = """
        <html><body>
        <div>As on 03-05-2026</div>
        <table>
          <tr><th>Category</th><th>Net INR Cr</th></tr>
          <tr><td>Equity</td><td>3500</td></tr>
          <tr><td>Debt</td><td>250</td></tr>
          <tr><td>Total</td><td>3750</td></tr>
        </table>
        </body></html>
        """
        provider = NSDLProvider(enabled=True)
        provider.session.get = mock.Mock(return_value=response_with_text(html))

        payload = provider.get_latest_fpi_flows()

        self.assertEqual(payload["source"], "nsdl")
        self.assertEqual(payload["as_of_date"], "2026-05-03")
        self.assertEqual(payload["equity_net_inr_cr"], 3500.0)
        self.assertEqual(payload["debt_net_inr_cr"], 250.0)
        self.assertEqual(payload["flow_bias"], "BULLISH")

    def test_negative_equity_flow_becomes_bearish(self):
        provider = NSDLProvider(enabled=True)
        payload = provider.normalize_latest_flows({
            "as_of_date": "2026-05-03",
            "equity_net_inr_cr": -8200,
            "debt_net_inr_cr": 0,
        })
        self.assertEqual(payload["flow_bias"], "BEARISH")
        self.assertEqual(payload["impact"], 8)

    def test_positive_equity_flow_becomes_bullish(self):
        provider = NSDLProvider(enabled=True)
        payload = provider.normalize_latest_flows({
            "as_of_date": "2026-05-03",
            "equity_net_inr_cr": 7200,
            "debt_net_inr_cr": 100,
        })
        self.assertEqual(payload["flow_bias"], "BULLISH")
        self.assertEqual(payload["impact"], 8)

    def test_malformed_html_returns_none(self):
        provider = NSDLProvider(enabled=True)
        provider.session.get = mock.Mock(return_value=response_with_text("<html><body>No usable table</body></html>"))
        self.assertIsNone(provider.get_latest_fpi_flows())


if __name__ == "__main__":
    unittest.main()
