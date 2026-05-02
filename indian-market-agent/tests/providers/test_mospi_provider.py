import unittest
from unittest import mock

from backend.providers.mospi_provider import MOSPIProvider


def response_with_text(text: str):
    response = mock.Mock()
    response.text = text
    response.raise_for_status.return_value = None
    return response


class MOSPIProviderTests(unittest.TestCase):
    def test_disabled_provider_returns_none(self):
        provider = MOSPIProvider(enabled=False)
        self.assertIsNone(provider.get_latest_cpi())

    def test_mocked_cpi_gdp_iip_samples_parse(self):
        provider = MOSPIProvider(enabled=True)
        self.assertEqual(provider.normalize_cpi("03-05-2026 CPI 6.2")["cpi_yoy"], 6.2)
        self.assertEqual(provider.normalize_gdp("03-05-2026 GDP 7.4 forecast 6.8")["gdp_growth_yoy"], 7.4)
        self.assertEqual(provider.normalize_iip("03-05-2026 IIP 3.1")["iip_yoy"], 3.1)

    def test_high_cpi_maps_to_bearish_or_caution(self):
        provider = MOSPIProvider(enabled=True)
        payload = provider.normalize_cpi({"as_of_date": "2026-05-03", "cpi_yoy": 6.8})
        self.assertIn(payload["inflation_bias"], {"BEARISH", "CAUTION"})
        self.assertGreaterEqual(payload["impact"], 7)

    def test_malformed_response_returns_none_safely(self):
        provider = MOSPIProvider(enabled=True)
        provider.session.get = mock.Mock(return_value=response_with_text("<html><body>bad</body></html>"))
        self.assertIsNone(provider.get_latest_cpi())


if __name__ == "__main__":
    unittest.main()
