import unittest
from unittest import mock

from backend.providers.rbi_provider import RBIProvider


def response_with_text(text: str):
    response = mock.Mock()
    response.text = text
    response.raise_for_status.return_value = None
    return response


class RBIProviderTests(unittest.TestCase):
    def test_disabled_provider_returns_none(self):
        provider = RBIProvider(enabled=False)
        self.assertIsNone(provider.get_policy_rate_snapshot())

    def test_mocked_html_parses_repo_crr_slr(self):
        html = """
        <html><body>
        <div>03-05-2026 Repo Rate 6.50 Reverse Repo 3.35 CRR 4.50 SLR 18.00 SDF 6.25 MSF 6.75 Bank Rate 6.75</div>
        </body></html>
        """
        provider = RBIProvider(enabled=True)
        provider.session.get = mock.Mock(return_value=response_with_text(html))

        payload = provider.get_policy_rate_snapshot()

        self.assertEqual(payload["repo_rate"], 6.5)
        self.assertEqual(payload["crr"], 4.5)
        self.assertEqual(payload["slr"], 18.0)
        self.assertEqual(payload["policy_bias"], "NEUTRAL")

    def test_repo_hike_from_previous_snapshot_maps_to_bearish(self):
        provider = RBIProvider(enabled=True)
        payload = provider.normalize_policy_rate_snapshot(
            {"as_of_date": "2026-05-03", "repo_rate": 6.5, "crr": 4.5, "slr": 18.0},
            previous_snapshot={"repo_rate": 6.25, "crr": 4.5},
        )
        self.assertEqual(payload["policy_bias"], "BEARISH")
        self.assertEqual(payload["impact"], 8)

    def test_malformed_response_returns_none(self):
        provider = RBIProvider(enabled=True)
        provider.session.get = mock.Mock(return_value=response_with_text("<html><body>invalid</body></html>"))
        self.assertIsNone(provider.get_policy_rate_snapshot())


if __name__ == "__main__":
    unittest.main()
