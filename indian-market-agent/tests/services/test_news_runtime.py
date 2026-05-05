import unittest
from unittest import mock

from backend.core.settings import ALLOWED_REFRESH_WINDOWS
from backend.services import news_runtime


class NewsRuntimeTests(unittest.TestCase):
    def test_get_news_refresh_seconds_returns_int(self):
        self.assertIsInstance(news_runtime.get_news_refresh_seconds(), int)

    def test_set_news_refresh_seconds_accepts_allowed_interval(self):
        original = news_runtime.get_news_refresh_seconds()
        allowed = ALLOWED_REFRESH_WINDOWS[0]
        try:
            with mock.patch("backend.services.news_runtime.persist_refresh_settings") as persist:
                current = news_runtime.set_news_refresh_seconds(allowed)
            self.assertEqual(current, allowed)
            persist.assert_called_once_with(allowed)
        finally:
            with mock.patch("backend.services.news_runtime.persist_refresh_settings"):
                news_runtime.set_news_refresh_seconds(original)

    def test_fetch_feed_articles_handles_mocked_feed(self):
        rss = b"""<?xml version="1.0"?>
        <rss><channel>
          <item>
            <title>RBI keeps repo rate unchanged</title>
            <link>https://example.com/rbi</link>
            <source url="https://example.com">Reuters</source>
            <description>Policy makers said inflation remains a market risk.</description>
            <pubDate>Thu, 30 Apr 2026 10:00:00 GMT</pubDate>
          </item>
        </channel></rss>
        """
        with mock.patch("backend.services.news_runtime._get_feed", return_value=rss):
            src, status, articles = news_runtime.fetch_feed_articles({
                "name": "Mock Feed",
                "url": "https://example.com/rss",
                "scope": "local",
            })
        self.assertEqual(src, "Mock Feed")
        self.assertTrue(status["ok"])
        self.assertEqual(len(articles), 1)
        self.assertEqual(articles[0]["source"], "Reuters")
        self.assertIn("impactMeta", articles[0])

    def test_news_runtime_status_returns_dict(self):
        status = news_runtime.news_runtime_status()
        self.assertIsInstance(status, dict)
        self.assertIn("feed_count", status)
        self.assertIn("refresh_seconds", status)


if __name__ == "__main__":
    unittest.main()
