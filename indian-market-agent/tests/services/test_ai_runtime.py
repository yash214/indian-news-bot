import unittest
import time
from unittest import mock

from backend.services import ai_runtime


class AiRuntimeTests(unittest.TestCase):
    def test_ai_chat_provider_name_returns_string(self):
        self.assertIsInstance(ai_runtime.ai_chat_provider_name(), str)

    def test_trim_text_compacts_and_truncates(self):
        self.assertEqual(ai_runtime._trim_text(" one\n two\tthree ", 50), "one two three")
        self.assertEqual(ai_runtime._trim_text("abcdef", 3), "abc...")

    def test_build_ai_chat_prompt_includes_context(self):
        prompt = ai_runtime.build_ai_chat_prompt(
            "why is bent crude up?",
            {"internetNews": [{"source": "Reuters", "title": "Brent rises"}]},
            history=[],
        )
        self.assertIn("interpret it as \"Brent crude\"", prompt)
        self.assertIn("internetNews", prompt)
        self.assertIn("Reuters", prompt)

    def test_ai_summary_cache_key_is_deterministic(self):
        article = {"id": "abc", "link": "https://example.com/a", "title": "Title"}
        self.assertEqual(ai_runtime.ai_summary_cache_key(article), ai_runtime.ai_summary_cache_key(dict(article)))

    def test_article_link_supports_direct_extraction_filters_google_news(self):
        self.assertFalse(ai_runtime.article_link_supports_direct_extraction("https://news.google.com/rss/articles/example"))
        self.assertTrue(ai_runtime.article_link_supports_direct_extraction("https://www.livemint.com/market/story.html"))

    def test_ai_summary_progress_for_articles_counts_complete_items(self):
        articles = [
            {"id": "ai", "summary": "AI summary", "summarySource": "ai", "ts": time.time()},
            {"id": "plain", "summary": "Feed summary", "ts": time.time()},
        ]
        progress = ai_runtime.ai_summary_progress_for_articles(articles)
        self.assertEqual(progress["total"], 2)
        self.assertEqual(progress["complete"], 1)

    def test_ai_runtime_status_returns_dict_without_key(self):
        with mock.patch.dict(ai_runtime.os.environ, {"BEDROCK_API_KEY": "", "AI_CHAT_PROVIDER": ""}, clear=False):
            status = ai_runtime.ai_runtime_status()
        self.assertIsInstance(status, dict)
        self.assertIn("chat_provider", status)
        self.assertIn("news_summaries_enabled", status)


if __name__ == "__main__":
    unittest.main()
