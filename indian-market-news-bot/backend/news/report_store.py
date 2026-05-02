"""SQLite storage helpers for news-intelligence article analyses and reports."""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path

try:
    from backend.core.persistence import db_connect, init_state_db
    from backend.core.settings import STATE_DB_PATH
    from backend.news.schemas import ArticleAIAnalysis, EventRisk, IndexNewsReport, StrategyEngineGuidance
    from backend.news.text import url_hash
except ModuleNotFoundError:
    from core.persistence import db_connect, init_state_db
    from core.settings import STATE_DB_PATH
    from news.schemas import ArticleAIAnalysis, EventRisk, IndexNewsReport, StrategyEngineGuidance
    from news.text import url_hash


def ensure_news_agent_tables(path: Path = STATE_DB_PATH) -> None:
    init_state_db(path)
    with db_connect(path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS article_ai_analysis (
                article_hash TEXT PRIMARY KEY,
                article_id TEXT NOT NULL,
                title TEXT NOT NULL,
                source TEXT NOT NULL,
                url TEXT NOT NULL,
                published_at TEXT NOT NULL,
                published_ts REAL NOT NULL DEFAULT 0,
                analyzed_at TEXT NOT NULL,
                analyzed_ts REAL NOT NULL DEFAULT 0,
                summary TEXT NOT NULL,
                sentiment TEXT NOT NULL,
                impact_score INTEGER NOT NULL,
                confidence REAL NOT NULL,
                category TEXT NOT NULL,
                affected_indices_json TEXT NOT NULL,
                affected_sectors_json TEXT NOT NULL,
                macro_tags_json TEXT NOT NULL,
                event_risk_json TEXT NOT NULL,
                trade_filter TEXT NOT NULL,
                strategy_engine_guidance_json TEXT NOT NULL,
                reasons_json TEXT NOT NULL,
                raw_llm_json TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS news_index_reports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                index_symbol TEXT NOT NULL,
                generated_at TEXT NOT NULL,
                generated_ts REAL NOT NULL,
                lookback_hours INTEGER NOT NULL,
                overall_sentiment TEXT NOT NULL,
                impact_score INTEGER NOT NULL,
                confidence REAL NOT NULL,
                trade_filter TEXT NOT NULL,
                market_regime_hint TEXT NOT NULL,
                major_drivers_json TEXT NOT NULL,
                bullish_factors_json TEXT NOT NULL,
                bearish_factors_json TEXT NOT NULL,
                affected_indices_json TEXT NOT NULL,
                affected_sectors_json TEXT NOT NULL,
                risk_events_json TEXT NOT NULL,
                strategy_engine_guidance_json TEXT NOT NULL,
                top_articles_json TEXT NOT NULL,
                summary TEXT NOT NULL,
                raw_report_json TEXT NOT NULL
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_article_ai_analysis_window "
            "ON article_ai_analysis(published_ts, analyzed_ts)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_news_index_reports_latest "
            "ON news_index_reports(index_symbol, generated_ts)"
        )
        conn.commit()


def article_analysis_hash(analysis: ArticleAIAnalysis | dict) -> str:
    payload = analysis.to_dict() if isinstance(analysis, ArticleAIAnalysis) else analysis
    seed = str(payload.get("article_id") or payload.get("url") or payload.get("title") or "")
    return url_hash(seed) if seed else ""


def save_article_ai_analysis(analysis: ArticleAIAnalysis | dict, path: Path = STATE_DB_PATH) -> None:
    analysis = analysis if isinstance(analysis, ArticleAIAnalysis) else ArticleAIAnalysis.from_dict(analysis)
    article_hash = article_analysis_hash(analysis)
    if not article_hash or not analysis.summary:
        return
    ensure_news_agent_tables(path)
    now = datetime.now(timezone.utc).isoformat()
    analyzed_ts = _iso_to_epoch(analysis.analyzed_at) or time.time()
    with db_connect(path) as conn:
        conn.execute(
            """
            INSERT INTO article_ai_analysis(
                article_hash, article_id, title, source, url, published_at, published_ts,
                analyzed_at, analyzed_ts, summary, sentiment, impact_score, confidence,
                category, affected_indices_json, affected_sectors_json, macro_tags_json,
                event_risk_json, trade_filter, strategy_engine_guidance_json, reasons_json,
                raw_llm_json, updated_at
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(article_hash) DO UPDATE SET
                article_id = excluded.article_id,
                title = excluded.title,
                source = excluded.source,
                url = excluded.url,
                published_at = excluded.published_at,
                published_ts = excluded.published_ts,
                analyzed_at = excluded.analyzed_at,
                analyzed_ts = excluded.analyzed_ts,
                summary = excluded.summary,
                sentiment = excluded.sentiment,
                impact_score = excluded.impact_score,
                confidence = excluded.confidence,
                category = excluded.category,
                affected_indices_json = excluded.affected_indices_json,
                affected_sectors_json = excluded.affected_sectors_json,
                macro_tags_json = excluded.macro_tags_json,
                event_risk_json = excluded.event_risk_json,
                trade_filter = excluded.trade_filter,
                strategy_engine_guidance_json = excluded.strategy_engine_guidance_json,
                reasons_json = excluded.reasons_json,
                raw_llm_json = excluded.raw_llm_json,
                updated_at = excluded.updated_at
            """,
            (
                article_hash,
                analysis.article_id,
                analysis.title,
                analysis.source,
                analysis.url,
                analysis.published_at,
                float(analysis.published_ts or 0.0),
                analysis.analyzed_at,
                analyzed_ts,
                analysis.summary,
                analysis.sentiment,
                int(analysis.impact_score),
                float(analysis.confidence),
                analysis.category,
                _json(analysis.affected_indices),
                _json(analysis.affected_sectors),
                _json(analysis.macro_tags),
                _json(analysis.event_risk.to_dict()),
                analysis.trade_filter,
                _json(analysis.strategy_engine_guidance.to_dict()),
                _json(analysis.reasons),
                _json(analysis.raw_llm_json),
                now,
            ),
        )
        conn.commit()


def load_recent_article_ai_analyses(lookback_hours: int = 24, path: Path = STATE_DB_PATH) -> list[ArticleAIAnalysis]:
    ensure_news_agent_tables(path)
    cutoff = time.time() - max(1, int(lookback_hours or 24)) * 3600
    with db_connect(path) as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM article_ai_analysis
            WHERE COALESCE(NULLIF(published_ts, 0), analyzed_ts, 0) >= ?
            ORDER BY impact_score DESC, confidence DESC, analyzed_ts DESC
            """,
            (cutoff,),
        ).fetchall()
    return [_analysis_from_row(row) for row in rows]


def save_index_news_report(report: IndexNewsReport | dict, path: Path = STATE_DB_PATH) -> None:
    report = report if isinstance(report, IndexNewsReport) else _report_from_dict(report)
    ensure_news_agent_tables(path)
    payload = report.to_dict()
    generated_ts = _iso_to_epoch(report.generated_at) or time.time()
    with db_connect(path) as conn:
        conn.execute(
            """
            INSERT INTO news_index_reports(
                index_symbol, generated_at, generated_ts, lookback_hours, overall_sentiment,
                impact_score, confidence, trade_filter, market_regime_hint, major_drivers_json,
                bullish_factors_json, bearish_factors_json, affected_indices_json,
                affected_sectors_json, risk_events_json, strategy_engine_guidance_json,
                top_articles_json, summary, raw_report_json
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                report.index,
                report.generated_at,
                generated_ts,
                report.lookback_hours,
                report.overall_sentiment,
                report.impact_score,
                report.confidence,
                report.trade_filter,
                report.market_regime_hint,
                _json(report.major_drivers),
                _json(report.bullish_factors),
                _json(report.bearish_factors),
                _json(report.affected_indices),
                _json(report.affected_sectors),
                _json(report.risk_events),
                _json(report.strategy_engine_guidance.to_dict()),
                _json(report.top_articles),
                report.summary,
                _json(payload),
            ),
        )
        conn.commit()


def load_latest_index_news_report(index: str = "NIFTY", path: Path = STATE_DB_PATH) -> dict | None:
    ensure_news_agent_tables(path)
    with db_connect(path) as conn:
        row = conn.execute(
            """
            SELECT raw_report_json
            FROM news_index_reports
            WHERE index_symbol = ?
            ORDER BY generated_ts DESC
            LIMIT 1
            """,
            (str(index or "NIFTY").upper().replace(" ", ""),),
        ).fetchone()
    if not row:
        return None
    try:
        payload = json.loads(row["raw_report_json"])
    except (TypeError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _analysis_from_row(row) -> ArticleAIAnalysis:
    return ArticleAIAnalysis(
        article_id=str(row["article_id"] or ""),
        title=str(row["title"] or ""),
        source=str(row["source"] or ""),
        url=str(row["url"] or ""),
        published_at=str(row["published_at"] or ""),
        analyzed_at=str(row["analyzed_at"] or ""),
        published_ts=float(row["published_ts"] or 0.0),
        summary=str(row["summary"] or ""),
        sentiment=str(row["sentiment"] or "neutral"),
        impact_score=int(row["impact_score"] or 0),
        confidence=float(row["confidence"] or 0.0),
        category=str(row["category"] or "general"),
        affected_indices=_loads_list(row["affected_indices_json"]),
        affected_sectors=_loads_list(row["affected_sectors_json"]),
        macro_tags=_loads_list(row["macro_tags_json"]),
        event_risk=EventRisk.from_dict(_loads_dict(row["event_risk_json"])),
        trade_filter=str(row["trade_filter"] or "NO_FILTER"),
        strategy_engine_guidance=StrategyEngineGuidance.from_dict(_loads_dict(row["strategy_engine_guidance_json"])),
        reasons=_loads_list(row["reasons_json"]),
        raw_llm_json=_loads_dict(row["raw_llm_json"]),
    )


def _report_from_dict(payload: dict) -> IndexNewsReport:
    guidance = StrategyEngineGuidance.from_dict(payload.get("strategy_engine_guidance"))
    clean = dict(payload)
    clean["strategy_engine_guidance"] = guidance
    return IndexNewsReport(**clean)


def _json(value) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _loads_list(value: str) -> list:
    try:
        parsed = json.loads(value or "[]")
    except (TypeError, json.JSONDecodeError):
        return []
    return parsed if isinstance(parsed, list) else []


def _loads_dict(value: str) -> dict:
    try:
        parsed = json.loads(value or "{}")
    except (TypeError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _iso_to_epoch(value: str) -> float:
    text = str(value or "").strip()
    if not text:
        return 0.0
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return 0.0
