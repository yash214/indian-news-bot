"""Read-only Market Regime Agent."""

from __future__ import annotations

from datetime import datetime, timedelta

try:
    from backend.agents.agent_output_store import save_agent_report
    from backend.agents.market_regime.regime_rules import (
        FILTER_WAIT,
        REGIME_BREAKDOWN,
        REGIME_BREAKOUT_UP,
        REGIME_CHOPPY,
        REGIME_HIGH_VOLATILITY,
        REGIME_RANGE_BOUND,
        REGIME_TRENDING_DOWN,
        REGIME_TRENDING_UP,
        REGIME_UNCLEAR,
        RULESET_VERSION,
    )
    from backend.agents.market_regime.schemas import (
        MarketFeatureSnapshot,
        MarketRegimeGuidance,
        MarketRegimeReport,
        is_supported_symbol,
        normalize_market_symbol,
        unsupported_symbol_warning,
    )
    from backend.agents.market_regime.scoring import score_market_regime
    from backend.core.settings import IST, MARKET_REGIME_REFRESH_SECONDS
except ModuleNotFoundError:
    from agents.agent_output_store import save_agent_report
    from agents.market_regime.regime_rules import (
        FILTER_WAIT,
        REGIME_BREAKDOWN,
        REGIME_BREAKOUT_UP,
        REGIME_CHOPPY,
        REGIME_HIGH_VOLATILITY,
        REGIME_RANGE_BOUND,
        REGIME_TRENDING_DOWN,
        REGIME_TRENDING_UP,
        REGIME_UNCLEAR,
        RULESET_VERSION,
    )
    from agents.market_regime.schemas import MarketFeatureSnapshot, MarketRegimeGuidance, MarketRegimeReport, is_supported_symbol, normalize_market_symbol, unsupported_symbol_warning
    from agents.market_regime.scoring import score_market_regime
    from core.settings import IST, MARKET_REGIME_REFRESH_SECONDS


class MarketRegimeAgent:
    AGENT_NAME = "market_regime_agent"

    def analyze(self, snapshot: MarketFeatureSnapshot | None, symbol: str | None = None) -> MarketRegimeReport:
        clean = normalize_market_symbol(symbol or (snapshot.symbol if snapshot else "NIFTY"))
        if not is_supported_symbol(clean):
            return self._safe_report(clean, warnings=[unsupported_symbol_warning(clean)])
        if snapshot is None:
            return self._safe_report(clean, warnings=[f"Market-regime snapshot unavailable for {clean}."])
        if not snapshot.candles or snapshot.latest_close is None:
            return self._safe_report(
                clean,
                warnings=["Invalid market-regime snapshot: missing candles or latest close."],
                source_status=snapshot.source_status,
            )

        score = score_market_regime(snapshot)
        guidance = _guidance_for_regime(score["primary_regime"], score["volatility_score"], snapshot.india_vix)
        generated_at = datetime.now(IST)

        # TODO: Future learning layer should compare predicted regime vs actual
        # move after 15/30/60 minutes, whether breakout held or failed, whether
        # range stayed intact, whether high-vol warning reduced losses, and
        # whether the choppy filter prevented bad trades.
        report = MarketRegimeReport(
            agent_name=self.AGENT_NAME,
            symbol=clean,
            generated_at=generated_at,
            valid_until=generated_at + timedelta(seconds=MARKET_REGIME_REFRESH_SECONDS),
            stale_after_seconds=MARKET_REGIME_REFRESH_SECONDS,
            primary_regime=score["primary_regime"],
            secondary_regime=score["secondary_regime"],
            confidence=score["confidence"],
            trend_score=score["trend_score"],
            range_score=score["range_score"],
            volatility_score=score["volatility_score"],
            chop_score=score["chop_score"],
            directional_bias=score["directional_bias"],
            trade_filter=score["trade_filter"],
            key_levels=_key_levels(snapshot, score),
            strategy_engine_guidance=guidance,
            reasons=score["reasons"],
            warnings=score["warnings"],
            source_status=snapshot.source_status,
        )
        self._persist_report(report)
        return report

    def _safe_report(self, symbol: str, *, warnings: list[str] | None = None, source_status: dict | None = None) -> MarketRegimeReport:
        generated_at = datetime.now(IST)
        report = MarketRegimeReport(
            agent_name=self.AGENT_NAME,
            symbol=normalize_market_symbol(symbol),
            generated_at=generated_at,
            valid_until=generated_at + timedelta(seconds=MARKET_REGIME_REFRESH_SECONDS),
            stale_after_seconds=MARKET_REGIME_REFRESH_SECONDS,
            primary_regime=REGIME_UNCLEAR,
            secondary_regime=None,
            confidence=0.35,
            trend_score=0,
            range_score=0,
            volatility_score=0,
            chop_score=0,
            directional_bias="NEUTRAL",
            trade_filter=FILTER_WAIT,
            key_levels={"ruleset_version": RULESET_VERSION},
            strategy_engine_guidance=MarketRegimeGuidance(
                avoid_directional_trade=True,
                reduce_position_size=False,
                notes="Market regime is unavailable or unsupported; future strategy engine should wait.",
            ),
            reasons=[],
            warnings=warnings or [],
            source_status=source_status or {},
        )
        self._persist_report(report)
        return report

    def _persist_report(self, report: MarketRegimeReport) -> None:
        try:
            save_agent_report(
                agent_name=self.AGENT_NAME,
                symbol=report.symbol,
                report_type="MARKET_REGIME_REPORT",
                payload=report.to_dict(),
                bias=report.primary_regime,
                confidence=report.confidence,
                ruleset_version="market_regime_rules_v1",
                agent_version="1.0.0",
            )
        except Exception:
            return


def _guidance_for_regime(primary_regime: str, volatility_score: int, india_vix: float | None) -> MarketRegimeGuidance:
    if primary_regime in {REGIME_TRENDING_UP, REGIME_BREAKOUT_UP}:
        return MarketRegimeGuidance(
            bullish_confidence_adjustment=10,
            bearish_confidence_adjustment=-8,
            avoid_directional_trade=False,
            prefer_breakout_strategy=True,
            notes="Market regime supports breakout-first bullish strategy filters without making a buy recommendation.",
        )
    if primary_regime in {REGIME_TRENDING_DOWN, REGIME_BREAKDOWN}:
        return MarketRegimeGuidance(
            bullish_confidence_adjustment=-8,
            bearish_confidence_adjustment=10,
            avoid_directional_trade=False,
            prefer_breakout_strategy=True,
            notes="Market regime supports breakout-first bearish strategy filters without making a sell recommendation.",
        )
    if primary_regime == REGIME_RANGE_BOUND:
        return MarketRegimeGuidance(
            avoid_directional_trade=True,
            prefer_mean_reversion=True,
            notes="Range-bound conditions favor waiting or mean-reversion filters over fresh directional conviction.",
        )
    if primary_regime == REGIME_CHOPPY:
        return MarketRegimeGuidance(
            avoid_directional_trade=True,
            reduce_position_size=True,
            notes="Choppy structure suggests avoiding directional trades and reducing risk.",
        )
    if primary_regime == REGIME_HIGH_VOLATILITY:
        extreme = volatility_score >= 90 or (india_vix is not None and india_vix >= 28)
        return MarketRegimeGuidance(
            avoid_directional_trade=extreme,
            reduce_position_size=True,
            notes="High volatility suggests smaller size; extreme volatility should block fresh directional conviction.",
        )
    return MarketRegimeGuidance(
        avoid_directional_trade=True,
        notes="Regime is unclear; future strategy engine should wait for cleaner structure.",
    )


def _key_levels(snapshot: MarketFeatureSnapshot, score: dict) -> dict:
    return {
        "vwap": snapshot.vwap,
        "ema_9": snapshot.ema_9,
        "ema_21": snapshot.ema_21,
        "opening_range_high": snapshot.opening_range_high,
        "opening_range_low": snapshot.opening_range_low,
        "previous_day_high": snapshot.previous_day_high,
        "previous_day_low": snapshot.previous_day_low,
        "previous_day_close": snapshot.previous_day_close,
        "day_high": snapshot.day_high,
        "day_low": snapshot.day_low,
        "india_vix": snapshot.india_vix,
        "latest_close": snapshot.latest_close,
        "bull_score": score.get("bull_score"),
        "bear_score": score.get("bear_score"),
        "ruleset_version": score.get("ruleset_version") or RULESET_VERSION,
    }
