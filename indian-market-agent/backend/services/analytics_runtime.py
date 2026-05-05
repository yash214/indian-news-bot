"""Market analytics and derivatives payload generation helpers."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
import re
from typing import Any

try:
    from backend.agents.news.scoring import build_sector_news_scores, sector_bias_label
    from backend.core.settings import IST
    from backend.market.catalog import PRIMARY_LEVEL_LABELS, SECTOR_TO_INDEX
    from backend.market.math import (
        bias_from_score,
        clamp,
        conviction_from_score,
        day_type_from_context,
        format_level,
        implied_move_points,
        intraday_range_pct,
        intraday_return,
        relative_gap,
        round_or_none,
        score_band,
    )
except ModuleNotFoundError:
    from agents.news.scoring import build_sector_news_scores, sector_bias_label
    from core.settings import IST
    from market.catalog import PRIMARY_LEVEL_LABELS, SECTOR_TO_INDEX
    from market.math import (
        bias_from_score,
        clamp,
        conviction_from_score,
        day_type_from_context,
        format_level,
        implied_move_points,
        intraday_range_pct,
        intraday_return,
        relative_gap,
        round_or_none,
        score_band,
    )


def build_live_only_signal(symbol: str, live_quote: dict) -> dict:
    price = live_quote["price"]
    day_high = live_quote.get("day_high", price)
    day_low = live_quote.get("day_low", price)
    pct = live_quote.get("pct", 0.0)
    if pct >= 1.25:
        trend = "Intraday strength"
    elif pct <= -1.25:
        trend = "Intraday weakness"
    else:
        trend = "Range"

    if day_high and price >= day_high * 0.9975:
        signal = "Near day high"
    elif day_low and price <= day_low * 1.0025:
        signal = "Near day low"
    elif pct >= 0.5:
        signal = "Buyer support"
    elif pct <= -0.5:
        signal = "Seller pressure"
    else:
        signal = "Wait for setup"

    breakout_gap = ((price / day_high) - 1) * 100 if day_high else None
    return {
        "symbol": _clean_general_symbol(symbol),
        "name": live_quote.get("name") or symbol,
        "price": round(price, 2),
        "change": round(live_quote.get("change", 0.0), 2),
        "pct": round(pct, 2),
        "trend": trend,
        "signal": signal,
        "rsi14": None,
        "ret5": None,
        "ret20": None,
        "vol20": None,
        "sma20": None,
        "sma50": None,
        "high20": round_or_none(day_high),
        "low20": round_or_none(day_low),
        "support": round_or_none(day_low),
        "resistance": round_or_none(day_high),
        "volumeRatio": None,
        "breakoutGap": round_or_none(breakout_gap),
        "drawdownFromHigh": round_or_none(breakout_gap),
    }


def build_symbol_signal(
    symbol: str,
    live_quote: dict | None = None,
    is_index: bool = False,
    context=None,
) -> dict | None:
    return build_live_only_signal(symbol, live_quote) if live_quote else None


def build_market_analytics_payload(
    articles: list[dict],
    ticks: dict,
    index_snapshot: dict,
    symbols: list[str],
    quote_map: dict[str, dict] | None = None,
    context=None,
) -> dict:
    sector_news = build_sector_news_scores(articles)
    sector_rows = []
    sector_map = {}

    for sector, label in SECTOR_TO_INDEX.items():
        snap = index_snapshot.get(label)
        news = sector_news.get(sector, {"score": 0.0, "count": 0, "bull": 0, "bear": 0})
        bias, tone = sector_bias_label(news["score"])
        row = {
            "sector": sector,
            "label": label,
            "pct": round_or_none(snap.get("pct") if snap else None),
            "price": round_or_none(snap.get("price") if snap else None),
            "count": news["count"],
            "bull": news["bull"],
            "bear": news["bear"],
            "newsScore": news["score"],
            "newsBias": bias,
            "tone": tone,
        }
        sector_rows.append(row)
        sector_map[sector] = row

    tradable_rows = [row for row in sector_rows if row["pct"] is not None and row["sector"] != "General"]
    leaders = sorted(tradable_rows, key=lambda row: row["pct"], reverse=True)
    positive = sum(1 for row in tradable_rows if row["pct"] > 0)
    negative = sum(1 for row in tradable_rows if row["pct"] < 0)

    primary_signals = []
    primary_map = {}
    if ticks or index_snapshot:
        for label in PRIMARY_LEVEL_LABELS:
            live_quote = index_snapshot.get(label) or ticks.get(label)
            signal = build_symbol_signal(label, live_quote=live_quote, is_index=True, context=context)
            if signal:
                primary_signals.append({"label": label, **signal})
                primary_map[label] = signal
            elif live_quote:
                primary_signals.append({
                    "label": label,
                    "symbol": label.upper().replace(" ", ""),
                    "name": label,
                    "price": live_quote["price"],
                    "change": live_quote["change"],
                    "pct": live_quote["pct"],
                    "trend": "Live",
                    "signal": "Live snapshot",
                })

    nifty = (index_snapshot.get("Nifty 50") or ticks.get("Nifty 50") or {})
    bank = (index_snapshot.get("Nifty Bank") or ticks.get("Nifty Bank") or {})
    it_idx = (index_snapshot.get("Nifty IT") or ticks.get("Nifty IT") or {})
    midcap = (index_snapshot.get("Nifty Midcap") or ticks.get("Nifty Midcap") or {})
    smallcap = (index_snapshot.get("Nifty Smallcap") or ticks.get("Nifty Smallcap") or {})
    vix = (index_snapshot.get("India VIX") or ticks.get("VIX") or {})
    crude = ticks.get("Crude Oil") or {}

    risk_score = 0
    nifty_pct = nifty.get("pct")
    bank_pct = bank.get("pct")
    vix_price = vix.get("price")
    vix_chg = vix.get("pct")
    crude_price = crude.get("price")
    crude_pct = crude.get("pct")
    if nifty_pct is not None:
        risk_score += 1 if nifty_pct > 0 else -1
    if bank_pct is not None and nifty_pct is not None:
        risk_score += 1 if bank_pct >= nifty_pct else -1
    if midcap.get("pct") is not None and nifty_pct is not None and midcap["pct"] > nifty_pct:
        risk_score += 1
    if smallcap.get("pct") is not None and nifty_pct is not None and smallcap["pct"] > nifty_pct:
        risk_score += 1
    if positive > negative:
        risk_score += 1
    elif negative > positive:
        risk_score -= 1
    if vix_price:
        risk_score += 1 if vix_price < 15 else -1
    if vix_chg is not None:
        risk_score += 1 if vix_chg < 0 else -1 if vix_chg > 1.5 else 0

    if risk_score >= 4:
        regime = {"label": "Risk-On Trend", "tone": "bull", "detail": "Breadth is supportive and volatility is contained."}
    elif risk_score <= -3:
        regime = {"label": "Risk-Off Tape", "tone": "bear", "detail": "Size down and respect headline risk while VIX is elevated."}
    else:
        regime = {"label": "Rotation Market", "tone": "neutral", "detail": "Leadership is selective, so sector selection matters more than headline index direction."}

    news_score = round(sum(item["score"] for item in sector_news.values()), 2)
    news_tone = "bull" if news_score > 5 else "bear" if news_score < -5 else "neutral"
    breadth_label = f"{positive} sectors up / {negative} down"
    leadership = leaders[0]["sector"] if leaders else "Mixed"
    laggard = leaders[-1]["sector"] if leaders else "Mixed"
    breadth_spread = positive - negative
    smid_gap = relative_gap(smallcap.get("pct"), nifty_pct)
    bank_vs_it = relative_gap(bank_pct, it_idx.get("pct"))
    crude_tone = "bull" if crude_pct and crude_pct >= 1 else "bear" if crude_pct and crude_pct <= -1 else "neutral"
    if crude_price is not None and crude_pct is not None:
        crude_detail = f"{crude_pct:+.2f}% on the day. Useful for tracking energy names, OMCs, and inflation-sensitive moves."
    elif crude_price is not None:
        crude_detail = "Tracking front-month crude oil futures for energy-sensitive setups."
    else:
        crude_detail = "Tracking front-month crude oil futures for energy-sensitive setups."

    overview_cards = [
        {"label": "Regime", "value": regime["label"], "detail": regime["detail"], "tone": regime["tone"]},
        {"label": "Breadth", "value": breadth_label, "detail": f"Spread {breadth_spread:+d} across key sectors", "tone": "bull" if breadth_spread > 0 else "bear" if breadth_spread < 0 else "neutral"},
        {"label": "Volatility", "value": f"{vix_price:.2f} VIX" if vix_price is not None else "Unavailable", "detail": "Calm tape" if vix_price and vix_price < 14 else "Higher hedging demand" if vix_price and vix_price >= 16 else "Middle of the range", "tone": "bull" if vix_price and vix_price < 14 else "bear" if vix_price and vix_price >= 16 else "neutral"},
        {"label": "Crude Oil", "value": f"${crude_price:.2f}" if crude_price is not None else "Unavailable", "detail": crude_detail, "tone": crude_tone},
        {"label": "Leadership", "value": leadership, "detail": f"Weakest pocket: {laggard}", "tone": "bull" if leaders and leaders[0]["pct"] and leaders[0]["pct"] > 0 else "neutral"},
        {"label": "SMID vs Nifty", "value": f"{smid_gap:+.2f}%" if smid_gap is not None else "Unavailable", "detail": "Positive means broader risk appetite is expanding beyond the headline index.", "tone": "bull" if smid_gap and smid_gap > 0 else "bear" if smid_gap and smid_gap < 0 else "neutral"},
        {"label": "News Pulse", "value": f"{news_score:+.1f}", "detail": "Weighted from recent high-impact headlines by sector.", "tone": news_tone},
    ]

    alerts = []
    if leaders:
        leader_row = leaders[0]
        alerts.append(f"{leader_row['sector']} is leading today at {leader_row['pct']:+.2f}% with {leader_row['newsBias'].lower()} news flow.")
    if vix_price is not None:
        if vix_price >= 16:
            alerts.append(f"India VIX is at {vix_price:.2f}. Expect wider intraday swings and demand cleaner entries.")
        elif vix_price <= 13.5:
            alerts.append(f"India VIX is muted at {vix_price:.2f}, which usually favors trend-following over panic hedging.")
    if crude_pct is not None and abs(crude_pct) >= 1.5:
        move = "spiking" if crude_pct > 0 else "sliding"
        alerts.append(f"Crude oil is {move} {abs(crude_pct):.2f}% today. Watch energy names, OMCs, and inflation-sensitive sectors for spillover.")
    if bank_vs_it is not None and abs(bank_vs_it) >= 0.75:
        lead = "Banks" if bank_vs_it > 0 else "IT"
        alerts.append(f"{lead} is outperforming the other leadership pocket by {abs(bank_vs_it):.2f}%, a useful clue for intraday sector rotation.")
    if smid_gap is not None and smid_gap >= 0.75:
        alerts.append(f"Smallcaps are beating Nifty 50 by {smid_gap:.2f}%. That usually signals broader participation and better breakout follow-through.")
    elif smid_gap is not None and smid_gap <= -0.75:
        alerts.append(f"Smallcaps are lagging Nifty 50 by {abs(smid_gap):.2f}%, which is often a warning that risk appetite is narrowing.")

    symbol_signals = []
    symbol_map = {}
    fetch_live_quote = _context_callable(context, "fetch_live_quote")
    for sym in symbols:
        live_quote = (quote_map or {}).get(sym)
        if live_quote is None and quote_map is None and fetch_live_quote is not None:
            try:
                live_quote = fetch_live_quote(sym)
            except Exception:
                live_quote = None
        try:
            signal = build_symbol_signal(sym, live_quote=live_quote, is_index=False, context=context)
        except Exception:
            signal = None
        if signal:
            symbol_signals.append(signal)
            symbol_map[signal["symbol"]] = signal

    key_levels = []
    for item in primary_signals:
        key_levels.append({
            "label": item["label"],
            "price": item.get("price"),
            "pct": item.get("pct"),
            "trend": item.get("trend"),
            "rsi14": item.get("rsi14"),
            "support": item.get("support"),
            "resistance": item.get("resistance"),
            "signal": item.get("signal"),
        })

    return {
        "generatedAt": _ist_now(context).strftime("%H:%M:%S"),
        "overviewCards": overview_cards,
        "alerts": alerts[:5],
        "sectorBoard": leaders,
        "sectorMap": sector_map,
        "keyLevels": key_levels,
        "watchlistSignals": symbol_signals,
        "symbolMap": symbol_map,
        "regime": regime,
        "primary": primary_signals,
    }


def build_derivatives_analysis_payload(
    articles: list[dict],
    ticks: dict,
    index_snapshot: dict,
    option_chain: dict | None = None,
    context=None,
    price_history: dict[str, list[float]] | None = None,
    market_status: dict | None = None,
) -> dict:
    price_history = price_history or {}
    market_status = market_status or _market_status(context)
    sector_news = build_sector_news_scores(articles)

    nifty = (index_snapshot.get("Nifty 50") or ticks.get("Nifty 50") or {})
    bank = (index_snapshot.get("Nifty Bank") or ticks.get("Nifty Bank") or {})
    it_idx = (index_snapshot.get("Nifty IT") or ticks.get("Nifty IT") or {})
    midcap = (index_snapshot.get("Nifty Midcap") or ticks.get("Nifty Midcap") or {})
    smallcap = (index_snapshot.get("Nifty Smallcap") or ticks.get("Nifty Smallcap") or {})
    vix = (index_snapshot.get("India VIX") or ticks.get("VIX") or {})
    crude = ticks.get("Crude Oil") or {}
    brent = ticks.get("Brent Crude") or {}
    usd_inr = ticks.get("USD/INR") or {}
    gold = ticks.get("Gold") or {}

    nifty_pct = nifty.get("pct")
    bank_pct = bank.get("pct")
    it_pct = it_idx.get("pct")
    midcap_pct = midcap.get("pct")
    smallcap_pct = smallcap.get("pct")
    vix_price = vix.get("price")
    vix_pct = vix.get("pct")
    crude_price = crude.get("price")
    crude_pct = crude.get("pct")
    usd_pct = usd_inr.get("pct")

    bank_vs_nifty = relative_gap(bank_pct, nifty_pct)
    it_vs_nifty = relative_gap(it_pct, nifty_pct)
    midcap_vs_nifty = relative_gap(midcap_pct, nifty_pct)
    smallcap_vs_nifty = relative_gap(smallcap_pct, nifty_pct)

    banking_news = sector_news.get("Banking", {"score": 0.0})
    it_news = sector_news.get("IT", {"score": 0.0})
    energy_news = sector_news.get("Energy", {"score": 0.0})
    general_news = sector_news.get("General", {"score": 0.0})
    headline_news_score = round(
        banking_news.get("score", 0.0) * 0.35
        + it_news.get("score", 0.0) * 0.2
        + energy_news.get("score", 0.0) * 0.15
        + general_news.get("score", 0.0) * 0.3,
        2,
    )

    nifty_hist = price_history.get("Nifty 50", [])
    bank_hist = price_history.get("Nifty Bank", [])
    vix_hist = price_history.get("VIX", [])
    nifty_flow_3 = intraday_return(nifty_hist, 3)
    nifty_flow_8 = intraday_return(nifty_hist, 8)
    bank_flow_3 = intraday_return(bank_hist, 3)
    bank_flow_8 = intraday_return(bank_hist, 8)
    vix_flow_3 = intraday_return(vix_hist, 3)
    focus_label = "Nifty Bank" if (bank_vs_nifty or 0) > 0.35 or abs(bank_flow_8 or 0) > abs(nifty_flow_8 or 0) else "Nifty 50"
    focus_display = "Bank Nifty" if focus_label == "Nifty Bank" else focus_label
    focus_hist = bank_hist if focus_label == "Nifty Bank" else nifty_hist
    focus_flow_3 = bank_flow_3 if focus_label == "Nifty Bank" else nifty_flow_3
    focus_flow_8 = bank_flow_8 if focus_label == "Nifty Bank" else nifty_flow_8
    focus_intraday_range = intraday_range_pct(focus_hist, 12)

    trend_component = score_band(nifty_pct, 0.75, 0.2, -0.2, -0.75) + score_band(nifty_flow_8, 0.45, 0.12, -0.12, -0.45)
    leadership_component = score_band(bank_vs_nifty, 0.6, 0.2, -0.2, -0.6) + score_band(smallcap_vs_nifty, 0.5, 0.15, -0.15, -0.5)

    vol_component = 0
    if vix_price is not None:
        vol_component += 1 if vix_price <= 14 else -1 if vix_price >= 16.5 else 0
    if vix_pct is not None:
        vol_component += 1 if vix_pct <= -1.5 else -1 if vix_pct >= 1.5 else 0

    macro_component = 0
    if crude_pct is not None:
        macro_component += 1 if crude_pct <= -0.8 else -1 if crude_pct >= 1 else 0
    if usd_pct is not None:
        macro_component += 1 if usd_pct <= -0.15 else -1 if usd_pct >= 0.15 else 0

    news_component = score_band(headline_news_score, 10, 2.5, -2.5, -10)
    flow_component = score_band(focus_flow_3, 0.35, 0.1, -0.1, -0.35) + score_band(focus_flow_8, 0.55, 0.15, -0.15, -0.55)
    composite_score = trend_component + leadership_component + vol_component + macro_component + news_component + flow_component

    data_points = sum(
        value is not None
        for value in [
            nifty_pct, bank_pct, bank_vs_nifty, smallcap_vs_nifty, vix_price, vix_pct, crude_pct, usd_pct,
            headline_news_score, nifty_flow_3, nifty_flow_8, bank_flow_3, bank_flow_8,
        ]
    )
    bias_label, bias_tone = bias_from_score(composite_score)
    conviction = conviction_from_score(composite_score, data_points)
    bull_prob = int(clamp(50 + composite_score * 5, 18, 82))
    bear_prob = 100 - bull_prob
    day_type, day_type_detail = day_type_from_context(composite_score, vix_price, focus_flow_3, focus_intraday_range)

    primary_signal_map = {}
    if ticks or index_snapshot:
        for label in PRIMARY_LEVEL_LABELS:
            live_quote = index_snapshot.get(label) or ticks.get(label) or (ticks.get("VIX") if label == "India VIX" else None)
            try:
                primary_signal_map[label] = build_symbol_signal(label, live_quote=live_quote, is_index=True, context=context)
            except Exception:
                primary_signal_map[label] = None

    focus_live_quote = bank if focus_label == "Nifty Bank" else nifty
    focus_signal = primary_signal_map.get(focus_label)
    focus_price = focus_signal.get("price") if focus_signal else focus_live_quote.get("price")
    expected_move_points, expected_move_pct = implied_move_points(focus_price, vix_price)

    overview_cards = [
        {
            "label": "Index Leader",
            "value": focus_display if focus_price is not None else "Waiting",
            "detail": "This is where the cleaner short-term derivatives expression is currently clustering.",
            "tone": bias_tone,
        },
        {
            "label": "Volatility Regime",
            "value": f"{vix_price:.2f} VIX" if vix_price is not None else "Unavailable",
            "detail": "Higher VIX usually means wider option premiums and faster sentiment flips."
            if vix_price is not None
            else "Waiting for volatility data.",
            "tone": "bear" if vix_price is not None and vix_price >= 16 else "bull" if vix_price is not None and vix_price <= 13.5 else "neutral",
        },
        {
            "label": "Bank vs Nifty",
            "value": f"{bank_vs_nifty:+.2f}%" if bank_vs_nifty is not None else "Unavailable",
            "detail": "Positive means Bank Nifty is outperforming the headline index.",
            "tone": "bull" if bank_vs_nifty is not None and bank_vs_nifty > 0 else "bear" if bank_vs_nifty is not None and bank_vs_nifty < 0 else "neutral",
        },
        {
            "label": "Short-Term Flow",
            "value": f"{focus_flow_8:+.2f}%" if focus_flow_8 is not None else "Unavailable",
            "detail": f"Recent {focus_display} tape over the latest dashboard ticks.",
            "tone": "bull" if focus_flow_8 is not None and focus_flow_8 > 0 else "bear" if focus_flow_8 is not None and focus_flow_8 < 0 else "neutral",
        },
        {
            "label": "Crude Impulse",
            "value": f"${crude_price:.2f}" if crude_price is not None else "Unavailable",
            "detail": "Crude matters for OMCs, inflation expectations, and broad risk tone.",
            "tone": "bull" if crude_pct is not None and crude_pct < 0 else "bear" if crude_pct is not None and crude_pct > 1 else "neutral",
        },
        {
            "label": "Rupee Pulse",
            "value": f"{usd_inr.get('price', 0):.2f}" if usd_inr.get("price") is not None else "Unavailable",
            "detail": "USD/INR pressure often feeds into imported inflation and foreign-flow sentiment.",
            "tone": "bear" if usd_pct is not None and usd_pct > 0.3 else "bull" if usd_pct is not None and usd_pct < -0.3 else "neutral",
        },
    ]

    prediction_cards = [
        {
            "label": "Model Bias",
            "value": bias_label,
            "detail": "Composite directional read from index trend, breadth, intraday tape, volatility, macro, and news.",
            "tone": bias_tone,
        },
        {
            "label": "Conviction",
            "value": f"{conviction} / 100",
            "detail": "Higher means more factors are aligned in the same direction. It is still context, not certainty.",
            "tone": "bull" if conviction >= 68 and bias_tone == "bull" else "bear" if conviction >= 68 and bias_tone == "bear" else "neutral",
        },
        {
            "label": "Bull Path",
            "value": f"{bull_prob}%",
            "detail": "Probability-weighted leaning toward upside continuation from the current composite score.",
            "tone": "bull" if bull_prob > 55 else "neutral",
        },
        {
            "label": "Bear Path",
            "value": f"{bear_prob}%",
            "detail": "Probability-weighted leaning toward downside continuation from the current composite score.",
            "tone": "bear" if bear_prob > 55 else "neutral",
        },
        {
            "label": "Day Type",
            "value": day_type,
            "detail": day_type_detail,
            "tone": "bull" if "Trend" in day_type and bias_tone == "bull" else "bear" if "Trend" in day_type and bias_tone == "bear" else "neutral",
        },
        {
            "label": "Expected Move",
            "value": f"{expected_move_points:,.0f} pts" if expected_move_points is not None else "Unavailable",
            "detail": f"Approx {expected_move_pct:.2f}% 1-day move from India VIX on {focus_display}."
            if expected_move_pct is not None
            else "Waiting for a valid price and VIX snapshot to estimate range.",
            "tone": "neutral",
        },
    ]

    context_notes = []
    context_notes.append(
        f"{focus_display} is the cleaner derivatives focus right now, with composite score {composite_score:+d} and {conviction}/100 conviction."
    )
    if bank_vs_nifty is not None:
        lead = "Banks" if bank_vs_nifty > 0 else "The headline index"
        context_notes.append(f"{lead} are leading by {abs(bank_vs_nifty):.2f}% versus Nifty 50, which matters for where the next clean impulse is most likely to show up.")
    if focus_flow_3 is not None and focus_flow_8 is not None:
        context_notes.append(f"Short-term tape check: {focus_display} is {focus_flow_3:+.2f}% over the last 3 ticks and {focus_flow_8:+.2f}% over the last 8 ticks.")
    if smallcap_vs_nifty is not None and abs(smallcap_vs_nifty) >= 0.5:
        breadth_mood = "broader participation is expanding" if smallcap_vs_nifty > 0 else "risk appetite is narrowing into larger names"
        context_notes.append(f"Smallcaps are {abs(smallcap_vs_nifty):.2f}% {'ahead of' if smallcap_vs_nifty > 0 else 'behind'} Nifty 50, suggesting {breadth_mood}.")
    if vix_price is not None:
        if vix_price >= 16:
            context_notes.append(f"India VIX at {vix_price:.2f} means intraday trend calls need more room and faster invalidation discipline.")
        elif vix_price <= 13.5:
            context_notes.append(f"India VIX at {vix_price:.2f} supports cleaner premium decay and better trend follow-through if price confirms.")
    if headline_news_score:
        context_notes.append(
            f"News pulse snapshot: Banking {banking_news['score']:+.1f}, IT {it_news['score']:+.1f}, Energy {energy_news['score']:+.1f}, Headline market {general_news['score']:+.1f}."
        )

    risk_flags = []
    if market_status.get("staleData"):
        risk_flags.append({"label": "Stale data", "detail": "One or more live feeds are stale. Reduce trust in short-term calls until the tape refreshes.", "tone": "bear"})
    if vix_price is not None and vix_price >= 16:
        risk_flags.append({"label": "Elevated volatility", "detail": "Option premiums are richer and reversals can be sharper than the raw index move suggests.", "tone": "bear"})
    if vix_pct is not None and vix_pct >= 2:
        risk_flags.append({"label": "Volatility repricing", "detail": "VIX is rising fast intraday, which can punish late directional entries.", "tone": "bear"})
    if bank_vs_nifty is not None and abs(bank_vs_nifty) >= 1:
        risk_flags.append({"label": "Leadership narrow", "detail": "A big Bank-vs-Nifty gap can be powerful, but it also means the move is less broad than it looks.", "tone": "neutral"})
    if smallcap_vs_nifty is not None and smallcap_vs_nifty <= -0.75:
        risk_flags.append({"label": "Breadth weak", "detail": "Smallcaps are lagging hard, which often reduces breakout durability.", "tone": "bear"})
    if crude_pct is not None and crude_pct >= 1:
        risk_flags.append({"label": "Crude pressure", "detail": "A sharp crude rise raises inflation sensitivity and can cap upside in rate-sensitive pockets.", "tone": "bear"})
    if usd_pct is not None and usd_pct >= 0.2:
        risk_flags.append({"label": "Rupee weakness", "detail": "A firm USD/INR move can add macro pressure and make equity upside less forgiving.", "tone": "bear"})
    if focus_intraday_range is not None and focus_intraday_range >= 1:
        risk_flags.append({"label": "Range already expanded", "detail": "A large early range means continuation entries need much cleaner structure than usual.", "tone": "neutral"})
    if len(focus_hist) < 6:
        risk_flags.append({"label": "Tape model warming up", "detail": "Short-term momentum signals are based on limited live history so far.", "tone": "neutral"})

    score_breakdown = [
        {"label": "Trend", "score": trend_component, "detail": "Daily index direction plus short-term tape follow-through."},
        {"label": "Leadership", "score": leadership_component, "detail": "Banking leadership and breadth expansion versus the headline index."},
        {"label": "Volatility", "score": vol_component, "detail": "India VIX level and whether volatility is being bid or offered."},
        {"label": "Macro", "score": macro_component, "detail": "Crude and USD/INR pressure or relief."},
        {"label": "News", "score": news_component, "detail": "Weighted pulse from market-sensitive news buckets."},
        {"label": "Flow", "score": flow_component, "detail": "Very short-term live tape direction from recent snapshots."},
    ]

    cross_asset_rows = [
        {
            "label": "Crude Oil",
            "price": crude.get("price"),
            "pct": crude.get("pct"),
            "unit": "$",
            "detail": "Energy sensitivity, inflation expectations, OMC context.",
        },
        {
            "label": "Brent Crude",
            "price": brent.get("price"),
            "pct": brent.get("pct"),
            "unit": "$",
            "detail": "Global oil benchmark and geopolitical risk barometer.",
        },
        {
            "label": "USD/INR",
            "price": usd_inr.get("price"),
            "pct": usd_inr.get("pct"),
            "unit": "",
            "detail": "Rupee pressure, imported inflation, and foreign-flow context.",
        },
        {
            "label": "Gold",
            "price": gold.get("price"),
            "pct": gold.get("pct"),
            "unit": "$",
            "detail": "Safe-haven tone and risk-aversion cross-check.",
        },
    ]

    relative_value_rows = [
        {
            "label": "Bank Nifty vs Nifty",
            "pct": bank_vs_nifty,
            "detail": "Tracks financial leadership against the headline index.",
        },
        {
            "label": "Nifty IT vs Nifty",
            "pct": it_vs_nifty,
            "detail": "Checks whether growth-sensitive tech is confirming or diverging.",
        },
        {
            "label": "Midcap vs Nifty",
            "pct": midcap_vs_nifty,
            "detail": "Useful for judging whether participation is broadening.",
        },
        {
            "label": "Smallcap vs Nifty",
            "pct": smallcap_vs_nifty,
            "detail": "A quick breadth read for risk appetite beyond the headline index.",
        },
    ]

    signal_matrix = []
    if ticks or index_snapshot:
        for label, live_quote in [
            ("Nifty 50", nifty),
            ("Nifty Bank", bank),
            ("Nifty IT", it_idx),
            ("India VIX", vix),
        ]:
            signal = primary_signal_map.get(label)
            short_3 = intraday_return(price_history.get("VIX" if label == "India VIX" else label, []), 3)
            short_8 = intraday_return(price_history.get("VIX" if label == "India VIX" else label, []), 8)
            signal_matrix.append({
                "label": "Bank Nifty" if label == "Nifty Bank" else label,
                "price": (signal or {}).get("price", live_quote.get("price")),
                "pct": (signal or {}).get("pct", live_quote.get("pct")),
                "short3": short_3,
                "short8": short_8,
                "trend": (signal or {}).get("trend", "Live"),
                "signal": (signal or {}).get("signal", "Live snapshot"),
            })

    trigger_map = []
    if ticks or index_snapshot:
        for label in PRIMARY_LEVEL_LABELS:
            signal = primary_signal_map.get(label)
            if not signal:
                continue
            trigger_map.append({
                "label": "Bank Nifty" if label == "Nifty Bank" else label,
                "price": signal.get("price"),
                "pct": signal.get("pct"),
                "trend": signal.get("trend"),
                "signal": signal.get("signal"),
                "support": signal.get("support"),
                "resistance": signal.get("resistance"),
                "rsi14": signal.get("rsi14"),
                "ret5": signal.get("ret5"),
            })

    trade_scenarios = []
    focus_support = (focus_signal or {}).get("support")
    focus_resistance = (focus_signal or {}).get("resistance")
    if focus_price is not None:
        bull_target = round_or_none((focus_resistance or focus_price) + ((expected_move_points or 0) * 0.7))
        bear_target = round_or_none((focus_support or focus_price) - ((expected_move_points or 0) * 0.7))
        fade_anchor = round_or_none(focus_price + ((expected_move_points or 0) * 0.35))
        trade_scenarios = [
            {
                "label": "Bull continuation",
                "tone": "bull",
                "trigger": f"Acceptance above {format_level(focus_resistance)}" if focus_resistance is not None else f"Strength above {format_level(focus_price)}",
                "target": format_level(bull_target),
                "invalidation": format_level(focus_support),
                "note": f"Works best when {focus_display} holds leadership and VIX stops expanding.",
            },
            {
                "label": "Bear continuation",
                "tone": "bear",
                "trigger": f"Break below {format_level(focus_support)}" if focus_support is not None else f"Weakness below {format_level(focus_price)}",
                "target": format_level(bear_target),
                "invalidation": format_level(focus_resistance),
                "note": "Cleaner if breadth narrows, USD/INR stays firm, or VIX keeps getting bid.",
            },
            {
                "label": "Fade / reversal",
                "tone": "neutral",
                "trigger": f"Failed move back inside {format_level(focus_support)} - {format_level(focus_resistance)}" if focus_support is not None and focus_resistance is not None else f"Failed extension around {format_level(fade_anchor)}",
                "target": format_level(focus_price),
                "invalidation": format_level(bull_target if composite_score >= 0 else bear_target),
                "note": "Most relevant when conviction is middling and the day type is rotation or high-gamma two-way.",
            },
        ]

    return {
        "generatedAt": _ist_now(context).strftime("%H:%M:%S"),
        "overviewCards": overview_cards,
        "predictionCards": prediction_cards,
        "contextNotes": context_notes[:6],
        "riskFlags": risk_flags[:6],
        "crossAssetRows": cross_asset_rows,
        "relativeValueRows": relative_value_rows,
        "scoreBreakdown": score_breakdown,
        "tradeScenarios": trade_scenarios,
        "signalMatrix": signal_matrix,
        "triggerMap": trigger_map,
    }


def analytics_runtime_status(context=None) -> dict:
    return {
        "marketAnalyticsBuilder": "analytics_runtime",
        "derivativesAnalyticsBuilder": "analytics_runtime",
        "readOnly": True,
    }


def _clean_general_symbol(symbol: str) -> str:
    return re.sub(r"[^A-Z0-9&.^-]", "", str(symbol or "").upper().strip())


def _market_status(context) -> dict:
    get_market_status = _context_callable(context, "get_market_status")
    if get_market_status is None:
        return {}
    try:
        return get_market_status() or {}
    except Exception:
        return {}


def _ist_now(context) -> datetime:
    ist_now = _context_callable(context, "ist_now")
    if ist_now is not None:
        try:
            return ist_now()
        except Exception:
            pass
    return datetime.now(IST)


def _context_callable(context: Any, name: str):
    value = _context_value(context, name)
    return value if callable(value) else None


def _context_value(context: Any, name: str, default=None):
    if context is None:
        return default
    try:
        value = getattr(context, name)
        return default if value is None else value
    except AttributeError:
        runtime_state = getattr(context, "runtime_state", None)
        if isinstance(runtime_state, Mapping):
            return runtime_state.get(name, default)
        return default
