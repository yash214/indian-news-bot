# India Market Desk Production Improvement Plan

This plan focuses on making the app more useful for day-to-day live trading while keeping the UI simple and the NSE fallback safe.

## Phase 1: AI News Quality and Queue Reliability

Status: in progress

- Improve the article summary prompt so the local model explains the event, important numbers, sector/index read-through, uncertainty, and what traders should watch next.
- Continue using accessible full-article extraction when available, with RSS/feed text as the fallback for paywalled or blocked pages.
- Keep summaries limited to recent articles so the model does not waste time on stale news.
- Add a priority queue that processes recent, local, high-impact articles first.
- Skip duplicate summary jobs, reuse persisted summaries, and retry failed summaries later with backoff.
- Expose queue status through the existing system-status pill so the layout stays clean.

## Phase 2: AI Tags and Major-Index Impact

Status: planned

- Replace purely rules-based bullish/bearish/neutral labels with a local AI classifier that returns structured JSON.
- Score each article against likely impact on Nifty, Bank Nifty, sector indices, India VIX, crude-sensitive sectors, and broad risk appetite.
- Keep deterministic safeguards: if article text is thin or model confidence is low, fall back to neutral/rules-based tags.
- Store AI tag decisions with prompt versioning so old labels can be recomputed safely after model improvements.
- Show confidence subtly in the card metadata only when it helps the trader, not as visual clutter.

## Phase 3: AI Analytics and Prediction Board

Status: planned

- Create a market-state engine that combines index breadth, sector rotation, volatility, crude, USD/INR, recent high-impact news, and derivative cues.
- Add a local AI layer that explains the dashboard in plain trading language: regime, risk-on/risk-off tone, strongest/weakest pockets, and caution areas.
- Keep prediction output probabilistic and explainable instead of pretending to know the future.
- Separate "signal" from "recommendation": the app can say what conditions suggest, but should not auto-trade or imply guaranteed outcomes.
- Add confidence, freshness, and data-source badges so stale or fallback data is clearly visible.

## Phase 4: Background Worker Separation

Status: planned

- Split the production deployment into a web process and one or more worker processes.
- Move slow tasks into workers: feed polling, article extraction, AI summaries, AI tagging, analytics snapshot building, and retry handling.
- Use a durable queue backed by SQLite initially, then Redis/RQ or Celery if scale demands it.
- Keep the Flask app responsible for fast HTTP routes and serving the latest persisted state.
- Add health endpoints for web, market data, queue depth, worker heartbeat, model availability, and data freshness.

## Phase 5: UI Simplification

Status: planned

- Reduce visual noise in filter controls and status chips while preserving every function.
- Make article cards easier to scan: headline, AI summary, tags, impact, source/time, and actions should have clear hierarchy.
- Add a compact "why this matters" treatment for AI summaries without adding a separate card.
- Improve empty states and loading states so background work feels calm, not flashy.
- Keep real-time updates silent unless the user explicitly needs attention.

## Phase 6: Production Hardening

Status: planned

- Add stronger tests for summary generation, queue behavior, fallback behavior, and app-state persistence.
- Add structured logging for provider status, queue jobs, AI failures, and source freshness.
- Add deployment checks for Lightsail: environment, service status, Nginx config, SSL, domain, static IP, and disk space.
- Add data freshness alarms for market open so lagged or fallback data is clearly flagged.
- Keep Upstox Market Data Feed V3 as the primary live layer when credentials exist, with NSE fallback intact.
