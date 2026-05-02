"""High-interval refresh schedule for macro context."""

from __future__ import annotations

from datetime import date, datetime, time as dt_time, timedelta
from zoneinfo import ZoneInfo

try:
    from backend.core.settings import (
        MACRO_AGENT_MIDDAY_TIME,
        MACRO_AGENT_OPEN_CHECK_TIME,
        MACRO_AGENT_POSTMARKET_TIME,
        MACRO_AGENT_PRE_CLOSE_TIME,
        MACRO_AGENT_PREMARKET_TIME,
        MACRO_AGENT_TIMEZONE,
    )
except ModuleNotFoundError:
    from core.settings import (
        MACRO_AGENT_MIDDAY_TIME,
        MACRO_AGENT_OPEN_CHECK_TIME,
        MACRO_AGENT_POSTMARKET_TIME,
        MACRO_AGENT_PRE_CLOSE_TIME,
        MACRO_AGENT_PREMARKET_TIME,
        MACRO_AGENT_TIMEZONE,
    )


SCHEDULE_WINDOW_MINUTES = 5


def macro_timezone() -> ZoneInfo:
    return ZoneInfo(MACRO_AGENT_TIMEZONE)


def should_run_premarket(now: datetime) -> bool:
    return _within_window(_normalize_now(now), MACRO_AGENT_PREMARKET_TIME)


def should_run_post_open(now: datetime) -> bool:
    return _within_window(_normalize_now(now), MACRO_AGENT_OPEN_CHECK_TIME)


def should_run_midday(now: datetime) -> bool:
    return _within_window(_normalize_now(now), MACRO_AGENT_MIDDAY_TIME)


def should_run_pre_close(now: datetime) -> bool:
    return _within_window(_normalize_now(now), MACRO_AGENT_PRE_CLOSE_TIME)


def should_run_postmarket(now: datetime) -> bool:
    return _within_window(_normalize_now(now), MACRO_AGENT_POSTMARKET_TIME)


def is_macro_refresh_due(now: datetime, last_run_at: datetime | None) -> bool:
    now = _normalize_now(now)
    due_slot = _current_or_most_recent_slot(now)
    if due_slot is None:
        return False
    if last_run_at is None:
        return True
    last_run_at = _normalize_now(last_run_at)
    return last_run_at < due_slot <= now


def get_next_macro_refresh_time(now: datetime) -> datetime | None:
    now = _normalize_now(now)
    slots = _schedule_slots_for_day(now.date())
    for slot in slots:
        if slot > now:
            return slot
    tomorrow_slots = _schedule_slots_for_day(now.date() + timedelta(days=1))
    return tomorrow_slots[0] if tomorrow_slots else None


def _schedule_slots_for_day(day: date) -> list[datetime]:
    tz = macro_timezone()
    return [
        datetime.combine(day, MACRO_AGENT_PREMARKET_TIME, tz),
        datetime.combine(day, MACRO_AGENT_OPEN_CHECK_TIME, tz),
        datetime.combine(day, MACRO_AGENT_MIDDAY_TIME, tz),
        datetime.combine(day, MACRO_AGENT_PRE_CLOSE_TIME, tz),
        datetime.combine(day, MACRO_AGENT_POSTMARKET_TIME, tz),
    ]


def _current_or_most_recent_slot(now: datetime) -> datetime | None:
    for slot in reversed(_schedule_slots_for_day(now.date())):
        if slot <= now and _within_window(now, slot.timetz().replace(tzinfo=None), allow_past=True):
            return slot
    return None


def _within_window(now: datetime, target_time: dt_time, *, allow_past: bool = False) -> bool:
    target = datetime.combine(now.date(), target_time, now.tzinfo)
    delta_seconds = (now - target).total_seconds()
    if allow_past:
        return 0 <= delta_seconds <= SCHEDULE_WINDOW_MINUTES * 60
    return abs(delta_seconds) <= SCHEDULE_WINDOW_MINUTES * 60


def _normalize_now(now: datetime) -> datetime:
    tz = macro_timezone()
    if now.tzinfo is None:
        return now.replace(tzinfo=tz)
    return now.astimezone(tz)
