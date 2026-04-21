from __future__ import annotations

from datetime import UTC, date, datetime, time
from zoneinfo import ZoneInfo

from .settings import get_settings


def market_tz() -> ZoneInfo:
    return ZoneInfo(get_settings().market_timezone)


def ensure_utc(timestamp: datetime) -> datetime:
    if timestamp.tzinfo is None:
        return timestamp.replace(tzinfo=UTC)
    return timestamp.astimezone(UTC)


def as_market_time(timestamp: datetime) -> datetime:
    return ensure_utc(timestamp).astimezone(market_tz())


def trading_date_for(timestamp: datetime) -> date:
    return as_market_time(timestamp).date()


def parse_clock(value: str) -> time:
    hour, minute = value.split(":")
    return time(hour=int(hour), minute=int(minute))


def in_market_hours(timestamp: datetime) -> bool:
    settings = get_settings()
    local_dt = as_market_time(timestamp)
    start = parse_clock(settings.market_open_ist)
    end = parse_clock(settings.market_close_ist)
    return start <= local_dt.time().replace(second=0, microsecond=0) <= end


def minute_of_day(timestamp: datetime) -> int:
    local_dt = as_market_time(timestamp)
    return local_dt.hour * 60 + local_dt.minute


def date_sk(value: date) -> int:
    return int(value.strftime("%Y%m%d"))


def time_sk(timestamp: datetime) -> int:
    return minute_of_day(timestamp)

