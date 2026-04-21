from __future__ import annotations

from datetime import date, datetime, timedelta

from .market_time import date_sk, minute_of_day, time_sk


def iter_date_dimension(trading_date: date) -> dict[str, object]:
    return {
        "date_sk": date_sk(trading_date),
        "calendar_date": trading_date,
        "year": trading_date.year,
        "quarter": ((trading_date.month - 1) // 3) + 1,
        "month": trading_date.month,
        "month_name": trading_date.strftime("%B"),
        "day": trading_date.day,
        "day_of_week": trading_date.isoweekday(),
        "is_weekend": trading_date.isoweekday() >= 6,
    }


def iter_time_dimension(anchor: datetime) -> dict[str, object]:
    mod = minute_of_day(anchor)
    local = anchor.astimezone(anchor.tzinfo)
    return {
        "time_sk": time_sk(anchor),
        "minute_of_day": mod,
        "hour": local.hour,
        "minute": local.minute,
        "label": local.strftime("%H:%M"),
    }


def minute_range(start: datetime, count: int) -> list[datetime]:
    return [start + timedelta(minutes=index) for index in range(count)]

