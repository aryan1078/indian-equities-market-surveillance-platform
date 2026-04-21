from datetime import UTC, datetime

from market_surveillance.market_time import as_market_time, in_market_hours, minute_of_day


def test_market_time_conversion_preserves_ist() -> None:
    utc_timestamp = datetime(2026, 3, 16, 3, 45, tzinfo=UTC)
    local = as_market_time(utc_timestamp)
    assert local.hour == 9
    assert local.minute == 15


def test_market_hours_bounds() -> None:
    assert in_market_hours(datetime(2026, 3, 16, 3, 45, tzinfo=UTC)) is True
    assert in_market_hours(datetime(2026, 3, 16, 10, 30, tzinfo=UTC)) is False


def test_minute_of_day() -> None:
    assert minute_of_day(datetime(2026, 3, 16, 3, 45, tzinfo=UTC)) == 555

