from datetime import UTC, datetime

from market_surveillance.models import EventSource, MarketTick, build_dedupe_key


def test_market_tick_derives_dedupe_key() -> None:
    tick = MarketTick(
        symbol="RELIANCE.BO",
        exchange="BSE",
        sector="Energy",
        timestamp_utc=datetime(2026, 3, 16, 3, 45, tzinfo=UTC),
        timestamp_ist=datetime(2026, 3, 16, 9, 15, tzinfo=UTC),
        trading_date="2026-03-16",
        open=1380.0,
        high=1382.9,
        low=1377.0,
        close=1377.25,
        volume=4872,
        source=EventSource(mode="backfill", run_id="unit-test"),
    )
    assert tick.dedupe_key == build_dedupe_key("RELIANCE.BO", "1m", tick.timestamp_utc)

