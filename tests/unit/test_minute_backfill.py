from datetime import date

from collector.minute_backfill import SESSION_MINUTES, DailyPartition, generate_partition_anomalies, generate_partition_ticks
from market_surveillance.models import EventSource
from market_surveillance.settings import get_settings


def test_generate_partition_ticks_builds_full_session():
    partition = DailyPartition(
        symbol="INFY.NS",
        exchange="NSE",
        sector="Information Technology",
        company_name="Infosys",
        trading_date=date(2026, 4, 20),
        open_price=1510.0,
        high_price=1542.0,
        low_price=1498.0,
        close_price=1538.0,
        volume=8_250_000,
        dividends=0.0,
        stock_splits=0.0,
    )

    ticks = generate_partition_ticks(
        partition,
        EventSource(provider="deterministic_daily_expansion", mode="minute_backfill", run_id="unit-test"),
    )

    assert len(ticks) == 375
    assert ticks[0].symbol == "INFY.NS"
    assert ticks[0].timestamp_ist.strftime("%H:%M") == "09:15"
    assert ticks[-1].timestamp_ist.strftime("%H:%M") == "15:29"
    assert sum(tick.volume for tick in ticks) == partition.volume
    assert max(tick.high for tick in ticks) == partition.high_price
    assert min(tick.low for tick in ticks) == partition.low_price


def test_generate_partition_anomalies_respects_warmup_window():
    partition = DailyPartition(
        symbol="SBIN.NS",
        exchange="NSE",
        sector="Banking",
        company_name="State Bank of India",
        trading_date=date(2026, 4, 20),
        open_price=790.0,
        high_price=824.0,
        low_price=782.0,
        close_price=820.0,
        volume=22_600_000,
        dividends=0.0,
        stock_splits=0.0,
    )

    ticks = generate_partition_ticks(
        partition,
        EventSource(provider="deterministic_daily_expansion", mode="minute_backfill", run_id="unit-test"),
    )
    anomalies = generate_partition_anomalies(ticks)

    assert len(anomalies) == SESSION_MINUTES - get_settings().anomaly_warmup_minutes
    assert anomalies[0]["symbol"] == "SBIN.NS"
    assert anomalies[-1]["source_run_id"] == "unit-test"
