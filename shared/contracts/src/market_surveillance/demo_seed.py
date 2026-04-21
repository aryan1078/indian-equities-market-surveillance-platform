from __future__ import annotations

import math
import random
from datetime import UTC, date, datetime, time, timedelta
from hashlib import sha1
from pathlib import Path
from typing import Iterable

import pandas as pd

from .history import store_daily_history, sync_metadata_profiles
from .market_time import as_market_time, market_tz
from .metadata import StockReference, load_stock_references
from .models import EventSource, MarketTick

BASE_PRICES: dict[str, float] = {
    "RELIANCE.BO": 1385.0,
    "HDFCBANK.NS": 1680.0,
    "ICICIBANK.NS": 1250.0,
    "SBIN.NS": 780.0,
    "AXISBANK.NS": 1085.0,
    "KOTAKBANK.NS": 1740.0,
    "BAJFINANCE.NS": 7050.0,
    "INFY.NS": 1715.0,
    "TCS.NS": 4020.0,
    "HCLTECH.NS": 1580.0,
    "WIPRO.NS": 510.0,
    "BHARTIARTL.NS": 1165.0,
    "ITC.NS": 412.0,
    "HINDUNILVR.NS": 2325.0,
    "ASIANPAINT.NS": 2990.0,
    "TITAN.NS": 3460.0,
    "SUNPHARMA.NS": 1760.0,
    "DRREDDY.NS": 6530.0,
    "LT.NS": 3550.0,
    "ADANIPORTS.NS": 1310.0,
    "MARUTI.NS": 12100.0,
    "TATAMOTORS.NS": 955.0,
    "M&M.NS": 2805.0,
    "NTPC.NS": 372.0,
    "POWERGRID.NS": 324.0,
}

BASE_VOLUMES: dict[str, int] = {
    "RELIANCE.BO": 650000,
    "HDFCBANK.NS": 820000,
    "ICICIBANK.NS": 910000,
    "SBIN.NS": 1500000,
    "AXISBANK.NS": 620000,
    "KOTAKBANK.NS": 360000,
    "BAJFINANCE.NS": 240000,
    "INFY.NS": 740000,
    "TCS.NS": 520000,
    "HCLTECH.NS": 410000,
    "WIPRO.NS": 980000,
    "BHARTIARTL.NS": 760000,
    "ITC.NS": 1220000,
    "HINDUNILVR.NS": 240000,
    "ASIANPAINT.NS": 185000,
    "TITAN.NS": 360000,
    "SUNPHARMA.NS": 470000,
    "DRREDDY.NS": 155000,
    "LT.NS": 260000,
    "ADANIPORTS.NS": 550000,
    "MARUTI.NS": 110000,
    "TATAMOTORS.NS": 1330000,
    "M&M.NS": 420000,
    "NTPC.NS": 940000,
    "POWERGRID.NS": 880000,
}

SECTOR_VOLATILITY: dict[str, float] = {
    "Banking": 0.011,
    "Information Technology": 0.010,
    "Energy": 0.009,
    "Telecom": 0.007,
    "Consumer Staples": 0.006,
    "Consumer Discretionary": 0.009,
    "Pharmaceuticals": 0.010,
    "Industrials": 0.009,
    "Ports & Logistics": 0.010,
    "Automotive": 0.011,
    "Power & Utilities": 0.007,
    "Financial Services": 0.012,
}

SECTOR_DRIFT: dict[str, float] = {
    "Banking": 0.0006,
    "Information Technology": 0.0005,
    "Energy": 0.0003,
    "Telecom": 0.0004,
    "Consumer Staples": 0.0003,
    "Consumer Discretionary": 0.0004,
    "Pharmaceuticals": 0.0004,
    "Industrials": 0.0004,
    "Ports & Logistics": 0.0005,
    "Automotive": 0.0005,
    "Power & Utilities": 0.0003,
    "Financial Services": 0.0006,
}


def _seed(*parts: str) -> int:
    joined = "|".join(parts)
    return int(sha1(joined.encode("utf-8")).hexdigest()[:12], 16)


def _selected_stocks(symbols: Iterable[str] | None = None) -> list[StockReference]:
    universe = [stock for stock in load_stock_references() if stock.is_active and stock.watchlist]
    if not universe:
        universe = [stock for stock in load_stock_references() if stock.is_active][:25]
    if not symbols:
        return universe
    chosen = {symbol.upper() for symbol in symbols}
    return [stock for stock in universe if stock.symbol.upper() in chosen]


def _business_days(end_date: date, sessions: int) -> list[date]:
    days: list[date] = []
    cursor = end_date
    while len(days) < sessions:
        if cursor.weekday() < 5:
            days.append(cursor)
        cursor -= timedelta(days=1)
    return list(reversed(days))


def seed_demo_daily_history(
    symbols: Iterable[str] | None = None,
    sessions: int = 55,
    end_date: date | None = None,
) -> dict[str, int]:
    sync_metadata_profiles()
    selected = _selected_stocks(symbols)
    final_date = end_date or (datetime.now(tz=UTC).date() - timedelta(days=1))
    trading_days = _business_days(final_date, sessions)
    results: dict[str, int] = {}

    for stock in selected:
        rng = random.Random(_seed(stock.symbol, "daily-history"))
        base_price = BASE_PRICES[stock.symbol]
        base_volume = BASE_VOLUMES[stock.symbol]
        sector_volatility = SECTOR_VOLATILITY.get(stock.sector, 0.009)
        sector_drift = SECTOR_DRIFT.get(stock.sector, 0.0004)
        phase = (_seed(stock.symbol, "phase") % 628) / 100
        previous_close = base_price * (1 + rng.uniform(-0.02, 0.02))
        records: list[dict[str, float | int]] = []

        for index, trading_day in enumerate(trading_days):
            day_rng = random.Random(_seed(stock.symbol, trading_day.isoformat(), "daily"))
            wave = math.sin((index / 4.6) + phase) * sector_volatility * 0.55
            trend = sector_drift * (index - (len(trading_days) / 2))
            market_shock = -0.018 if trading_day == date(2026, 4, 6) else 0.0
            event_boost = 0.0
            volume_boost = 0.0

            if stock.sector in {"Banking", "Information Technology"} and date(2026, 3, 12) <= trading_day <= date(2026, 3, 18):
                event_boost += 0.009
                volume_boost += 0.45
            if stock.sector == "Power & Utilities" and date(2026, 4, 8) <= trading_day <= date(2026, 4, 10):
                event_boost += 0.004
                volume_boost += 0.18

            daily_return = trend + wave + market_shock + event_boost + day_rng.uniform(-sector_volatility, sector_volatility)
            open_price = previous_close * (1 + day_rng.uniform(-sector_volatility * 0.45, sector_volatility * 0.45))
            close_price = max(50.0, previous_close * (1 + daily_return))
            high_price = max(open_price, close_price) * (1 + abs(day_rng.uniform(0.002, sector_volatility * 1.3)))
            low_price = min(open_price, close_price) * (1 - abs(day_rng.uniform(0.002, sector_volatility * 1.2)))
            volume = max(
                10000,
                int(
                    base_volume
                    * (1 + abs(daily_return) * 15 + volume_boost + day_rng.uniform(-0.16, 0.16))
                ),
            )

            records.append(
                {
                    "Date": pd.Timestamp(trading_day),
                    "Open": round(open_price, 4),
                    "High": round(high_price, 4),
                    "Low": round(low_price, 4),
                    "Close": round(close_price, 4),
                    "Adj Close": round(close_price, 4),
                    "Volume": volume,
                    "Dividends": 0.0,
                    "Stock Splits": 0.0,
                }
            )
            previous_close = close_price

        frame = pd.DataFrame.from_records(records).set_index("Date")
        results[stock.symbol] = store_daily_history(stock.symbol, frame)

    return results


def _daily_close_lookup(trading_day: date) -> dict[str, float]:
    from .db import pg_connection

    with pg_connection() as conn:
        rows = conn.execute(
            """
            SELECT symbol, close
            FROM operational.stock_daily_bars
            WHERE trading_date = %s
            """,
            (trading_day,),
        ).fetchall()
    return {row["symbol"]: float(row["close"]) for row in rows}


def generate_demo_replay_fixture(
    output_path: Path,
    trading_day: date,
    symbols: Iterable[str] | None = None,
    minutes: int = 28,
) -> int:
    selected = _selected_stocks(symbols)
    close_lookup = _daily_close_lookup(trading_day)
    start_local = datetime.combine(trading_day, time(hour=9, minute=15), tzinfo=market_tz())
    source = EventSource(provider="fixture", mode="replay", run_id="fixture-expanded-sector-demo")
    records: list[MarketTick] = []

    for stock in selected:
        rng = random.Random(_seed(stock.symbol, trading_day.isoformat(), "replay"))
        base_price = close_lookup.get(stock.symbol, BASE_PRICES[stock.symbol])
        base_volume = max(int(BASE_VOLUMES[stock.symbol] / 18), 12000)
        price = base_price * (1 + rng.uniform(-0.012, 0.012))

        for minute_index in range(minutes):
            ts_local = start_local + timedelta(minutes=minute_index)
            ts_utc = ts_local.astimezone(UTC)
            minute_rng = random.Random(_seed(stock.symbol, trading_day.isoformat(), str(minute_index), "minute"))
            event_return = 0.0
            volume_multiplier = 1.0
            noise_floor = 0.00024

            if stock.sector == "Banking":
                if 8 <= minute_index <= 14:
                    event_return += 0.0024 + (0.00042 * (minute_index - 8))
                    volume_multiplier += 1.2 + (0.12 * (minute_index - 8))
                elif 15 <= minute_index <= 23:
                    event_return += 0.00125
                    volume_multiplier += 0.95
                elif 24 <= minute_index <= minutes - 1:
                    event_return += 0.0041 + (0.00025 * (minute_index - 24))
                    volume_multiplier += 2.05
                noise_floor = 0.00015
            elif stock.sector == "Information Technology":
                if 13 <= minute_index <= 18:
                    event_return += 0.0021 + (0.00035 * (minute_index - 13))
                    volume_multiplier += 1.0 + (0.11 * (minute_index - 13))
                elif 19 <= minute_index <= 24:
                    event_return += 0.0011
                    volume_multiplier += 0.8
                elif 25 <= minute_index <= minutes - 1:
                    event_return += 0.0038 + (0.0002 * (minute_index - 25))
                    volume_multiplier += 1.85
                noise_floor = 0.00016
            elif stock.sector in {"Automobile", "Consumer", "Consumer Discretionary", "Consumer Staples"}:
                noise_floor = 0.0001
            elif stock.sector in {"Telecom", "Pharmaceuticals", "Industrials", "Energy"}:
                noise_floor = 0.00012
            elif stock.sector in {"Utilities", "Power & Utilities"}:
                noise_floor = 0.00009

            noise = minute_rng.uniform(-noise_floor, noise_floor)
            open_price = price
            close_price = max(20.0, price * (1 + noise + event_return))
            high_price = max(open_price, close_price) * (1 + abs(minute_rng.uniform(0.0005, 0.0019)))
            low_price = min(open_price, close_price) * (1 - abs(minute_rng.uniform(0.0005, 0.0018)))
            volume = int(base_volume * volume_multiplier * (1 + minute_rng.uniform(-0.16, 0.16)))

            records.append(
                MarketTick(
                    symbol=stock.symbol,
                    exchange=stock.exchange,
                    sector=stock.sector,
                    interval="1m",
                    timestamp_utc=ts_utc,
                    timestamp_ist=as_market_time(ts_utc),
                    trading_date=trading_day,
                    open=round(open_price, 4),
                    high=round(high_price, 4),
                    low=round(low_price, 4),
                    close=round(close_price, 4),
                    volume=max(volume, 1000),
                    dividends=0.0,
                    stock_splits=0.0,
                    source=source,
                )
            )
            price = close_price

    output_path.parent.mkdir(parents=True, exist_ok=True)
    ordered = sorted(records, key=lambda item: (item.timestamp_utc, item.symbol))
    with output_path.open("w", encoding="utf-8") as handle:
        for record in ordered:
            handle.write(record.model_dump_json())
            handle.write("\n")
    return len(ordered)
