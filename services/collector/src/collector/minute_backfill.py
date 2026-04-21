from __future__ import annotations

import math
import random
import json
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from hashlib import sha1
from typing import Any

from cassandra.concurrent import execute_concurrent_with_args

from anomaly_engine.main import score_tick
from market_surveillance.db import get_cassandra_session, pg_connection
from market_surveillance.market_time import as_market_time, market_tz
from market_surveillance.metadata import load_stock_references
from market_surveillance.models import EventSource, MarketTick
SESSION_MINUTES = 375
DEFAULT_FLUSH_ROWS = 6000
DEFAULT_CASSANDRA_CONCURRENCY = 128
PROGRESS_PARTITION_INTERVAL = 250


@dataclass(frozen=True)
class DailyPartition:
    symbol: str
    exchange: str
    sector: str
    company_name: str
    trading_date: date
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: int
    dividends: float
    stock_splits: float


def _seed(*parts: str) -> int:
    joined = "|".join(parts)
    return int(sha1(joined.encode("utf-8")).hexdigest()[:12], 16)


def _selected_nse_symbols(symbols_limit: int | None = None) -> list[str]:
    symbols = sorted(
        stock.symbol
        for stock in load_stock_references()
        if stock.is_active and stock.exchange.upper() == "NSE"
    )
    if symbols_limit is not None:
        return symbols[: max(symbols_limit, 0)]
    return symbols


def _resolve_trading_dates(
    selected_symbols: list[str],
    trading_days: int,
    start_date: date | None,
    end_date: date | None,
) -> list[date]:
    if not selected_symbols:
        return []

    filters = ["symbol = ANY(%s)"]
    params: list[Any] = [selected_symbols]

    if start_date:
        filters.append("trading_date >= %s")
        params.append(start_date)
    if end_date:
        filters.append("trading_date <= %s")
        params.append(end_date)

    limit_clause = ""
    if start_date is None and end_date is None:
        limit_clause = "LIMIT %s"
        params.append(max(trading_days, 1))

    with pg_connection() as conn:
        rows = conn.execute(
            f"""
            SELECT DISTINCT trading_date
            FROM operational.stock_daily_bars
            WHERE {' AND '.join(filters)}
            ORDER BY trading_date DESC
            {limit_clause}
            """,
            tuple(params),
        ).fetchall()

    return sorted(row["trading_date"] for row in rows)


def _load_daily_partitions(selected_symbols: list[str], trading_dates: list[date]) -> list[DailyPartition]:
    if not selected_symbols or not trading_dates:
        return []

    reference_map = {stock.symbol: stock for stock in load_stock_references()}
    with pg_connection() as conn:
        rows = conn.execute(
            """
            SELECT symbol, trading_date, open, high, low, close, volume, dividends, stock_splits
            FROM operational.stock_daily_bars
            WHERE symbol = ANY(%s) AND trading_date = ANY(%s)
            ORDER BY trading_date ASC, symbol ASC
            """,
            (selected_symbols, trading_dates),
        ).fetchall()

    partitions: list[DailyPartition] = []
    for row in rows:
        reference = reference_map.get(row["symbol"])
        if reference is None:
            continue
        day_high = max(float(row["high"]), float(row["open"]), float(row["close"]), float(row["low"]))
        day_low = min(float(row["low"]), float(row["open"]), float(row["close"]), float(row["high"]))
        partitions.append(
            DailyPartition(
                symbol=row["symbol"],
                exchange=reference.exchange,
                sector=reference.sector,
                company_name=reference.company_name,
                trading_date=row["trading_date"],
                open_price=float(row["open"]),
                high_price=day_high,
                low_price=day_low,
                close_price=float(row["close"]),
                volume=int(row["volume"]),
                dividends=float(row["dividends"]),
                stock_splits=float(row["stock_splits"]),
            )
        )
    return partitions


def _segment_curve(
    boundary_prices: list[float],
    start_index: int,
    end_index: int,
    low_price: float,
    high_price: float,
    amplitude: float,
    direction: float,
) -> None:
    if end_index <= start_index:
        return

    start_price = boundary_prices[start_index]
    end_price = boundary_prices[end_index]
    span = end_index - start_index
    for index in range(start_index + 1, end_index):
        progress = (index - start_index) / span
        base = start_price + ((end_price - start_price) * progress)
        bend = math.sin(progress * math.pi) * amplitude * direction
        boundary_prices[index] = min(max(base + bend, low_price), high_price)


def _boundary_path(partition: DailyPartition) -> tuple[list[float], set[int], set[int]]:
    low_price = partition.low_price
    high_price = partition.high_price
    open_price = min(max(partition.open_price, low_price), high_price)
    close_price = min(max(partition.close_price, low_price), high_price)
    daily_range = max(high_price - low_price, max(open_price, close_price, 1.0) * 0.002)

    direction_up = close_price >= open_price
    first_extreme = low_price if direction_up else high_price
    second_extreme = high_price if direction_up else low_price

    first_boundary = 28 + (_seed(partition.symbol, partition.trading_date.isoformat(), "first") % 88)
    second_boundary = 205 + (_seed(partition.symbol, partition.trading_date.isoformat(), "second") % 108)
    if second_boundary <= first_boundary + 36:
        second_boundary = min(first_boundary + 72, SESSION_MINUTES - 18)

    boundaries = [open_price] * (SESSION_MINUTES + 1)
    boundaries[0] = open_price
    boundaries[first_boundary] = first_extreme
    boundaries[second_boundary] = second_extreme
    boundaries[SESSION_MINUTES] = close_price

    amp_one = daily_range * (0.035 + ((_seed(partition.symbol, partition.trading_date.isoformat(), "amp1") % 9) / 200))
    amp_two = daily_range * (0.022 + ((_seed(partition.symbol, partition.trading_date.isoformat(), "amp2") % 7) / 250))
    amp_three = daily_range * (0.030 + ((_seed(partition.symbol, partition.trading_date.isoformat(), "amp3") % 11) / 220))

    _segment_curve(boundaries, 0, first_boundary, low_price, high_price, amp_one, 1.0 if direction_up else -1.0)
    _segment_curve(boundaries, first_boundary, second_boundary, low_price, high_price, amp_two, -1.0 if direction_up else 1.0)
    _segment_curve(boundaries, second_boundary, SESSION_MINUTES, low_price, high_price, amp_three, 1.0 if direction_up else -1.0)

    boundaries[first_boundary] = first_extreme
    boundaries[second_boundary] = second_extreme
    boundaries[SESSION_MINUTES] = close_price

    high_boundaries = {first_boundary} if first_extreme == high_price else {second_boundary}
    low_boundaries = {first_boundary} if first_extreme == low_price else {second_boundary}
    return boundaries, high_boundaries, low_boundaries


def _intraday_volumes(partition: DailyPartition, boundaries: list[float]) -> list[int]:
    total_volume = max(partition.volume, SESSION_MINUTES)
    mid = (SESSION_MINUTES - 1) / 2
    day_range = max(partition.high_price - partition.low_price, partition.close_price * 0.001)
    weights: list[float] = []

    for index in range(SESSION_MINUTES):
        open_price = boundaries[index]
        close_price = boundaries[index + 1]
        taper = abs((index - mid) / mid)
        move_ratio = min(abs(close_price - open_price) / day_range, 1.5)
        close_boost = 0.30 if index >= SESSION_MINUTES - 18 else 0.0
        opening_boost = 0.18 if index <= 16 else 0.0
        weight = 0.70 + (1.05 * (taper**1.65)) + (0.85 * move_ratio) + close_boost + opening_boost
        weights.append(weight)

    total_weight = sum(weights) or 1.0
    raw_volumes = [(weight / total_weight) * total_volume for weight in weights]
    minute_volumes = [max(1, int(value)) for value in raw_volumes]
    remainder = total_volume - sum(minute_volumes)
    if remainder != 0:
        ranking = sorted(
            range(SESSION_MINUTES),
            key=lambda index: raw_volumes[index] - int(raw_volumes[index]),
            reverse=remainder > 0,
        )
        for index in ranking[: abs(remainder)]:
            next_value = minute_volumes[index] + (1 if remainder > 0 else -1)
            minute_volumes[index] = max(1, next_value)
    return minute_volumes


def generate_partition_ticks(partition: DailyPartition, source: EventSource) -> list[MarketTick]:
    start_local = datetime.combine(partition.trading_date, time(hour=9, minute=15), tzinfo=market_tz())
    boundaries, high_boundaries, low_boundaries = _boundary_path(partition)
    minute_volumes = _intraday_volumes(partition, boundaries)
    daily_range = max(partition.high_price - partition.low_price, partition.close_price * 0.001)

    ticks: list[MarketTick] = []
    for index in range(SESSION_MINUTES):
        timestamp_local = start_local + timedelta(minutes=index)
        timestamp_utc = timestamp_local.astimezone(UTC)
        open_price = boundaries[index]
        close_price = boundaries[index + 1]
        rng = random.Random(_seed(partition.symbol, partition.trading_date.isoformat(), str(index), "wick"))
        upper_wick = daily_range * (0.002 + (rng.random() * 0.01))
        lower_wick = daily_range * (0.002 + (rng.random() * 0.01))

        high_price = min(partition.high_price, max(open_price, close_price) + upper_wick)
        low_price = max(partition.low_price, min(open_price, close_price) - lower_wick)

        boundary_index = index + 1
        if boundary_index in high_boundaries:
            high_price = partition.high_price
        if boundary_index in low_boundaries:
            low_price = partition.low_price

        tick = MarketTick(
            symbol=partition.symbol,
            exchange=partition.exchange,
            sector=partition.sector,
            interval="1m",
            timestamp_utc=timestamp_utc,
            timestamp_ist=as_market_time(timestamp_utc),
            trading_date=partition.trading_date,
            open=round(open_price, 4),
            high=round(max(high_price, open_price, close_price), 4),
            low=round(min(low_price, open_price, close_price), 4),
            close=round(close_price, 4),
            volume=minute_volumes[index],
            dividends=partition.dividends if index == 0 else 0.0,
            stock_splits=partition.stock_splits if index == 0 else 0.0,
            source=source,
        )
        ticks.append(tick)
    return ticks


def generate_partition_anomalies(ticks: list[MarketTick]) -> list[dict[str, Any]]:
    detections: list[dict[str, Any]] = []
    stats = None
    from anomaly_engine.math_engine import StreamingStats

    stats = StreamingStats()
    for tick in ticks:
        stats, detection = score_tick(tick, stats)
        if detection is None:
            continue
        detections.append(
            {
                "symbol": detection.symbol,
                "trading_date": detection.trading_date,
                "timestamp_utc": detection.timestamp_utc,
                "timestamp_ist": detection.timestamp_ist.isoformat(),
                "exchange": detection.exchange,
                "sector": detection.sector,
                "interval": detection.interval,
                "close": detection.close,
                "volume": detection.volume,
                "return_pct": detection.return_pct,
                "ewma_mean": detection.ewma_mean,
                "ewma_variance": detection.ewma_variance,
                "rolling_volatility": detection.rolling_volatility,
                "volume_mean": detection.volume_mean,
                "volume_variance": detection.volume_variance,
                "price_z_score": detection.price_z_score,
                "volume_z_score": detection.volume_z_score,
                "composite_score": detection.composite_score,
                "is_anomalous": detection.is_anomalous,
                "explainability": detection.explainability,
                "source_run_id": detection.source_run_id,
                "dedupe_key": detection.dedupe_key,
            }
        )
    return detections


def prepare_statements():
    session = get_cassandra_session()
    tick_stmt = session.prepare(
        """
        INSERT INTO market_ticks (
            symbol, trading_date, timestamp_utc, timestamp_ist, exchange, sector, interval,
            open, high, low, close, volume, dividends, stock_splits,
            source_mode, source_provider, source_run_id, dedupe_key, ingest_ts
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, toTimestamp(now()))
        """
    )
    anomaly_stmt = session.prepare(
        """
        INSERT INTO anomaly_metrics (
            symbol, trading_date, timestamp_utc, timestamp_ist, exchange, sector, interval, close, volume,
            return_pct, ewma_mean, ewma_variance, rolling_volatility,
            volume_mean, volume_variance, price_z_score, volume_z_score,
            composite_score, is_anomalous, explainability, source_run_id, dedupe_key
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
    )
    latest_stmt = session.prepare(
        """
        INSERT INTO latest_market_state (
            symbol, trading_date, timestamp_utc, close, volume, composite_score, is_anomalous, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, toTimestamp(now()))
        """
    )
    return session, tick_stmt, anomaly_stmt, latest_stmt


def _flush_rows(session, statement, rows: list[tuple[Any, ...]], concurrency: int) -> None:
    if not rows:
        return
    execute_concurrent_with_args(
        session,
        statement,
        rows,
        concurrency=max(concurrency, 1),
        raise_on_first_error=True,
        results_generator=False,
    )
    rows.clear()


def _update_run_progress(run_id: str, records_seen: int, records_published: int, notes: dict[str, Any]) -> None:
    with pg_connection() as conn:
        conn.execute(
            """
            UPDATE operational.ingestion_runs
            SET records_seen = %s,
                records_published = %s,
                notes = %s::jsonb
            WHERE run_id = %s
            """,
            (records_seen, records_published, json.dumps(notes), run_id),
        )


def bulk_backfill_minutes(
    run_id: str,
    trading_days: int = 12,
    start_date: date | None = None,
    end_date: date | None = None,
    symbols_limit: int | None = None,
    flush_rows: int = DEFAULT_FLUSH_ROWS,
    cassandra_concurrency: int = DEFAULT_CASSANDRA_CONCURRENCY,
) -> dict[str, Any]:
    selected_symbols = _selected_nse_symbols(symbols_limit)
    trading_dates = _resolve_trading_dates(selected_symbols, trading_days, start_date, end_date)
    partitions = _load_daily_partitions(selected_symbols, trading_dates)
    if not partitions:
        return {
            "exchange": "NSE",
            "requested_symbol_count": len(selected_symbols),
            "hydrated_symbol_count": 0,
            "trading_day_count": len(trading_dates),
            "partition_count": 0,
            "tick_rows_written": 0,
            "anomaly_rows_written": 0,
            "latest_state_rows_written": 0,
            "provider": "deterministic_daily_expansion",
            "session_minutes": SESSION_MINUTES,
        }

    source = EventSource(provider="deterministic_daily_expansion", mode="minute_backfill", run_id=run_id)
    session, tick_stmt, anomaly_stmt, latest_stmt = prepare_statements()

    tick_rows: list[tuple[Any, ...]] = []
    anomaly_rows: list[tuple[Any, ...]] = []
    latest_rows: list[tuple[Any, ...]] = []

    symbols_loaded: set[str] = set()
    total_ticks = 0
    total_anomalies = 0
    total_latest = 0

    notes = {
        "exchange": "NSE",
        "requested_symbol_count": len(selected_symbols),
        "trading_day_count": len(trading_dates),
        "partition_count": len(partitions),
        "provider": source.provider,
        "session_minutes": SESSION_MINUTES,
        "window_start": trading_dates[0].isoformat(),
        "window_end": trading_dates[-1].isoformat(),
        "assumption": "non_overlapping_bulk_window",
    }

    for partition_index, partition in enumerate(partitions, start=1):
        ticks = generate_partition_ticks(partition, source)
        detections = generate_partition_anomalies(ticks)
        latest_tick = ticks[-1]
        latest_detection = detections[-1] if detections else None

        tick_rows.extend(
            (
                tick.symbol,
                tick.trading_date,
                tick.timestamp_utc,
                tick.timestamp_ist.isoformat(),
                tick.exchange,
                tick.sector,
                tick.interval,
                tick.open,
                tick.high,
                tick.low,
                tick.close,
                tick.volume,
                tick.dividends,
                tick.stock_splits,
                tick.source.mode,
                tick.source.provider,
                tick.source.run_id,
                tick.dedupe_key,
            )
            for tick in ticks
        )
        anomaly_rows.extend(
            (
                detection["symbol"],
                detection["trading_date"],
                detection["timestamp_utc"],
                detection["timestamp_ist"],
                detection["exchange"],
                detection["sector"],
                detection["interval"],
                detection["close"],
                detection["volume"],
                detection["return_pct"],
                detection["ewma_mean"],
                detection["ewma_variance"],
                detection["rolling_volatility"],
                detection["volume_mean"],
                detection["volume_variance"],
                detection["price_z_score"],
                detection["volume_z_score"],
                detection["composite_score"],
                detection["is_anomalous"],
                detection["explainability"],
                detection["source_run_id"],
                detection["dedupe_key"],
            )
            for detection in detections
        )
        latest_rows.append(
            (
                latest_tick.symbol,
                latest_tick.trading_date,
                latest_tick.timestamp_utc,
                latest_tick.close,
                latest_tick.volume,
                float(latest_detection["composite_score"]) if latest_detection else 0.0,
                bool(latest_detection["is_anomalous"]) if latest_detection else False,
            )
        )

        symbols_loaded.add(partition.symbol)
        total_ticks += len(ticks)
        total_anomalies += len(detections)
        total_latest += 1

        if len(tick_rows) >= max(flush_rows, SESSION_MINUTES):
            _flush_rows(session, tick_stmt, tick_rows, cassandra_concurrency)
        if len(anomaly_rows) >= max(flush_rows, SESSION_MINUTES):
            _flush_rows(session, anomaly_stmt, anomaly_rows, cassandra_concurrency)
        if len(latest_rows) >= 1000:
            _flush_rows(session, latest_stmt, latest_rows, cassandra_concurrency)

        if partition_index % PROGRESS_PARTITION_INTERVAL == 0:
            progress_notes = {
                **notes,
                "hydrated_symbol_count": len(symbols_loaded),
                "tick_rows_written": total_ticks,
                "anomaly_rows_written": total_anomalies,
                "latest_state_rows_written": total_latest,
                "partitions_completed": partition_index,
                "status": "running",
            }
            _update_run_progress(run_id, total_ticks, total_ticks, progress_notes)

    _flush_rows(session, tick_stmt, tick_rows, cassandra_concurrency)
    _flush_rows(session, anomaly_stmt, anomaly_rows, cassandra_concurrency)
    _flush_rows(session, latest_stmt, latest_rows, cassandra_concurrency)

    return {
        **notes,
        "hydrated_symbol_count": len(symbols_loaded),
        "tick_rows_written": total_ticks,
        "anomaly_rows_written": total_anomalies,
        "latest_state_rows_written": total_latest,
        "partitions_completed": len(partitions),
        "status": "completed",
    }
