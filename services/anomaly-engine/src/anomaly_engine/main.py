from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime

from market_surveillance.alerts import emit_anomaly_alert
from market_surveillance.db import get_cassandra_session, get_redis, pg_connection
from market_surveillance.messaging import build_consumer, build_producer
from market_surveillance.models import AnomalyDetection, MarketTick
from market_surveillance.serialization import loads
from market_surveillance.settings import get_settings

from .math_engine import StreamingStats, ewma_alpha, update_ewma, z_score


def state_key(symbol: str, trading_date: str) -> str:
    return f"state:anomaly:{symbol}:{trading_date}"


def latest_market_key(symbol: str) -> str:
    return f"latest:market:{symbol}"


def latest_anomaly_key(symbol: str) -> str:
    return f"latest:anomaly:{symbol}"


def load_state(symbol: str, trading_date: str) -> StreamingStats:
    redis = get_redis()
    raw = redis.get(state_key(symbol, trading_date))
    if not raw:
        return StreamingStats()
    return StreamingStats(**json.loads(raw))


def save_state(symbol: str, trading_date: str, stats: StreamingStats) -> None:
    redis = get_redis()
    redis.set(state_key(symbol, trading_date), json.dumps(asdict(stats)))


def write_coverage(tick: MarketTick, coverage_state: str) -> None:
    with pg_connection() as conn:
        conn.execute(
            """
            INSERT INTO operational.surveillance_coverage (
                symbol, trading_date, timestamp_utc, timestamp_ist, source_run_id, coverage_state
            ) VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol, timestamp_utc) DO UPDATE
            SET coverage_state = EXCLUDED.coverage_state,
                source_run_id = EXCLUDED.source_run_id
            """,
            (tick.symbol, tick.trading_date, tick.timestamp_utc, tick.timestamp_ist, tick.source.run_id, coverage_state),
        )


def prepare_statement():
    session = get_cassandra_session()
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
    return anomaly_stmt, latest_stmt


def score_tick(tick: MarketTick, stats: StreamingStats) -> tuple[StreamingStats, AnomalyDetection | None]:
    settings = get_settings()
    alpha = ewma_alpha(settings.anomaly_warmup_minutes)
    return_pct = None if stats.last_close in (None, 0) else ((tick.close - stats.last_close) / stats.last_close) * 100.0

    next_stats = StreamingStats(**asdict(stats))
    next_stats.sample_count += 1

    if return_pct is not None:
        next_stats.return_mean, next_stats.return_variance = update_ewma(
            next_stats.return_mean,
            next_stats.return_variance,
            return_pct,
            alpha,
        )

    next_stats.volume_mean, next_stats.volume_variance = update_ewma(
        next_stats.volume_mean,
        next_stats.volume_variance,
        float(tick.volume),
        alpha,
    )
    next_stats.last_close = tick.close

    if next_stats.sample_count <= settings.anomaly_warmup_minutes or return_pct is None:
        return next_stats, None

    price_z = z_score(return_pct, next_stats.return_mean, next_stats.return_variance)
    volume_z = z_score(float(tick.volume), next_stats.volume_mean, next_stats.volume_variance)
    rolling_volatility = max(next_stats.return_variance, 1e-12) ** 0.5
    composite = (0.6 * abs(price_z)) + (0.4 * abs(volume_z))
    is_anomalous = (
        abs(price_z) >= settings.anomaly_price_z_threshold
        or abs(volume_z) >= settings.anomaly_volume_z_threshold
        or composite >= settings.anomaly_composite_threshold
    )
    explainability = (
        f"price_z={price_z:.3f}; volume_z={volume_z:.3f}; "
        f"rolling_volatility={rolling_volatility:.5f}; return_pct={return_pct:.5f}"
    )
    detection = AnomalyDetection(
        symbol=tick.symbol,
        exchange=tick.exchange,
        sector=tick.sector,
        interval=tick.interval,
        timestamp_utc=tick.timestamp_utc,
        timestamp_ist=tick.timestamp_ist,
        trading_date=tick.trading_date,
        close=tick.close,
        volume=tick.volume,
        return_pct=return_pct,
        ewma_mean=next_stats.return_mean,
        ewma_variance=next_stats.return_variance,
        rolling_volatility=rolling_volatility,
        volume_mean=next_stats.volume_mean,
        volume_variance=next_stats.volume_variance,
        price_z_score=price_z,
        volume_z_score=volume_z,
        composite_score=composite,
        is_anomalous=is_anomalous,
        explainability=explainability,
        source_run_id=tick.source.run_id,
        dedupe_key=tick.dedupe_key,
    )
    return next_stats, detection


def persist_detection(detection: AnomalyDetection, anomaly_stmt, latest_stmt) -> None:
    session = get_cassandra_session()
    session.execute(
        anomaly_stmt,
        (
            detection.symbol,
            detection.trading_date,
            detection.timestamp_utc,
            detection.timestamp_ist.isoformat(),
            detection.exchange,
            detection.sector,
            detection.interval,
            detection.close,
            detection.volume,
            detection.return_pct,
            detection.ewma_mean,
            detection.ewma_variance,
            detection.rolling_volatility,
            detection.volume_mean,
            detection.volume_variance,
            detection.price_z_score,
            detection.volume_z_score,
            detection.composite_score,
            detection.is_anomalous,
            detection.explainability,
            detection.source_run_id,
            detection.dedupe_key,
        ),
    )
    session.execute(
        latest_stmt,
        (
            detection.symbol,
            detection.trading_date,
            detection.timestamp_utc,
            detection.close,
            detection.volume,
            detection.composite_score,
            detection.is_anomalous,
        ),
    )


def publish_live_state(tick: MarketTick, detection: AnomalyDetection | None) -> None:
    redis = get_redis()
    ttl_seconds = 900
    live_payload = {
        "symbol": tick.symbol,
        "sector": tick.sector,
        "exchange": tick.exchange,
        "timestamp_ist": tick.timestamp_ist.isoformat(),
        "close": tick.close,
        "volume": tick.volume,
    }
    redis.set(latest_market_key(tick.symbol), json.dumps(live_payload), ex=ttl_seconds)
    redis.set("system:last_tick", tick.timestamp_utc.isoformat(), ex=ttl_seconds)

    if detection:
        payload = detection.model_dump(mode="json")
        redis.set(latest_anomaly_key(detection.symbol), json.dumps(payload), ex=ttl_seconds)
        redis.set(f"sector:latest:{detection.sector}:{detection.symbol}", json.dumps(payload), ex=ttl_seconds)


def main() -> None:
    settings = get_settings()
    consumer = build_consumer(settings.kafka_market_ticks_topic, settings.kafka_consumer_group_anomaly)
    producer = build_producer()
    anomaly_stmt, latest_stmt = prepare_statement()

    while True:
        polled = consumer.poll(timeout_ms=1000, max_records=100)
        for batch in polled.values():
            for record in batch:
                tick = MarketTick.model_validate(loads(record.value))
                stats = load_state(tick.symbol, tick.trading_date.isoformat())
                next_stats, detection = score_tick(tick, stats)
                save_state(tick.symbol, tick.trading_date.isoformat(), next_stats)
                coverage_state = "active" if detection else "warmup"
                write_coverage(tick, coverage_state)
                publish_live_state(tick, detection)

                if detection is None:
                    continue

                persist_detection(detection, anomaly_stmt, latest_stmt)
                if detection.is_anomalous:
                    emit_anomaly_alert(detection)
                    producer.send(
                        settings.kafka_anomaly_detections_topic,
                        key=detection.symbol.encode("utf-8"),
                        value=detection.model_dump(mode="json"),
                    )
            producer.flush()


if __name__ == "__main__":
    main()
