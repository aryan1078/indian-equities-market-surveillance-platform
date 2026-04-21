from __future__ import annotations

from cassandra.query import BatchStatement, PreparedStatement
from kafka.consumer.fetcher import ConsumerRecord

from market_surveillance.db import get_cassandra_session
from market_surveillance.messaging import build_consumer, build_producer
from market_surveillance.metadata import load_stock_references
from market_surveillance.models import MarketTick
from market_surveillance.serialization import loads
from market_surveillance.settings import get_settings


def seed_stock_reference() -> None:
    session = get_cassandra_session()
    statement = session.prepare(
        """
        INSERT INTO stock_reference (symbol, exchange, sector, company_name, country, is_active)
        VALUES (?, ?, ?, ?, ?, ?)
        """
    )
    for stock in load_stock_references():
        session.execute(statement, (stock.symbol, stock.exchange, stock.sector, stock.company_name, stock.country, stock.is_active))


def prepare_statements() -> tuple[PreparedStatement, PreparedStatement]:
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
    latest_stmt = session.prepare(
        """
        INSERT INTO latest_market_state (
            symbol, trading_date, timestamp_utc, close, volume, composite_score, is_anomalous, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, toTimestamp(now()))
        """
    )
    return tick_stmt, latest_stmt


def send_to_dlq(producer, raw_payload: bytes, reason: str) -> None:
    settings = get_settings()
    producer.send(
        settings.kafka_dlq_topic,
        value={"reason": reason, "payload": raw_payload.decode("utf-8", errors="replace")},
    )


def handle_batch(records: list[ConsumerRecord], tick_stmt: PreparedStatement, latest_stmt: PreparedStatement, producer) -> None:
    session = get_cassandra_session()
    batch = BatchStatement()
    latest_batch = BatchStatement()
    processed = 0

    for record in records:
        try:
            tick = MarketTick.model_validate(loads(record.value))
        except Exception as exc:
            send_to_dlq(producer, record.value, f"validation_error:{exc}")
            continue

        batch.add(
            tick_stmt,
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
            ),
        )
        latest_batch.add(
            latest_stmt,
            (
                tick.symbol,
                tick.trading_date,
                tick.timestamp_utc,
                tick.close,
                tick.volume,
                0.0,
                False,
            ),
        )
        processed += 1

    if processed:
        session.execute(batch)
        session.execute(latest_batch)


def main() -> None:
    settings = get_settings()
    seed_stock_reference()
    consumer = build_consumer(settings.kafka_market_ticks_topic, settings.kafka_consumer_group_storage)
    producer = build_producer()
    tick_stmt, latest_stmt = prepare_statements()

    while True:
        polled = consumer.poll(timeout_ms=1000, max_records=100)
        for batch in polled.values():
            handle_batch(batch, tick_stmt, latest_stmt, producer)


if __name__ == "__main__":
    main()
