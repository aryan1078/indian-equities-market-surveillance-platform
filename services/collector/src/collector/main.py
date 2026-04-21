from __future__ import annotations

import argparse
import json
import time
from collections import defaultdict
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Iterable

import pandas as pd
import yfinance as yf

from market_surveillance.bootstrap import ensure_runtime_dirs
from market_surveillance.db import get_cassandra_session, get_redis, pg_connection
from market_surveillance.demo_seed import generate_demo_replay_fixture, seed_demo_daily_history
from market_surveillance.history import hydrate_daily_history, sync_metadata_profiles
from market_surveillance.market_time import as_market_time, ensure_utc, in_market_hours
from market_surveillance.messaging import build_producer
from market_surveillance.metadata import StockReference, active_symbols, load_stock_references, sector_lookup, watchlist_symbols
from market_surveillance.models import EventSource, MarketTick
from market_surveillance.serialization import loads
from market_surveillance.settings import get_settings

from .minute_backfill import bulk_backfill_minutes


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Market data collector")
    subparsers = parser.add_subparsers(dest="command", required=True)

    backfill = subparsers.add_parser("backfill")
    backfill.add_argument("--symbols", nargs="*", default=[])
    backfill.add_argument("--period", default="5d")
    backfill.add_argument("--interval", default="1m")
    backfill.add_argument("--persist", action="store_true")

    hydrate_daily = subparsers.add_parser("hydrate-daily")
    hydrate_daily.add_argument("--symbols", nargs="*", default=[])
    hydrate_daily.add_argument("--period", default="3mo")

    seed_history = subparsers.add_parser("seed-history")
    seed_history.add_argument("--symbols", nargs="*", default=[])
    seed_history.add_argument("--sessions", type=int, default=55)
    seed_history.add_argument("--end-date")

    generate_replay = subparsers.add_parser("generate-replay")
    generate_replay.add_argument("--symbols", nargs="*", default=[])
    generate_replay.add_argument("--trading-date", default=get_settings().default_trading_date)
    generate_replay.add_argument("--minutes", type=int, default=28)
    generate_replay.add_argument("--output", default="tests/fixtures/replay_ticks.jsonl")

    minute_backfill = subparsers.add_parser("minute-backfill")
    minute_backfill.add_argument("--trading-days", type=int, default=12)
    minute_backfill.add_argument("--start-date")
    minute_backfill.add_argument("--end-date")
    minute_backfill.add_argument("--symbols-limit", type=int)
    minute_backfill.add_argument("--flush-rows", type=int, default=6000)
    minute_backfill.add_argument("--cassandra-concurrency", type=int, default=128)

    live = subparsers.add_parser("live")
    live.add_argument("--symbols", nargs="*", default=[])
    live.add_argument("--poll-seconds", type=int, default=60)
    live.add_argument("--period", default="1d")
    live.add_argument("--interval", default="1m")
    live.add_argument("--once", action="store_true")

    replay = subparsers.add_parser("replay")
    replay.add_argument("--fixture", required=True)
    replay.add_argument("--speed", type=float, default=30.0)

    return parser


def start_ingestion_run(mode: str, symbol_count: int) -> str:
    source = EventSource(mode=mode)
    with pg_connection() as conn:
        conn.execute(
            """
            INSERT INTO operational.ingestion_runs (run_id, mode, symbol_count)
            VALUES (%s, %s, %s)
            ON CONFLICT (run_id) DO NOTHING
            """,
            (source.run_id, mode, symbol_count),
        )
    return source.run_id


def finish_ingestion_run(run_id: str, records_seen: int, records_published: int, status: str) -> None:
    with pg_connection() as conn:
        conn.execute(
            """
            UPDATE operational.ingestion_runs
            SET finished_at = now(),
                records_seen = %s,
                records_published = %s,
                status = %s
            WHERE run_id = %s
            """,
            (records_seen, records_published, status, run_id),
        )


def annotate_ingestion_run(run_id: str, notes: dict[str, object]) -> None:
    with pg_connection() as conn:
        conn.execute(
            """
            UPDATE operational.ingestion_runs
            SET notes = %s::jsonb
            WHERE run_id = %s
            """,
            (json.dumps(notes), run_id),
        )


def demo_symbols() -> list[str]:
    return watchlist_symbols()


def universe_symbols() -> list[str]:
    return active_symbols()


def _reference_for(symbol: str) -> StockReference:
    reference = sector_lookup().get(symbol)
    if reference:
        return reference
    return StockReference(symbol=symbol, exchange="NSE", sector="Unknown", company_name=symbol)


def normalize_frame(symbol: str, frame: pd.DataFrame, source: EventSource) -> list[MarketTick]:
    if frame.empty:
        return []

    reference = _reference_for(symbol)
    rows = frame.reset_index()
    timestamp_column = rows.columns[0]
    normalized: list[MarketTick] = []

    for _, row in rows.iterrows():
        timestamp = row[timestamp_column].to_pydatetime() if hasattr(row[timestamp_column], "to_pydatetime") else row[timestamp_column]
        timestamp_utc = ensure_utc(timestamp)
        if source.mode == "backfill" and not in_market_hours(timestamp_utc):
            continue

        if pd.isna(row.get("Close")):
            continue

        tick = MarketTick(
            symbol=symbol,
            exchange=reference.exchange,
            sector=reference.sector,
            interval="1m",
            timestamp_utc=timestamp_utc,
            timestamp_ist=as_market_time(timestamp_utc),
            trading_date=as_market_time(timestamp_utc).date(),
            open=float(row.get("Open", 0.0)),
            high=float(row.get("High", 0.0)),
            low=float(row.get("Low", 0.0)),
            close=float(row.get("Close", 0.0)),
            volume=int(row.get("Volume", 0) or 0),
            dividends=float(row.get("Dividends", 0.0) or 0.0),
            stock_splits=float(row.get("Stock Splits", 0.0) or 0.0),
            source=source,
        )
        normalized.append(tick)

    return normalized


def persist_ticks(ticks: Iterable[MarketTick], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        for tick in ticks:
            handle.write(tick.model_dump_json())
            handle.write("\n")


def publish_ticks(ticks: Iterable[MarketTick]) -> int:
    settings = get_settings()
    producer = build_producer()
    count = 0
    for tick in ticks:
        producer.send(
            settings.kafka_market_ticks_topic,
            key=tick.symbol.encode("utf-8"),
            value=tick.model_dump(mode="json"),
        )
        count += 1
    producer.flush()
    return count


def reset_replay_state(records: list[MarketTick]) -> None:
    if not records:
        return

    symbols_by_date: dict[date, set[str]] = defaultdict(set)
    for record in records:
        symbols_by_date[record.trading_date].add(record.symbol)

    redis = get_redis()
    stale_patterns = [
        "latest:market:*",
        "latest:anomaly:*",
        "latest:contagion:*",
        "latest:alert:*",
        "sector:latest:*",
    ]
    for pattern in stale_patterns:
        keys = list(redis.scan_iter(pattern))
        if keys:
            redis.delete(*keys)
    redis.delete("system:last_tick")

    for trading_day, symbols in symbols_by_date.items():
        for symbol in symbols:
            redis.delete(f"state:anomaly:{symbol}:{trading_day.isoformat()}")

    session = get_cassandra_session()
    delete_tick_stmt = session.prepare("DELETE FROM market_ticks WHERE symbol = ? AND trading_date = ?")
    delete_anomaly_stmt = session.prepare("DELETE FROM anomaly_metrics WHERE symbol = ? AND trading_date = ?")
    delete_latest_stmt = session.prepare("DELETE FROM latest_market_state WHERE symbol = ?")

    for trading_day, symbols in symbols_by_date.items():
        for symbol in symbols:
            session.execute(delete_tick_stmt, (symbol, trading_day))
            session.execute(delete_anomaly_stmt, (symbol, trading_day))
            session.execute(delete_latest_stmt, (symbol,))

    trading_days = sorted(symbols_by_date)
    with pg_connection() as conn:
        conn.execute(
            "DELETE FROM operational.contagion_events WHERE trading_date = ANY(%s)",
            (trading_days,),
        )
        conn.execute(
            "DELETE FROM operational.surveillance_coverage WHERE trading_date = ANY(%s)",
            (trading_days,),
        )
        conn.execute(
            "DELETE FROM operational.alert_events WHERE trading_date = ANY(%s)",
            (trading_days,),
        )


def hydrate_daily(symbols: list[str], period: str) -> None:
    ensure_runtime_dirs()
    sync_metadata_profiles()
    selected = symbols or universe_symbols()
    run_id = start_ingestion_run("hydrate_daily", len(selected))
    try:
        results = hydrate_daily_history(selected, period=period)
        finish_ingestion_run(run_id, sum(results.values()), len(results), "completed")
        with pg_connection() as conn:
            conn.execute(
                """
                UPDATE operational.ingestion_runs
                SET notes = %s::jsonb
                WHERE run_id = %s
                """,
                (json.dumps({"period": period, "hydrated_symbols": sorted(results)}), run_id),
            )
    except Exception:
        finish_ingestion_run(run_id, 0, 0, "failed")
        raise


def seed_history(symbols: list[str], sessions: int, end_date: str | None) -> None:
    ensure_runtime_dirs()
    sync_metadata_profiles()
    selected = symbols or demo_symbols()
    run_id = start_ingestion_run("seed_history", len(selected))
    seed_end_date = date.fromisoformat(end_date) if end_date else None
    try:
        results = seed_demo_daily_history(selected, sessions=sessions, end_date=seed_end_date)
        finish_ingestion_run(run_id, sum(results.values()), len(results), "completed")
        with pg_connection() as conn:
            conn.execute(
                """
                UPDATE operational.ingestion_runs
                SET notes = %s::jsonb
                WHERE run_id = %s
                """,
                (
                    json.dumps(
                        {
                            "sessions": sessions,
                            "end_date": seed_end_date.isoformat() if seed_end_date else None,
                            "seeded_symbols": sorted(results),
                        }
                    ),
                    run_id,
                ),
            )
    except Exception:
        finish_ingestion_run(run_id, 0, 0, "failed")
        raise


def generate_replay(symbols: list[str], trading_day: str, minutes: int, output: str) -> None:
    ensure_runtime_dirs()
    selected = symbols or demo_symbols()
    run_id = start_ingestion_run("generate_replay_fixture", len(selected))
    try:
        output_path = Path(output)
        row_count = generate_demo_replay_fixture(output_path, date.fromisoformat(trading_day), selected, minutes=minutes)
        finish_ingestion_run(run_id, row_count, row_count, "completed")
        with pg_connection() as conn:
            conn.execute(
                """
                UPDATE operational.ingestion_runs
                SET notes = %s::jsonb
                WHERE run_id = %s
                """,
                (
                    json.dumps(
                        {
                            "output": str(output_path),
                            "trading_date": trading_day,
                            "minutes": minutes,
                        }
                    ),
                    run_id,
                ),
            )
    except Exception:
        finish_ingestion_run(run_id, 0, 0, "failed")
        raise


def _live_state_key(symbol: str) -> str:
    return f"collector:live:last:{symbol}"


def _load_live_watermark(symbol: str) -> datetime | None:
    redis = get_redis()
    raw = redis.get(_live_state_key(symbol))
    if not raw:
        return None
    return ensure_utc(datetime.fromisoformat(raw.replace("Z", "+00:00")))


def _save_live_watermark(symbol: str, timestamp: datetime) -> None:
    redis = get_redis()
    redis.set(_live_state_key(symbol), ensure_utc(timestamp).isoformat())


def _collect_live_ticks(symbols: list[str], source: EventSource, period: str, interval: str) -> list[MarketTick]:
    collected: list[MarketTick] = []
    for symbol in symbols:
        frame = yf.download(symbol, period=period, interval=interval, auto_adjust=False, progress=False, threads=False)
        ticks = normalize_frame(symbol, frame, source)
        watermark = _load_live_watermark(symbol)
        fresh = [tick for tick in ticks if watermark is None or tick.timestamp_utc > watermark]
        if fresh:
            _save_live_watermark(symbol, fresh[-1].timestamp_utc)
            collected.extend(fresh)
    return collected


def live(symbols: list[str], poll_seconds: int, period: str, interval: str, once: bool) -> None:
    ensure_runtime_dirs()
    sync_metadata_profiles()
    selected = symbols or demo_symbols()
    run_id = start_ingestion_run("live", len(selected))
    source = EventSource(mode="live", run_id=run_id)
    records_seen = 0
    records_published = 0

    try:
        while True:
            ticks = _collect_live_ticks(selected, source, period, interval)
            records_seen += len(ticks)
            records_published += publish_ticks(ticks)
            if once:
                break
            time.sleep(max(poll_seconds, 5))
    except KeyboardInterrupt:
        finish_ingestion_run(run_id, records_seen, records_published, "stopped")
        return

    finish_ingestion_run(run_id, records_seen, records_published, "completed")
    annotate_ingestion_run(
        run_id,
        {
            "poll_seconds": poll_seconds,
            "period": period,
            "interval": interval,
            "mode": "live",
        },
    )


def backfill(symbols: list[str], period: str, interval: str, persist: bool) -> None:
    ensure_runtime_dirs()
    sync_metadata_profiles()
    selected = symbols or demo_symbols()
    run_id = start_ingestion_run("backfill", len(selected))
    source = EventSource(mode="backfill", run_id=run_id)
    all_ticks: list[MarketTick] = []

    for symbol in selected:
        frame = yf.download(symbol, period=period, interval=interval, auto_adjust=False, progress=False, threads=False)
        all_ticks.extend(normalize_frame(symbol, frame, source))

    published = publish_ticks(all_ticks)
    if persist and all_ticks:
        trading_day = all_ticks[0].trading_date
        persist_ticks(all_ticks, get_settings().data_root / "replay" / f"{trading_day}_backfill.jsonl")
    finish_ingestion_run(run_id, len(all_ticks), published, "completed")
    annotate_ingestion_run(
        run_id,
        {
            "period": period,
            "interval": interval,
            "persisted_fixture": persist,
            "provider": "yfinance",
            "symbol_count": len(selected),
        },
    )


def minute_backfill(
    trading_days: int,
    start_date: str | None,
    end_date: str | None,
    symbols_limit: int | None,
    flush_rows: int,
    cassandra_concurrency: int,
) -> None:
    ensure_runtime_dirs()
    sync_metadata_profiles()
    run_id = start_ingestion_run("minute_backfill", symbols_limit or len(universe_symbols()))
    final_start = date.fromisoformat(start_date) if start_date else None
    final_end = date.fromisoformat(end_date) if end_date else None
    try:
        notes = bulk_backfill_minutes(
            run_id=run_id,
            trading_days=trading_days,
            start_date=final_start,
            end_date=final_end,
            symbols_limit=symbols_limit,
            flush_rows=flush_rows,
            cassandra_concurrency=cassandra_concurrency,
        )
        finish_ingestion_run(run_id, int(notes["tick_rows_written"]), int(notes["tick_rows_written"]), "completed")
        annotate_ingestion_run(run_id, notes)
    except Exception:
        finish_ingestion_run(run_id, 0, 0, "failed")
        raise


def replay(fixture_path: Path, speed: float) -> None:
    ensure_runtime_dirs()
    sync_metadata_profiles()
    records = [MarketTick.model_validate(loads(line)) for line in fixture_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    if not records:
        return

    reset_replay_state(records)
    run_id = start_ingestion_run("replay", len({record.symbol for record in records}))
    producer = build_producer()
    last_ts: datetime | None = None
    settings = get_settings()

    for record in records:
        replay_tick = record.model_copy(
            update={
                "source": EventSource(mode="replay", run_id=run_id),
                "timestamp_utc": record.timestamp_utc,
                "timestamp_ist": record.timestamp_ist,
                "trading_date": record.trading_date,
            }
        )
        producer.send(
            settings.kafka_market_ticks_topic,
            key=replay_tick.symbol.encode("utf-8"),
            value=replay_tick.model_dump(mode="json"),
        )
        if last_ts is not None:
            delay = max((replay_tick.timestamp_utc - last_ts).total_seconds() / max(speed, 1.0), 0.0)
            time.sleep(min(delay, 0.5))
        last_ts = replay_tick.timestamp_utc

    producer.flush()
    with pg_connection() as conn:
        conn.execute(
            """
            INSERT INTO operational.ingestion_runs (run_id, mode, symbol_count, records_seen, records_published, status, finished_at)
            VALUES (%s, %s, %s, %s, %s, 'completed', now())
            ON CONFLICT (run_id) DO UPDATE
            SET records_seen = EXCLUDED.records_seen,
                records_published = EXCLUDED.records_published,
                status = EXCLUDED.status,
                finished_at = EXCLUDED.finished_at
            """,
            (run_id, "replay", len({record.symbol for record in records}), len(records), len(records)),
        )
    with pg_connection() as conn:
        conn.execute(
            """
            UPDATE operational.ingestion_runs
            SET notes = %s::jsonb
            WHERE run_id = %s
            """,
            (
                json.dumps(
                    {
                        "fixture": str(fixture_path),
                        "speed": speed,
                        "trading_date": records[0].trading_date.isoformat(),
                    }
                ),
                run_id,
            ),
        )


def main() -> None:
    args = build_parser().parse_args()
    if args.command == "backfill":
        backfill(args.symbols, args.period, args.interval, args.persist)
    elif args.command == "minute-backfill":
        minute_backfill(
            args.trading_days,
            args.start_date,
            args.end_date,
            args.symbols_limit,
            args.flush_rows,
            args.cassandra_concurrency,
        )
    elif args.command == "hydrate-daily":
        hydrate_daily(args.symbols, args.period)
    elif args.command == "seed-history":
        seed_history(args.symbols, args.sessions, args.end_date)
    elif args.command == "generate-replay":
        generate_replay(args.symbols, args.trading_date, args.minutes, args.output)
    elif args.command == "live":
        live(args.symbols, args.poll_seconds, args.period, args.interval, args.once)
    elif args.command == "replay":
        replay(Path(args.fixture), args.speed)


if __name__ == "__main__":
    main()
