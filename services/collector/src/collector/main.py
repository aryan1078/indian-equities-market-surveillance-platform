from __future__ import annotations

import argparse
import json
import time
from collections import defaultdict
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Iterable

import pandas as pd

from market_surveillance.bootstrap import ensure_runtime_dirs
from market_surveillance.db import get_cassandra_session, get_redis, pg_connection
from market_surveillance.history import hydrate_daily_history, significant_intraday_symbols, sync_metadata_profiles
from market_surveillance.market_data import download_market_frames, is_intraday_interval, is_real_source, preferred_market_data_provider
from market_surveillance.market_time import as_market_time, ensure_utc, in_market_hours
from market_surveillance.messaging import build_producer
from market_surveillance.metadata import StockReference, active_symbols, sector_lookup, watchlist_symbols
from market_surveillance.models import EventSource, MarketTick
from market_surveillance.serialization import loads
from market_surveillance.settings import get_settings


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Market data collector")
    subparsers = parser.add_subparsers(dest="command", required=True)

    backfill = subparsers.add_parser("backfill")
    backfill.add_argument("--symbols", nargs="*", default=[])
    backfill.add_argument("--period", default="5d")
    backfill.add_argument("--interval", default="1m")
    backfill.add_argument("--start-date")
    backfill.add_argument("--end-date")
    backfill.add_argument("--persist", action="store_true")

    hydrate_daily = subparsers.add_parser("hydrate-daily")
    hydrate_daily.add_argument("--symbols", nargs="*", default=[])
    hydrate_daily.add_argument("--period", default="3mo")

    capture_replay = subparsers.add_parser("capture-replay")
    capture_replay.add_argument("--symbols", nargs="*", default=[])
    capture_replay.add_argument("--trading-date")
    capture_replay.add_argument("--period", default="5d")
    capture_replay.add_argument("--interval", default="1m")
    capture_replay.add_argument("--output", default="tests/fixtures/replay_ticks.real.jsonl")

    live = subparsers.add_parser("live")
    live.add_argument("--symbols", nargs="*", default=[])
    live.add_argument("--poll-seconds", type=int, default=60)
    live.add_argument("--period", default="1d")
    live.add_argument("--interval", default="1m")
    live.add_argument("--once", action="store_true")

    replay = subparsers.add_parser("replay")
    replay.add_argument("--fixture", required=True)
    replay.add_argument("--speed", type=float, default=30.0)

    purge = subparsers.add_parser("purge-derived")
    purge.add_argument("--keep-ingestion-runs", action="store_true")

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
            SET notes = COALESCE(notes, '{}'::jsonb) || %s::jsonb
            WHERE run_id = %s
            """,
            (json.dumps(notes), run_id),
        )


def demo_symbols() -> list[str]:
    return significant_intraday_symbols()


def universe_symbols() -> list[str]:
    return active_symbols()


def _reference_for(symbol: str) -> StockReference:
    reference = sector_lookup().get(symbol)
    if reference:
        return reference
    return StockReference(symbol=symbol, exchange="NSE", sector="Unknown", company_name=symbol)


def _coerce_date(value: str | None) -> date | None:
    if not value:
        return None
    return date.fromisoformat(value)


def normalize_frame(symbol: str, frame: pd.DataFrame, source: EventSource, interval: str) -> list[MarketTick]:
    if frame.empty:
        return []

    reference = _reference_for(symbol)
    rows = frame.reset_index()
    timestamp_column = rows.columns[0]
    normalized: list[MarketTick] = []

    for _, row in rows.iterrows():
        timestamp = row[timestamp_column].to_pydatetime() if hasattr(row[timestamp_column], "to_pydatetime") else row[timestamp_column]
        timestamp_utc = ensure_utc(timestamp)
        if is_intraday_interval(interval) and source.mode in {"backfill", "capture_replay"} and not in_market_hours(timestamp_utc):
            continue

        if pd.isna(row.get("Close")):
            continue

        tick = MarketTick(
            symbol=symbol,
            exchange=reference.exchange,
            sector=reference.sector,
            interval=interval,
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


def _sort_ticks(ticks: Iterable[MarketTick]) -> list[MarketTick]:
    return sorted(ticks, key=lambda tick: (tick.trading_date, tick.timestamp_utc, tick.symbol))


def persist_ticks(ticks: Iterable[MarketTick], output_path: Path) -> None:
    ordered = _sort_ticks(ticks)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        for tick in ordered:
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
        "collector:live:last:*",
        "state:anomaly:*",
    ]
    for pattern in stale_patterns:
        keys = list(redis.scan_iter(pattern))
        if keys:
            redis.delete(*keys)
    redis.delete("system:last_tick")

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
        conn.execute("DELETE FROM operational.contagion_events WHERE trading_date = ANY(%s)", (trading_days,))
        conn.execute("DELETE FROM operational.surveillance_coverage WHERE trading_date = ANY(%s)", (trading_days,))
        conn.execute("DELETE FROM operational.alert_events WHERE trading_date = ANY(%s)", (trading_days,))


def hydrate_daily(symbols: list[str], period: str) -> None:
    ensure_runtime_dirs()
    sync_metadata_profiles()
    selected = symbols or universe_symbols()
    run_id = start_ingestion_run("hydrate_daily", len(selected))
    provider_name = preferred_market_data_provider()
    try:
        results = hydrate_daily_history(selected, period=period)
        finish_ingestion_run(run_id, sum(results.values()), len(results), "completed")
        annotate_ingestion_run(
            run_id,
            {
                "period": period,
                "hydrated_symbols": sorted(results),
                "provider": provider_name,
            },
        )
    except Exception:
        finish_ingestion_run(run_id, 0, 0, "failed")
        raise


def _download_ticks(
    symbols: list[str],
    interval: str,
    mode: str,
    period: str | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    run_id: str | None = None,
) -> list[MarketTick]:
    frames = download_market_frames(symbols, interval=interval, period=period, start_date=start_date, end_date=end_date)
    ticks: list[MarketTick] = []
    for symbol, provider_frame in frames.items():
        source = EventSource(provider=provider_frame.provider, mode=mode, run_id=run_id or EventSource(mode=mode).run_id)
        ticks.extend(normalize_frame(symbol, provider_frame.frame, source, interval))
    return _sort_ticks(ticks)


def _latest_trading_day(ticks: list[MarketTick]) -> date | None:
    if not ticks:
        return None
    return max(tick.trading_date for tick in ticks)


def _persist_latest_session_fixture(ticks: list[MarketTick], output_path: Path) -> date | None:
    latest_day = _latest_trading_day(ticks)
    if latest_day is None:
        return None
    latest_ticks = [tick for tick in ticks if tick.trading_date == latest_day]
    persist_ticks(latest_ticks, output_path)
    return latest_day


def capture_replay(symbols: list[str], trading_day: str | None, period: str, interval: str, output: str) -> None:
    ensure_runtime_dirs()
    sync_metadata_profiles()
    selected = symbols or demo_symbols()
    run_id = start_ingestion_run("capture_replay", len(selected))
    try:
        selected_date = _coerce_date(trading_day)
        ticks = _download_ticks(
            selected,
            interval=interval,
            mode="capture_replay",
            period=period,
            start_date=selected_date,
            end_date=selected_date,
            run_id=run_id,
        )
        if selected_date is not None:
            ticks = [tick for tick in ticks if tick.trading_date == selected_date]
        output_path = Path(output)
        final_day = selected_date
        if not final_day:
            final_day = _persist_latest_session_fixture(ticks, output_path)
        else:
            persist_ticks([tick for tick in ticks if tick.trading_date == final_day], output_path)
        published_rows = len([tick for tick in ticks if final_day is None or tick.trading_date == final_day])
        finish_ingestion_run(run_id, len(ticks), published_rows, "completed")
        annotate_ingestion_run(
            run_id,
            {
                "fixture": str(output_path),
                "trading_date": final_day.isoformat() if final_day else None,
                "interval": interval,
                "period": period,
                "providers": sorted({tick.source.provider for tick in ticks}),
            },
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


def _collect_live_ticks(symbols: list[str], period: str, interval: str, run_id: str) -> list[MarketTick]:
    collected = _download_ticks(symbols, interval=interval, mode="live", period=period, run_id=run_id)
    fresh_ticks: list[MarketTick] = []
    for tick in collected:
        watermark = _load_live_watermark(tick.symbol)
        if watermark is not None and tick.timestamp_utc <= watermark:
            continue
        _save_live_watermark(tick.symbol, tick.timestamp_utc)
        fresh_ticks.append(tick)
    return fresh_ticks


def live(symbols: list[str], poll_seconds: int, period: str, interval: str, once: bool) -> None:
    ensure_runtime_dirs()
    sync_metadata_profiles()
    selected = symbols or demo_symbols()
    run_id = start_ingestion_run("live", len(selected))
    records_seen = 0
    records_published = 0

    try:
        while True:
            ticks = _collect_live_ticks(selected, period, interval, run_id)
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
            "provider_policy": get_settings().market_data_provider,
        },
    )


def backfill(
    symbols: list[str],
    period: str,
    interval: str,
    persist: bool,
    start_date: str | None,
    end_date: str | None,
) -> None:
    ensure_runtime_dirs()
    sync_metadata_profiles()
    selected = symbols or demo_symbols()
    run_id = start_ingestion_run("backfill", len(selected))
    all_ticks = _download_ticks(
        selected,
        interval=interval,
        mode="backfill",
        period=period,
        start_date=_coerce_date(start_date),
        end_date=_coerce_date(end_date),
        run_id=run_id,
    )

    published = publish_ticks(all_ticks)
    persisted_fixture = None
    if persist and all_ticks:
        latest_day = _latest_trading_day(all_ticks)
        persisted_fixture = get_settings().fixture_root / "replay_ticks.real.jsonl"
        if latest_day is not None:
            persist_ticks([tick for tick in all_ticks if tick.trading_date == latest_day], persisted_fixture)
    trading_dates = sorted({tick.trading_date for tick in all_ticks})
    providers = sorted({tick.source.provider for tick in all_ticks})
    finish_ingestion_run(run_id, len(all_ticks), published, "completed")
    annotate_ingestion_run(
        run_id,
        {
            "period": period,
            "interval": interval,
            "start_date": start_date,
            "end_date": end_date,
            "window_start": trading_dates[0].isoformat() if trading_dates else None,
            "window_end": trading_dates[-1].isoformat() if trading_dates else None,
            "trading_day_count": len(trading_dates),
            "persisted_fixture": str(persisted_fixture) if persisted_fixture else None,
            "providers": providers,
            "provider": providers[0] if len(providers) == 1 else "mixed",
            "symbol_count": len(selected),
            "requested_symbol_count": len(selected),
            "tick_rows_written": len(all_ticks),
        },
    )


def _validate_real_fixture(records: list[MarketTick]) -> None:
    settings = get_settings()
    if not settings.strict_real_data_only:
        return
    for record in records:
        if not is_real_source(record.source.provider, record.source.mode):
            raise RuntimeError(
                f"Fixture contains non-real data for {record.symbol} at {record.timestamp_utc.isoformat()} "
                f"from provider={record.source.provider!r}, mode={record.source.mode!r}"
            )


def replay(fixture_path: Path, speed: float) -> None:
    ensure_runtime_dirs()
    sync_metadata_profiles()
    records = [MarketTick.model_validate(loads(line)) for line in fixture_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    if not records:
        return

    _validate_real_fixture(records)
    reset_replay_state(records)
    run_id = start_ingestion_run("replay", len({record.symbol for record in records}))
    producer = build_producer()
    last_ts: datetime | None = None
    settings = get_settings()

    for record in records:
        replay_tick = record.model_copy(
            update={
                "source": EventSource(provider=record.source.provider, mode="replay", run_id=run_id),
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
            UPDATE operational.ingestion_runs
            SET records_seen = %s,
                records_published = %s,
                status = 'completed',
                finished_at = now()
            WHERE run_id = %s
            """,
            (len(records), len(records), run_id),
        )
    annotate_ingestion_run(
        run_id,
        {
            "fixture": str(fixture_path),
            "speed": speed,
            "trading_date": records[0].trading_date.isoformat(),
            "providers": sorted({record.source.provider for record in records}),
            "real_only": True,
        },
    )


def purge_derived(keep_ingestion_runs: bool) -> None:
    redis = get_redis()
    for pattern in [
        "latest:market:*",
        "latest:anomaly:*",
        "latest:contagion:*",
        "latest:alert:*",
        "sector:latest:*",
        "collector:live:last:*",
        "state:anomaly:*",
    ]:
        keys = list(redis.scan_iter(pattern))
        if keys:
            redis.delete(*keys)
    redis.delete("system:last_tick")

    session = get_cassandra_session()
    for table_name in ["market_ticks", "anomaly_metrics", "latest_market_state"]:
        session.execute(f"TRUNCATE {table_name}")

    with pg_connection() as conn:
        conn.execute("TRUNCATE staging.anomaly_metrics_stage")
        conn.execute("TRUNCATE operational.surveillance_coverage")
        conn.execute("TRUNCATE operational.contagion_events")
        conn.execute("TRUNCATE operational.alert_events")
        conn.execute("TRUNCATE operational.etl_runs")
        conn.execute("TRUNCATE warehouse.fact_surveillance_coverage")
        conn.execute("TRUNCATE warehouse.fact_contagion_event")
        conn.execute("TRUNCATE warehouse.fact_market_day")
        conn.execute("TRUNCATE warehouse.fact_anomaly_minute")
        if not keep_ingestion_runs:
            conn.execute(
                """
                DELETE FROM operational.ingestion_runs
                WHERE mode <> 'hydrate_daily'
                   OR (notes ->> 'provider') IN ('fixture', 'deterministic_daily_expansion')
                """
            )
        for view_name in [
            "warehouse.mv_sector_daily_summary",
            "warehouse.mv_sector_monthly_summary",
            "warehouse.mv_sector_regime_summary",
            "warehouse.mv_stock_signal_leaders",
            "warehouse.mv_sector_momentum_summary",
            "warehouse.mv_stock_persistence_summary",
            "warehouse.mv_intraday_pressure_profile",
        ]:
            conn.execute(f"REFRESH MATERIALIZED VIEW {view_name}")


def main() -> None:
    args = build_parser().parse_args()
    if args.command == "backfill":
        backfill(args.symbols, args.period, args.interval, args.persist, args.start_date, args.end_date)
    elif args.command == "hydrate-daily":
        hydrate_daily(args.symbols, args.period)
    elif args.command == "capture-replay":
        capture_replay(args.symbols, args.trading_date, args.period, args.interval, args.output)
    elif args.command == "live":
        live(args.symbols, args.poll_seconds, args.period, args.interval, args.once)
    elif args.command == "replay":
        replay(Path(args.fixture), args.speed)
    elif args.command == "purge-derived":
        purge_derived(args.keep_ingestion_runs)


if __name__ == "__main__":
    main()
