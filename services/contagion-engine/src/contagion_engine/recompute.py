from __future__ import annotations

import argparse
from datetime import UTC, date, datetime, timedelta
from hashlib import sha1
from typing import Any

from cassandra.concurrent import execute_concurrent_with_args

from market_surveillance.db import get_cassandra_session, pg_connection
from market_surveillance.metadata import load_stock_references, sector_lookup, valid_peer_sector
from market_surveillance.models import AnomalyDetection
from market_surveillance.settings import get_settings

from .main import ObservationWindow, build_event, flush_expired, write_event


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Recompute contagion events for a trading date")
    parser.add_argument("--trading-date")
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--latest-backfill-window", "--latest-minute-window", dest="latest_backfill_window", action="store_true")
    parser.add_argument("--source-run-id")
    return parser.parse_args()


def _coerce_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if not text:
        return None
    return date.fromisoformat(text)


def latest_completed_backfill_window() -> tuple[date, date] | None:
    with pg_connection() as conn:
        row = conn.execute(
            """
            SELECT notes
            FROM operational.ingestion_runs
            WHERE mode = 'backfill'
              AND status = 'completed'
            ORDER BY started_at DESC
            LIMIT 1
            """
        ).fetchone()
    if not row:
        return None
    notes = row["notes"] if isinstance(row["notes"], dict) else {}
    start_date = _coerce_date(notes.get("window_start"))
    end_date = _coerce_date(notes.get("window_end"))
    if start_date is None or end_date is None:
        return None
    return start_date, end_date


def trading_dates_for_window(start_date: date, end_date: date) -> list[date]:
    with pg_connection() as conn:
        rows = conn.execute(
            """
            SELECT DISTINCT trading_date
            FROM operational.stock_daily_bars
            WHERE trading_date BETWEEN %s AND %s
            ORDER BY trading_date ASC
            """,
            (start_date, end_date),
        ).fetchall()
    return [row["trading_date"] for row in rows]


def load_anomalies(trading_date: date, source_run_id: str | None) -> list[AnomalyDetection]:
    session = get_cassandra_session()
    stmt = session.prepare(
        """
        SELECT symbol, trading_date, timestamp_utc, timestamp_ist, exchange, sector, interval, close, volume,
               return_pct, ewma_mean, ewma_variance, rolling_volatility,
               volume_mean, volume_variance, price_z_score, volume_z_score,
               composite_score, is_anomalous, explainability, source_run_id, dedupe_key
        FROM anomaly_metrics
        WHERE symbol = ? AND trading_date = ?
        """
    )
    rows: list[AnomalyDetection] = []
    query_args = [(stock.symbol, trading_date) for stock in load_stock_references()]
    for success, result in execute_concurrent_with_args(
        session,
        stmt,
        query_args,
        concurrency=128,
        raise_on_first_error=True,
    ):
        if not success:
            raise result
        for row in result:
            if source_run_id and row["source_run_id"] != source_run_id:
                continue
            normalized = dict(row)
            normalized["trading_date"] = date.fromisoformat(str(normalized["trading_date"]))
            normalized["timestamp_ist"] = datetime.fromisoformat(str(normalized["timestamp_ist"]))
            rows.append(AnomalyDetection.model_validate(normalized))
    rows.sort(key=lambda item: (item.timestamp_utc, item.symbol))
    return rows


def recompute(trading_date: date, source_run_id: str | None = None) -> int:
    print(f"[contagion-recompute] starting trading_date={trading_date.isoformat()}", flush=True)
    anomalies = load_anomalies(trading_date, source_run_id)
    print(
        f"[contagion-recompute] loaded anomaly detections trading_date={trading_date.isoformat()} rows={len(anomalies)}",
        flush=True,
    )
    with pg_connection() as conn:
        conn.execute("DELETE FROM operational.contagion_events WHERE trading_date = %s", (trading_date,))
        conn.execute(
            "DELETE FROM operational.alert_events WHERE event_category = 'contagion' AND trading_date = %s",
            (trading_date,),
        )

    lookup = sector_lookup()
    sector_members: dict[str, set[str]] = {}
    for stock in load_stock_references():
        if not valid_peer_sector(stock.sector):
            continue
        sector_members.setdefault(stock.sector, set()).add(stock.symbol)

    active_windows: dict[str, ObservationWindow] = {}
    for detection in anomalies:
        flush_expired(active_windows, detection.timestamp_utc, update_live_cache=False, emit_alerts=False)

        for trigger_symbol, window in active_windows.items():
            if (
                detection.symbol != trigger_symbol
                and detection.symbol in sector_members.get(window.trigger_sector, set())
                and detection.timestamp_utc <= window.end
                and detection.is_anomalous
            ):
                window.affected_symbols.add(detection.symbol)
                window.peer_scores.append(detection.composite_score)
                write_event(
                    build_event(window, detection.timestamp_utc),
                    update_live_cache=False,
                    emit_alerts=False,
                )

        if detection.symbol in active_windows or not detection.is_anomalous:
            continue

        reference = lookup.get(detection.symbol)
        if not reference or not valid_peer_sector(reference.sector):
            continue
        if len(sector_members.get(reference.sector, set())) < 2:
            continue

        active_windows[detection.symbol] = ObservationWindow(
            trigger_symbol=detection.symbol,
            trigger_sector=reference.sector,
            start=detection.timestamp_utc,
            end=detection.timestamp_utc + timedelta(minutes=get_settings().contagion_window_minutes),
            trigger_score=detection.composite_score,
            source_run_id=detection.source_run_id,
            event_id=sha1(
                f"{detection.symbol}|{detection.timestamp_utc.isoformat()}|{detection.source_run_id}".encode("utf-8")
            ).hexdigest(),
        )

    flush_expired(active_windows, datetime.now(tz=UTC), update_live_cache=False, emit_alerts=False)
    with pg_connection() as conn:
        event_count = int(
            conn.execute(
                "SELECT COUNT(*) AS row_count FROM operational.contagion_events WHERE trading_date = %s",
                (trading_date,),
            ).fetchone()["row_count"]
        )
    print(
        f"[contagion-recompute] completed trading_date={trading_date.isoformat()} events={event_count}",
        flush=True,
    )
    return event_count


def recompute_window(start_date: date, end_date: date, source_run_id: str | None = None) -> list[tuple[date, int]]:
    trading_dates = trading_dates_for_window(start_date, end_date)
    if not trading_dates:
        return []

    print(
        f"[contagion-recompute] starting window start_date={start_date.isoformat()} end_date={end_date.isoformat()} dates={len(trading_dates)}",
        flush=True,
    )
    results: list[tuple[date, int]] = []
    for index, trading_date in enumerate(trading_dates, start=1):
        print(
            f"[contagion-recompute] window progress {index}/{len(trading_dates)} trading_date={trading_date.isoformat()}",
            flush=True,
        )
        results.append((trading_date, recompute(trading_date, source_run_id)))
    print(
        f"[contagion-recompute] completed window start_date={start_date.isoformat()} end_date={end_date.isoformat()}",
        flush=True,
    )
    return results


def main() -> None:
    args = parse_args()
    if args.latest_backfill_window:
        latest_window = latest_completed_backfill_window()
        if latest_window is None:
            raise SystemExit("No completed backfill window with window_start/window_end metadata was found.")
        start_date, end_date = latest_window
        recompute_window(start_date, end_date, args.source_run_id)
        return

    if args.start_date or args.end_date:
        if not args.start_date or not args.end_date:
            raise SystemExit("Both --start-date and --end-date are required for contagion window recompute.")
        start_date = date.fromisoformat(args.start_date)
        end_date = date.fromisoformat(args.end_date)
        if start_date > end_date:
            raise SystemExit("start-date must be on or before end-date")
        recompute_window(start_date, end_date, args.source_run_id)
        return

    if not args.trading_date:
        raise SystemExit("Provide --trading-date, or use --start-date/--end-date, or --latest-backfill-window.")
    recompute(date.fromisoformat(args.trading_date), args.source_run_id)


if __name__ == "__main__":
    main()
