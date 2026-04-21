from __future__ import annotations

import argparse
from datetime import UTC, date, datetime, timedelta
from hashlib import sha1

from market_surveillance.db import get_cassandra_session, pg_connection
from market_surveillance.metadata import load_stock_references, sector_lookup, valid_peer_sector
from market_surveillance.models import AnomalyDetection

from .main import ObservationWindow, build_event, flush_expired, write_event


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Recompute contagion events for a trading date")
    parser.add_argument("--trading-date", required=True)
    parser.add_argument("--source-run-id")
    return parser.parse_args()


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
    for stock in load_stock_references():
        for row in session.execute(stmt, (stock.symbol, trading_date)):
            if source_run_id and row["source_run_id"] != source_run_id:
                continue
            normalized = dict(row)
            normalized["trading_date"] = date.fromisoformat(str(normalized["trading_date"]))
            normalized["timestamp_ist"] = datetime.fromisoformat(str(normalized["timestamp_ist"]))
            rows.append(AnomalyDetection.model_validate(normalized))
    rows.sort(key=lambda item: (item.timestamp_utc, item.symbol))
    return rows


def recompute(trading_date: date, source_run_id: str | None = None) -> None:
    anomalies = load_anomalies(trading_date, source_run_id)
    with pg_connection() as conn:
        conn.execute("DELETE FROM operational.contagion_events WHERE trading_date = %s", (trading_date,))

    lookup = sector_lookup()
    sector_members: dict[str, set[str]] = {}
    for stock in load_stock_references():
        if not valid_peer_sector(stock.sector):
            continue
        sector_members.setdefault(stock.sector, set()).add(stock.symbol)

    active_windows: dict[str, ObservationWindow] = {}
    for detection in anomalies:
        flush_expired(active_windows, detection.timestamp_utc)

        for trigger_symbol, window in active_windows.items():
            if (
                detection.symbol != trigger_symbol
                and detection.symbol in sector_members.get(window.trigger_sector, set())
                and detection.timestamp_utc <= window.end
                and detection.is_anomalous
            ):
                window.affected_symbols.add(detection.symbol)
                window.peer_scores.append(detection.composite_score)
                write_event(build_event(window, detection.timestamp_utc))

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
            end=detection.timestamp_utc + timedelta(minutes=5),
            trigger_score=detection.composite_score,
            source_run_id=detection.source_run_id,
            event_id=sha1(
                f"{detection.symbol}|{detection.timestamp_utc.isoformat()}|{detection.source_run_id}".encode("utf-8")
            ).hexdigest(),
        )

    flush_expired(active_windows, datetime.now(tz=UTC))


def main() -> None:
    args = parse_args()
    recompute(date.fromisoformat(args.trading_date), args.source_run_id)


if __name__ == "__main__":
    main()
