from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from hashlib import sha1

from market_surveillance.alerts import emit_contagion_alert
from market_surveillance.db import get_redis, pg_connection
from market_surveillance.messaging import build_consumer
from market_surveillance.metadata import load_stock_references, sector_lookup, valid_peer_sector
from market_surveillance.models import AnomalyDetection, ContagionEvent
from market_surveillance.serialization import loads
from market_surveillance.settings import get_settings

logger = logging.getLogger(__name__)


@dataclass
class ObservationWindow:
    trigger_symbol: str
    trigger_sector: str
    start: datetime
    end: datetime
    trigger_score: float
    source_run_id: str
    event_id: str
    affected_symbols: set[str] = field(default_factory=set)
    peer_scores: list[float] = field(default_factory=list)


def write_event(
    event: ContagionEvent,
    *,
    update_live_cache: bool = True,
    emit_alerts: bool = True,
) -> None:
    with pg_connection() as conn:
        conn.execute(
            """
            INSERT INTO operational.contagion_events (
                event_id, event_timestamp, trading_date, trigger_symbol, trigger_sector,
                affected_symbols, affected_count, observation_window_start, observation_window_end,
                trigger_composite_score, peer_average_score, risk_score, rationale, source_run_id
            ) VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO UPDATE
            SET event_timestamp = EXCLUDED.event_timestamp,
                affected_symbols = EXCLUDED.affected_symbols,
                affected_count = EXCLUDED.affected_count,
                observation_window_start = EXCLUDED.observation_window_start,
                observation_window_end = EXCLUDED.observation_window_end,
                trigger_composite_score = EXCLUDED.trigger_composite_score,
                peer_average_score = EXCLUDED.peer_average_score,
                risk_score = EXCLUDED.risk_score,
                rationale = EXCLUDED.rationale,
                source_run_id = EXCLUDED.source_run_id
            """,
            (
                event.event_id,
                event.event_timestamp,
                event.trading_date,
                event.trigger_symbol,
                event.trigger_sector,
                json.dumps(event.affected_symbols),
                event.affected_count,
                event.observation_window_start,
                event.observation_window_end,
                event.trigger_composite_score,
                event.peer_average_score,
                event.risk_score,
                event.rationale,
                event.source_run_id,
            ),
        )
    if update_live_cache:
        redis = get_redis()
        redis.set(f"latest:contagion:{event.event_id}", event.model_dump_json(), ex=600)
    if emit_alerts:
        emit_contagion_alert(event)
    logger.info(
        "persisted contagion event trigger=%s affected=%s risk=%.3f live_cache=%s emit_alerts=%s",
        event.trigger_symbol,
        event.affected_count,
        event.risk_score,
        update_live_cache,
        emit_alerts,
    )


def build_event(window: ObservationWindow, event_timestamp: datetime) -> ContagionEvent:
    peer_average = sum(window.peer_scores) / len(window.peer_scores)
    risk_score = window.trigger_score + peer_average + (0.35 * len(window.affected_symbols))
    return ContagionEvent(
        event_id=window.event_id,
        event_timestamp=event_timestamp,
        trading_date=window.start.date(),
        trigger_symbol=window.trigger_symbol,
        trigger_sector=window.trigger_sector,
        affected_symbols=sorted(window.affected_symbols),
        observation_window_start=window.start,
        observation_window_end=window.end,
        trigger_composite_score=window.trigger_score,
        peer_average_score=peer_average,
        risk_score=risk_score,
        rationale="Sector peers crossed anomaly thresholds within the configured 5-minute observation window.",
        source_run_id=window.source_run_id,
    )


def flush_expired(
    active: dict[str, ObservationWindow],
    now: datetime,
    *,
    update_live_cache: bool = True,
    emit_alerts: bool = True,
) -> None:
    expired = [symbol for symbol, window in active.items() if window.end <= now]
    for symbol in expired:
        window = active.pop(symbol)
        if not window.affected_symbols:
            continue
        write_event(
            build_event(window, window.end),
            update_live_cache=update_live_cache,
            emit_alerts=emit_alerts,
        )


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    settings = get_settings()
    consumer = build_consumer(settings.kafka_anomaly_detections_topic, settings.kafka_consumer_group_contagion)
    lookup = sector_lookup()
    sector_members: dict[str, set[str]] = {}
    for stock in load_stock_references():
        if not valid_peer_sector(stock.sector):
            continue
        sector_members.setdefault(stock.sector, set()).add(stock.symbol)

    active_windows: dict[str, ObservationWindow] = {}

    while True:
        flush_expired(active_windows, datetime.now(tz=UTC))
        polled = consumer.poll(timeout_ms=1000, max_records=100)
        for batch in polled.values():
            for record in batch:
                detection = AnomalyDetection.model_validate(loads(record.value))
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
                    end=detection.timestamp_utc + timedelta(minutes=settings.contagion_window_minutes),
                    trigger_score=detection.composite_score,
                    source_run_id=detection.source_run_id,
                    event_id=sha1(
                        f"{detection.symbol}|{detection.timestamp_utc.isoformat()}|{detection.source_run_id}".encode("utf-8")
                    ).hexdigest(),
                )
            flush_expired(active_windows, datetime.now(tz=UTC))


if __name__ == "__main__":
    main()
