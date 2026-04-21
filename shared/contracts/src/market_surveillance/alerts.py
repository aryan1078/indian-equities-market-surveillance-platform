from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from hashlib import sha1
from typing import Any

import httpx

from .db import get_redis, pg_connection
from .market_time import ensure_utc
from .models import AnomalyDetection, ContagionEvent
from .settings import get_settings

logger = logging.getLogger(__name__)

SEVERITY_RANK = {
    "low": 0,
    "medium": 1,
    "high": 2,
    "critical": 3,
}


def _cooldown_key(symbol: str, category: str) -> str:
    return f"alert:cooldown:{category}:{symbol}"


def _latest_alert_key(event_id: str) -> str:
    return f"latest:alert:{event_id}"


def _parse_iso_datetime(value: str) -> datetime:
    return ensure_utc(datetime.fromisoformat(value.replace("Z", "+00:00")))


def _notification_summary(payload: dict[str, Any]) -> str:
    return (
        f"[{str(payload['severity']).upper()}] "
        f"{payload['symbol']} {payload['event_category']} | {payload['message']}"
    )


def _safe_payload(payload: dict[str, Any]) -> dict[str, Any]:
    return json.loads(json.dumps(payload, default=str))


def _should_notify(payload: dict[str, Any]) -> bool:
    settings = get_settings()
    if not settings.alert_webhook_url:
        return False
    payload_rank = SEVERITY_RANK.get(str(payload.get("severity", "low")), 0)
    minimum_rank = SEVERITY_RANK.get(settings.alert_notify_min_severity, 2)
    return payload_rank >= minimum_rank


def _send_webhook_notification(payload: dict[str, Any]) -> list[str]:
    settings = get_settings()
    if not _should_notify(payload):
        return []

    safe_payload = _safe_payload(payload)
    summary = _notification_summary(payload)
    channel = f"webhook:{settings.alert_webhook_type}"

    if settings.alert_webhook_type == "slack":
        body = {
            "text": summary,
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": (
                            f"*{safe_payload['title']}*\n"
                            f"{safe_payload['message']}\n"
                            f"Symbol: `{safe_payload['symbol']}` | "
                            f"Severity: `{safe_payload['severity']}` | "
                            f"Category: `{safe_payload['event_category']}`"
                        ),
                    },
                }
            ],
        }
    elif settings.alert_webhook_type == "discord":
        body = {"content": summary}
    else:
        body = {"summary": summary, "alert": safe_payload}

    try:
        with httpx.Client(timeout=settings.alert_webhook_timeout_seconds) as client:
            response = client.post(settings.alert_webhook_url, json=body)
            response.raise_for_status()
        return [channel]
    except Exception as exc:
        logger.warning("webhook notification failed for %s: %s", payload["event_id"], exc)
        return []


def _persist_alert(payload: dict[str, Any]) -> None:
    with pg_connection() as conn:
        conn.execute(
            """
            INSERT INTO operational.alert_events (
                event_id, symbol, trading_date, event_category, severity, status, title, message,
                detected_at, source_run_id, composite_score, price_z_score, volume_z_score,
                event_payload, notified_channels
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb)
            ON CONFLICT (event_id) DO UPDATE
            SET severity = EXCLUDED.severity,
                status = EXCLUDED.status,
                title = EXCLUDED.title,
                message = EXCLUDED.message,
                composite_score = EXCLUDED.composite_score,
                price_z_score = EXCLUDED.price_z_score,
                volume_z_score = EXCLUDED.volume_z_score,
                event_payload = EXCLUDED.event_payload,
                notified_channels = EXCLUDED.notified_channels
            """,
            (
                payload["event_id"],
                payload["symbol"],
                payload["trading_date"],
                payload["event_category"],
                payload["severity"],
                payload["status"],
                payload["title"],
                payload["message"],
                payload["detected_at"],
                payload.get("source_run_id"),
                payload.get("composite_score"),
                payload.get("price_z_score"),
                payload.get("volume_z_score"),
                json.dumps(payload.get("event_payload", {})),
                json.dumps(payload.get("notified_channels", [])),
            ),
        )

    redis = get_redis()
    redis.set(_latest_alert_key(payload["event_id"]), json.dumps(payload, default=str), ex=3600)


def _mark_cooldown(symbol: str, category: str, detected_at: datetime) -> None:
    redis = get_redis()
    ttl_seconds = max(get_settings().alert_cooldown_minutes * 120, 600)
    redis.set(_cooldown_key(symbol, category), detected_at.isoformat(), ex=ttl_seconds)


def _is_in_cooldown(symbol: str, category: str, detected_at: datetime) -> bool:
    redis = get_redis()
    raw = redis.get(_cooldown_key(symbol, category))
    if not raw:
        return False
    last_emitted = _parse_iso_datetime(raw)
    return detected_at <= last_emitted + timedelta(minutes=get_settings().alert_cooldown_minutes)


def severity_for_anomaly(detection: AnomalyDetection) -> str:
    if detection.composite_score >= 3.0 or (
        (detection.price_z_score or 0.0) >= 2.6 and (detection.volume_z_score or 0.0) >= 2.2
    ):
        return "critical"
    if detection.composite_score >= 2.6 or (detection.price_z_score or 0.0) >= 2.4:
        return "high"
    if detection.composite_score >= 2.2 or (detection.volume_z_score or 0.0) >= 2.0:
        return "medium"
    return "low"


def emit_anomaly_alert(detection: AnomalyDetection) -> dict[str, Any] | None:
    category = "anomaly"
    detected_at = ensure_utc(detection.timestamp_utc)
    if _is_in_cooldown(detection.symbol, category, detected_at):
        return None

    rounded_minute = detected_at.replace(second=0, microsecond=0)
    payload = {
        "event_id": sha1(f"{detection.symbol}|{category}|{rounded_minute.isoformat()}".encode("utf-8")).hexdigest(),
        "symbol": detection.symbol,
        "trading_date": detection.trading_date,
        "event_category": category,
        "severity": severity_for_anomaly(detection),
        "status": "open",
        "title": f"{detection.symbol} anomaly",
        "message": f"Composite {detection.composite_score:.3f} with price z {detection.price_z_score or 0:.2f} and volume z {detection.volume_z_score or 0:.2f}.",
        "detected_at": detected_at,
        "source_run_id": detection.source_run_id,
        "composite_score": detection.composite_score,
        "price_z_score": detection.price_z_score,
        "volume_z_score": detection.volume_z_score,
        "event_payload": {
            "sector": detection.sector,
            "exchange": detection.exchange,
            "timestamp_ist": detection.timestamp_ist.isoformat(),
            "explainability": detection.explainability,
        },
        "notified_channels": [],
    }
    payload["notified_channels"] = _send_webhook_notification(payload)
    _persist_alert(payload)
    _mark_cooldown(detection.symbol, category, detected_at)
    return payload


def emit_contagion_alert(event: ContagionEvent) -> dict[str, Any]:
    payload = {
        "event_id": sha1(f"{event.event_id}|contagion".encode("utf-8")).hexdigest(),
        "symbol": event.trigger_symbol,
        "trading_date": event.trading_date,
        "event_category": "contagion",
        "severity": "critical" if event.affected_count >= 2 else "high",
        "status": "open",
        "title": f"{event.trigger_symbol} contagion",
        "message": f"{event.affected_count} peer names confirmed the move inside the contagion window.",
        "detected_at": ensure_utc(event.event_timestamp),
        "source_run_id": event.source_run_id,
        "composite_score": event.risk_score,
        "price_z_score": None,
        "volume_z_score": None,
        "event_payload": {
            "sector": event.trigger_sector,
            "affected_symbols": event.affected_symbols,
            "peer_average_score": event.peer_average_score,
            "risk_score": event.risk_score,
            "observation_window_start": event.observation_window_start.isoformat(),
            "observation_window_end": event.observation_window_end.isoformat(),
        },
        "notified_channels": [],
    }
    payload["notified_channels"] = _send_webhook_notification(payload)
    _persist_alert(payload)
    return payload
