from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime
from time import monotonic
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from market_surveillance.analytics import compute_daily_indicators
from market_surveillance.db import get_cassandra_session, get_redis, pg_connection
from market_surveillance.history import candidate_symbols, ensure_daily_history, sync_metadata_profiles
from market_surveillance.market_time import as_market_time, ensure_utc
from market_surveillance.metadata import load_stock_references, valid_peer_sector
from market_surveillance.settings import get_settings

SESSION_MINUTES_PER_DAY = 375
TRADING_DAYS_PER_YEAR = 250
CACHE_MISS = object()


@dataclass
class CacheEntry:
    expires_at: float
    value: Any


_API_CACHE: dict[str, CacheEntry] = {}


def _cache_read(key: str) -> Any:
    entry = _API_CACHE.get(key)
    if entry is None:
        return CACHE_MISS
    if entry.expires_at <= monotonic():
        _API_CACHE.pop(key, None)
        return CACHE_MISS
    return entry.value


def _cache_store(key: str, ttl_seconds: float, value: Any) -> Any:
    _API_CACHE[key] = CacheEntry(expires_at=monotonic() + ttl_seconds, value=value)
    return value


def _cached(key: str, ttl_seconds: float, loader: Callable[[], Any]) -> Any:
    cached_value = _cache_read(key)
    if cached_value is not CACHE_MISS:
        return cached_value
    return _cache_store(key, ttl_seconds, loader())


def _relation_row_count(conn, relation: str) -> int:
    try:
        row = conn.execute(f"SELECT COUNT(*) AS row_count FROM {relation}").fetchone()
    except Exception:
        return 0
    return int(row["row_count"]) if row else 0


def _clear_api_cache(prefix: str | None = None) -> None:
    if prefix is None:
        _API_CACHE.clear()
        return
    for key in [cache_key for cache_key in _API_CACHE if cache_key.startswith(prefix)]:
        _API_CACHE.pop(key, None)


@asynccontextmanager
async def lifespan(_: FastAPI):
    sync_metadata_profiles()
    _clear_api_cache()
    yield


app = FastAPI(title="Market Surveillance API", version="0.2.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def redis_json_values(pattern: str) -> list[dict[str, Any]]:
    redis = get_redis()
    payloads: list[dict[str, Any]] = []
    for key in redis.scan_iter(pattern):
        raw = redis.get(key)
        if not raw:
            continue
        payloads.append(json.loads(raw))
    return payloads


def _record_timestamp(record: dict[str, Any]) -> datetime | None:
    raw = record.get("timestamp_utc") or record.get("timestamp_ist")
    if raw is None:
        return None
    if isinstance(raw, datetime):
        return ensure_utc(raw)
    if isinstance(raw, str):
        return ensure_utc(datetime.fromisoformat(raw.replace("Z", "+00:00")))
    return None


def _merge_latest_record(base: dict[str, Any] | None, overlay: dict[str, Any]) -> dict[str, Any]:
    if base is None:
        return overlay
    base_ts = _record_timestamp(base)
    overlay_ts = _record_timestamp(overlay)
    if overlay_ts is None or (base_ts is not None and overlay_ts < base_ts):
        return base
    return {**base, **overlay}


def _record_trading_date(record: dict[str, Any]) -> date | None:
    raw = record.get("trading_date")
    if isinstance(raw, datetime):
        return raw.date()
    if isinstance(raw, date):
        return raw
    if isinstance(raw, str):
        text = raw.strip()
        if text:
            if "T" in text:
                return datetime.fromisoformat(text.replace("Z", "+00:00")).date()
            return date.fromisoformat(text)
    timestamp = _record_timestamp(record)
    if timestamp is None:
        return None
    return as_market_time(timestamp).date()


def _filter_latest_trading_session(records: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    latest_trading_date = max(
        (_record_trading_date(record) for record in records.values()),
        default=None,
    )
    if latest_trading_date is None:
        return records
    return {
        symbol: record
        for symbol, record in records.items()
        if _record_trading_date(record) == latest_trading_date
    }


def _streaming_counts_from_bulk_runs(rows: list[dict[str, Any]]) -> dict[str, int]:
    tick_rows = 0
    anomaly_rows = 0
    for row in rows:
        notes = row.get("notes")
        if not isinstance(notes, dict):
            continue
        if "tick_rows_written" not in notes and "anomaly_rows_written" not in notes:
            continue
        tick_rows += int(notes.get("tick_rows_written") or 0)
        anomaly_rows += int(notes.get("anomaly_rows_written") or 0)
    return {"market_ticks": tick_rows, "anomaly_metrics": anomaly_rows}


def _load_profiles() -> dict[str, dict[str, Any]]:
    records = {stock.symbol: stock.model_dump() for stock in load_stock_references()}
    with pg_connection() as conn:
        rows = conn.execute(
            """
            SELECT symbol, company_name, exchange, sector, country, aliases, source, metadata, last_refreshed_at
            FROM operational.stock_profiles
            """
        ).fetchall()
    for row in rows:
        aliases = row["aliases"]
        metadata = row["metadata"] if isinstance(row["metadata"], dict) else {}
        records[row["symbol"]] = {
            "symbol": row["symbol"],
            "company_name": row["company_name"],
            "exchange": row["exchange"],
            "sector": row["sector"],
            "country": row["country"],
            "aliases": aliases if isinstance(aliases, list) else [],
            "source": row["source"],
            "metadata": metadata,
            "watchlist": bool(metadata.get("watchlist")),
        }
    return records


def _profiles() -> dict[str, dict[str, Any]]:
    return _cached("profiles", 300.0, _load_profiles)


def _load_history_coverage_map() -> dict[str, dict[str, Any]]:
    with pg_connection() as conn:
        rows = conn.execute(
            """
            SELECT symbol, COUNT(*) AS daily_bar_count, MAX(trading_date) AS last_daily_date
            FROM operational.stock_daily_bars
            GROUP BY symbol
            """
        ).fetchall()
    return {row["symbol"]: {"daily_bar_count": int(row["daily_bar_count"]), "last_daily_date": row["last_daily_date"]} for row in rows}


def _history_coverage_map() -> dict[str, dict[str, Any]]:
    return _cached("history:coverage", 180.0, _load_history_coverage_map)


def _search_rank(query: str, record: dict[str, Any]) -> int:
    symbol = str(record.get("symbol", "")).upper()
    company_name = str(record.get("company_name", "")).upper()
    aliases = [str(alias).upper() for alias in record.get("aliases", [])]
    if symbol == query:
        return 120
    if query in aliases:
        return 110
    if symbol.startswith(query):
        return 100
    if company_name.startswith(query):
        return 90
    if any(alias.startswith(query) for alias in aliases):
        return 80
    if query in company_name:
        return 70
    if query in symbol:
        return 60
    if any(query in alias for alias in aliases):
        return 50
    return 0


def _known_sector_count(records: list[dict[str, Any]]) -> int:
    return len([record for record in records if valid_peer_sector(record.get("sector"))])


def _sector_options(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts: dict[str, int] = defaultdict(int)
    for record in records:
        sector = str(record.get("sector") or "").strip() if valid_peer_sector(record.get("sector")) else "Unknown"
        counts[sector] += 1
    return [
        {"sector": sector, "count": count, "known": sector != "Unknown"}
        for sector, count in sorted(counts.items(), key=lambda item: (item[0] == "Unknown", item[0]))
    ]


def _load_latest_market_map() -> dict[str, dict[str, Any]]:
    profiles = _profiles()
    session = get_cassandra_session()
    rows = session.execute(
        """
        SELECT symbol, trading_date, timestamp_utc, close, volume, composite_score, is_anomalous
        FROM latest_market_state
        """
    )
    records: dict[str, dict[str, Any]] = {}
    for row in rows:
        profile = profiles.get(row["symbol"], {})
        timestamp_utc = row["timestamp_utc"]
        timestamp_ist = as_market_time(timestamp_utc)
        records[row["symbol"]] = {
            "symbol": row["symbol"],
            "sector": profile.get("sector", "Unknown"),
            "exchange": profile.get("exchange", "Unknown"),
            "trading_date": str(row["trading_date"]),
            "timestamp_utc": timestamp_utc,
            "timestamp_ist": timestamp_ist.isoformat(),
            "close": row["close"],
            "volume": row["volume"],
            "composite_score": row["composite_score"],
            "is_anomalous": row["is_anomalous"],
        }
    for item in redis_json_values("latest:market:*"):
        symbol = item.get("symbol")
        if not symbol:
            continue
        records[symbol] = _merge_latest_record(records.get(symbol), item)
    return _filter_latest_trading_session(records)


def _latest_market_map() -> dict[str, dict[str, Any]]:
    return _cached("latest:market", 15.0, _load_latest_market_map)


def _load_latest_anomaly_map() -> dict[str, dict[str, Any]]:
    latest_market = _latest_market_map()
    if not latest_market:
        return {}

    session = get_cassandra_session()
    stmt = session.prepare(
        """
        SELECT timestamp_utc, timestamp_ist, exchange, sector, interval, close, volume,
               price_z_score, volume_z_score, composite_score, is_anomalous, explainability
        FROM anomaly_metrics
        WHERE symbol = ? AND trading_date = ?
        ORDER BY timestamp_utc DESC
        LIMIT 1
        """
    )
    records: dict[str, dict[str, Any]] = {}
    for symbol, item in latest_market.items():
        if not item.get("is_anomalous"):
            continue
        row = session.execute(stmt, (symbol, date.fromisoformat(str(item["trading_date"])))).one()
        if not row:
            continue
        records[symbol] = {
            "symbol": symbol,
            "exchange": row["exchange"],
            "sector": row["sector"],
            "interval": row["interval"],
            "timestamp_utc": row["timestamp_utc"],
            "timestamp_ist": row["timestamp_ist"],
            "trading_date": str(item["trading_date"]),
            "close": row["close"],
            "volume": row["volume"],
            "price_z_score": row["price_z_score"],
            "volume_z_score": row["volume_z_score"],
            "composite_score": row["composite_score"],
            "is_anomalous": row["is_anomalous"],
            "explainability": row["explainability"],
        }
    for item in redis_json_values("latest:anomaly:*"):
        symbol = item.get("symbol")
        if not symbol or symbol not in latest_market:
            continue
        records[symbol] = _merge_latest_record(records.get(symbol), item)
    return records


def _latest_anomaly_map() -> dict[str, dict[str, Any]]:
    return _cached("latest:anomaly", 15.0, _load_latest_anomaly_map)


def _open_alert_count() -> int:
    def _loader() -> int:
        with pg_connection() as conn:
            row = conn.execute("SELECT COUNT(*) AS row_count FROM operational.alert_events WHERE status = 'open'").fetchone()
        return int(row["row_count"]) if row else 0

    return _cached("alerts:open_count", 15.0, _loader)


def _recent_alerts(limit: int = 20, status: str | None = "open") -> list[dict[str, Any]]:
    cache_key = f"alerts:recent:{status or 'all'}:{limit}"

    def _loader() -> list[dict[str, Any]]:
        clause = "WHERE status = %s" if status else ""
        params: tuple[Any, ...] = (status, limit) if status else (limit,)
        with pg_connection() as conn:
            rows = conn.execute(
                f"""
                SELECT event_id, symbol, trading_date, event_category, severity, status, title, message,
                       detected_at, composite_score, price_z_score, volume_z_score, event_payload, acknowledged_at
                FROM operational.alert_events
                {clause}
                ORDER BY detected_at DESC
                LIMIT %s
                """,
                params,
            ).fetchall()
        return [dict(row) for row in rows]

    return _cached(cache_key, 15.0, _loader)


def _latest_run(mode: str | None = None) -> dict[str, Any] | None:
    filter_clause = "WHERE mode = %s" if mode else ""
    params: tuple[Any, ...] = (mode,) if mode else ()
    with pg_connection() as conn:
        row = conn.execute(
            f"""
            SELECT run_id, mode, started_at, finished_at, symbol_count, records_seen, records_published, status, notes
            FROM operational.ingestion_runs
            {filter_clause}
            ORDER BY started_at DESC
            LIMIT 1
            """,
            params,
        ).fetchone()
    return dict(row) if row else None


def _system_scale_projection(
    listed_symbols: int,
    hydrated_trading_days: int,
    actual_materialized_rows: int,
) -> dict[str, Any]:
    minute_rows_per_trading_day = listed_symbols * SESSION_MINUTES_PER_DAY
    minute_rows_for_loaded_window = minute_rows_per_trading_day * max(hydrated_trading_days, 0)
    minute_rows_per_year = minute_rows_per_trading_day * TRADING_DAYS_PER_YEAR
    tick_and_anomaly_rows_for_loaded_window = minute_rows_for_loaded_window * 2
    tick_and_anomaly_rows_per_year = minute_rows_per_year * 2
    five_year_tick_and_anomaly_rows = tick_and_anomaly_rows_per_year * 5
    actual_vs_loaded_window_pct = (
        round((actual_materialized_rows / tick_and_anomaly_rows_for_loaded_window) * 100, 4)
        if tick_and_anomaly_rows_for_loaded_window
        else 0.0
    )

    return {
        "session_minutes": SESSION_MINUTES_PER_DAY,
        "trading_days_per_year": TRADING_DAYS_PER_YEAR,
        "listed_symbols": listed_symbols,
        "hydrated_trading_days": hydrated_trading_days,
        "minute_rows_per_trading_day": minute_rows_per_trading_day,
        "minute_rows_for_loaded_window": minute_rows_for_loaded_window,
        "minute_rows_per_year": minute_rows_per_year,
        "tick_and_anomaly_rows_for_loaded_window": tick_and_anomaly_rows_for_loaded_window,
        "tick_and_anomaly_rows_per_year": tick_and_anomaly_rows_per_year,
        "five_year_tick_and_anomaly_rows": five_year_tick_and_anomaly_rows,
        "crosses_crore_in_loaded_window": tick_and_anomaly_rows_for_loaded_window >= 10_000_000,
        "crosses_crore_annually": tick_and_anomaly_rows_per_year >= 10_000_000,
        "actual_materialized_vs_loaded_window_pct": actual_vs_loaded_window_pct,
    }


def _system_scale_snapshot(
    profiles: dict[str, dict[str, Any]] | None = None,
    history_map: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    if profiles is None and history_map is None:
        return _cached("system:scale", 15.0, lambda: _system_scale_snapshot(_profiles(), _history_coverage_map()))

    profiles = profiles or _profiles()
    history_map = history_map or _history_coverage_map()
    with pg_connection() as conn:
        profile_count = int(conn.execute("SELECT COUNT(*) AS row_count FROM operational.stock_profiles").fetchone()["row_count"])
        daily_bar_count = int(conn.execute("SELECT COUNT(*) AS row_count FROM operational.stock_daily_bars").fetchone()["row_count"])
        alert_count = int(conn.execute("SELECT COUNT(*) AS row_count FROM operational.alert_events").fetchone()["row_count"])
        contagion_count = int(conn.execute("SELECT COUNT(*) AS row_count FROM operational.contagion_events").fetchone()["row_count"])
        ingestion_run_count = int(conn.execute("SELECT COUNT(*) AS row_count FROM operational.ingestion_runs").fetchone()["row_count"])
        etl_run_count = int(conn.execute("SELECT COUNT(*) AS row_count FROM operational.etl_runs").fetchone()["row_count"])
        coverage_count = int(conn.execute("SELECT COUNT(*) AS row_count FROM operational.surveillance_coverage").fetchone()["row_count"])
        anomaly_fact_count = int(conn.execute("SELECT COUNT(*) AS row_count FROM warehouse.fact_anomaly_minute").fetchone()["row_count"])
        market_day_count = int(conn.execute("SELECT COUNT(*) AS row_count FROM warehouse.fact_market_day").fetchone()["row_count"])
        contagion_fact_count = int(conn.execute("SELECT COUNT(*) AS row_count FROM warehouse.fact_contagion_event").fetchone()["row_count"])
        coverage_fact_count = int(conn.execute("SELECT COUNT(*) AS row_count FROM warehouse.fact_surveillance_coverage").fetchone()["row_count"])
        sector_daily_count = int(conn.execute("SELECT COUNT(*) AS row_count FROM warehouse.mv_sector_daily_summary").fetchone()["row_count"])
        sector_monthly_count = int(conn.execute("SELECT COUNT(*) AS row_count FROM warehouse.mv_sector_monthly_summary").fetchone()["row_count"])
        sector_regime_count = _relation_row_count(conn, "warehouse.mv_sector_regime_summary")
        stock_leader_count = _relation_row_count(conn, "warehouse.mv_stock_signal_leaders")
        coverage_window = conn.execute(
            """
            SELECT COUNT(DISTINCT trading_date) AS trading_days_loaded,
                   MIN(trading_date) AS first_daily_date,
                   MAX(trading_date) AS last_daily_date
            FROM operational.stock_daily_bars
            """
        ).fetchone()
        active_ingestion_runs = [
            dict(row)
            for row in conn.execute(
                """
                SELECT status, mode, records_published, notes
                FROM operational.ingestion_runs
                WHERE status IN ('completed', 'running')
                ORDER BY started_at ASC
                """
            ).fetchall()
        ]
        completed_ingestion_runs = [row for row in active_ingestion_runs if row["status"] == "completed"]

    session = get_cassandra_session()
    bulk_streaming_counts = _streaming_counts_from_bulk_runs(active_ingestion_runs)
    completed_bulk_counts = _streaming_counts_from_bulk_runs(completed_ingestion_runs)
    streaming_counts: dict[str, int | None] = {
        "market_ticks": bulk_streaming_counts["market_ticks"] or None,
        "anomaly_metrics": bulk_streaming_counts["anomaly_metrics"] or None,
        "latest_market_state": len(_latest_market_map()),
        "inflight_market_ticks": max(bulk_streaming_counts["market_ticks"] - completed_bulk_counts["market_ticks"], 0),
        "inflight_anomaly_metrics": max(
            bulk_streaming_counts["anomaly_metrics"] - completed_bulk_counts["anomaly_metrics"], 0
        ),
    }
    for key, table_name in {
        "market_ticks": "market_ticks",
        "anomaly_metrics": "anomaly_metrics",
    }.items():
        if streaming_counts[key] is not None:
            continue
        try:
            row = session.execute(f"SELECT count(*) FROM {table_name}").one()
            streaming_counts[key] = int(row["count"]) if row else 0
        except Exception:
            streaming_counts[key] = None

    redis = get_redis()
    streaming_counts["redis_keys"] = int(redis.dbsize())

    operational_counts = {
        "stock_profiles": profile_count,
        "stock_daily_bars": daily_bar_count,
        "alert_events": alert_count,
        "contagion_events": contagion_count,
        "ingestion_runs": ingestion_run_count,
        "etl_runs": etl_run_count,
        "surveillance_coverage": coverage_count,
    }
    warehouse_counts = {
        "fact_anomaly_minute": anomaly_fact_count,
        "fact_market_day": market_day_count,
        "fact_contagion_event": contagion_fact_count,
        "fact_surveillance_coverage": coverage_fact_count,
        "mv_sector_daily_summary": sector_daily_count,
        "mv_sector_monthly_summary": sector_monthly_count,
        "mv_sector_regime_summary": sector_regime_count,
        "mv_stock_signal_leaders": stock_leader_count,
    }
    operational_total_rows = sum(operational_counts.values())
    warehouse_total_rows = sum(warehouse_counts.values())
    streaming_total_rows = sum(
        value
        for key, value in streaming_counts.items()
        if key not in {"redis_keys", "inflight_market_ticks", "inflight_anomaly_metrics"} and value is not None
    )
    materialized_total_rows = operational_total_rows + warehouse_total_rows + streaming_total_rows
    trading_days_loaded = int(coverage_window["trading_days_loaded"] or 0)

    return {
        "actual": {
            "operational": operational_counts,
            "warehouse": warehouse_counts,
            "streaming": streaming_counts,
            "operational_total_rows": operational_total_rows,
            "warehouse_total_rows": warehouse_total_rows,
            "streaming_total_rows": streaming_total_rows,
            "materialized_total_rows": materialized_total_rows,
        },
        "coverage": {
            "listed_symbols": len(profiles),
            "watchlist_symbols": len([item for item in profiles.values() if item.get("watchlist")]),
            "hydrated_symbols": len(history_map),
            "first_daily_date": str(coverage_window["first_daily_date"]) if coverage_window["first_daily_date"] else None,
            "last_daily_date": str(coverage_window["last_daily_date"]) if coverage_window["last_daily_date"] else None,
            "trading_days_loaded": trading_days_loaded,
        },
        "projection": _system_scale_projection(
            listed_symbols=len(profiles),
            hydrated_trading_days=trading_days_loaded,
            actual_materialized_rows=materialized_total_rows,
        ),
    }


def _daily_rows(symbol: str, days: int) -> list[dict[str, Any]]:
    cache_key = f"history:daily:{symbol}:{max(days, 1)}"

    def _loader() -> list[dict[str, Any]]:
        with pg_connection() as conn:
            rows = conn.execute(
                """
                SELECT symbol, trading_date, open, high, low, close, adj_close, volume, dividends, stock_splits
                FROM operational.stock_daily_bars
                WHERE symbol = %s
                ORDER BY trading_date DESC
                LIMIT %s
                """,
                (symbol, max(days, 1)),
            ).fetchall()
        return list(reversed([dict(row) for row in rows]))

    return _cached(cache_key, 120.0, _loader)


def _window_return(rows: list[dict[str, Any]], sessions: int) -> float | None:
    if len(rows) <= sessions:
        return None
    latest_close = rows[-1]["close"]
    anchor_close = rows[-(sessions + 1)]["close"]
    if anchor_close in (None, 0):
        return None
    return float(((latest_close / anchor_close) - 1) * 100)


def _history_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {
            "first_trading_date": None,
            "last_trading_date": None,
            "session_count": 0,
            "period_high": None,
            "period_low": None,
            "avg_volume_20d": None,
            "range_position_pct": None,
            "return_5d_pct": None,
            "return_20d_pct": None,
            "return_45d_pct": None,
        }

    latest_close = rows[-1]["close"]
    highs = [float(row["high"]) for row in rows if row.get("high") is not None]
    lows = [float(row["low"]) for row in rows if row.get("low") is not None]
    volumes = [int(row["volume"]) for row in rows if row.get("volume") is not None]
    period_high = max(highs) if highs else None
    period_low = min(lows) if lows else None
    avg_volume_20d = None
    trailing_volumes = volumes[-20:]
    if trailing_volumes:
        avg_volume_20d = float(sum(trailing_volumes) / len(trailing_volumes))
    range_position_pct = None
    if period_high is not None and period_low is not None and period_high != period_low:
        range_position_pct = float(((latest_close - period_low) / (period_high - period_low)) * 100)

    return {
        "first_trading_date": str(rows[0]["trading_date"]),
        "last_trading_date": str(rows[-1]["trading_date"]),
        "session_count": len(rows),
        "period_high": period_high,
        "period_low": period_low,
        "avg_volume_20d": avg_volume_20d,
        "range_position_pct": range_position_pct,
        "return_5d_pct": _window_return(rows, 5),
        "return_20d_pct": _window_return(rows, 20),
        "return_45d_pct": _window_return(rows, 45),
    }


def _anomaly_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {
            "point_count": 0,
            "flagged_count": 0,
            "peak_composite_score": None,
            "latest_flagged_at": None,
        }
    flagged = [row for row in rows if row.get("is_anomalous")]
    scores = [float(row["composite_score"]) for row in rows if row.get("composite_score") is not None]
    latest_flagged_at = flagged[-1]["timestamp_ist"] if flagged else None
    return {
        "point_count": len(rows),
        "flagged_count": len(flagged),
        "peak_composite_score": max(scores) if scores else None,
        "latest_flagged_at": latest_flagged_at,
    }


def _alert_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    severity_breakdown = {"critical": 0, "high": 0, "medium": 0, "low": 0}
    open_count = 0
    acknowledged_count = 0
    latest_severity = None

    for index, row in enumerate(rows):
        severity = str(row.get("severity", "")).lower()
        if severity in severity_breakdown:
            severity_breakdown[severity] += 1
        if row.get("status") == "open":
            open_count += 1
        if row.get("status") == "acknowledged":
            acknowledged_count += 1
        if index == 0:
            latest_severity = severity or None

    return {
        "open_count": open_count,
        "acknowledged_count": acknowledged_count,
        "latest_severity": latest_severity,
        "severity_breakdown": severity_breakdown,
    }


def _peer_comparison(symbol: str, sector: str | None, days: int, limit: int = 6) -> list[dict[str, Any]]:
    if not valid_peer_sector(sector):
        return []

    profiles = [item for item in _profiles().values() if item.get("sector") == sector and item["symbol"] != symbol]
    if not profiles:
        return []

    latest_market = _latest_market_map()
    latest_anomalies = _latest_anomaly_map()
    open_alerts = {item["symbol"]: item for item in _recent_alerts(limit=500, status="open")}
    severity_rank = {"critical": 0, "high": 1, "medium": 2, "low": 3}
    profiles.sort(
        key=lambda profile: (
            0 if profile["symbol"] in open_alerts else 1,
            0 if profile["symbol"] in latest_anomalies else 1,
            0 if profile["symbol"] in latest_market else 1,
            0 if profile.get("watchlist") else 1,
            profile["symbol"],
        )
    )
    profiles = profiles[: max(limit * 4, 24)]

    peers: list[dict[str, Any]] = []
    for profile in profiles:
        daily_rows = _daily_rows(profile["symbol"], days)
        indicators = compute_daily_indicators(daily_rows)
        latest_alert = open_alerts.get(profile["symbol"])
        latest_anomaly = latest_anomalies.get(profile["symbol"])
        peers.append(
            {
                "symbol": profile["symbol"],
                "company_name": profile.get("company_name"),
                "exchange": profile.get("exchange"),
                "sector": profile.get("sector"),
                "last_close": indicators.get("last_close"),
                "return_20d_pct": indicators.get("return_20d_pct"),
                "rsi_14": indicators.get("rsi_14"),
                "volume_ratio_20d": indicators.get("volume_ratio_20d"),
                "latest_alert_severity": latest_alert.get("severity") if latest_alert else None,
                "latest_anomaly_score": latest_anomaly.get("composite_score") if latest_anomaly else None,
                "is_anomalous": bool(latest_anomaly and latest_anomaly.get("is_anomalous")),
                "latest_market_close": (latest_market.get(profile["symbol"]) or {}).get("close"),
            }
        )

    peers.sort(
        key=lambda item: (
            severity_rank.get(str(item.get("latest_alert_severity", "")).lower(), 99),
            -1 if item.get("is_anomalous") else 0,
            -float(item.get("latest_anomaly_score") or -9999),
            -float(item.get("return_20d_pct") or -9999),
            item["symbol"],
        )
    )
    return peers[:limit]


def _resolve_workspace_symbol(
    symbol_input: str,
    days: int,
    profiles: dict[str, dict[str, Any]],
    history_map: dict[str, dict[str, Any]],
) -> str | None:
    minimum_days = max(days, 1)
    for candidate in candidate_symbols(symbol_input):
        coverage = history_map.get(candidate)
        if candidate in profiles and coverage and int(coverage.get("daily_bar_count") or 0) >= minimum_days:
            return candidate
    return None


def _related_contagion(symbol: str, sector: str | None, limit: int = 10) -> list[dict[str, Any]]:
    sector_name = sector or ""
    with pg_connection() as conn:
        rows = conn.execute(
            """
            SELECT event_id, trigger_symbol, trigger_sector, affected_symbols, affected_count,
                   risk_score, peer_average_score, event_timestamp, rationale
            FROM operational.contagion_events
            WHERE trigger_symbol = %s
               OR trigger_sector = %s
               OR affected_symbols @> %s::jsonb
            ORDER BY event_timestamp DESC
            LIMIT %s
            """,
            (symbol, sector_name, json.dumps([symbol]), limit),
        ).fetchall()
    return [dict(row) for row in rows]


def _intraday_snapshot(symbol: str) -> tuple[dict[str, Any] | None, list[dict[str, Any]], list[dict[str, Any]]]:
    session = get_cassandra_session()
    latest_stmt = session.prepare(
        """
        SELECT trading_date, timestamp_utc, close, volume, composite_score, is_anomalous
        FROM latest_market_state
        WHERE symbol = ?
        """
    )
    latest_row = session.execute(latest_stmt, (symbol,)).one()
    if latest_row is None:
        return None, [], []

    latest_payload = dict(latest_row)
    latest_payload["trading_date"] = str(latest_row["trading_date"])
    latest_payload["timestamp_ist"] = as_market_time(latest_row["timestamp_utc"]).isoformat()

    trading_date = latest_row["trading_date"]
    tick_stmt = session.prepare(
        """
        SELECT timestamp_utc, timestamp_ist, open, high, low, close, volume, dividends, stock_splits
        FROM market_ticks WHERE symbol = ? AND trading_date = ?
        """
    )
    anomaly_stmt = session.prepare(
        """
        SELECT timestamp_utc, timestamp_ist, composite_score, price_z_score, volume_z_score, is_anomalous, explainability
        FROM anomaly_metrics WHERE symbol = ? AND trading_date = ?
        """
    )
    ticks = [dict(row) for row in session.execute(tick_stmt, (symbol, trading_date))]
    anomalies = [dict(row) for row in session.execute(anomaly_stmt, (symbol, trading_date))]
    return latest_payload, ticks, anomalies


def _screener_row(profile: dict[str, Any], daily_rows: list[dict[str, Any]], latest_market: dict[str, Any] | None, latest_anomaly: dict[str, Any] | None, latest_alert: dict[str, Any] | None) -> dict[str, Any]:
    indicators = compute_daily_indicators(daily_rows)
    return {
        "symbol": profile["symbol"],
        "company_name": profile.get("company_name"),
        "exchange": profile.get("exchange"),
        "sector": profile.get("sector"),
        "daily_points": len(daily_rows),
        "indicators": indicators,
        "latest_market": latest_market,
        "latest_anomaly": latest_anomaly,
        "latest_alert": latest_alert,
    }


@app.get("/api/system/health")
def system_health() -> dict[str, Any]:
    redis = get_redis()
    settings = get_settings()
    profiles = _profiles()
    history_map = _history_coverage_map()
    with pg_connection() as conn:
        latest_etl = conn.execute(
            "SELECT run_id, trading_date, finished_at, status FROM operational.etl_runs ORDER BY started_at DESC LIMIT 1"
        ).fetchone()
        latest_ingestion = conn.execute(
            "SELECT run_id, mode, finished_at, status FROM operational.ingestion_runs ORDER BY started_at DESC LIMIT 1"
        ).fetchone()
        daily_bar_count = conn.execute("SELECT COUNT(*) AS row_count FROM operational.stock_daily_bars").fetchone()["row_count"]
        alert_count = conn.execute("SELECT COUNT(*) AS row_count FROM operational.alert_events").fetchone()["row_count"]
        profile_count = conn.execute("SELECT COUNT(*) AS row_count FROM operational.stock_profiles").fetchone()["row_count"]
    last_tick = redis.get("system:last_tick")
    if not last_tick:
        latest_market = _latest_market_map()
        timestamps = [str(item.get("timestamp_ist")) for item in latest_market.values() if item.get("timestamp_ist")]
        if timestamps:
            last_tick = max(timestamps)
    known_sector_symbols = _known_sector_count(list(profiles.values()))
    unknown_sector_symbols = max(len(profiles) - known_sector_symbols, 0)
    return {
        "api": "ok",
        "redis": redis.ping(),
        "last_tick": last_tick,
        "latest_etl_run": dict(latest_etl) if latest_etl else None,
        "latest_ingestion_run": dict(latest_ingestion) if latest_ingestion else None,
        "database_inventory": {
            "stock_profiles": profile_count,
            "daily_bars": daily_bar_count,
            "alert_events": alert_count,
        },
        "universe_inventory": {
            "listed_symbols": len(profiles),
            "watchlist_symbols": len([item for item in profiles.values() if item.get("watchlist")]),
            "hydrated_symbols": len(history_map),
            "known_sector_symbols": known_sector_symbols,
            "unknown_sector_symbols": unknown_sector_symbols,
            "sector_coverage_pct": round((known_sector_symbols / len(profiles)) * 100, 2) if profiles else 0.0,
        },
        "notifications": {
            "webhook_enabled": bool(settings.alert_webhook_url),
            "webhook_type": settings.alert_webhook_type,
            "min_severity": settings.alert_notify_min_severity,
        },
    }


@app.get("/api/system/scale")
def system_scale() -> dict[str, Any]:
    profiles = _profiles()
    history_map = _history_coverage_map()
    return _system_scale_snapshot(profiles=profiles, history_map=history_map)


@app.get("/api/overview")
def overview() -> dict[str, Any]:
    profiles = _profiles()
    history_map = _history_coverage_map()
    latest_market = sorted(_latest_market_map().values(), key=lambda item: (item["sector"], item["symbol"]))
    latest_anomalies = [item for item in _latest_anomaly_map().values() if item.get("is_anomalous")]
    sector_scores: dict[str, list[float]] = defaultdict(list)
    for anomaly in latest_anomalies:
        sector_scores[anomaly["sector"]].append(anomaly["composite_score"])
    sector_heatmap = [
        {
            "sector": sector,
            "avg_composite_score": round(sum(scores) / len(scores), 4),
            "active_anomalies": len(scores),
        }
        for sector, scores in sorted(sector_scores.items())
    ]
    alerts = _recent_alerts(limit=8, status="open")
    with pg_connection() as conn:
        contagion = conn.execute(
            """
            SELECT event_id, trigger_symbol, trigger_sector, affected_count, risk_score, event_timestamp
            FROM operational.contagion_events
            ORDER BY event_timestamp DESC
            LIMIT 8
            """
        ).fetchall()
    latest_run = _latest_run()
    as_of = None
    if latest_market:
        as_of = max(item["timestamp_ist"] for item in latest_market if item.get("timestamp_ist"))
    return {
        "as_of": as_of,
        "market_mode": latest_run["mode"] if latest_run else None,
        "live_market": latest_market,
        "top_anomalies": sorted(latest_anomalies, key=lambda item: item["composite_score"], reverse=True)[:10],
        "sector_heatmap": sector_heatmap,
        "recent_contagion_events": [dict(row) for row in contagion],
        "recent_alerts": alerts,
        "open_alert_count": _open_alert_count(),
        "tracked_symbol_count": len(profiles),
        "tracked_sector_count": len({item["sector"] for item in profiles.values() if item.get("sector")}),
        "hydrated_symbol_count": len(history_map),
        "watchlist_symbol_count": len([item for item in profiles.values() if item.get("watchlist")]),
        "live_symbol_count": len(latest_market),
        "live_sector_count": len({item["sector"] for item in latest_market}),
    }


@app.get("/api/reference/stocks")
def reference_stocks(
    q: str | None = Query(None),
    limit: int = Query(100, ge=1, le=10000),
    offset: int = Query(0, ge=0),
    watchlist_only: bool = Query(False),
    history_state: str = Query("all"),
    sector: str | None = Query(None),
    sector_state: str = Query("all"),
) -> dict[str, Any]:
    profiles = list(_profiles().values())
    history_map = _history_coverage_map()
    total_count = len(profiles)
    query = q.strip().upper() if q else ""
    sector_filter = sector.strip() if sector else ""
    if query:
        filtered = []
        for record in profiles:
            rank = _search_rank(query, record)
            if rank <= 0:
                continue
            filtered.append((rank, record))
        filtered.sort(key=lambda item: (-item[0], item[1]["symbol"]))
        rows = [item[1] for item in filtered]
    else:
        rows = sorted(profiles, key=lambda item: (0 if item.get("watchlist") else 1, item["symbol"]))

    if watchlist_only:
        rows = [item for item in rows if item.get("watchlist")]

    if sector_state == "known":
        rows = [item for item in rows if valid_peer_sector(item.get("sector"))]
    elif sector_state == "unknown":
        rows = [item for item in rows if not valid_peer_sector(item.get("sector"))]

    if sector_filter:
        rows = [item for item in rows if str(item.get("sector") or "").casefold() == sector_filter.casefold()]

    enriched = []
    for item in rows:
        history = history_map.get(item["symbol"], {})
        enriched.append(
            {
                **item,
                "daily_bar_count": int(history.get("daily_bar_count", 0)),
                "last_daily_date": str(history["last_daily_date"]) if history.get("last_daily_date") else None,
                "has_history": item["symbol"] in history_map,
            }
        )

    if history_state == "hydrated":
        enriched = [item for item in enriched if item["has_history"]]
    elif history_state == "unhydrated":
        enriched = [item for item in enriched if not item["has_history"]]

    known_sector_count = _known_sector_count(enriched)
    unknown_sector_count = max(len(enriched) - known_sector_count, 0)
    sector_options = _sector_options(enriched)
    page = enriched[offset : offset + limit]
    return {
        "stocks": page,
        "total_count": total_count,
        "filtered_count": len(enriched),
        "symbol_count": len(page),
        "sector_count": len({stock["sector"] for stock in enriched if stock.get("sector")}),
        "sector_options": sector_options,
        "known_sector_count": known_sector_count,
        "unknown_sector_count": unknown_sector_count,
        "watchlist_count": len([stock for stock in profiles if stock.get("watchlist")]),
        "hydrated_count": len(history_map),
    }


@app.get("/api/reference/search")
def reference_search(q: str = Query(..., min_length=1), limit: int = Query(12, ge=1, le=50)) -> dict[str, Any]:
    query = q.strip().upper()
    matches = []
    for record in _profiles().values():
        rank = _search_rank(query, record)
        if rank <= 0:
            continue
        matches.append({"rank": rank, **record})
    matches.sort(key=lambda item: (-item["rank"], item["symbol"]))
    top = matches[:limit]
    if not top and "." not in query:
        for candidate in candidate_symbols(query)[:2]:
            top.append(
                {
                    "symbol": candidate,
                    "company_name": candidate,
                    "exchange": "Unknown",
                    "sector": "Unknown",
                    "aliases": [query],
                    "source": "candidate",
                    "metadata": {},
                    "rank": 10,
                }
            )
    return {"matches": top}


@app.get("/api/alerts/live")
def alerts_live(limit: int = Query(20, ge=1, le=100), status: str | None = Query("open")) -> dict[str, Any]:
    alerts = _recent_alerts(limit=limit, status=status)
    return {"items": alerts, "open_count": _open_alert_count()}


@app.post("/api/alerts/{event_id}/ack")
def acknowledge_alert(event_id: str) -> dict[str, Any]:
    with pg_connection() as conn:
        conn.execute(
            """
            UPDATE operational.alert_events
            SET status = 'acknowledged',
                acknowledged_at = now()
            WHERE event_id = %s
            """,
            (event_id,),
        )
        row = conn.execute("SELECT event_id, status, acknowledged_at FROM operational.alert_events WHERE event_id = %s", (event_id,)).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Alert not found")
    _clear_api_cache("alerts:")
    _clear_api_cache("system:scale")
    return dict(row)


@app.get("/api/stocks/screener")
def stock_screener(
    days: int = Query(45, ge=20, le=180),
    limit: int = Query(100, ge=1, le=500),
    only_hydrated: bool = Query(True),
) -> dict[str, Any]:
    all_profiles = sorted(_profiles().values(), key=lambda item: item["symbol"])
    history_map = _history_coverage_map()
    latest_market = _latest_market_map()
    latest_anomalies = _latest_anomaly_map()
    alert_rows = _recent_alerts(limit=500, status="open")
    latest_alerts = {}
    for alert in alert_rows:
        latest_alerts.setdefault(alert["symbol"], alert)

    if only_hydrated:
        eligible_symbols = set(history_map)
        eligible_symbols.update(latest_market)
        eligible_symbols.update(latest_anomalies)
        eligible_symbols.update(latest_alerts)
        profiles = [profile for profile in all_profiles if profile["symbol"] in eligible_symbols]
        if not profiles:
            profiles = [profile for profile in all_profiles if profile.get("watchlist")]
    else:
        profiles = all_profiles

    profiles.sort(
        key=lambda profile: (
            0 if profile["symbol"] in latest_alerts else 1,
            0 if profile["symbol"] in latest_anomalies else 1,
            0 if profile["symbol"] in latest_market else 1,
            0 if profile.get("watchlist") else 1,
            profile["symbol"],
        )
    )
    profile_budget = min(len(profiles), max(limit * 3, 150))
    profiles = profiles[:profile_budget]

    rows = []
    for profile in profiles:
        daily_rows = _daily_rows(profile["symbol"], days)
        rows.append(
            _screener_row(
                profile,
                daily_rows,
                latest_market.get(profile["symbol"]),
                latest_anomalies.get(profile["symbol"]),
                latest_alerts.get(profile["symbol"]),
            )
        )

    severity_rank = {"critical": 0, "high": 1, "medium": 2, "low": 3}
    rows.sort(
        key=lambda item: (
            severity_rank.get((item["latest_alert"] or {}).get("severity", "zzz"), 99),
            -1 if (item["latest_anomaly"] or {}).get("is_anomalous") else 0,
            -float(item["indicators"].get("return_20d_pct") or -9999),
            item["symbol"],
        )
    )
    return {"items": rows[:limit], "count": len(rows)}


@app.get("/api/stocks/{symbol}/workspace")
def stock_workspace(symbol: str, days: int = Query(45, ge=20, le=180)) -> dict[str, Any]:
    profiles = _profiles()
    history_map = _history_coverage_map()
    resolved_symbol = _resolve_workspace_symbol(symbol, days, profiles, history_map)
    if not resolved_symbol:
        resolved_symbol = ensure_daily_history(symbol, minimum_days=days)
        _clear_api_cache("profiles")
        _clear_api_cache("history:")
        _clear_api_cache("system:scale")
        profiles = _profiles()

    if not resolved_symbol:
        raise HTTPException(status_code=404, detail="Unable to resolve symbol")

    profile = profiles.get(resolved_symbol, {"symbol": resolved_symbol, "company_name": resolved_symbol})
    daily_rows = _daily_rows(resolved_symbol, days)
    indicators = compute_daily_indicators(daily_rows)
    latest_market, ticks, anomalies = _intraday_snapshot(resolved_symbol)

    with pg_connection() as conn:
        alerts = conn.execute(
            """
            SELECT event_id, symbol, event_category, severity, status, title, message,
                   detected_at, composite_score, event_payload, acknowledged_at
            FROM operational.alert_events
            WHERE symbol = %s
            ORDER BY detected_at DESC
            LIMIT 25
            """,
            (resolved_symbol,),
        ).fetchall()

    alert_items = [dict(row) for row in alerts]
    latest_anomaly = _latest_anomaly_map().get(resolved_symbol)
    history_summary = _history_summary(daily_rows)
    anomaly_summary = _anomaly_summary(anomalies)
    alert_summary = _alert_summary(alert_items)
    peer_comparison = _peer_comparison(resolved_symbol, profile.get("sector"), days)
    related_contagion = _related_contagion(resolved_symbol, profile.get("sector"))
    return {
        "symbol": symbol,
        "resolved_symbol": resolved_symbol,
        "reference": profile,
        "history": daily_rows,
        "history_summary": history_summary,
        "indicators": indicators,
        "latest_market": latest_market,
        "latest_anomaly": latest_anomaly,
        "anomaly_summary": anomaly_summary,
        "ticks": ticks,
        "anomalies": anomalies,
        "alerts": alert_items,
        "alert_summary": alert_summary,
        "peer_comparison": peer_comparison,
        "related_contagion": related_contagion,
    }


@app.get("/api/stocks/{symbol}")
def stock_detail(symbol: str, trading_date: date = Query(...)) -> dict[str, Any]:
    session = get_cassandra_session()
    tick_stmt = session.prepare(
        """
        SELECT timestamp_utc, timestamp_ist, open, high, low, close, volume, dividends, stock_splits
        FROM market_ticks WHERE symbol = ? AND trading_date = ?
        """
    )
    anomaly_stmt = session.prepare(
        """
        SELECT timestamp_utc, timestamp_ist, composite_score, price_z_score, volume_z_score, is_anomalous, explainability
        FROM anomaly_metrics WHERE symbol = ? AND trading_date = ?
        """
    )
    ticks = list(session.execute(tick_stmt, (symbol, trading_date)))
    anomalies = list(session.execute(anomaly_stmt, (symbol, trading_date)))
    if not ticks:
        raise HTTPException(status_code=404, detail="No data found for symbol/date")
    reference = _profiles().get(symbol)
    return {
        "symbol": symbol,
        "trading_date": trading_date,
        "reference": reference,
        "ticks": ticks,
        "anomalies": anomalies,
    }


@app.get("/api/contagion")
def contagion_events(limit: int = Query(25, ge=1, le=100)) -> list[dict[str, Any]]:
    with pg_connection() as conn:
        rows = conn.execute(
            """
            SELECT event_id, trigger_symbol, trigger_sector, affected_symbols, affected_count,
                   risk_score, peer_average_score, event_timestamp, rationale
            FROM operational.contagion_events
            ORDER BY event_timestamp DESC
            LIMIT %s
            """,
            (limit,),
        ).fetchall()
    return [dict(row) for row in rows]


@app.get("/api/contagion/{event_id}")
def contagion_event_detail(event_id: str) -> dict[str, Any]:
    with pg_connection() as conn:
        row = conn.execute("SELECT * FROM operational.contagion_events WHERE event_id = %s", (event_id,)).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Contagion event not found")
    return dict(row)


@app.get("/api/warehouse/sector-rollups")
def sector_rollups() -> list[dict[str, Any]]:
    with pg_connection() as conn:
        rows = conn.execute(
            """
            SELECT calendar_date, sector_name, active_minutes, avg_composite_score, max_composite_score, contagion_minutes
            FROM warehouse.mv_sector_daily_summary
            ORDER BY calendar_date DESC, avg_composite_score DESC
            LIMIT 100
            """
        ).fetchall()
    return [dict(row) for row in rows]


@app.get("/api/warehouse/summary")
def warehouse_summary() -> dict[str, Any]:
    with pg_connection() as conn:
        row = conn.execute(
            """
            WITH market_window AS (
                SELECT
                    COUNT(*) AS market_day_rows,
                    COUNT(DISTINCT s.symbol) AS stocks_covered,
                    COUNT(DISTINCT f.sector_sk) AS sectors_covered,
                    COUNT(DISTINCT f.date_sk) AS trading_days_loaded,
                    COALESCE(SUM(f.anomaly_count), 0) AS total_anomalies,
                    COALESCE(SUM(f.contagion_event_count), 0) AS total_contagion_events,
                    COALESCE(MAX(f.max_composite_score), 0) AS peak_daily_composite_score
                FROM warehouse.fact_market_day f
                JOIN warehouse.dim_stock s ON s.stock_sk = f.stock_sk
            ),
            date_window AS (
                SELECT
                    MIN(d.calendar_date) AS first_calendar_date,
                    MAX(d.calendar_date) AS last_calendar_date
                FROM warehouse.fact_market_day f
                JOIN warehouse.dim_date d ON d.date_sk = f.date_sk
            )
            SELECT
                market_window.market_day_rows,
                market_window.stocks_covered,
                market_window.sectors_covered,
                market_window.trading_days_loaded,
                market_window.total_anomalies,
                market_window.total_contagion_events,
                market_window.peak_daily_composite_score,
                date_window.first_calendar_date,
                date_window.last_calendar_date,
                (SELECT COUNT(*) FROM warehouse.fact_anomaly_minute) AS anomaly_minute_rows,
                (SELECT COUNT(*) FROM warehouse.fact_contagion_event) AS contagion_event_rows,
                (SELECT COUNT(*) FROM warehouse.fact_surveillance_coverage) AS coverage_rows
            FROM market_window
            CROSS JOIN date_window
            """
        ).fetchone()
    return dict(row) if row else {}


@app.get("/api/warehouse/monthly-rollups")
def monthly_rollups() -> list[dict[str, Any]]:
    with pg_connection() as conn:
        rows = conn.execute(
            """
            SELECT year, quarter, month, sector_name, avg_daily_composite_score, max_daily_composite_score, contagion_event_count
            FROM warehouse.mv_sector_monthly_summary
            ORDER BY year DESC, month DESC, avg_daily_composite_score DESC
            LIMIT 100
            """
        ).fetchall()
    return [dict(row) for row in rows]


@app.get("/api/warehouse/sector-regimes")
def warehouse_sector_regimes(limit: int = Query(25, ge=1, le=100)) -> list[dict[str, Any]]:
    with pg_connection() as conn:
        rows = conn.execute(
            """
            SELECT
                sector_name,
                sessions_covered,
                symbols_covered,
                anomaly_minutes,
                total_anomalies,
                contagion_minutes,
                contagion_event_count,
                avg_daily_composite_score,
                peak_daily_composite_score,
                latest_calendar_date
            FROM warehouse.mv_sector_regime_summary
            ORDER BY peak_daily_composite_score DESC, total_anomalies DESC, sector_name
            LIMIT %s
            """,
            (limit,),
        ).fetchall()
    return [dict(row) for row in rows]


@app.get("/api/warehouse/stock-outliers")
def warehouse_stock_outliers(limit: int = Query(50, ge=1, le=200)) -> list[dict[str, Any]]:
    with pg_connection() as conn:
        rows = conn.execute(
            """
            SELECT
                d.calendar_date,
                s.symbol,
                s.company_name,
                sec.sector_name,
                f.anomaly_count,
                f.max_composite_score,
                f.avg_composite_score,
                f.avg_volume_z_score,
                f.contagion_event_count
            FROM warehouse.fact_market_day f
            JOIN warehouse.dim_stock s ON s.stock_sk = f.stock_sk
            JOIN warehouse.dim_date d ON d.date_sk = f.date_sk
            JOIN warehouse.dim_sector sec ON sec.sector_sk = f.sector_sk
            ORDER BY d.calendar_date DESC, f.max_composite_score DESC, f.anomaly_count DESC, s.symbol
            LIMIT %s
            """,
            (limit,),
        ).fetchall()
    return [dict(row) for row in rows]


@app.get("/api/warehouse/stock-leaders")
def warehouse_stock_leaders(limit: int = Query(50, ge=1, le=200)) -> list[dict[str, Any]]:
    with pg_connection() as conn:
        rows = conn.execute(
            """
            SELECT
                symbol,
                company_name,
                sector_name,
                sessions_covered,
                anomaly_days,
                total_anomalies,
                avg_daily_composite_score,
                peak_daily_composite_score,
                contagion_event_count,
                latest_calendar_date,
                latest_anomaly_count,
                latest_peak_score
            FROM warehouse.mv_stock_signal_leaders
            ORDER BY peak_daily_composite_score DESC, total_anomalies DESC, symbol
            LIMIT %s
            """,
            (limit,),
        ).fetchall()
    return [dict(row) for row in rows]


@app.get("/api/system/runs")
def system_runs() -> dict[str, Any]:
    with pg_connection() as conn:
        ingestion = conn.execute("SELECT * FROM operational.ingestion_runs ORDER BY started_at DESC LIMIT 20").fetchall()
        etl = conn.execute("SELECT * FROM operational.etl_runs ORDER BY started_at DESC LIMIT 20").fetchall()
    return {"ingestion_runs": [dict(row) for row in ingestion], "etl_runs": [dict(row) for row in etl]}


@app.get("/api/replay/status")
def replay_status() -> dict[str, Any]:
    row = _latest_run(mode="replay")
    return row if row else {"mode": "replay", "status": "idle"}


def main() -> None:
    import uvicorn

    settings = get_settings()
    uvicorn.run(app, host=settings.api_host, port=settings.api_port)


if __name__ == "__main__":
    main()
