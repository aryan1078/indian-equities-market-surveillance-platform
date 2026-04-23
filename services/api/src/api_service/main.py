from __future__ import annotations

import json
import math
from collections import defaultdict
from collections.abc import Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from time import monotonic
from typing import Any, Literal

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from pydantic import BaseModel, Field

from market_surveillance.analytics import compute_daily_indicators
from market_surveillance.db import get_cassandra_session, get_redis, pg_connection
from market_surveillance.history import candidate_symbols, ensure_daily_history, sync_metadata_profiles
from market_surveillance.market_data import preferred_market_data_provider
from market_surveillance.market_time import as_market_time, ensure_utc
from market_surveillance.metadata import load_stock_references, valid_peer_sector
from market_surveillance.settings import get_settings

SESSION_MINUTES_PER_DAY = 375
TRADING_DAYS_PER_YEAR = 250
INTRADAY_FEED_MODES = ("live", "replay", "capture_replay", "backfill")
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


@dataclass(frozen=True)
class WarehouseFieldDef:
    label: str
    sql: str
    description: str
    kind: Literal["string", "integer", "number", "date", "time", "datetime"] = "string"


@dataclass(frozen=True)
class WarehouseDatasetDef:
    key: str
    label: str
    description: str
    grain: str
    relation: str
    base_sql: str
    dimensions: dict[str, WarehouseFieldDef]
    measures: dict[str, WarehouseFieldDef]
    default_dimensions: tuple[str, ...]
    default_measures: tuple[str, ...]
    default_sort_field: str
    default_sort_direction: Literal["asc", "desc"] = "desc"
    default_limit: int = 100
    chart_preference: Literal["auto", "line", "bar"] = "auto"
    supports_date: bool = False
    supports_sector: bool = False
    supports_exchange: bool = False
    supports_symbol_search: bool = False
    supports_min_signal: bool = False
    date_sql: str | None = None
    sector_sql: str | None = None
    exchange_sql: str | None = None
    symbol_sql: str | None = None
    company_sql: str | None = None
    signal_sql: str | None = None
    suggested_window_days: int | None = None


class WarehouseQueryRequest(BaseModel):
    dataset: str = Field(default="stock_day")
    dimensions: list[str] = Field(default_factory=list, max_length=4)
    measures: list[str] = Field(default_factory=list, max_length=5)
    date_from: date | None = None
    date_to: date | None = None
    sector: str | None = None
    exchange: str | None = None
    symbol_search: str | None = None
    min_signal: float | None = None
    sort_field: str | None = None
    sort_direction: Literal["asc", "desc"] = "desc"
    limit: int = Field(default=100, ge=1, le=500)


def _warehouse_query_catalog() -> dict[str, WarehouseDatasetDef]:
    threshold = get_settings().anomaly_composite_threshold
    return {
        "stock_day": WarehouseDatasetDef(
            key="stock_day",
            label="Stock daily facts",
            description="Daily stock-level warehouse rows for anomaly counts, score peaks, and contagion counts.",
            grain="One row per stock per trading day in the warehouse.",
            relation="warehouse.fact_market_day",
            base_sql="""
                FROM warehouse.fact_market_day f
                JOIN warehouse.dim_stock s ON s.stock_sk = f.stock_sk
                JOIN warehouse.dim_date d ON d.date_sk = f.date_sk
                JOIN warehouse.dim_sector sec ON sec.sector_sk = f.sector_sk
                JOIN warehouse.dim_exchange ex ON ex.exchange_sk = f.exchange_sk
            """,
            dimensions={
                "calendar_date": WarehouseFieldDef("Trading date", "d.calendar_date", "Warehouse trading date.", "date"),
                "symbol": WarehouseFieldDef("Symbol", "s.symbol", "Exchange-traded stock symbol."),
                "company_name": WarehouseFieldDef("Company", "s.company_name", "Company name carried by the stock dimension."),
                "sector_name": WarehouseFieldDef("Sector", "sec.sector_name", "Current sector classification."),
                "exchange_code": WarehouseFieldDef("Exchange", "ex.exchange_code", "Exchange code from the warehouse exchange dimension."),
            },
            measures={
                "market_day_rows": WarehouseFieldDef("Rows", "COUNT(*)", "Number of stock-day rows in the grouped result.", "integer"),
                "anomaly_count": WarehouseFieldDef("Anomaly count", "COALESCE(SUM(f.anomaly_count), 0)", "Total anomalous minute flags across the grouped stock-day rows.", "integer"),
                "avg_composite_score": WarehouseFieldDef("Avg composite score", "AVG(f.avg_composite_score)", "Average daily composite score across the grouped rows.", "number"),
                "peak_composite_score": WarehouseFieldDef("Peak composite score", "MAX(f.max_composite_score)", "Maximum daily composite score inside the grouped result.", "number"),
                "avg_volume_z_score": WarehouseFieldDef("Avg volume z", "AVG(f.avg_volume_z_score)", "Average daily volume surprise across the grouped rows.", "number"),
                "contagion_event_count": WarehouseFieldDef("Contagion events", "COALESCE(SUM(f.contagion_event_count), 0)", "Total contagion windows linked to the grouped stock-day rows.", "integer"),
            },
            default_dimensions=("calendar_date", "sector_name", "symbol"),
            default_measures=("anomaly_count", "peak_composite_score", "contagion_event_count"),
            default_sort_field="peak_composite_score",
            supports_date=True,
            supports_sector=True,
            supports_exchange=True,
            supports_symbol_search=True,
            supports_min_signal=True,
            date_sql="d.calendar_date",
            sector_sql="sec.sector_name",
            exchange_sql="ex.exchange_code",
            symbol_sql="s.symbol",
            company_sql="s.company_name",
            signal_sql="f.max_composite_score",
            suggested_window_days=14,
        ),
        "sector_day": WarehouseDatasetDef(
            key="sector_day",
            label="Sector daily rollups",
            description="Sector-by-day materialized warehouse summary for stress ranking and cross-sector comparisons.",
            grain="One row per sector per trading day in the warehouse summary view.",
            relation="warehouse.mv_sector_daily_summary",
            base_sql="FROM warehouse.mv_sector_daily_summary sd",
            dimensions={
                "calendar_date": WarehouseFieldDef("Trading date", "sd.calendar_date", "Trading date for the sector summary row.", "date"),
                "sector_name": WarehouseFieldDef("Sector", "sd.sector_name", "Sector represented by the summary row."),
            },
            measures={
                "group_rows": WarehouseFieldDef("Rows", "COUNT(*)", "Number of grouped summary rows returned.", "integer"),
                "active_minutes": WarehouseFieldDef("Active minutes", "COALESCE(SUM(sd.active_minutes), 0)", "Minute rows contributing to the grouped sector summary.", "integer"),
                "avg_composite_score": WarehouseFieldDef("Avg composite score", "AVG(sd.avg_composite_score)", "Average sector composite score across grouped rows.", "number"),
                "max_composite_score": WarehouseFieldDef("Peak composite score", "MAX(sd.max_composite_score)", "Highest sector composite score inside the grouped result.", "number"),
                "contagion_minutes": WarehouseFieldDef("Contagion minutes", "COALESCE(SUM(sd.contagion_minutes), 0)", "Minutes in the grouped result marked as contagion-linked.", "integer"),
            },
            default_dimensions=("calendar_date", "sector_name"),
            default_measures=("active_minutes", "max_composite_score", "contagion_minutes"),
            default_sort_field="max_composite_score",
            supports_date=True,
            supports_sector=True,
            supports_min_signal=True,
            date_sql="sd.calendar_date",
            sector_sql="sd.sector_name",
            signal_sql="sd.max_composite_score",
            suggested_window_days=21,
        ),
        "minute_signals": WarehouseDatasetDef(
            key="minute_signals",
            label="Minute signal facts",
            description="Minute-grain anomaly facts for intraday pressure analysis across date, time, sector, and symbol.",
            grain="One row per stock per trading date per minute in the anomaly fact table.",
            relation="warehouse.fact_anomaly_minute",
            base_sql="""
                FROM warehouse.fact_anomaly_minute f
                JOIN warehouse.dim_stock s ON s.stock_sk = f.stock_sk
                JOIN warehouse.dim_date d ON d.date_sk = f.date_sk
                JOIN warehouse.dim_time t ON t.time_sk = f.time_sk
                JOIN warehouse.dim_sector sec ON sec.sector_sk = f.sector_sk
                JOIN warehouse.dim_exchange ex ON ex.exchange_sk = f.exchange_sk
            """,
            dimensions={
                "calendar_date": WarehouseFieldDef("Trading date", "d.calendar_date", "Trading date of the minute fact row.", "date"),
                "time_label": WarehouseFieldDef("Time", "t.label", "IST minute bucket label from the warehouse time dimension.", "time"),
                "sector_name": WarehouseFieldDef("Sector", "sec.sector_name", "Sector linked to the minute fact row."),
                "symbol": WarehouseFieldDef("Symbol", "s.symbol", "Stock symbol on the minute fact row."),
                "exchange_code": WarehouseFieldDef("Exchange", "ex.exchange_code", "Exchange code linked to the minute fact row."),
            },
            measures={
                "minute_rows": WarehouseFieldDef("Minute rows", "COUNT(*)", "Number of anomaly-minute fact rows in the grouped result.", "integer"),
                "flagged_minutes": WarehouseFieldDef("Flagged minutes", f"SUM(CASE WHEN f.composite_score >= {threshold} THEN 1 ELSE 0 END)", "Minute rows meeting or exceeding the anomaly composite threshold.", "integer"),
                "avg_composite_score": WarehouseFieldDef("Avg composite score", "AVG(f.composite_score)", "Average minute-level composite score inside the grouped result.", "number"),
                "peak_composite_score": WarehouseFieldDef("Peak composite score", "MAX(f.composite_score)", "Highest minute-level composite score inside the grouped result.", "number"),
                "avg_price_z_score": WarehouseFieldDef("Avg price z", "AVG(f.price_z_score)", "Average minute-level price surprise.", "number"),
                "avg_volume_z_score": WarehouseFieldDef("Avg volume z", "AVG(f.volume_z_score)", "Average minute-level volume surprise.", "number"),
                "contagion_minutes": WarehouseFieldDef("Contagion minutes", "SUM(CASE WHEN f.contagion_flag THEN 1 ELSE 0 END)", "Minute rows tagged as contagion-linked.", "integer"),
            },
            default_dimensions=("calendar_date", "time_label", "sector_name"),
            default_measures=("flagged_minutes", "peak_composite_score", "contagion_minutes"),
            default_sort_field="peak_composite_score",
            supports_date=True,
            supports_sector=True,
            supports_exchange=True,
            supports_symbol_search=True,
            supports_min_signal=True,
            date_sql="d.calendar_date",
            sector_sql="sec.sector_name",
            exchange_sql="ex.exchange_code",
            symbol_sql="s.symbol",
            company_sql="s.company_name",
            signal_sql="f.composite_score",
            suggested_window_days=5,
        ),
        "contagion_events": WarehouseDatasetDef(
            key="contagion_events",
            label="Contagion events",
            description="Warehouse contagion events for trigger symbol, sector spread, risk score, and affected-count analysis.",
            grain="One row per persisted contagion event.",
            relation="warehouse.fact_contagion_event",
            base_sql="""
                FROM warehouse.fact_contagion_event c
                JOIN warehouse.dim_stock s ON s.stock_sk = c.stock_sk
                JOIN warehouse.dim_date d ON d.date_sk = c.date_sk
                JOIN warehouse.dim_sector sec ON sec.sector_sk = c.sector_sk
            """,
            dimensions={
                "calendar_date": WarehouseFieldDef("Trading date", "d.calendar_date", "Trading date of the contagion event.", "date"),
                "event_timestamp": WarehouseFieldDef("Event time", "c.event_timestamp", "Timestamp at which the contagion event was recorded.", "datetime"),
                "trigger_symbol": WarehouseFieldDef("Trigger symbol", "s.symbol", "Symbol that initiated the contagion window."),
                "sector_name": WarehouseFieldDef("Sector", "sec.sector_name", "Sector associated with the contagion event."),
                "exchange_code": WarehouseFieldDef("Exchange", "s.exchange_code", "Exchange code carried by the current stock dimension."),
            },
            measures={
                "event_count": WarehouseFieldDef("Event count", "COUNT(*)", "Number of contagion events in the grouped result.", "integer"),
                "avg_risk_score": WarehouseFieldDef("Avg risk score", "AVG(c.risk_score)", "Average contagion risk score across grouped events.", "number"),
                "max_risk_score": WarehouseFieldDef("Peak risk score", "MAX(c.risk_score)", "Maximum contagion risk score inside the grouped result.", "number"),
                "avg_affected_count": WarehouseFieldDef("Avg affected count", "AVG(c.affected_count)", "Average number of affected peers in the grouped result.", "number"),
                "total_affected_count": WarehouseFieldDef("Total affected count", "COALESCE(SUM(c.affected_count), 0)", "Total affected peers counted across grouped contagion events.", "integer"),
                "avg_peer_average_score": WarehouseFieldDef("Avg peer score", "AVG(c.peer_average_score)", "Average peer score participating in grouped contagion windows.", "number"),
            },
            default_dimensions=("calendar_date", "trigger_symbol", "sector_name"),
            default_measures=("event_count", "max_risk_score", "total_affected_count"),
            default_sort_field="max_risk_score",
            supports_date=True,
            supports_sector=True,
            supports_exchange=True,
            supports_symbol_search=True,
            supports_min_signal=True,
            date_sql="d.calendar_date",
            sector_sql="sec.sector_name",
            exchange_sql="s.exchange_code",
            symbol_sql="s.symbol",
            company_sql="s.company_name",
            signal_sql="c.risk_score",
            suggested_window_days=21,
        ),
        "stock_persistence": WarehouseDatasetDef(
            key="stock_persistence",
            label="Stock persistence summary",
            description="Cross-session persistence view for repeat offenders, anomaly-day ratios, and durability metrics.",
            grain="One row per stock in the stock persistence materialized view.",
            relation="warehouse.mv_stock_persistence_summary",
            base_sql="FROM warehouse.mv_stock_persistence_summary sp",
            dimensions={
                "symbol": WarehouseFieldDef("Symbol", "sp.symbol", "Stock symbol in the persistence view."),
                "company_name": WarehouseFieldDef("Company", "sp.company_name", "Company name in the persistence view."),
                "sector_name": WarehouseFieldDef("Sector", "sp.sector_name", "Sector classification in the persistence view."),
            },
            measures={
                "tracked_symbols": WarehouseFieldDef("Tracked symbols", "COUNT(*)", "Number of symbol rows in the grouped result.", "integer"),
                "anomaly_days": WarehouseFieldDef("Anomaly days", "COALESCE(SUM(sp.anomaly_days), 0)", "Total stock-days with anomaly activity across the grouped result.", "integer"),
                "total_anomalies": WarehouseFieldDef("Total anomalies", "COALESCE(SUM(sp.total_anomalies), 0)", "Total anomalous minute points accumulated by the grouped result.", "integer"),
                "avg_anomaly_day_ratio": WarehouseFieldDef("Avg anomaly-day ratio", "AVG(sp.anomaly_day_ratio)", "Average share of sessions with anomaly activity across the grouped result.", "number"),
                "avg_anomalies_per_active_day": WarehouseFieldDef("Avg anomalies per active day", "AVG(sp.avg_anomalies_per_active_day)", "Average anomaly density on sessions where activity occurred.", "number"),
                "recent_5_session_anomalies": WarehouseFieldDef("Recent 5-session anomalies", "COALESCE(SUM(sp.recent_5_session_anomalies), 0)", "Anomaly activity accumulated in the recent five-session window.", "integer"),
                "peak_daily_composite_score": WarehouseFieldDef("Peak composite score", "MAX(sp.peak_daily_composite_score)", "Highest daily composite score visible inside the grouped result.", "number"),
            },
            default_dimensions=("symbol", "sector_name"),
            default_measures=("total_anomalies", "avg_anomaly_day_ratio", "peak_daily_composite_score"),
            default_sort_field="avg_anomaly_day_ratio",
            supports_sector=True,
            supports_symbol_search=True,
            supports_min_signal=True,
            sector_sql="sp.sector_name",
            symbol_sql="sp.symbol",
            company_sql="sp.company_name",
            signal_sql="sp.peak_daily_composite_score",
            suggested_window_days=None,
        ),
        "sector_momentum": WarehouseDatasetDef(
            key="sector_momentum",
            label="Sector momentum summary",
            description="Recent-versus-prior sector regime view for identifying acceleration, cooling, and contagion drift.",
            grain="One row per sector in the sector momentum materialized view.",
            relation="warehouse.mv_sector_momentum_summary",
            base_sql="FROM warehouse.mv_sector_momentum_summary sm",
            dimensions={
                "sector_name": WarehouseFieldDef("Sector", "sm.sector_name", "Sector represented by the momentum summary row."),
            },
            measures={
                "sector_rows": WarehouseFieldDef("Rows", "COUNT(*)", "Number of grouped momentum rows.", "integer"),
                "recent_total_anomalies": WarehouseFieldDef("Recent anomalies", "COALESCE(SUM(sm.recent_total_anomalies), 0)", "Total anomalies in the recent momentum window.", "integer"),
                "prior_total_anomalies": WarehouseFieldDef("Prior anomalies", "COALESCE(SUM(sm.prior_total_anomalies), 0)", "Total anomalies in the prior comparison window.", "integer"),
                "anomaly_delta": WarehouseFieldDef("Anomaly delta", "COALESCE(SUM(sm.anomaly_delta), 0)", "Recent-minus-prior anomaly change.", "number"),
                "score_delta": WarehouseFieldDef("Score delta", "AVG(sm.score_delta)", "Recent-minus-prior composite-score change.", "number"),
                "contagion_delta": WarehouseFieldDef("Contagion delta", "COALESCE(SUM(sm.contagion_delta), 0)", "Recent-minus-prior contagion-event change.", "number"),
                "recent_peak_daily_composite_score": WarehouseFieldDef("Recent peak score", "MAX(sm.recent_peak_daily_composite_score)", "Highest recent daily composite score in the grouped result.", "number"),
            },
            default_dimensions=("sector_name",),
            default_measures=("anomaly_delta", "score_delta", "contagion_delta"),
            default_sort_field="anomaly_delta",
            chart_preference="bar",
            supports_sector=True,
            supports_min_signal=True,
            sector_sql="sm.sector_name",
            signal_sql="sm.recent_peak_daily_composite_score",
            suggested_window_days=None,
        ),
    }


def _warehouse_date_bounds() -> dict[str, date | None]:
    def _loader() -> dict[str, date | None]:
        with pg_connection() as conn:
            row = conn.execute(
                """
                SELECT
                    MIN(d.calendar_date) AS first_calendar_date,
                    MAX(d.calendar_date) AS last_calendar_date
                FROM warehouse.fact_market_day f
                JOIN warehouse.dim_date d ON d.date_sk = f.date_sk
                """
            ).fetchone()
        return {
            "first_calendar_date": row["first_calendar_date"] if row else None,
            "last_calendar_date": row["last_calendar_date"] if row else None,
        }

    return _cached("warehouse:query:date_bounds", 300.0, _loader)


def _warehouse_presets(date_bounds: dict[str, date | None]) -> list[dict[str, Any]]:
    latest_date = date_bounds.get("last_calendar_date")

    def _window_start(days: int) -> str | None:
        if latest_date is None:
            return None
        return (latest_date - timedelta(days=max(days - 1, 0))).isoformat()

    latest_text = latest_date.isoformat() if latest_date else None
    return [
        {
            "id": "sector-stress",
            "label": "Sector stress by day",
            "description": "Track daily sector pressure, peak scores, and contagion-linked minutes over the recent warehouse window.",
            "request": {
                "dataset": "sector_day",
                "dimensions": ["calendar_date", "sector_name"],
                "measures": ["active_minutes", "max_composite_score", "contagion_minutes"],
                "date_from": _window_start(21),
                "date_to": latest_text,
                "sort_field": "max_composite_score",
                "sort_direction": "desc",
                "limit": 60,
            },
        },
        {
            "id": "stock-anomaly-leaders",
            "label": "Daily stock anomaly drill-down",
            "description": "Review the highest daily stock anomalies with sector and contagion context over the recent warehouse window.",
            "request": {
                "dataset": "stock_day",
                "dimensions": ["calendar_date", "symbol", "sector_name"],
                "measures": ["anomaly_count", "peak_composite_score", "contagion_event_count"],
                "date_from": _window_start(14),
                "date_to": latest_text,
                "sort_field": "peak_composite_score",
                "sort_direction": "desc",
                "limit": 80,
            },
        },
        {
            "id": "minute-pressure",
            "label": "Minute pressure scan",
            "description": "Query minute-grain signal pressure by date, time, and sector without scanning the full warehouse horizon by default.",
            "request": {
                "dataset": "minute_signals",
                "dimensions": ["calendar_date", "time_label", "sector_name"],
                "measures": ["flagged_minutes", "peak_composite_score", "contagion_minutes"],
                "date_from": _window_start(5),
                "date_to": latest_text,
                "sort_field": "peak_composite_score",
                "sort_direction": "desc",
                "limit": 120,
            },
        },
        {
            "id": "contagion-audit",
            "label": "Contagion audit trail",
            "description": "Inspect trigger symbols, affected counts, and risk scores across recent contagion windows.",
            "request": {
                "dataset": "contagion_events",
                "dimensions": ["calendar_date", "trigger_symbol", "sector_name"],
                "measures": ["event_count", "max_risk_score", "total_affected_count"],
                "date_from": _window_start(21),
                "date_to": latest_text,
                "sort_field": "max_risk_score",
                "sort_direction": "desc",
                "limit": 60,
            },
        },
        {
            "id": "persistent-names",
            "label": "Persistent names",
            "description": "Find the stocks that keep reappearing across sessions rather than flashing once and disappearing.",
            "request": {
                "dataset": "stock_persistence",
                "dimensions": ["symbol", "sector_name"],
                "measures": ["total_anomalies", "avg_anomaly_day_ratio", "peak_daily_composite_score"],
                "sort_field": "avg_anomaly_day_ratio",
                "sort_direction": "desc",
                "limit": 50,
            },
        },
        {
            "id": "sector-acceleration",
            "label": "Sector acceleration",
            "description": "Compare recent sector behavior with the prior window to identify acceleration, cooling, and contagion drift.",
            "request": {
                "dataset": "sector_momentum",
                "dimensions": ["sector_name"],
                "measures": ["anomaly_delta", "score_delta", "contagion_delta"],
                "sort_field": "anomaly_delta",
                "sort_direction": "desc",
                "limit": 25,
            },
        },
    ]


def _warehouse_query_metadata() -> dict[str, Any]:
    def _loader() -> dict[str, Any]:
        catalog = _warehouse_query_catalog()
        date_bounds = _warehouse_date_bounds()
        with pg_connection() as conn:
            sectors = [row["sector_name"] for row in conn.execute("SELECT sector_name FROM warehouse.dim_sector ORDER BY sector_name").fetchall()]
            exchanges = [row["exchange_code"] for row in conn.execute("SELECT exchange_code FROM warehouse.dim_exchange ORDER BY exchange_code").fetchall()]
            row_counts = {dataset.key: _relation_row_count(conn, dataset.relation) for dataset in catalog.values()}

        latest_date = date_bounds.get("last_calendar_date")
        datasets = []
        for dataset in catalog.values():
            default_date_from = None
            default_date_to = None
            if dataset.supports_date and latest_date is not None:
                default_date_to = latest_date.isoformat()
                if dataset.suggested_window_days:
                    default_date_from = (latest_date - timedelta(days=max(dataset.suggested_window_days - 1, 0))).isoformat()

            datasets.append(
                {
                    "key": dataset.key,
                    "label": dataset.label,
                    "description": dataset.description,
                    "grain": dataset.grain,
                    "row_count": row_counts.get(dataset.key, 0),
                    "supports": {
                        "date": dataset.supports_date,
                        "sector": dataset.supports_sector,
                        "exchange": dataset.supports_exchange,
                        "symbol_search": dataset.supports_symbol_search,
                        "min_signal": dataset.supports_min_signal,
                    },
                    "dimensions": [
                        {
                            "key": key,
                            "label": field.label,
                            "description": field.description,
                            "kind": field.kind,
                            "default_selected": key in dataset.default_dimensions,
                        }
                        for key, field in dataset.dimensions.items()
                    ],
                    "measures": [
                        {
                            "key": key,
                            "label": field.label,
                            "description": field.description,
                            "kind": field.kind,
                            "default_selected": key in dataset.default_measures,
                        }
                        for key, field in dataset.measures.items()
                    ],
                    "defaults": {
                        "dimensions": list(dataset.default_dimensions),
                        "measures": list(dataset.default_measures),
                        "sort_field": dataset.default_sort_field,
                        "sort_direction": dataset.default_sort_direction,
                        "limit": dataset.default_limit,
                        "date_from": default_date_from,
                        "date_to": default_date_to,
                        "suggested_window_days": dataset.suggested_window_days,
                    },
                }
            )

        return {
            "generated_at": datetime.now(UTC).isoformat(),
            "date_window": {
                "first_calendar_date": date_bounds.get("first_calendar_date").isoformat() if date_bounds.get("first_calendar_date") else None,
                "last_calendar_date": latest_date.isoformat() if latest_date else None,
            },
            "sectors": sectors,
            "exchanges": exchanges,
            "datasets": datasets,
            "presets": _warehouse_presets(date_bounds),
        }

    return _cached("warehouse:query:metadata", 300.0, _loader)


def _warehouse_unique(items: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        ordered.append(item)
    return ordered


def _warehouse_normalize_query(request: WarehouseQueryRequest) -> dict[str, Any]:
    catalog = _warehouse_query_catalog()
    dataset = catalog.get(request.dataset)
    if dataset is None:
        raise HTTPException(status_code=400, detail="Unknown warehouse dataset")

    dimensions = [key for key in _warehouse_unique(request.dimensions) if key in dataset.dimensions][:4]
    measures = [key for key in _warehouse_unique(request.measures) if key in dataset.measures][:5]
    if not dimensions:
        dimensions = list(dataset.default_dimensions)
    if not measures:
        measures = list(dataset.default_measures)

    sort_candidates = set(dimensions) | set(measures)
    sort_field = request.sort_field if request.sort_field in sort_candidates else dataset.default_sort_field
    if sort_field not in sort_candidates:
        sort_field = measures[0] if measures else dimensions[0]

    date_from = request.date_from
    date_to = request.date_to
    if date_from and date_to and date_from > date_to:
        date_from, date_to = date_to, date_from

    symbol_search = request.symbol_search.strip() if request.symbol_search else None

    return {
        "dataset": dataset,
        "dataset_key": dataset.key,
        "dimensions": dimensions,
        "measures": measures,
        "date_from": date_from,
        "date_to": date_to,
        "sector": request.sector.strip() if request.sector else None,
        "exchange": request.exchange.strip() if request.exchange else None,
        "symbol_search": symbol_search or None,
        "min_signal": request.min_signal,
        "sort_field": sort_field,
        "sort_direction": "asc" if request.sort_direction == "asc" else "desc",
        "limit": max(1, min(request.limit, 500)),
    }


def _warehouse_query_filters(dataset: WarehouseDatasetDef, query: dict[str, Any]) -> tuple[list[str], list[Any]]:
    where_clauses: list[str] = []
    params: list[Any] = []

    if dataset.supports_date and dataset.date_sql and query["date_from"] is not None:
        where_clauses.append(f"{dataset.date_sql} >= %s")
        params.append(query["date_from"])
    if dataset.supports_date and dataset.date_sql and query["date_to"] is not None:
        where_clauses.append(f"{dataset.date_sql} <= %s")
        params.append(query["date_to"])
    if dataset.supports_sector and dataset.sector_sql and query["sector"]:
        where_clauses.append(f"{dataset.sector_sql} = %s")
        params.append(query["sector"])
    if dataset.supports_exchange and dataset.exchange_sql and query["exchange"]:
        where_clauses.append(f"{dataset.exchange_sql} = %s")
        params.append(query["exchange"])
    if dataset.supports_symbol_search and dataset.symbol_sql and query["symbol_search"]:
        pattern = f"%{query['symbol_search'].upper()}%"
        if dataset.company_sql:
            where_clauses.append(f"(UPPER({dataset.symbol_sql}) LIKE %s OR UPPER({dataset.company_sql}) LIKE %s)")
            params.extend([pattern, pattern])
        else:
            where_clauses.append(f"UPPER({dataset.symbol_sql}) LIKE %s")
            params.append(pattern)
    if dataset.supports_min_signal and dataset.signal_sql and query["min_signal"] is not None:
        where_clauses.append(f"{dataset.signal_sql} >= %s")
        params.append(query["min_signal"])

    return where_clauses, params


def _warehouse_value_sort_key(value: Any) -> float:
    if value is None:
        return float("-inf")
    if isinstance(value, bool):
        return float(int(value))
    if isinstance(value, (int, float)):
        if isinstance(value, float) and math.isnan(value):
            return float("-inf")
        return float(value)
    if isinstance(value, datetime):
        return value.timestamp()
    if isinstance(value, date):
        return float(datetime.combine(value, datetime.min.time(), UTC).timestamp())
    return float("-inf")


def _warehouse_format_value(value: Any, kind: str | None = None) -> str:
    if value is None:
        return "N/A"
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, float):
        if math.isnan(value):
            return "N/A"
        digits = 0 if kind == "integer" else 3
        return f"{value:,.{digits}f}"
    if isinstance(value, int):
        return f"{value:,}"
    return str(value)


def _warehouse_query_preview(dataset: WarehouseDatasetDef, query: dict[str, Any]) -> str:
    dimension_labels = [dataset.dimensions[key].label for key in query["dimensions"]]
    measure_labels = [dataset.measures[key].label for key in query["measures"]]
    filters: list[str] = []
    if query["date_from"] or query["date_to"]:
        start = query["date_from"].isoformat() if query["date_from"] else "start"
        end = query["date_to"].isoformat() if query["date_to"] else "latest"
        filters.append(f"date {start} to {end}")
    if query["sector"]:
        filters.append(f"sector {query['sector']}")
    if query["exchange"]:
        filters.append(f"exchange {query['exchange']}")
    if query["symbol_search"]:
        filters.append(f"symbol/company matching \"{query['symbol_search']}\"")
    if query["min_signal"] is not None:
        filters.append(f"signal >= {query['min_signal']:.2f}")
    filter_text = ", ".join(filters) if filters else "full available scope"
    return (
        f"{dataset.label} | group by {', '.join(dimension_labels)} | measure {', '.join(measure_labels)} | "
        f"{filter_text} | sorted by {query['sort_field']} {query['sort_direction']} | limit {query['limit']}"
    )


def _warehouse_chart_config(dataset: WarehouseDatasetDef, query: dict[str, Any]) -> dict[str, Any] | None:
    if not query["dimensions"] or not query["measures"]:
        return None
    label_key = query["dimensions"][0]
    value_key = query["measures"][0]
    kind = dataset.chart_preference
    if kind == "auto":
        kind = "line" if dataset.dimensions[label_key].kind in {"date", "time", "datetime"} else "bar"
    return {
        "kind": kind,
        "label_key": label_key,
        "value_key": value_key,
        "title": f"{dataset.measures[value_key].label} by {dataset.dimensions[label_key].label}",
    }


def _warehouse_report(dataset: WarehouseDatasetDef, query: dict[str, Any], rows: list[dict[str, Any]], query_time_ms: int) -> dict[str, Any]:
    primary_measure = query["measures"][0] if query["measures"] else None
    dimension_keys = query["dimensions"]
    top_row = None
    if primary_measure and rows:
        top_row = max(rows, key=lambda row: _warehouse_value_sort_key(row.get(primary_measure)))

    scope_parts: list[str] = []
    if query["date_from"] or query["date_to"]:
        scope_parts.append(
            f"{query['date_from'].isoformat() if query['date_from'] else 'start'} to {query['date_to'].isoformat() if query['date_to'] else 'latest'}"
        )
    if query["sector"]:
        scope_parts.append(query["sector"])
    if query["exchange"]:
        scope_parts.append(query["exchange"])
    if query["symbol_search"]:
        scope_parts.append(f"matching {query['symbol_search']}")

    highlights = [
        {"label": "Dataset", "value": dataset.label},
        {"label": "Rows returned", "value": str(len(rows))},
        {"label": "Query time", "value": f"{query_time_ms} ms"},
    ]
    if top_row and primary_measure:
        descriptor = " | ".join(
            _warehouse_format_value(top_row.get(dimension_key), dataset.dimensions[dimension_key].kind)
            for dimension_key in dimension_keys
        ) if dimension_keys else dataset.label
        measure_kind = dataset.measures[primary_measure].kind
        highlights.append(
            {
                "label": "Top finding",
                "value": f"{descriptor} -> {_warehouse_format_value(top_row.get(primary_measure), measure_kind)}",
            }
        )

    findings = [
        f"This report scans the {dataset.label.lower()} surface at the grain '{dataset.grain}'.",
        f"It groups by {', '.join(dataset.dimensions[key].label for key in query['dimensions'])} and evaluates {', '.join(dataset.measures[key].label for key in query['measures'])}.",
    ]
    if top_row and primary_measure:
        measure = dataset.measures[primary_measure]
        descriptor = " | ".join(
            _warehouse_format_value(top_row.get(dimension_key), dataset.dimensions[dimension_key].kind)
            for dimension_key in dimension_keys
        ) if dimension_keys else dataset.label
        findings.append(
            f"The strongest row in the current result set is {descriptor} with {measure.label.lower()} {_warehouse_format_value(top_row.get(primary_measure), measure.kind)}."
        )
    if scope_parts:
        findings.append(f"The current scope is constrained to {' | '.join(scope_parts)}.")
    if len(rows) >= query["limit"]:
        findings.append("The result set has reached the current row limit. Raise the limit or tighten filters for a more targeted scan.")

    return {
        "headline": f"{dataset.label} report",
        "subheadline": f"{len(rows)} rows returned from the warehouse workbench.",
        "highlights": highlights,
        "findings": findings,
    }


def _warehouse_execute_query(query: dict[str, Any]) -> dict[str, Any]:
    dataset: WarehouseDatasetDef = query["dataset"]
    select_parts = [f"{dataset.dimensions[key].sql} AS {key}" for key in query["dimensions"]]
    select_parts.extend(f"{dataset.measures[key].sql} AS {key}" for key in query["measures"])
    where_clauses, params = _warehouse_query_filters(dataset, query)
    group_by = [dataset.dimensions[key].sql for key in query["dimensions"]]
    order_field = query["sort_field"]

    sql_parts = [
        "SELECT",
        "    " + ",\n    ".join(select_parts),
        dataset.base_sql.strip(),
    ]
    if where_clauses:
        sql_parts.append("WHERE " + " AND ".join(where_clauses))
    if group_by:
        sql_parts.append("GROUP BY " + ", ".join(group_by))
    sql_parts.append(f"ORDER BY {order_field} {query['sort_direction'].upper()} NULLS LAST")
    sql_parts.append("LIMIT %s")
    params.append(query["limit"])
    sql_text = "\n".join(sql_parts)

    started = monotonic()
    with pg_connection() as conn:
        rows = [dict(row) for row in conn.execute(sql_text, params).fetchall()]
    query_time_ms = int((monotonic() - started) * 1000)

    columns = [
        {
            "key": key,
            "label": dataset.dimensions[key].label,
            "kind": dataset.dimensions[key].kind,
            "role": "dimension",
            "description": dataset.dimensions[key].description,
        }
        for key in query["dimensions"]
    ]
    columns.extend(
        {
            "key": key,
            "label": dataset.measures[key].label,
            "kind": dataset.measures[key].kind,
            "role": "measure",
            "description": dataset.measures[key].description,
        }
        for key in query["measures"]
    )
    metadata = _warehouse_query_metadata()
    dataset_meta = next((item for item in metadata.get("datasets", []) if item.get("key") == dataset.key), None)
    available_rows = int(dataset_meta.get("row_count") or 0) if isinstance(dataset_meta, dict) else 0

    return {
        "dataset": {
            "key": dataset.key,
            "label": dataset.label,
            "description": dataset.description,
            "grain": dataset.grain,
            "available_rows": available_rows,
        },
        "query": {
            "dimensions": query["dimensions"],
            "measures": query["measures"],
            "date_from": query["date_from"].isoformat() if query["date_from"] else None,
            "date_to": query["date_to"].isoformat() if query["date_to"] else None,
            "sector": query["sector"],
            "exchange": query["exchange"],
            "symbol_search": query["symbol_search"],
            "min_signal": query["min_signal"],
            "sort_field": query["sort_field"],
            "sort_direction": query["sort_direction"],
            "limit": query["limit"],
            "preview": _warehouse_query_preview(dataset, query),
        },
        "columns": columns,
        "rows": rows,
        "row_count": len(rows),
        "query_time_ms": query_time_ms,
        "chart": _warehouse_chart_config(dataset, query),
        "report": _warehouse_report(dataset, query, rows, query_time_ms),
        "generated_at": datetime.now(UTC).isoformat(),
    }


def _warehouse_query_response(request: WarehouseQueryRequest) -> dict[str, Any]:
    normalized = _warehouse_normalize_query(request)
    cache_payload = {
        "dataset": normalized["dataset_key"],
        "dimensions": normalized["dimensions"],
        "measures": normalized["measures"],
        "date_from": normalized["date_from"].isoformat() if normalized["date_from"] else None,
        "date_to": normalized["date_to"].isoformat() if normalized["date_to"] else None,
        "sector": normalized["sector"],
        "exchange": normalized["exchange"],
        "symbol_search": normalized["symbol_search"],
        "min_signal": normalized["min_signal"],
        "sort_field": normalized["sort_field"],
        "sort_direction": normalized["sort_direction"],
        "limit": normalized["limit"],
    }
    cache_key = f"warehouse:query:{json.dumps(cache_payload, sort_keys=True)}"
    return _cached(cache_key, 45.0, lambda: _warehouse_execute_query(normalized))


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
app.add_middleware(GZipMiddleware, minimum_size=2048)


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
    # User-facing universe views should reflect the currently active listed set.
    records = {stock.symbol: stock.model_dump() for stock in load_stock_references() if stock.is_active}
    with pg_connection() as conn:
        rows = conn.execute(
            """
            SELECT symbol, company_name, exchange, sector, country, aliases, source, metadata, last_refreshed_at
            FROM operational.stock_profiles
            """
        ).fetchall()
    for row in rows:
        if row["symbol"] not in records:
            continue
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
            "company_name": profile.get("company_name", row["symbol"]),
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


def _partition_row_count(
    session,
    table_name: str,
    latest_market: dict[str, dict[str, Any]],
) -> int | None:
    if not latest_market:
        return 0

    stmt = session.prepare(f"SELECT count(*) FROM {table_name} WHERE symbol = ? AND trading_date = ?")
    total = 0
    seen: set[tuple[str, date]] = set()
    try:
        for symbol, item in latest_market.items():
            trading_date = date.fromisoformat(str(item["trading_date"]))
            partition = (symbol, trading_date)
            if partition in seen:
                continue
            seen.add(partition)
            row = session.execute(stmt, partition).one()
            total += int(row["count"]) if row and row.get("count") is not None else 0
    except Exception:
        return None
    return total


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


def _coerce_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if not text:
        return None
    if "T" in text:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).date()
    return date.fromisoformat(text)


def _latest_active_trading_date() -> date | None:
    def _loader() -> date | None:
        latest_dates = [
            trading_date
            for record in _latest_market_map().values()
            if (trading_date := _record_trading_date(record)) is not None
        ]
        if latest_dates:
            return max(latest_dates)

        anomaly_dates = [
            trading_date
            for record in _latest_anomaly_map().values()
            if (trading_date := _record_trading_date(record)) is not None
        ]
        if anomaly_dates:
            return max(anomaly_dates)

        with pg_connection() as conn:
            row = conn.execute(
                """
                SELECT COALESCE(
                    (SELECT MAX(trading_date) FROM operational.stock_daily_bars),
                    (SELECT MAX(trading_date) FROM operational.alert_events),
                    (SELECT MAX(trading_date) FROM operational.contagion_events)
                ) AS latest_trading_date
                """
            ).fetchone()
        return row["latest_trading_date"] if row else None

    return _cached("market:latest_trading_date", 15.0, _loader)


def _resolve_alert_scope(counts_by_date: dict[date, int], current_trading_date: date | None) -> dict[str, Any]:
    reference_date = current_trading_date or (max(counts_by_date) if counts_by_date else None)
    stale_dates = sorted(
        [trading_date for trading_date in counts_by_date if reference_date and trading_date != reference_date],
        reverse=True,
    )
    return {
        "current_trading_date": reference_date,
        "current_open_count": counts_by_date.get(reference_date, 0) if reference_date else 0,
        "stale_open_count": sum(counts_by_date[trading_date] for trading_date in stale_dates),
        "total_open_count": sum(counts_by_date.values()),
        "latest_stale_alert_date": stale_dates[0] if stale_dates else None,
    }


def _load_alert_scope_snapshot() -> dict[str, Any]:
    with pg_connection() as conn:
        rows = conn.execute(
            """
            SELECT trading_date, COUNT(*) AS row_count
            FROM operational.alert_events
            WHERE status = 'open'
            GROUP BY trading_date
            ORDER BY trading_date DESC
            """
        ).fetchall()
    counts_by_date = {
        row["trading_date"]: int(row["row_count"])
        for row in rows
        if row["trading_date"] is not None
    }
    return _resolve_alert_scope(counts_by_date, _latest_active_trading_date())


def _alert_scope_snapshot() -> dict[str, Any]:
    return _cached("alerts:scope", 15.0, _load_alert_scope_snapshot)


def _annotate_alert_rows(rows: list[dict[str, Any]], reference_date: date | None = None) -> list[dict[str, Any]]:
    current_trading_date = reference_date if reference_date is not None else _alert_scope_snapshot()["current_trading_date"]
    annotated: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        alert_trading_date = _coerce_date(item.get("trading_date"))
        item["is_stale"] = bool(current_trading_date and alert_trading_date and alert_trading_date != current_trading_date)
        annotated.append(item)
    return annotated


def _open_alert_count() -> int:
    return int(_alert_scope_snapshot()["current_open_count"])


def _recent_alerts(limit: int = 20, status: str | None = "open", scope: str = "all") -> list[dict[str, Any]]:
    cache_key = f"alerts:recent:{status or 'all'}:{scope}:{limit}"

    def _loader() -> list[dict[str, Any]]:
        params: list[Any] = []
        clauses: list[str] = []
        reference_date = _alert_scope_snapshot()["current_trading_date"]

        if status:
            clauses.append("status = %s")
            params.append(status)
        if scope == "current" and reference_date is not None:
            clauses.append("trading_date = %s")
            params.append(reference_date)
        elif scope == "stale":
            if reference_date is None:
                return []
            clauses.append("trading_date <> %s")
            params.append(reference_date)

        clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
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
                tuple([*params, limit]),
            ).fetchall()
        return _annotate_alert_rows([dict(row) for row in rows], reference_date)

    return _cached(cache_key, 15.0, _loader)


def _latest_run(mode: str | None = None, modes: tuple[str, ...] | None = None) -> dict[str, Any] | None:
    if mode and modes:
        raise ValueError("Pass either mode or modes, not both")

    filter_clause = ""
    params: tuple[Any, ...] = ()
    if mode:
        filter_clause = "WHERE mode = %s"
        params = (mode,)
    elif modes:
        placeholders = ", ".join(["%s"] * len(modes))
        filter_clause = f"WHERE mode IN ({placeholders})"
        params = tuple(modes)

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


def _overview_feed_mode(
    latest_run: dict[str, Any] | None,
    latest_intraday_run: dict[str, Any] | None,
    latest_market: list[dict[str, Any]] | None = None,
) -> str | None:
    latest_market = latest_market or []
    latest_market_date = max((_record_trading_date(record) for record in latest_market), default=None)
    today_market_date = as_market_time(datetime.now(UTC)).date()

    if latest_intraday_run:
        latest_mode = str(latest_intraday_run["mode"])
        if latest_mode == "live" and latest_market_date != today_market_date:
            fallback_run = _latest_run(modes=("replay", "capture_replay", "backfill"))
            if fallback_run:
                return str(fallback_run["mode"])
        return latest_mode
    if latest_run:
        return str(latest_run["mode"])
    return None


def _system_scale_projection(
    listed_symbols: int,
    intraday_symbols_loaded: int,
    intraday_trading_days_loaded: int,
    actual_intraday_tick_and_anomaly_rows: int,
) -> dict[str, Any]:
    active_symbols = max(intraday_symbols_loaded, 0)
    active_sessions = max(intraday_trading_days_loaded, 0)
    minute_rows_per_trading_day = active_symbols * SESSION_MINUTES_PER_DAY
    minute_rows_for_loaded_window = minute_rows_per_trading_day * active_sessions
    minute_rows_per_year = minute_rows_per_trading_day * TRADING_DAYS_PER_YEAR
    tick_and_anomaly_rows_for_loaded_window = minute_rows_for_loaded_window * 2
    tick_and_anomaly_rows_per_year = minute_rows_per_year * 2
    five_year_tick_and_anomaly_rows = tick_and_anomaly_rows_per_year * 5
    actual_capture_pct = (
        round((actual_intraday_tick_and_anomaly_rows / tick_and_anomaly_rows_for_loaded_window) * 100, 4)
        if tick_and_anomaly_rows_for_loaded_window
        else 0.0
    )
    current_scope_share_of_listed_universe_pct = (
        round((active_symbols / listed_symbols) * 100, 2)
        if listed_symbols
        else 0.0
    )

    return {
        "session_minutes": SESSION_MINUTES_PER_DAY,
        "trading_days_per_year": TRADING_DAYS_PER_YEAR,
        "listed_symbols": listed_symbols,
        "intraday_symbols_loaded": active_symbols,
        "intraday_trading_days_loaded": active_sessions,
        "minute_rows_per_trading_day": minute_rows_per_trading_day,
        "minute_rows_for_loaded_window": minute_rows_for_loaded_window,
        "minute_rows_per_year": minute_rows_per_year,
        "tick_and_anomaly_rows_for_loaded_window": tick_and_anomaly_rows_for_loaded_window,
        "tick_and_anomaly_rows_per_year": tick_and_anomaly_rows_per_year,
        "five_year_tick_and_anomaly_rows": five_year_tick_and_anomaly_rows,
        "crosses_crore_in_loaded_window": tick_and_anomaly_rows_for_loaded_window >= 10_000_000,
        "crosses_crore_annually": tick_and_anomaly_rows_per_year >= 10_000_000,
        "actual_capture_vs_loaded_window_pct": actual_capture_pct,
        "current_scope_share_of_listed_universe_pct": current_scope_share_of_listed_universe_pct,
    }


def _system_scale_snapshot(
    profiles: dict[str, dict[str, Any]] | None = None,
    history_map: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    if profiles is None and history_map is None:
        return _cached("system:scale", 120.0, lambda: _system_scale_snapshot(_profiles(), _history_coverage_map()))

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
        sector_momentum_count = _relation_row_count(conn, "warehouse.mv_sector_momentum_summary")
        stock_persistence_count = _relation_row_count(conn, "warehouse.mv_stock_persistence_summary")
        intraday_profile_count = _relation_row_count(conn, "warehouse.mv_intraday_pressure_profile")
        coverage_window = conn.execute(
            """
            SELECT COUNT(DISTINCT trading_date) AS trading_days_loaded,
                   MIN(trading_date) AS first_daily_date,
                   MAX(trading_date) AS last_daily_date
            FROM operational.stock_daily_bars
            """
        ).fetchone()
        intraday_window = conn.execute(
            """
            SELECT COUNT(DISTINCT trading_date) AS trading_days_loaded,
                   COUNT(DISTINCT symbol) AS intraday_symbols_loaded,
                   MIN(trading_date) AS first_intraday_date,
                   MAX(trading_date) AS last_intraday_date
            FROM operational.surveillance_coverage
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
    latest_market = _latest_market_map()
    bulk_streaming_counts = _streaming_counts_from_bulk_runs(active_ingestion_runs)
    completed_bulk_counts = _streaming_counts_from_bulk_runs(completed_ingestion_runs)
    streaming_counts: dict[str, int | None] = {
        "market_ticks": bulk_streaming_counts["market_ticks"] or None,
        "anomaly_metrics": bulk_streaming_counts["anomaly_metrics"] or None,
        "latest_market_state": len(latest_market),
        "inflight_market_ticks": max(bulk_streaming_counts["market_ticks"] - completed_bulk_counts["market_ticks"], 0),
        "inflight_anomaly_metrics": max(
            bulk_streaming_counts["anomaly_metrics"] - completed_bulk_counts["anomaly_metrics"], 0
        ),
    }
    for key, table_name in {
        "market_ticks": "market_ticks",
        "anomaly_metrics": "anomaly_metrics",
    }.items():
        try:
            partition_count = _partition_row_count(session, table_name, latest_market)
            if partition_count is not None:
                actual_count = partition_count
            else:
                row = session.execute(f"SELECT count(*) FROM {table_name}").one()
                actual_count = int(row["count"]) if row else 0
            streaming_counts[key] = max(int(streaming_counts[key] or 0), actual_count)
        except Exception:
            if streaming_counts[key] is None:
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
        "mv_sector_momentum_summary": sector_momentum_count,
        "mv_stock_persistence_summary": stock_persistence_count,
        "mv_intraday_pressure_profile": intraday_profile_count,
    }
    operational_total_rows = sum(operational_counts.values())
    warehouse_total_rows = sum(warehouse_counts.values())
    streaming_total_rows = sum(
        value
        for key, value in streaming_counts.items()
        if key not in {"redis_keys", "inflight_market_ticks", "inflight_anomaly_metrics"} and value is not None
    )
    materialized_total_rows = operational_total_rows + warehouse_total_rows + streaming_total_rows
    daily_trading_days_loaded = int(coverage_window["trading_days_loaded"] or 0)
    intraday_trading_days_loaded = int(intraday_window["trading_days_loaded"] or 0)
    intraday_symbols_loaded = int(intraday_window["intraday_symbols_loaded"] or 0)
    actual_intraday_tick_and_anomaly_rows = int(streaming_counts["market_ticks"] or 0) + int(
        streaming_counts["anomaly_metrics"] or 0
    )

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
            "daily_trading_days_loaded": daily_trading_days_loaded,
            "intraday_symbols_loaded": intraday_symbols_loaded,
            "intraday_trading_days_loaded": intraday_trading_days_loaded,
            "first_intraday_date": str(intraday_window["first_intraday_date"]) if intraday_window["first_intraday_date"] else None,
            "last_intraday_date": str(intraday_window["last_intraday_date"]) if intraday_window["last_intraday_date"] else None,
            "trading_days_loaded": daily_trading_days_loaded,
        },
        "projection": _system_scale_projection(
            listed_symbols=len(profiles),
            intraday_symbols_loaded=intraday_symbols_loaded,
            intraday_trading_days_loaded=intraday_trading_days_loaded,
            actual_intraday_tick_and_anomaly_rows=actual_intraday_tick_and_anomaly_rows,
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


def _bulk_daily_rows(symbols: list[str], days: int) -> dict[str, list[dict[str, Any]]]:
    normalized = [symbol for symbol in dict.fromkeys(symbols) if symbol]
    if not normalized:
        return {}

    cache_key = f"history:daily-bulk:{max(days, 1)}:{json.dumps(normalized)}"

    def _loader() -> dict[str, list[dict[str, Any]]]:
        with pg_connection() as conn:
            rows = conn.execute(
                """
                WITH ranked AS (
                    SELECT
                        symbol,
                        trading_date,
                        open,
                        high,
                        low,
                        close,
                        adj_close,
                        volume,
                        dividends,
                        stock_splits,
                        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY trading_date DESC) AS row_rank
                    FROM operational.stock_daily_bars
                    WHERE symbol = ANY(%s)
                )
                SELECT symbol, trading_date, open, high, low, close, adj_close, volume, dividends, stock_splits
                FROM ranked
                WHERE row_rank <= %s
                ORDER BY symbol, trading_date ASC
                """,
                (normalized, max(days, 1)),
            ).fetchall()
        payload: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            payload[row["symbol"]].append(dict(row))
        return dict(payload)

    return _cached(cache_key, 120.0, _loader)


def _window_return(rows: list[dict[str, Any]], sessions: int) -> float | None:
    if len(rows) <= sessions:
        return None
    latest_close = rows[-1]["close"]
    anchor_close = rows[-(sessions + 1)]["close"]
    if anchor_close in (None, 0):
        return None
    return float(((latest_close / anchor_close) - 1) * 100)


def _descending_numeric_key(value: Any) -> float:
    if value is None:
        return math.inf
    try:
        return -float(value)
    except (TypeError, ValueError):
        return math.inf


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
    open_alerts = {item["symbol"]: item for item in _recent_alerts(limit=500, status="open", scope="current")}
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
    daily_row_map = _bulk_daily_rows([profile["symbol"] for profile in profiles], days)

    peers: list[dict[str, Any]] = []
    for profile in profiles:
        daily_rows = daily_row_map.get(profile["symbol"], [])
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
            _descending_numeric_key(item.get("latest_anomaly_score")),
            _descending_numeric_key(item.get("return_20d_pct")),
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


def _sort_screener_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    severity_rank = {"critical": 0, "high": 1, "medium": 2, "low": 3}
    rows.sort(
        key=lambda item: (
            severity_rank.get((item["latest_alert"] or {}).get("severity", "zzz"), 99),
            -1 if (item["latest_anomaly"] or {}).get("is_anomalous") else 0,
            _descending_numeric_key(item["indicators"].get("return_20d_pct")),
            item["symbol"],
        )
    )
    return rows


def _top_movers(rows: list[dict[str, Any]], limit: int = 10) -> list[dict[str, Any]]:
    return sorted(
        rows,
        key=lambda item: (
            float(item["indicators"].get("return_20d_pct") or -9999),
            float(item["indicators"].get("volume_ratio_20d") or -9999),
            item["symbol"],
        ),
        reverse=True,
    )[:limit]


def _build_screener(days: int, limit: int, only_hydrated: bool) -> dict[str, Any]:
    cache_key = f"screener:{max(days, 1)}:{max(limit, 1)}:{int(only_hydrated)}"

    def _loader() -> dict[str, Any]:
        all_profiles = sorted(_profiles().values(), key=lambda item: item["symbol"])
        history_map = _history_coverage_map()
        latest_market = _latest_market_map()
        latest_anomalies = _latest_anomaly_map()
        alert_rows = _recent_alerts(limit=500, status="open", scope="current")
        latest_alerts: dict[str, dict[str, Any]] = {}
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
        profile_budget = min(len(profiles), max(limit * 2, 100 if only_hydrated else 150))
        profiles = profiles[:profile_budget]
        daily_row_map = _bulk_daily_rows([profile["symbol"] for profile in profiles], days)

        rows = [
            _screener_row(
                profile,
                daily_row_map.get(profile["symbol"], []),
                latest_market.get(profile["symbol"]),
                latest_anomalies.get(profile["symbol"]),
                latest_alerts.get(profile["symbol"]),
            )
            for profile in profiles
        ]
        return {"items": _sort_screener_rows(rows)[:limit], "count": len(rows)}

    return _cached(cache_key, 30.0, _loader)


@app.get("/api/system/health")
def system_health() -> dict[str, Any]:
    redis = get_redis()
    settings = get_settings()
    profiles = _profiles()
    history_map = _history_coverage_map()
    with pg_connection() as conn:
        latest_etl_attempt = conn.execute(
            """
            SELECT run_id, trading_date, started_at, finished_at, status, inserted_rows, aggregate_rows, notes
            FROM operational.etl_runs
            ORDER BY started_at DESC
            LIMIT 1
            """
        ).fetchone()
        latest_successful_etl = conn.execute(
            """
            SELECT run_id, trading_date, started_at, finished_at, status, inserted_rows, aggregate_rows, notes
            FROM operational.etl_runs
            WHERE status = 'completed'
            ORDER BY started_at DESC
            LIMIT 1
            """
        ).fetchone()
        latest_failed_etl = conn.execute(
            """
            SELECT run_id, trading_date, started_at, finished_at, status, inserted_rows, aggregate_rows, notes
            FROM operational.etl_runs
            WHERE status = 'failed'
            ORDER BY started_at DESC
            LIMIT 1
            """
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
        "latest_etl_run": dict(latest_successful_etl) if latest_successful_etl else (dict(latest_etl_attempt) if latest_etl_attempt else None),
        "latest_etl_attempt": dict(latest_etl_attempt) if latest_etl_attempt else None,
        "latest_successful_etl_run": dict(latest_successful_etl) if latest_successful_etl else None,
        "latest_failed_etl_run": dict(latest_failed_etl) if latest_failed_etl else None,
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
        "data_policy": {
            "strict_real_data_only": settings.strict_real_data_only,
            "market_data_provider": preferred_market_data_provider(),
            "configured_provider": settings.market_data_provider,
            "upstox_configured": bool(settings.upstox_access_token),
        },
    }


@app.get("/api/system/scale")
def system_scale() -> dict[str, Any]:
    return _system_scale_snapshot()


@app.get("/api/methodology")
def methodology() -> dict[str, Any]:
    settings = get_settings()
    ewma_half_life_alpha = 1 - math.exp(math.log(0.5) / max(settings.anomaly_warmup_minutes, 1))
    return {
        "market": {
            "timezone": settings.market_timezone,
            "session_open": settings.market_open_ist,
            "session_close": settings.market_close_ist,
            "session_minutes": SESSION_MINUTES_PER_DAY,
            "scope": "Indian equities, IST market calendar, and real bars at the best available interval for each horizon.",
            "provider_policy": preferred_market_data_provider(),
            "strict_real_only": settings.strict_real_data_only,
        },
        "anomaly": {
            "warmup_minutes": settings.anomaly_warmup_minutes,
            "ewma_alpha": round(ewma_half_life_alpha, 6),
            "price_z_threshold": settings.anomaly_price_z_threshold,
            "volume_z_threshold": settings.anomaly_volume_z_threshold,
            "composite_threshold": settings.anomaly_composite_threshold,
            "composite_weights": {"price_z": 0.6, "volume_z": 0.4},
            "threshold_rationale": (
                "The thresholds are currently set to keep real captured sessions interpretable while still surfacing "
                "enough activity for analyst review. A production rollout would tighten them with backtesting by "
                "sector, liquidity bucket, and false-positive cost."
            ),
            "formulas": [
                {
                    "name": "Return percentage",
                    "formula": "((close_t - close_(t-1)) / close_(t-1)) * 100",
                    "meaning": "The latest bar-to-bar price move, expressed as a percentage for whichever real interval is being monitored.",
                },
                {
                    "name": "EWMA alpha",
                    "formula": "1 - exp(log(0.5) / warmup_minutes)",
                    "meaning": "Converts the warmup horizon into the decay factor used by the streaming mean and variance.",
                },
                {
                    "name": "EWMA mean",
                    "formula": "alpha * x_t + (1 - alpha) * mean_(t-1)",
                    "meaning": "Recent observations matter more than older ones, but history is never discarded completely.",
                },
                {
                    "name": "EWMA variance",
                    "formula": "alpha * (x_t - mean_t)^2 + (1 - alpha) * variance_(t-1)",
                    "meaning": "Streaming dispersion estimate used to normalize price and volume moves.",
                },
                {
                    "name": "Price z-score",
                    "formula": "(return_pct - ewma_return_mean) / sqrt(ewma_return_variance)",
                    "meaning": "How unusual the latest one-minute return is relative to the recent return distribution.",
                },
                {
                    "name": "Volume z-score",
                    "formula": "(volume_t - ewma_volume_mean) / sqrt(ewma_volume_variance)",
                    "meaning": "How unusual the latest volume print is relative to the recent volume distribution.",
                },
                {
                    "name": "Composite score",
                    "formula": "0.6 * |price_z| + 0.4 * |volume_z|",
                    "meaning": "Weighted blend used to rank signals when price and volume both matter.",
                },
            ],
            "flag_rule": (
                "A point is flagged when |price z| crosses the price threshold, or |volume z| crosses the volume "
                "threshold, or the weighted composite score crosses the composite threshold."
            ),
            "severity_bands": [
                {
                    "severity": "low",
                    "rule": "Any persisted anomaly alert below the medium band.",
                },
                {
                    "severity": "medium",
                    "rule": "Composite >= 2.2 or volume z >= 2.0.",
                },
                {
                    "severity": "high",
                    "rule": "Composite >= 2.6 or price z >= 2.4.",
                },
                {
                    "severity": "critical",
                    "rule": "Composite >= 3.0, or price z >= 2.6 together with volume z >= 2.2.",
                },
            ],
        },
        "alerts": {
            "cooldown_minutes": settings.alert_cooldown_minutes,
            "notification_min_severity": settings.alert_notify_min_severity,
            "logic": (
                "Alerts are persisted operator events. Anomaly alerts are deduplicated to one event per rounded minute "
                "and then throttled with a cooldown so the queue stays usable during noisy bursts."
            ),
        },
        "contagion": {
            "window_minutes": settings.contagion_window_minutes,
            "trigger_rule": "A new contagion window opens only when a symbol is anomalous and belongs to a valid sector peer set.",
            "peer_rule": "Peers contribute only if they are anomalous, belong to the same sector, and arrive before the window closes.",
            "risk_score_formula": "trigger_composite_score + peer_average_score + 0.35 * affected_count",
            "why": (
                "This keeps v1 explainable: one trigger stock, a bounded five-minute sector window, and a risk score "
                "that increases with both peer intensity and peer count."
            ),
        },
        "warehouse": {
            "facts": [
                "fact_anomaly_minute for real intraday surveillance bars",
                "fact_market_day for stock-level daily summaries",
                "fact_contagion_event for persisted propagation windows",
                "fact_surveillance_coverage as a factless monitoring ledger",
            ],
            "why": (
                "Warehouse facts are separated by grain so OLAP questions stay defensible. Intraday anomaly bars, "
                "daily summaries, contagion windows, and monitoring coverage are modeled separately instead of being "
                "mixed into one ambiguous fact table."
            ),
        },
    }


@app.get("/api/overview")
def overview() -> dict[str, Any]:
    def _loader() -> dict[str, Any]:
        profiles = _profiles()
        history_map = _history_coverage_map()
        alert_scope = _alert_scope_snapshot()
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
        alerts = _recent_alerts(limit=8, status="open", scope="current")
        screener = _build_screener(days=45, limit=40, only_hydrated=True)
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
        latest_intraday_run = _latest_run(modes=INTRADAY_FEED_MODES)
        as_of = None
        if latest_market:
            as_of = max(item["timestamp_ist"] for item in latest_market if item.get("timestamp_ist"))
        return {
            "as_of": as_of,
            "market_mode": _overview_feed_mode(latest_run, latest_intraday_run, latest_market),
            "latest_ingestion_mode": latest_run["mode"] if latest_run else None,
            "live_market": latest_market,
            "top_anomalies": sorted(latest_anomalies, key=lambda item: item["composite_score"], reverse=True)[:10],
            "top_movers": _top_movers(screener["items"], limit=10),
            "sector_heatmap": sector_heatmap,
            "recent_contagion_events": [dict(row) for row in contagion],
            "recent_alerts": alerts,
            "open_alert_count": alert_scope["current_open_count"],
            "total_open_alert_count": alert_scope["total_open_count"],
            "stale_open_alert_count": alert_scope["stale_open_count"],
            "current_alert_trading_date": str(alert_scope["current_trading_date"]) if alert_scope["current_trading_date"] else None,
            "latest_stale_alert_date": str(alert_scope["latest_stale_alert_date"]) if alert_scope["latest_stale_alert_date"] else None,
            "tracked_symbol_count": len(profiles),
            "tracked_sector_count": len({item["sector"] for item in profiles.values() if item.get("sector")}),
            "hydrated_symbol_count": len(history_map),
            "watchlist_symbol_count": len([item for item in profiles.values() if item.get("watchlist")]),
            "live_symbol_count": len(latest_market),
            "live_sector_count": len({item["sector"] for item in latest_market}),
        }

    return _cached("overview", 10.0, _loader)


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
    alert_scope = _alert_scope_snapshot()
    display_scope = "all"
    scope_reference_date = alert_scope["current_trading_date"]
    if status == "open":
        alerts = _recent_alerts(limit=limit, status=status, scope="current")
        display_scope = "current"
        if not alerts and alert_scope["stale_open_count"]:
            alerts = _recent_alerts(limit=limit, status=status, scope="stale")
            display_scope = "stale"
            scope_reference_date = alert_scope["latest_stale_alert_date"]
    else:
        alerts = _recent_alerts(limit=limit, status=status, scope="all")
    return {
        "items": alerts,
        "open_count": alert_scope["total_open_count"],
        "active_open_count": alert_scope["current_open_count"],
        "stale_open_count": alert_scope["stale_open_count"],
        "display_scope": display_scope,
        "current_trading_date": str(alert_scope["current_trading_date"]) if alert_scope["current_trading_date"] else None,
        "scope_reference_date": str(scope_reference_date) if scope_reference_date else None,
    }


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
    return _build_screener(days=days, limit=limit, only_hydrated=only_hydrated)


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
            SELECT event_id, symbol, trading_date, event_category, severity, status, title, message,
                   detected_at, composite_score, event_payload, acknowledged_at
            FROM operational.alert_events
            WHERE symbol = %s
            ORDER BY detected_at DESC
            LIMIT 25
            """,
            (resolved_symbol,),
        ).fetchall()

    alert_items = _annotate_alert_rows([dict(row) for row in alerts])
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


@app.get("/api/warehouse/query-metadata")
def warehouse_query_metadata() -> dict[str, Any]:
    return _warehouse_query_metadata()


@app.post("/api/warehouse/query")
def warehouse_query(request: WarehouseQueryRequest) -> dict[str, Any]:
    return _warehouse_query_response(request)


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
    def _loader() -> dict[str, Any]:
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
                    (SELECT COUNT(*) FROM warehouse.fact_surveillance_coverage) AS coverage_rows,
                    (SELECT COUNT(*) FROM warehouse.mv_sector_momentum_summary) AS sector_momentum_rows,
                    (SELECT COUNT(*) FROM warehouse.mv_stock_persistence_summary) AS stock_persistence_rows,
                    (SELECT COUNT(*) FROM warehouse.mv_intraday_pressure_profile) AS intraday_profile_rows
                FROM market_window
                CROSS JOIN date_window
                """
            ).fetchone()
        return dict(row) if row else {}

    return _cached("warehouse:summary", 30.0, _loader)


@app.get("/api/warehouse/monthly-rollups")
def monthly_rollups() -> list[dict[str, Any]]:
    def _loader() -> list[dict[str, Any]]:
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

    return _cached("warehouse:monthly", 60.0, _loader)


@app.get("/api/warehouse/sector-regimes")
def warehouse_sector_regimes(limit: int = Query(25, ge=1, le=100)) -> list[dict[str, Any]]:
    def _loader() -> list[dict[str, Any]]:
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

    return _cached(f"warehouse:sector-regimes:{limit}", 30.0, _loader)


@app.get("/api/warehouse/stock-outliers")
def warehouse_stock_outliers(limit: int = Query(50, ge=1, le=200)) -> list[dict[str, Any]]:
    def _loader() -> list[dict[str, Any]]:
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

    return _cached(f"warehouse:stock-outliers:{limit}", 30.0, _loader)


@app.get("/api/warehouse/stock-leaders")
def warehouse_stock_leaders(limit: int = Query(50, ge=1, le=200)) -> list[dict[str, Any]]:
    def _loader() -> list[dict[str, Any]]:
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

    return _cached(f"warehouse:stock-leaders:{limit}", 30.0, _loader)


@app.get("/api/warehouse/sector-momentum")
def warehouse_sector_momentum(limit: int = Query(25, ge=1, le=100)) -> list[dict[str, Any]]:
    def _loader() -> list[dict[str, Any]]:
        with pg_connection() as conn:
            rows = conn.execute(
                """
                SELECT
                    sector_name,
                    recent_sessions,
                    prior_sessions,
                    recent_total_anomalies,
                    prior_total_anomalies,
                    recent_avg_daily_composite_score,
                    prior_avg_daily_composite_score,
                    recent_peak_daily_composite_score,
                    prior_peak_daily_composite_score,
                    recent_contagion_event_count,
                    prior_contagion_event_count,
                    anomaly_delta,
                    score_delta,
                    contagion_delta
                FROM warehouse.mv_sector_momentum_summary
                ORDER BY anomaly_delta DESC, score_delta DESC, sector_name
                LIMIT %s
                """,
                (limit,),
            ).fetchall()
        return [dict(row) for row in rows]

    return _cached(f"warehouse:sector-momentum:{limit}", 30.0, _loader)


@app.get("/api/warehouse/stock-persistence")
def warehouse_stock_persistence(limit: int = Query(50, ge=1, le=200)) -> list[dict[str, Any]]:
    def _loader() -> list[dict[str, Any]]:
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
                    last_anomaly_date,
                    recent_5_session_anomalies,
                    recent_5_session_anomaly_days,
                    anomaly_day_ratio,
                    avg_anomalies_per_active_day,
                    days_since_last_anomaly
                FROM warehouse.mv_stock_persistence_summary
                ORDER BY anomaly_day_ratio DESC, total_anomalies DESC, symbol
                LIMIT %s
                """,
                (limit,),
            ).fetchall()
        return [dict(row) for row in rows]

    return _cached(f"warehouse:stock-persistence:{limit}", 30.0, _loader)


@app.get("/api/warehouse/intraday-profile")
def warehouse_intraday_profile(limit: int = Query(375, ge=1, le=400)) -> list[dict[str, Any]]:
    def _loader() -> list[dict[str, Any]]:
        with pg_connection() as conn:
            rows = conn.execute(
                """
                SELECT
                    time_sk,
                    time_label,
                    hour,
                    minute,
                    anomaly_minutes,
                    distinct_stocks,
                    sessions_covered,
                    avg_composite_score,
                    peak_composite_score,
                    contagion_minutes
                FROM warehouse.mv_intraday_pressure_profile
                ORDER BY time_sk ASC
                LIMIT %s
                """,
                (limit,),
            ).fetchall()
        return [dict(row) for row in rows]

    return _cached(f"warehouse:intraday-profile:{limit}", 30.0, _loader)


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
