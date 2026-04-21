from __future__ import annotations

import json
import time
from collections.abc import Iterable
from datetime import UTC, date, datetime, timedelta
from typing import Any

import pandas as pd
import yfinance as yf

from .db import pg_connection
from .metadata import StockReference, load_stock_references, valid_peer_sector
from .settings import get_settings


def normalize_symbol_input(symbol: str) -> str:
    return symbol.strip().upper().replace(" ", "")


def candidate_symbols(symbol: str) -> list[str]:
    normalized = normalize_symbol_input(symbol)
    if normalized.endswith(".NS") or normalized.endswith(".BO"):
        return [normalized]
    return [f"{normalized}.NS", f"{normalized}.BO", normalized]


def _metadata_lookup() -> dict[str, StockReference]:
    return {stock.symbol: stock for stock in load_stock_references()}


def upsert_stock_profile(
    symbol: str,
    company_name: str,
    exchange: str | None,
    sector: str | None,
    aliases: list[str] | None = None,
    source: str = "metadata",
    metadata: dict[str, Any] | None = None,
) -> None:
    with pg_connection() as conn:
        conn.execute(
            """
            INSERT INTO operational.stock_profiles (
                symbol, company_name, exchange, sector, aliases, source, metadata, last_refreshed_at
            ) VALUES (%s, %s, %s, %s, %s::jsonb, %s, %s::jsonb, now())
            ON CONFLICT (symbol) DO UPDATE
            SET company_name = COALESCE(NULLIF(EXCLUDED.company_name, ''), operational.stock_profiles.company_name),
                exchange = CASE
                    WHEN EXCLUDED.exchange IS NULL OR EXCLUDED.exchange IN ('', 'Unknown')
                        THEN operational.stock_profiles.exchange
                    ELSE EXCLUDED.exchange
                END,
                sector = CASE
                    WHEN EXCLUDED.sector IS NULL OR EXCLUDED.sector IN ('', 'Unknown')
                        THEN COALESCE(operational.stock_profiles.sector, EXCLUDED.sector)
                    ELSE EXCLUDED.sector
                END,
                aliases = EXCLUDED.aliases,
                source = CASE
                    WHEN EXCLUDED.source = 'metadata'
                         AND operational.stock_profiles.source IN ('metadata_enriched', 'yfinance')
                         AND (EXCLUDED.sector IS NULL OR EXCLUDED.sector IN ('', 'Unknown'))
                        THEN operational.stock_profiles.source
                    ELSE EXCLUDED.source
                END,
                metadata = operational.stock_profiles.metadata || EXCLUDED.metadata,
                last_refreshed_at = EXCLUDED.last_refreshed_at
            """,
            (
                symbol,
                company_name,
                exchange,
                sector,
                json.dumps(aliases or []),
                source,
                json.dumps(metadata or {}),
            ),
        )


def sync_metadata_profiles() -> None:
    symbols: set[str] = set()
    for stock in load_stock_references():
        symbols.add(stock.symbol)
        upsert_stock_profile(
            symbol=stock.symbol,
            company_name=stock.company_name,
            exchange=stock.exchange,
            sector=stock.sector,
            aliases=stock.aliases,
            source="metadata",
            metadata={
                **stock.metadata,
                "watchlist": stock.watchlist,
            },
        )
    _prune_stale_metadata_profiles(symbols)


def _prune_stale_metadata_profiles(active_symbols: set[str]) -> None:
    if not active_symbols:
        return
    with pg_connection() as conn:
        conn.execute(
            """
            DELETE FROM operational.stock_profiles
            WHERE source = 'metadata'
              AND NOT (symbol = ANY(%s))
            """,
            (sorted(active_symbols),),
        )


def _empty_frame(frame: pd.DataFrame | None) -> bool:
    return frame is None or frame.empty or frame.get("Close") is None or frame["Close"].dropna().empty


def _normalize_daily_frame(frame: pd.DataFrame) -> pd.DataFrame:
    normalized = frame.copy()
    normalized.index = pd.to_datetime(normalized.index)
    if "Adj Close" not in normalized.columns:
        normalized["Adj Close"] = normalized["Close"]
    for column in ["Dividends", "Stock Splits"]:
        if column not in normalized.columns:
            normalized[column] = 0.0
    normalized = normalized.dropna(subset=["Close"])
    return normalized


def _download_batch(symbols: list[str], period: str) -> dict[str, pd.DataFrame]:
    if not symbols:
        return {}
    payload = yf.download(
        tickers=" ".join(symbols),
        period=period,
        interval="1d",
        auto_adjust=False,
        progress=False,
        threads=False,
        group_by="ticker",
    )
    if isinstance(payload.columns, pd.MultiIndex):
        frames: dict[str, pd.DataFrame] = {}
        for symbol in symbols:
            try:
                candidate = payload[symbol]
            except KeyError:
                continue
            candidate = _normalize_daily_frame(candidate)
            if not candidate.empty:
                frames[symbol] = candidate
        return frames

    single = _normalize_daily_frame(payload)
    return {symbols[0]: single} if not single.empty else {}


def _download_with_retry(symbol: str, period: str, attempts: int = 4) -> pd.DataFrame | None:
    for attempt in range(attempts):
        try:
            frames = _download_batch([symbol], period)
            frame = frames.get(symbol)
            if frame is not None and not frame.empty:
                return frame
        except Exception:
            pass
        time.sleep(min(1.5 * (attempt + 1), 6.0))
    return None


def _chunked(values: list[str], size: int) -> Iterable[list[str]]:
    chunk_size = max(size, 1)
    for index in range(0, len(values), chunk_size):
        yield values[index : index + chunk_size]


def store_daily_history(symbol: str, frame: pd.DataFrame) -> int:
    if frame.empty:
        return 0

    inserted = 0
    with pg_connection() as conn:
        for timestamp, row in frame.iterrows():
            trading_day = pd.Timestamp(timestamp).date()
            conn.execute(
                """
                INSERT INTO operational.stock_daily_bars (
                    symbol, trading_date, open, high, low, close, adj_close, volume, dividends, stock_splits, source, refreshed_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'yfinance', now())
                ON CONFLICT (symbol, trading_date) DO UPDATE
                SET open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    adj_close = EXCLUDED.adj_close,
                    volume = EXCLUDED.volume,
                    dividends = EXCLUDED.dividends,
                    stock_splits = EXCLUDED.stock_splits,
                    refreshed_at = EXCLUDED.refreshed_at
                """,
                (
                    symbol,
                    trading_day,
                    float(row.get("Open", 0.0)),
                    float(row.get("High", 0.0)),
                    float(row.get("Low", 0.0)),
                    float(row.get("Close", 0.0)),
                    float(row.get("Adj Close", row.get("Close", 0.0))),
                    int(row.get("Volume", 0) or 0),
                    float(row.get("Dividends", 0.0) or 0.0),
                    float(row.get("Stock Splits", 0.0) or 0.0),
                ),
            )
            inserted += 1
    return inserted


def hydrate_daily_history(symbols: Iterable[str] | None = None, period: str | None = None) -> dict[str, int]:
    sync_metadata_profiles()
    settings = get_settings()
    selected = list(dict.fromkeys(symbols or [stock.symbol for stock in load_stock_references() if stock.is_active]))
    results: dict[str, int] = {}
    period_value = period or settings.daily_history_period
    batch_size = max(settings.daily_history_batch_size, 1)
    pause_seconds = max(settings.daily_history_pause_seconds, 0.0)

    for chunk in _chunked(selected, batch_size):
        frames: dict[str, pd.DataFrame] = {}
        try:
            frames = _download_batch(chunk, period_value)
        except Exception:
            frames = {}

        missing_symbols: list[str] = []
        for symbol in chunk:
            frame = frames.get(symbol)
            if _empty_frame(frame):
                missing_symbols.append(symbol)
                continue
            results[symbol] = store_daily_history(symbol, frame)

        for symbol in missing_symbols:
            frame = _download_with_retry(symbol, period_value)
            if _empty_frame(frame):
                continue
            results[symbol] = store_daily_history(symbol, frame)
            if pause_seconds:
                time.sleep(min(pause_seconds, 0.75))

        if pause_seconds:
            time.sleep(pause_seconds)
    return results


def _history_state(symbol: str) -> tuple[int, date | None]:
    with pg_connection() as conn:
        row = conn.execute(
            """
            SELECT COUNT(*) AS row_count, MAX(trading_date) AS latest_date
            FROM operational.stock_daily_bars
            WHERE symbol = %s
            """,
            (symbol,),
        ).fetchone()
    return int(row["row_count"]), row["latest_date"]


def needs_history_refresh(symbol: str, minimum_days: int | None = None) -> bool:
    row_count, latest_date = _history_state(symbol)
    if row_count == 0:
        return True
    required_days = minimum_days or get_settings().stock_history_days
    if row_count < required_days:
        return True
    if latest_date is None:
        return True
    freshness_cutoff = datetime.now(tz=UTC).date() - timedelta(days=3)
    return latest_date < freshness_cutoff


def _normalize_exchange(value: str | None) -> str | None:
    if not value:
        return None
    normalized = value.strip().upper()
    if normalized in {"NSI", "NSE"}:
        return "NSE"
    if normalized in {"BOM", "BSE"}:
        return "BSE"
    return normalized


def _resolve_unknown_profile(symbol: str, attempts: int = 3) -> tuple[str, str | None, str | None]:
    company_name = symbol
    exchange = None
    sector = None
    for attempt in range(attempts):
        try:
            ticker = yf.Ticker(symbol)
            info = ticker.info or {}
            company_name = info.get("longName") or info.get("shortName") or company_name
            exchange = _normalize_exchange(info.get("exchange")) or exchange
            sector = info.get("sector") or sector
            if company_name != symbol or exchange or sector:
                break
        except Exception:
            pass
        time.sleep(min(1.0 + attempt, 3.0))
    return company_name, exchange, sector


def ensure_daily_history(symbol_input: str, minimum_days: int | None = None) -> str | None:
    lookup = _metadata_lookup()
    candidates = candidate_symbols(symbol_input)

    for symbol in candidates:
        reference = lookup.get(symbol)
        if not reference or needs_history_refresh(symbol, minimum_days):
            continue
        if valid_peer_sector(reference.sector) and _normalize_exchange(reference.exchange):
            return symbol
        company_name, exchange, sector = _resolve_unknown_profile(symbol)
        upsert_stock_profile(
            symbol=symbol,
            company_name=company_name or reference.company_name,
            exchange=exchange or _normalize_exchange(reference.exchange) or reference.exchange,
            sector=sector or reference.sector,
            aliases=reference.aliases,
            source="metadata_enriched",
            metadata={**reference.metadata, "watchlist": reference.watchlist},
        )
        return symbol

    for symbol in candidates:
        frame = _download_with_retry(symbol, get_settings().daily_history_period)
        if _empty_frame(frame):
            continue
        store_daily_history(symbol, frame)
        reference = lookup.get(symbol)
        if reference:
            company_name = reference.company_name
            exchange = reference.exchange
            sector = reference.sector
            source = "metadata"
            if not valid_peer_sector(reference.sector):
                resolved_name, resolved_exchange, resolved_sector = _resolve_unknown_profile(symbol)
                company_name = resolved_name or company_name
                exchange = resolved_exchange or exchange
                sector = resolved_sector or sector
                source = "metadata_enriched"
            upsert_stock_profile(
                symbol=symbol,
                company_name=company_name,
                exchange=exchange,
                sector=sector,
                aliases=reference.aliases,
                source=source,
                metadata={**reference.metadata, "watchlist": reference.watchlist},
            )
        else:
            company_name, exchange, sector = _resolve_unknown_profile(symbol)
            upsert_stock_profile(
                symbol=symbol,
                company_name=company_name,
                exchange=exchange,
                sector=sector,
                aliases=[normalize_symbol_input(symbol_input)],
                source="yfinance",
            )
        return symbol

    return None
