from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from functools import lru_cache
from typing import Literal

import httpx
import pandas as pd
import yfinance as yf

from .metadata import load_stock_references
from .settings import get_settings

REAL_PROVIDERS = {"yfinance", "upstox"}
SYNTHETIC_PROVIDERS = {"fixture", "deterministic_daily_expansion"}
SYNTHETIC_MODES = {"seed_history", "generate_replay_fixture", "minute_backfill"}


@dataclass(frozen=True)
class ProviderFrame:
    symbol: str
    provider: str
    interval: str
    frame: pd.DataFrame


def is_real_provider(provider: str | None) -> bool:
    return (provider or "").strip().lower() in REAL_PROVIDERS


def is_real_source(provider: str | None, mode: str | None) -> bool:
    normalized_provider = (provider or "").strip().lower()
    normalized_mode = (mode or "").strip().lower()
    if normalized_provider in SYNTHETIC_PROVIDERS:
        return False
    if normalized_mode in SYNTHETIC_MODES:
        return False
    return normalized_provider in REAL_PROVIDERS or normalized_mode in {"backfill", "live", "hydrate_daily", "capture_replay", "replay"}


def is_intraday_interval(interval: str) -> bool:
    normalized = interval.lower()
    return normalized.endswith("m") or normalized.endswith("h")


def preferred_market_data_provider() -> str:
    settings = get_settings()
    configured = settings.market_data_provider.lower()
    if configured == "auto":
        return "upstox" if settings.upstox_access_token else "yfinance"
    return configured


def _period_window(period: str, end_date: date | None = None) -> tuple[date, date]:
    anchor = end_date or datetime.now(tz=UTC).date()
    normalized = (period or "").strip().lower()
    if normalized in {"", "max"}:
        return date(2000, 1, 1), anchor

    match = re.fullmatch(r"(\d+)([a-z]+)", normalized)
    if not match:
        return anchor - timedelta(days=5), anchor

    quantity = max(int(match.group(1)), 1)
    unit = match.group(2)
    if unit == "d":
        delta = timedelta(days=quantity)
    elif unit in {"wk", "w"}:
        delta = timedelta(weeks=quantity)
    elif unit in {"mo", "m"}:
        delta = timedelta(days=30 * quantity)
    elif unit == "y":
        delta = timedelta(days=365 * quantity)
    else:
        delta = timedelta(days=quantity)
    return anchor - delta, anchor


def _normalize_downloaded_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame()
    normalized = frame.copy()
    normalized.index = pd.to_datetime(normalized.index)
    for column in ["Open", "High", "Low", "Close", "Volume"]:
        if column not in normalized.columns:
            normalized[column] = 0.0
    for column in ["Dividends", "Stock Splits"]:
        if column not in normalized.columns:
            normalized[column] = 0.0
    if "Adj Close" not in normalized.columns and "Close" in normalized.columns:
        normalized["Adj Close"] = normalized["Close"]
    normalized = normalized.dropna(subset=["Close"])
    normalized = normalized.sort_index()
    return normalized


def _yfinance_interval(interval: str) -> str:
    normalized = interval.lower()
    if normalized == "1h":
        return "60m"
    if normalized == "1wk":
        return "1wk"
    if normalized == "1mo":
        return "1mo"
    return normalized


def _download_yfinance_frames(
    symbols: list[str],
    interval: str,
    period: str | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
) -> dict[str, ProviderFrame]:
    frames: dict[str, ProviderFrame] = {}
    yf_interval = _yfinance_interval(interval)
    for symbol in symbols:
        kwargs: dict[str, object] = {
            "tickers": symbol,
            "interval": yf_interval,
            "auto_adjust": False,
            "progress": False,
            "threads": False,
        }
        if start_date or end_date:
            if start_date:
                kwargs["start"] = start_date.isoformat()
            if end_date:
                kwargs["end"] = (end_date + timedelta(days=1)).isoformat()
        else:
            kwargs["period"] = period or ("5d" if is_intraday_interval(interval) else "3mo")
        payload = yf.download(**kwargs)
        frame = _normalize_downloaded_frame(payload)
        if frame.empty:
            continue
        frames[symbol] = ProviderFrame(symbol=symbol, provider="yfinance", interval=interval, frame=frame)
    return frames


def _upstox_unit_interval(interval: str) -> tuple[str, str]:
    normalized = interval.lower()
    if normalized.endswith("m"):
        return "minutes", normalized[:-1]
    if normalized.endswith("h"):
        return "hours", normalized[:-1]
    if normalized in {"1d", "day"}:
        return "days", "1"
    if normalized in {"1wk", "1w", "week"}:
        return "weeks", "1"
    if normalized in {"1mo", "1mth", "month"}:
        return "months", "1"
    raise ValueError(f"Unsupported Upstox interval: {interval}")


@lru_cache(maxsize=1)
def _reference_lookup() -> dict[str, object]:
    return {stock.symbol: stock for stock in load_stock_references()}


def _upstox_instrument_key(symbol: str) -> str:
    reference = _reference_lookup().get(symbol)
    if reference is None:
        raise ValueError(f"Unknown symbol: {symbol}")
    metadata = getattr(reference, "metadata", {}) or {}
    isin = metadata.get("isin")
    if not isin:
        raise ValueError(f"Missing ISIN metadata for {symbol}")
    exchange = getattr(reference, "exchange", "NSE").upper()
    venue = "NSE_EQ" if exchange == "NSE" else "BSE_EQ"
    return f"{venue}|{isin}"


def _upstox_headers() -> dict[str, str]:
    settings = get_settings()
    if not settings.upstox_access_token:
        raise RuntimeError("UPSTOX_ACCESS_TOKEN is required for Upstox market data")
    return {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {settings.upstox_access_token}",
    }


def _upstox_frame_from_payload(candles: list[list[object]]) -> pd.DataFrame:
    records: list[dict[str, object]] = []
    for candle in candles:
        if len(candle) < 6:
            continue
        records.append(
            {
                "Datetime": pd.to_datetime(str(candle[0])),
                "Open": float(candle[1]),
                "High": float(candle[2]),
                "Low": float(candle[3]),
                "Close": float(candle[4]),
                "Volume": int(candle[5] or 0),
                "Dividends": 0.0,
                "Stock Splits": 0.0,
                "Adj Close": float(candle[4]),
            }
        )
    if not records:
        return pd.DataFrame()
    frame = pd.DataFrame.from_records(records).set_index("Datetime").sort_index()
    return _normalize_downloaded_frame(frame)


def _download_upstox_symbol(
    symbol: str,
    interval: str,
    start_date: date | None,
    end_date: date | None,
) -> ProviderFrame | None:
    settings = get_settings()
    unit, value = _upstox_unit_interval(interval)
    instrument_key = _upstox_instrument_key(symbol)
    today = datetime.now(tz=UTC).date()
    effective_end = end_date or today
    effective_start = start_date or effective_end
    if effective_start > effective_end:
        effective_start, effective_end = effective_end, effective_start

    if effective_start == effective_end == today and is_intraday_interval(interval):
        url = f"{settings.upstox_api_base_url}/v3/historical-candle/intraday/{instrument_key}/{unit}/{value}"
    else:
        url = (
            f"{settings.upstox_api_base_url}/v3/historical-candle/"
            f"{instrument_key}/{unit}/{value}/{effective_end.isoformat()}/{effective_start.isoformat()}"
        )

    with httpx.Client(timeout=settings.market_data_timeout_seconds) as client:
        response = client.get(url, headers=_upstox_headers())
        response.raise_for_status()
        payload = response.json()
    candles = payload.get("data", {}).get("candles", [])
    frame = _upstox_frame_from_payload(candles)
    if frame.empty:
        return None
    return ProviderFrame(symbol=symbol, provider="upstox", interval=interval, frame=frame)


def _download_upstox_frames(
    symbols: list[str],
    interval: str,
    period: str | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
) -> dict[str, ProviderFrame]:
    if start_date is None and end_date is None:
        start_date, end_date = _period_window(period or ("5d" if is_intraday_interval(interval) else "3mo"))
    frames: dict[str, ProviderFrame] = {}
    for symbol in symbols:
        provider_frame = _download_upstox_symbol(symbol, interval, start_date, end_date)
        if provider_frame is not None:
            frames[symbol] = provider_frame
    return frames


def download_market_frames(
    symbols: list[str],
    interval: str,
    period: str | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
) -> dict[str, ProviderFrame]:
    provider_name = preferred_market_data_provider()
    if provider_name == "upstox":
        return _download_upstox_frames(symbols, interval, period=period, start_date=start_date, end_date=end_date)
    return _download_yfinance_frames(symbols, interval, period=period, start_date=start_date, end_date=end_date)

