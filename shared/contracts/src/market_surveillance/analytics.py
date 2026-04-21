from __future__ import annotations

from collections.abc import Sequence
from math import sqrt

import pandas as pd


def _series(values: Sequence[float]) -> pd.Series:
    return pd.Series(list(values), dtype="float64")


def compute_daily_indicators(rows: Sequence[dict]) -> dict[str, float | None]:
    if not rows:
        return {
            "last_close": None,
            "day_change_pct": None,
            "return_20d_pct": None,
            "sma_20": None,
            "ema_12": None,
            "ema_26": None,
            "rsi_14": None,
            "atr_14": None,
            "volatility_20d_pct": None,
            "volume_ratio_20d": None,
            "distance_from_20d_high_pct": None,
            "distance_from_20d_low_pct": None,
        }

    closes = _series(row["close"] for row in rows)
    highs = _series(row["high"] for row in rows)
    lows = _series(row["low"] for row in rows)
    volumes = _series(row["volume"] for row in rows)
    prev_closes = closes.shift(1)

    returns = closes.pct_change()
    gain = returns.clip(lower=0).rolling(14).mean()
    loss = (-returns.clip(upper=0)).rolling(14).mean()
    rs = gain / loss.replace(0, pd.NA)
    rsi = 100 - (100 / (1 + rs))

    true_range = pd.concat(
        [
            highs - lows,
            (highs - prev_closes).abs(),
            (lows - prev_closes).abs(),
        ],
        axis=1,
    ).max(axis=1)
    atr_14 = true_range.rolling(14).mean()

    high_20 = closes.rolling(20).max()
    low_20 = closes.rolling(20).min()
    volume_mean_20 = volumes.rolling(20).mean()
    volatility_20 = returns.rolling(20).std() * sqrt(252) * 100

    last_close = closes.iloc[-1]
    previous_close = closes.iloc[-2] if len(closes) > 1 else pd.NA
    close_20 = closes.iloc[-20] if len(closes) >= 20 else pd.NA

    return {
        "last_close": float(last_close),
        "day_change_pct": None if pd.isna(previous_close) or previous_close == 0 else float(((last_close / previous_close) - 1) * 100),
        "return_20d_pct": None if pd.isna(close_20) or close_20 == 0 else float(((last_close / close_20) - 1) * 100),
        "sma_20": None if pd.isna(closes.rolling(20).mean().iloc[-1]) else float(closes.rolling(20).mean().iloc[-1]),
        "ema_12": float(closes.ewm(span=12, adjust=False).mean().iloc[-1]),
        "ema_26": float(closes.ewm(span=26, adjust=False).mean().iloc[-1]),
        "rsi_14": None if pd.isna(rsi.iloc[-1]) else float(rsi.iloc[-1]),
        "atr_14": None if pd.isna(atr_14.iloc[-1]) else float(atr_14.iloc[-1]),
        "volatility_20d_pct": None if pd.isna(volatility_20.iloc[-1]) else float(volatility_20.iloc[-1]),
        "volume_ratio_20d": None
        if pd.isna(volume_mean_20.iloc[-1]) or volume_mean_20.iloc[-1] == 0
        else float(volumes.iloc[-1] / volume_mean_20.iloc[-1]),
        "distance_from_20d_high_pct": None
        if pd.isna(high_20.iloc[-1]) or high_20.iloc[-1] == 0
        else float(((last_close / high_20.iloc[-1]) - 1) * 100),
        "distance_from_20d_low_pct": None
        if pd.isna(low_20.iloc[-1]) or low_20.iloc[-1] == 0
        else float(((last_close / low_20.iloc[-1]) - 1) * 100),
    }
