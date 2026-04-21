from __future__ import annotations

from datetime import UTC, date, datetime
from hashlib import sha1
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, Field, computed_field, field_validator

from .market_time import as_market_time, ensure_utc, trading_date_for


def build_dedupe_key(symbol: str, interval: str, timestamp_utc: datetime) -> str:
    raw = f"{symbol}|{interval}|{ensure_utc(timestamp_utc).isoformat()}"
    return sha1(raw.encode("utf-8")).hexdigest()


class EventSource(BaseModel):
    provider: str = "yfinance"
    mode: str = "backfill"
    run_id: str = Field(default_factory=lambda: uuid4().hex)
    collected_at_utc: datetime = Field(default_factory=lambda: datetime.now(tz=UTC))


class MarketTick(BaseModel):
    symbol: str
    exchange: str
    sector: str
    interval: str = "1m"
    timestamp_utc: datetime
    timestamp_ist: datetime
    trading_date: date
    open: float
    high: float
    low: float
    close: float
    volume: int
    dividends: float = 0.0
    stock_splits: float = 0.0
    source: EventSource = Field(default_factory=EventSource)
    dedupe_key: str | None = None

    @field_validator("timestamp_utc", "timestamp_ist", mode="before")
    @classmethod
    def ensure_datetime(cls, value: datetime) -> datetime:
        return ensure_utc(value) if isinstance(value, datetime) else value

    @field_validator("trading_date", mode="before")
    @classmethod
    def derive_trading_date(cls, value: Any, info: Any) -> date:
        if value is not None:
            return value
        timestamp = info.data.get("timestamp_utc")
        return trading_date_for(timestamp)

    @computed_field
    @property
    def is_replay(self) -> bool:
        return self.source.mode == "replay"

    def model_post_init(self, __context: Any) -> None:
        if not self.dedupe_key:
            object.__setattr__(self, "dedupe_key", build_dedupe_key(self.symbol, self.interval, self.timestamp_utc))
        object.__setattr__(self, "timestamp_utc", ensure_utc(self.timestamp_utc))
        object.__setattr__(self, "timestamp_ist", as_market_time(self.timestamp_utc))
        object.__setattr__(self, "trading_date", trading_date_for(self.timestamp_utc))


class AnomalyDetection(BaseModel):
    symbol: str
    exchange: str
    sector: str
    interval: str = "1m"
    timestamp_utc: datetime
    timestamp_ist: datetime
    trading_date: date
    close: float
    volume: int
    return_pct: float | None = None
    ewma_mean: float | None = None
    ewma_variance: float | None = None
    rolling_volatility: float | None = None
    volume_mean: float | None = None
    volume_variance: float | None = None
    price_z_score: float | None = None
    volume_z_score: float | None = None
    composite_score: float = 0.0
    is_anomalous: bool = False
    explainability: str = ""
    source_run_id: str
    dedupe_key: str

    @field_validator("timestamp_utc", mode="before")
    @classmethod
    def ensure_detection_datetime(cls, value: datetime) -> datetime:
        return ensure_utc(value) if isinstance(value, datetime) else value

    def model_post_init(self, __context: Any) -> None:
        object.__setattr__(self, "timestamp_utc", ensure_utc(self.timestamp_utc))
        object.__setattr__(self, "timestamp_ist", as_market_time(self.timestamp_utc))
        object.__setattr__(self, "trading_date", trading_date_for(self.timestamp_utc))


class ContagionEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: uuid4().hex)
    event_timestamp: datetime
    trading_date: date
    trigger_symbol: str
    trigger_sector: str
    affected_symbols: list[str] = Field(default_factory=list)
    observation_window_start: datetime
    observation_window_end: datetime
    trigger_composite_score: float
    peer_average_score: float = 0.0
    risk_score: float = 0.0
    rationale: str
    source_run_id: str

    @field_validator("event_timestamp", "observation_window_start", "observation_window_end", mode="before")
    @classmethod
    def ensure_event_datetime(cls, value: datetime) -> datetime:
        return ensure_utc(value) if isinstance(value, datetime) else value

    @computed_field
    @property
    def affected_count(self) -> int:
        return len(self.affected_symbols)


class OverviewMetric(BaseModel):
    symbol: str
    sector: str
    close: float
    composite_score: float
    volume: int
    timestamp_ist: datetime


class ReplayStatus(BaseModel):
    mode: str
    source_file: str
    speed: float
    trading_date: date
    last_emitted_at: datetime | None = None
