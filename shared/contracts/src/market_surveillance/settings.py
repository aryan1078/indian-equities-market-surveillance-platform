from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    project_name: str = Field(default="market-surveillance", alias="PROJECT_NAME")
    market_timezone: str = Field(default="Asia/Kolkata", alias="MARKET_TIMEZONE")
    market_open_ist: str = Field(default="09:15", alias="MARKET_OPEN_IST")
    market_close_ist: str = Field(default="15:30", alias="MARKET_CLOSE_IST")
    default_trading_date: str = Field(default="2026-03-16", alias="DEFAULT_TRADING_DATE")
    data_root: Path = Field(default=Path("./data"), alias="DATA_ROOT")
    fixture_root: Path = Field(default=Path("./tests/fixtures"), alias="FIXTURE_ROOT")

    kafka_bootstrap_servers: str = Field(default="localhost:9092", alias="KAFKA_BOOTSTRAP_SERVERS")
    kafka_market_ticks_topic: str = Field(default="market_ticks", alias="KAFKA_MARKET_TICKS_TOPIC")
    kafka_anomaly_detections_topic: str = Field(default="anomaly_detections", alias="KAFKA_ANOMALY_DETECTIONS_TOPIC")
    kafka_dlq_topic: str = Field(default="tick_dlq", alias="KAFKA_DLQ_TOPIC")
    kafka_consumer_group_storage: str = Field(default="storage-consumer", alias="KAFKA_CONSUMER_GROUP_STORAGE")
    kafka_consumer_group_anomaly: str = Field(default="anomaly-engine", alias="KAFKA_CONSUMER_GROUP_ANOMALY")
    kafka_consumer_group_contagion: str = Field(default="contagion-engine", alias="KAFKA_CONSUMER_GROUP_CONTAGION")

    cassandra_hosts: str = Field(default="localhost", alias="CASSANDRA_HOSTS")
    cassandra_port: int = Field(default=9042, alias="CASSANDRA_PORT")
    cassandra_keyspace: str = Field(default="market_surveillance", alias="CASSANDRA_KEYSPACE")
    cassandra_consistency: Literal["ONE", "QUORUM", "LOCAL_QUORUM"] = Field(default="ONE", alias="CASSANDRA_CONSISTENCY")

    redis_url: str = Field(default="redis://localhost:6379/0", alias="REDIS_URL")

    postgres_host: str = Field(default="localhost", alias="POSTGRES_HOST")
    postgres_port: int = Field(default=5432, alias="POSTGRES_PORT")
    postgres_db: str = Field(default="market_surveillance", alias="POSTGRES_DB")
    postgres_user: str = Field(default="market_user", alias="POSTGRES_USER")
    postgres_password: str = Field(default="market_pass", alias="POSTGRES_PASSWORD")
    postgres_dsn: str = Field(
        default="postgresql://market_user:market_pass@localhost:5432/market_surveillance",
        alias="POSTGRES_DSN",
    )

    api_host: str = Field(default="0.0.0.0", alias="API_HOST")
    api_port: int = Field(default=8000, alias="API_PORT")

    market_data_provider: Literal["auto", "yfinance", "upstox"] = Field(default="auto", alias="MARKET_DATA_PROVIDER")
    market_data_timeout_seconds: float = Field(default=20.0, alias="MARKET_DATA_TIMEOUT_SECONDS")
    strict_real_data_only: bool = Field(default=True, alias="STRICT_REAL_DATA_ONLY")
    upstox_api_base_url: str = Field(default="https://api.upstox.com", alias="UPSTOX_API_BASE_URL")
    upstox_access_token: str | None = Field(default=None, alias="UPSTOX_ACCESS_TOKEN")

    anomaly_price_z_threshold: float = Field(default=2.4, alias="ANOMALY_PRICE_Z_THRESHOLD")
    anomaly_volume_z_threshold: float = Field(default=2.0, alias="ANOMALY_VOLUME_Z_THRESHOLD")
    anomaly_composite_threshold: float = Field(default=2.2, alias="ANOMALY_COMPOSITE_THRESHOLD")
    anomaly_warmup_minutes: int = Field(default=20, alias="ANOMALY_WARMUP_MINUTES")
    contagion_window_minutes: int = Field(default=5, alias="CONTAGION_WINDOW_MINUTES")
    alert_cooldown_minutes: int = Field(default=10, alias="ALERT_COOLDOWN_MINUTES")
    alert_webhook_url: str | None = Field(default=None, alias="ALERT_WEBHOOK_URL")
    alert_webhook_type: Literal["generic", "slack", "discord"] = Field(default="generic", alias="ALERT_WEBHOOK_TYPE")
    alert_webhook_timeout_seconds: float = Field(default=5.0, alias="ALERT_WEBHOOK_TIMEOUT_SECONDS")
    alert_notify_min_severity: Literal["low", "medium", "high", "critical"] = Field(
        default="high",
        alias="ALERT_NOTIFY_MIN_SEVERITY",
    )
    daily_history_period: str = Field(default="3mo", alias="DAILY_HISTORY_PERIOD")
    daily_history_batch_size: int = Field(default=40, alias="DAILY_HISTORY_BATCH_SIZE")
    daily_history_pause_seconds: float = Field(default=0.2, alias="DAILY_HISTORY_PAUSE_SECONDS")
    stock_history_days: int = Field(default=45, alias="STOCK_HISTORY_DAYS")
    live_poll_seconds: int = Field(default=60, alias="LIVE_POLL_SECONDS")
    live_interval: str = Field(default="1m", alias="LIVE_INTERVAL")

    metadata_path: Path = Field(default=Path("./shared/metadata/stocks.json"))

    @property
    def cassandra_contact_points(self) -> list[str]:
        return [host.strip() for host in self.cassandra_hosts.split(",") if host.strip()]


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
