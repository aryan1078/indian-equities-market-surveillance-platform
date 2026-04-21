CREATE KEYSPACE IF NOT EXISTS market_surveillance
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE market_surveillance;

CREATE TABLE IF NOT EXISTS stock_reference (
    symbol text PRIMARY KEY,
    exchange text,
    sector text,
    company_name text,
    country text,
    is_active boolean
);

CREATE TABLE IF NOT EXISTS market_ticks (
    symbol text,
    trading_date date,
    timestamp_utc timestamp,
    timestamp_ist text,
    exchange text,
    sector text,
    interval text,
    open double,
    high double,
    low double,
    close double,
    volume bigint,
    dividends double,
    stock_splits double,
    source_mode text,
    source_provider text,
    source_run_id text,
    dedupe_key text,
    ingest_ts timestamp,
    PRIMARY KEY ((symbol, trading_date), timestamp_utc)
) WITH CLUSTERING ORDER BY (timestamp_utc ASC);

CREATE TABLE IF NOT EXISTS anomaly_metrics (
    symbol text,
    trading_date date,
    timestamp_utc timestamp,
    timestamp_ist text,
    exchange text,
    sector text,
    interval text,
    close double,
    volume bigint,
    return_pct double,
    ewma_mean double,
    ewma_variance double,
    rolling_volatility double,
    volume_mean double,
    volume_variance double,
    price_z_score double,
    volume_z_score double,
    composite_score double,
    is_anomalous boolean,
    explainability text,
    source_run_id text,
    dedupe_key text,
    PRIMARY KEY ((symbol, trading_date), timestamp_utc)
) WITH CLUSTERING ORDER BY (timestamp_utc ASC);

CREATE TABLE IF NOT EXISTS latest_market_state (
    symbol text PRIMARY KEY,
    trading_date date,
    timestamp_utc timestamp,
    close double,
    volume bigint,
    composite_score double,
    is_anomalous boolean,
    updated_at timestamp
);

