CREATE SCHEMA IF NOT EXISTS operational;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS warehouse;

CREATE TABLE IF NOT EXISTS operational.ingestion_runs (
    run_id text PRIMARY KEY,
    mode text NOT NULL,
    started_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    symbol_count integer NOT NULL DEFAULT 0,
    records_seen integer NOT NULL DEFAULT 0,
    records_published integer NOT NULL DEFAULT 0,
    status text NOT NULL DEFAULT 'running',
    notes jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS operational.surveillance_coverage (
    symbol text NOT NULL,
    trading_date date NOT NULL,
    timestamp_utc timestamptz NOT NULL,
    timestamp_ist timestamptz NOT NULL,
    source_run_id text NOT NULL,
    coverage_state text NOT NULL,
    PRIMARY KEY (symbol, timestamp_utc)
);

CREATE TABLE IF NOT EXISTS operational.contagion_events (
    event_id text PRIMARY KEY,
    event_timestamp timestamptz NOT NULL,
    trading_date date NOT NULL,
    trigger_symbol text NOT NULL,
    trigger_sector text NOT NULL,
    affected_symbols jsonb NOT NULL DEFAULT '[]'::jsonb,
    affected_count integer NOT NULL DEFAULT 0,
    observation_window_start timestamptz NOT NULL,
    observation_window_end timestamptz NOT NULL,
    trigger_composite_score double precision NOT NULL,
    peer_average_score double precision NOT NULL DEFAULT 0,
    risk_score double precision NOT NULL DEFAULT 0,
    rationale text NOT NULL,
    source_run_id text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS operational.etl_runs (
    run_id text PRIMARY KEY,
    trading_date date NOT NULL,
    started_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    extracted_rows integer NOT NULL DEFAULT 0,
    staged_rows integer NOT NULL DEFAULT 0,
    excluded_rows integer NOT NULL DEFAULT 0,
    inserted_rows integer NOT NULL DEFAULT 0,
    skipped_conflicts integer NOT NULL DEFAULT 0,
    aggregate_rows integer NOT NULL DEFAULT 0,
    checksum text,
    status text NOT NULL DEFAULT 'running',
    notes jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS staging.anomaly_metrics_stage (
    run_id text NOT NULL,
    symbol text NOT NULL,
    trading_date date NOT NULL,
    timestamp_utc timestamptz NOT NULL,
    timestamp_ist timestamptz NOT NULL,
    exchange text NOT NULL,
    sector text NOT NULL,
    close double precision NOT NULL,
    volume bigint NOT NULL,
    return_pct double precision,
    rolling_volatility double precision,
    price_z_score double precision,
    volume_z_score double precision,
    composite_score double precision,
    is_anomalous boolean NOT NULL,
    source_run_id text NOT NULL,
    dedupe_key text NOT NULL,
    contagion_flag boolean NOT NULL DEFAULT false,
    PRIMARY KEY (run_id, symbol, timestamp_utc)
);

CREATE TABLE IF NOT EXISTS warehouse.dim_sector (
    sector_sk bigserial PRIMARY KEY,
    sector_name text NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS warehouse.dim_exchange (
    exchange_sk bigserial PRIMARY KEY,
    exchange_code text NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS warehouse.dim_stock (
    stock_sk bigserial PRIMARY KEY,
    symbol text NOT NULL,
    company_name text NOT NULL,
    sector_name text NOT NULL,
    exchange_code text NOT NULL,
    valid_from date NOT NULL DEFAULT CURRENT_DATE,
    valid_to date,
    is_current boolean NOT NULL DEFAULT true,
    UNIQUE (symbol, valid_from)
);

CREATE TABLE IF NOT EXISTS warehouse.dim_date (
    date_sk integer PRIMARY KEY,
    calendar_date date NOT NULL UNIQUE,
    year integer NOT NULL,
    quarter integer NOT NULL,
    month integer NOT NULL,
    month_name text NOT NULL,
    day integer NOT NULL,
    day_of_week integer NOT NULL,
    is_weekend boolean NOT NULL
);

CREATE TABLE IF NOT EXISTS warehouse.dim_time (
    time_sk integer PRIMARY KEY,
    minute_of_day integer NOT NULL UNIQUE,
    hour integer NOT NULL,
    minute integer NOT NULL,
    label text NOT NULL
);

CREATE TABLE IF NOT EXISTS warehouse.fact_anomaly_minute (
    stock_sk bigint NOT NULL REFERENCES warehouse.dim_stock(stock_sk),
    date_sk integer NOT NULL REFERENCES warehouse.dim_date(date_sk),
    time_sk integer NOT NULL REFERENCES warehouse.dim_time(time_sk),
    sector_sk bigint NOT NULL REFERENCES warehouse.dim_sector(sector_sk),
    exchange_sk bigint NOT NULL REFERENCES warehouse.dim_exchange(exchange_sk),
    composite_score double precision NOT NULL,
    price_z_score double precision,
    volume_z_score double precision,
    rolling_volatility double precision,
    contagion_flag boolean NOT NULL DEFAULT false,
    dedupe_key text NOT NULL,
    source_run_id text NOT NULL,
    PRIMARY KEY (stock_sk, date_sk, time_sk)
);

CREATE TABLE IF NOT EXISTS warehouse.fact_market_day (
    stock_sk bigint NOT NULL REFERENCES warehouse.dim_stock(stock_sk),
    date_sk integer NOT NULL REFERENCES warehouse.dim_date(date_sk),
    sector_sk bigint NOT NULL REFERENCES warehouse.dim_sector(sector_sk),
    exchange_sk bigint NOT NULL REFERENCES warehouse.dim_exchange(exchange_sk),
    anomaly_count integer NOT NULL DEFAULT 0,
    max_composite_score double precision NOT NULL DEFAULT 0,
    avg_composite_score double precision NOT NULL DEFAULT 0,
    avg_volume_z_score double precision NOT NULL DEFAULT 0,
    contagion_event_count integer NOT NULL DEFAULT 0,
    PRIMARY KEY (stock_sk, date_sk)
);

CREATE TABLE IF NOT EXISTS warehouse.fact_contagion_event (
    event_id text PRIMARY KEY,
    stock_sk bigint NOT NULL REFERENCES warehouse.dim_stock(stock_sk),
    date_sk integer NOT NULL REFERENCES warehouse.dim_date(date_sk),
    sector_sk bigint NOT NULL REFERENCES warehouse.dim_sector(sector_sk),
    event_timestamp timestamptz NOT NULL,
    affected_count integer NOT NULL,
    peer_average_score double precision NOT NULL,
    risk_score double precision NOT NULL,
    rationale text NOT NULL
);

CREATE TABLE IF NOT EXISTS warehouse.fact_surveillance_coverage (
    stock_sk bigint NOT NULL REFERENCES warehouse.dim_stock(stock_sk),
    date_sk integer NOT NULL REFERENCES warehouse.dim_date(date_sk),
    time_sk integer NOT NULL REFERENCES warehouse.dim_time(time_sk),
    coverage_state text NOT NULL,
    source_run_id text NOT NULL,
    PRIMARY KEY (stock_sk, date_sk, time_sk)
);

CREATE MATERIALIZED VIEW IF NOT EXISTS warehouse.mv_sector_daily_summary AS
SELECT
    d.calendar_date,
    s.sector_name,
    COUNT(*) AS active_minutes,
    AVG(f.composite_score) AS avg_composite_score,
    MAX(f.composite_score) AS max_composite_score,
    SUM(CASE WHEN f.contagion_flag THEN 1 ELSE 0 END) AS contagion_minutes
FROM warehouse.fact_anomaly_minute f
JOIN warehouse.dim_date d ON d.date_sk = f.date_sk
JOIN warehouse.dim_sector s ON s.sector_sk = f.sector_sk
GROUP BY d.calendar_date, s.sector_name;

CREATE MATERIALIZED VIEW IF NOT EXISTS warehouse.mv_sector_monthly_summary AS
WITH market_summary AS (
    SELECT
        d.year,
        d.quarter,
        d.month,
        s.sector_name,
        AVG(f.avg_composite_score) AS avg_daily_composite_score,
        MAX(f.max_composite_score) AS max_daily_composite_score
    FROM warehouse.fact_market_day f
    JOIN warehouse.dim_date d ON d.date_sk = f.date_sk
    JOIN warehouse.dim_sector s ON s.sector_sk = f.sector_sk
    GROUP BY d.year, d.quarter, d.month, s.sector_name
),
contagion_summary AS (
    SELECT
        d.year,
        d.quarter,
        d.month,
        s.sector_name,
        COUNT(*) AS contagion_event_count
    FROM warehouse.fact_contagion_event f
    JOIN warehouse.dim_date d ON d.date_sk = f.date_sk
    JOIN warehouse.dim_sector s ON s.sector_sk = f.sector_sk
    GROUP BY d.year, d.quarter, d.month, s.sector_name
)
SELECT
    market_summary.year,
    market_summary.quarter,
    market_summary.month,
    market_summary.sector_name,
    market_summary.avg_daily_composite_score,
    market_summary.max_daily_composite_score,
    COALESCE(contagion_summary.contagion_event_count, 0) AS contagion_event_count
FROM market_summary
LEFT JOIN contagion_summary
  ON contagion_summary.year = market_summary.year
 AND contagion_summary.quarter = market_summary.quarter
 AND contagion_summary.month = market_summary.month
 AND contagion_summary.sector_name = market_summary.sector_name;
