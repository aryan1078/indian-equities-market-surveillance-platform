CREATE TABLE IF NOT EXISTS operational.stock_profiles (
    symbol text PRIMARY KEY,
    company_name text NOT NULL,
    exchange text,
    sector text,
    country text NOT NULL DEFAULT 'India',
    aliases jsonb NOT NULL DEFAULT '[]'::jsonb,
    source text NOT NULL DEFAULT 'metadata',
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    last_refreshed_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS operational.stock_daily_bars (
    symbol text NOT NULL,
    trading_date date NOT NULL,
    open double precision NOT NULL,
    high double precision NOT NULL,
    low double precision NOT NULL,
    close double precision NOT NULL,
    adj_close double precision NOT NULL,
    volume bigint NOT NULL,
    dividends double precision NOT NULL DEFAULT 0,
    stock_splits double precision NOT NULL DEFAULT 0,
    source text NOT NULL DEFAULT 'yfinance',
    refreshed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol, trading_date)
);

CREATE INDEX IF NOT EXISTS idx_stock_daily_bars_trade_date
    ON operational.stock_daily_bars (trading_date DESC);

CREATE INDEX IF NOT EXISTS idx_stock_daily_bars_symbol_date
    ON operational.stock_daily_bars (symbol, trading_date DESC);

CREATE TABLE IF NOT EXISTS operational.alert_events (
    event_id text PRIMARY KEY,
    symbol text NOT NULL,
    trading_date date NOT NULL,
    event_category text NOT NULL,
    severity text NOT NULL,
    status text NOT NULL DEFAULT 'open',
    title text NOT NULL,
    message text NOT NULL,
    detected_at timestamptz NOT NULL,
    source_run_id text,
    composite_score double precision,
    price_z_score double precision,
    volume_z_score double precision,
    event_payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    notified_channels jsonb NOT NULL DEFAULT '[]'::jsonb,
    acknowledged_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_alert_events_detected_at
    ON operational.alert_events (detected_at DESC);

CREATE INDEX IF NOT EXISTS idx_alert_events_symbol_detected_at
    ON operational.alert_events (symbol, detected_at DESC);

CREATE INDEX IF NOT EXISTS idx_alert_events_status_detected_at
    ON operational.alert_events (status, detected_at DESC);

CREATE MATERIALIZED VIEW IF NOT EXISTS warehouse.mv_sector_regime_summary AS
WITH daily_summary AS (
    SELECT
        sec.sector_name,
        COUNT(DISTINCT f.date_sk) AS sessions_covered,
        COUNT(DISTINCT ds.symbol) AS symbols_covered,
        SUM(f.anomaly_count) AS total_anomalies,
        AVG(f.avg_composite_score) AS avg_daily_composite_score,
        MAX(f.max_composite_score) AS peak_daily_composite_score,
        SUM(f.contagion_event_count) AS contagion_event_count,
        MAX(d.calendar_date) AS latest_calendar_date
    FROM warehouse.fact_market_day f
    JOIN warehouse.dim_stock ds ON ds.stock_sk = f.stock_sk
    JOIN warehouse.dim_sector sec ON sec.sector_sk = f.sector_sk
    JOIN warehouse.dim_date d ON d.date_sk = f.date_sk
    GROUP BY sec.sector_name
),
minute_summary AS (
    SELECT
        sec.sector_name,
        COUNT(*) AS anomaly_minutes,
        SUM(CASE WHEN f.contagion_flag THEN 1 ELSE 0 END) AS contagion_minutes
    FROM warehouse.fact_anomaly_minute f
    JOIN warehouse.dim_sector sec ON sec.sector_sk = f.sector_sk
    GROUP BY sec.sector_name
)
SELECT
    daily_summary.sector_name,
    daily_summary.sessions_covered,
    daily_summary.symbols_covered,
    COALESCE(minute_summary.anomaly_minutes, 0) AS anomaly_minutes,
    daily_summary.total_anomalies,
    COALESCE(minute_summary.contagion_minutes, 0) AS contagion_minutes,
    daily_summary.contagion_event_count,
    daily_summary.avg_daily_composite_score,
    daily_summary.peak_daily_composite_score,
    daily_summary.latest_calendar_date
FROM daily_summary
LEFT JOIN minute_summary
  ON minute_summary.sector_name = daily_summary.sector_name;

CREATE MATERIALIZED VIEW IF NOT EXISTS warehouse.mv_stock_signal_leaders AS
WITH daily_facts AS (
    SELECT
        ds.symbol,
        d.calendar_date,
        f.anomaly_count,
        f.avg_composite_score,
        f.max_composite_score,
        f.contagion_event_count
    FROM warehouse.fact_market_day f
    JOIN warehouse.dim_stock ds ON ds.stock_sk = f.stock_sk
    JOIN warehouse.dim_date d ON d.date_sk = f.date_sk
),
current_stock AS (
    SELECT DISTINCT ON (symbol)
        symbol,
        company_name,
        sector_name
    FROM warehouse.dim_stock
    WHERE is_current = true
    ORDER BY symbol, valid_from DESC
),
latest_snapshot AS (
    SELECT DISTINCT ON (symbol)
        symbol,
        calendar_date AS latest_calendar_date,
        anomaly_count AS latest_anomaly_count,
        max_composite_score AS latest_peak_score
    FROM daily_facts
    ORDER BY symbol, calendar_date DESC
)
SELECT
    latest_snapshot.symbol,
    cs.company_name,
    cs.sector_name,
    COUNT(*) AS sessions_covered,
    COUNT(*) FILTER (WHERE f.anomaly_count > 0) AS anomaly_days,
    SUM(f.anomaly_count) AS total_anomalies,
    AVG(f.avg_composite_score) AS avg_daily_composite_score,
    MAX(f.max_composite_score) AS peak_daily_composite_score,
    SUM(f.contagion_event_count) AS contagion_event_count,
    latest_snapshot.latest_calendar_date,
    latest_snapshot.latest_anomaly_count,
    latest_snapshot.latest_peak_score
FROM daily_facts f
JOIN latest_snapshot ON latest_snapshot.symbol = f.symbol
JOIN current_stock cs ON cs.symbol = latest_snapshot.symbol
GROUP BY
    latest_snapshot.symbol,
    cs.company_name,
    cs.sector_name,
    latest_snapshot.latest_calendar_date,
    latest_snapshot.latest_anomaly_count,
    latest_snapshot.latest_peak_score;
