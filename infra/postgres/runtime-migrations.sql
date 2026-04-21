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
