export const DEFAULT_STOCK_HISTORY_DAYS = 45;

const SERVER_API_BASE_URL =
  process.env.API_BASE_URL ??
  process.env.NEXT_PUBLIC_API_BASE_URL ??
  "http://localhost:8000";

const BROWSER_API_BASE_URL = process.env.NEXT_PUBLIC_API_BASE_URL ?? "";

export function apiBaseUrl() {
  return typeof window === "undefined" ? SERVER_API_BASE_URL : BROWSER_API_BASE_URL;
}

export function apiUrl(path: string) {
  return `${apiBaseUrl()}${path}`;
}

async function getJson<T>(path: string): Promise<T | null> {
  try {
    const response = await fetch(apiUrl(path), { cache: "no-store" });
    if (!response.ok) {
      return null;
    }
    return (await response.json()) as T;
  } catch {
    return null;
  }
}

async function postJson<T>(path: string, body: unknown): Promise<T | null> {
  try {
    const response = await fetch(apiUrl(path), {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(body),
      cache: "no-store",
    });
    if (!response.ok) {
      return null;
    }
    return (await response.json()) as T;
  } catch {
    return null;
  }
}

export type StockReference = {
  symbol: string;
  exchange?: string | null;
  sector?: string | null;
  company_name: string;
  country?: string | null;
  watchlist?: boolean;
  has_history?: boolean;
  daily_bar_count?: number;
  last_daily_date?: string | null;
  aliases?: string[];
  source?: string;
  metadata?: Record<string, unknown>;
};

export type SectorOption = {
  sector: string;
  count: number;
  known: boolean;
};

export type ReferenceStocksResponse = {
  stocks: StockReference[];
  total_count: number;
  filtered_count: number;
  symbol_count: number;
  sector_count: number;
  sector_options: SectorOption[];
  known_sector_count: number;
  unknown_sector_count: number;
  watchlist_count: number;
  hydrated_count: number;
};

export type LatestMarket = {
  symbol: string;
  company_name?: string | null;
  sector: string;
  exchange: string;
  trading_date?: string;
  timestamp_utc?: string;
  timestamp_ist: string;
  close: number;
  volume: number;
  composite_score?: number;
  is_anomalous?: boolean;
};

export type LatestAnomaly = {
  symbol: string;
  exchange: string;
  sector: string;
  interval: string;
  timestamp_utc: string;
  timestamp_ist: string;
  trading_date: string;
  close: number;
  volume: number;
  price_z_score: number;
  volume_z_score: number;
  composite_score: number;
  is_anomalous: boolean;
  explainability: string;
};

export type AlertItem = {
  event_id: string;
  symbol: string;
  trading_date: string;
  event_category: string;
  severity: string;
  status: string;
  title: string;
  message: string;
  detected_at: string;
  composite_score?: number | null;
  price_z_score?: number | null;
  volume_z_score?: number | null;
  event_payload?: Record<string, unknown>;
  acknowledged_at?: string | null;
  is_stale?: boolean;
};

export type OverviewResponse = {
  as_of?: string | null;
  market_mode?: string | null;
  live_market: LatestMarket[];
  top_anomalies: LatestAnomaly[];
  top_movers?: ScreenerItem[];
  sector_heatmap: Array<{
    sector: string;
    avg_composite_score: number;
    active_anomalies: number;
  }>;
  recent_contagion_events: ContagionItem[];
  recent_alerts: AlertItem[];
  open_alert_count: number;
  total_open_alert_count?: number;
  stale_open_alert_count?: number;
  current_alert_trading_date?: string | null;
  latest_stale_alert_date?: string | null;
  tracked_symbol_count: number;
  tracked_sector_count: number;
  hydrated_symbol_count?: number;
  watchlist_symbol_count?: number;
  live_symbol_count: number;
  live_sector_count: number;
};

export type ScreenerIndicators = {
  last_close?: number | null;
  day_change_pct?: number | null;
  return_20d_pct?: number | null;
  sma_20?: number | null;
  ema_12?: number | null;
  ema_26?: number | null;
  rsi_14?: number | null;
  atr_14?: number | null;
  volatility_20d_pct?: number | null;
  volume_ratio_20d?: number | null;
  distance_from_20d_high_pct?: number | null;
  distance_from_20d_low_pct?: number | null;
};

export type ScreenerItem = {
  symbol: string;
  company_name: string;
  exchange?: string | null;
  sector?: string | null;
  daily_points: number;
  indicators: ScreenerIndicators;
  latest_market?: LatestMarket | null;
  latest_anomaly?: LatestAnomaly | null;
  latest_alert?: AlertItem | null;
};

export type ScreenerResponse = {
  items: ScreenerItem[];
  count: number;
};

export type StockWorkspaceResponse = {
  symbol: string;
  resolved_symbol: string;
  reference: StockReference;
  history: Array<{
    symbol: string;
    trading_date: string;
    open: number;
    high: number;
    low: number;
    close: number;
    adj_close: number;
    volume: number;
    dividends: number;
    stock_splits: number;
  }>;
  history_summary: {
    first_trading_date?: string | null;
    last_trading_date?: string | null;
    session_count: number;
    period_high?: number | null;
    period_low?: number | null;
    avg_volume_20d?: number | null;
    range_position_pct?: number | null;
    return_5d_pct?: number | null;
    return_20d_pct?: number | null;
    return_45d_pct?: number | null;
  };
  indicators: ScreenerIndicators;
  latest_market?: LatestMarket | null;
  latest_anomaly?: LatestAnomaly | null;
  anomaly_summary: {
    point_count: number;
    flagged_count: number;
    peak_composite_score?: number | null;
    latest_flagged_at?: string | null;
  };
  ticks: Array<{
    timestamp_utc: string;
    timestamp_ist: string;
    open: number;
    high: number;
    low: number;
    close: number;
    volume: number;
    dividends: number;
    stock_splits: number;
  }>;
  anomalies: Array<{
    timestamp_utc: string;
    timestamp_ist: string;
    composite_score: number;
    price_z_score: number;
    volume_z_score: number;
    is_anomalous: boolean;
    explainability: string;
  }>;
  alerts: AlertItem[];
  alert_summary: {
    open_count: number;
    acknowledged_count: number;
    latest_severity?: string | null;
    severity_breakdown: Record<string, number>;
  };
  peer_comparison: Array<{
    symbol: string;
    company_name?: string | null;
    exchange?: string | null;
    sector?: string | null;
    last_close?: number | null;
    return_20d_pct?: number | null;
    rsi_14?: number | null;
    volume_ratio_20d?: number | null;
    latest_alert_severity?: string | null;
    latest_anomaly_score?: number | null;
    is_anomalous?: boolean;
    latest_market_close?: number | null;
  }>;
  related_contagion: ContagionItem[];
};

export type ContagionItem = {
  event_id: string;
  trigger_symbol: string;
  trigger_sector: string;
  affected_symbols?: string[];
  affected_count: number;
  risk_score: number;
  peer_average_score?: number;
  event_timestamp: string;
  rationale?: string;
};

export type WarehouseDailyRollup = {
  calendar_date: string;
  sector_name: string;
  active_minutes?: number;
  avg_composite_score: number;
  max_composite_score: number;
  contagion_minutes: number;
};

export type WarehouseSummary = {
  market_day_rows: number;
  stocks_covered: number;
  sectors_covered: number;
  trading_days_loaded: number;
  total_anomalies: number;
  total_contagion_events: number;
  peak_daily_composite_score: number;
  first_calendar_date?: string | null;
  last_calendar_date?: string | null;
  anomaly_minute_rows: number;
  contagion_event_rows: number;
  coverage_rows: number;
  sector_momentum_rows?: number;
  stock_persistence_rows?: number;
  intraday_profile_rows?: number;
};

export type WarehouseMonthlyRollup = {
  year: number;
  quarter: number;
  month: number;
  sector_name: string;
  avg_daily_composite_score: number;
  max_daily_composite_score: number;
  contagion_event_count: number;
};

export type WarehouseSectorRegime = {
  sector_name: string;
  sessions_covered: number;
  symbols_covered: number;
  anomaly_minutes: number;
  total_anomalies: number;
  contagion_minutes: number;
  contagion_event_count: number;
  avg_daily_composite_score: number;
  peak_daily_composite_score: number;
  latest_calendar_date: string;
};

export type WarehouseStockOutlier = {
  calendar_date: string;
  symbol: string;
  company_name: string;
  sector_name: string;
  anomaly_count: number;
  max_composite_score: number;
  avg_composite_score: number;
  avg_volume_z_score: number;
  contagion_event_count: number;
};

export type WarehouseStockLeader = {
  symbol: string;
  company_name: string;
  sector_name: string;
  sessions_covered: number;
  anomaly_days: number;
  total_anomalies: number;
  avg_daily_composite_score: number;
  peak_daily_composite_score: number;
  contagion_event_count: number;
  latest_calendar_date: string;
  latest_anomaly_count: number;
  latest_peak_score: number;
};

export type WarehouseSectorMomentum = {
  sector_name: string;
  recent_sessions: number;
  prior_sessions: number;
  recent_total_anomalies: number;
  prior_total_anomalies: number;
  recent_avg_daily_composite_score: number;
  prior_avg_daily_composite_score: number;
  recent_peak_daily_composite_score: number;
  prior_peak_daily_composite_score: number;
  recent_contagion_event_count: number;
  prior_contagion_event_count: number;
  anomaly_delta: number;
  score_delta: number;
  contagion_delta: number;
};

export type WarehouseStockPersistence = {
  symbol: string;
  company_name: string;
  sector_name: string;
  sessions_covered: number;
  anomaly_days: number;
  total_anomalies: number;
  avg_daily_composite_score: number;
  peak_daily_composite_score: number;
  contagion_event_count: number;
  last_anomaly_date?: string | null;
  recent_5_session_anomalies: number;
  recent_5_session_anomaly_days: number;
  anomaly_day_ratio: number;
  avg_anomalies_per_active_day: number;
  days_since_last_anomaly?: number | null;
};

export type WarehouseIntradayProfilePoint = {
  time_sk: number;
  time_label: string;
  hour: number;
  minute: number;
  anomaly_minutes: number;
  distinct_stocks: number;
  sessions_covered: number;
  avg_composite_score: number;
  peak_composite_score: number;
  contagion_minutes: number;
};

export type WarehouseQueryField = {
  key: string;
  label: string;
  description: string;
  kind: "string" | "integer" | "number" | "date" | "time" | "datetime";
  default_selected?: boolean;
};

export type WarehouseQueryDataset = {
  key: string;
  label: string;
  description: string;
  grain: string;
  row_count: number;
  supports: {
    date: boolean;
    sector: boolean;
    exchange: boolean;
    symbol_search: boolean;
    min_signal: boolean;
  };
  dimensions: WarehouseQueryField[];
  measures: WarehouseQueryField[];
  defaults: {
    dimensions: string[];
    measures: string[];
    sort_field: string;
    sort_direction: "asc" | "desc";
    limit: number;
    date_from?: string | null;
    date_to?: string | null;
    suggested_window_days?: number | null;
  };
};

export type WarehouseQueryPreset = {
  id: string;
  label: string;
  description: string;
  request: WarehouseQueryRequest;
};

export type WarehouseQueryMetadataResponse = {
  generated_at: string;
  date_window: {
    first_calendar_date?: string | null;
    last_calendar_date?: string | null;
  };
  sectors: string[];
  exchanges: string[];
  datasets: WarehouseQueryDataset[];
  presets: WarehouseQueryPreset[];
};

export type WarehouseQueryRequest = {
  dataset: string;
  dimensions?: string[];
  measures?: string[];
  date_from?: string | null;
  date_to?: string | null;
  sector?: string | null;
  exchange?: string | null;
  symbol_search?: string | null;
  min_signal?: number | null;
  sort_field?: string | null;
  sort_direction?: "asc" | "desc";
  limit?: number;
};

export type WarehouseQueryResponse = {
  dataset: {
    key: string;
    label: string;
    description: string;
    grain: string;
    available_rows: number;
  };
  query: {
    dimensions: string[];
    measures: string[];
    date_from?: string | null;
    date_to?: string | null;
    sector?: string | null;
    exchange?: string | null;
    symbol_search?: string | null;
    min_signal?: number | null;
    sort_field: string;
    sort_direction: "asc" | "desc";
    limit: number;
    preview: string;
  };
  columns: Array<{
    key: string;
    label: string;
    kind: "string" | "integer" | "number" | "date" | "time" | "datetime";
    role: "dimension" | "measure";
    description: string;
  }>;
  rows: Array<Record<string, string | number | null>>;
  row_count: number;
  query_time_ms: number;
  chart?: {
    kind: "line" | "bar";
    label_key: string;
    value_key: string;
    title: string;
  } | null;
  report: {
    headline: string;
    subheadline: string;
    highlights: Array<{
      label: string;
      value: string;
    }>;
    findings: string[];
  };
  generated_at: string;
};

export type SystemHealthResponse = {
  api?: string;
  redis?: boolean;
  last_tick?: string | null;
  latest_etl_run?: EtlHealthRun | null;
  latest_etl_attempt?: EtlHealthRun | null;
  latest_successful_etl_run?: EtlHealthRun | null;
  latest_failed_etl_run?: EtlHealthRun | null;
  latest_ingestion_run?: {
    run_id: string;
    mode: string;
    finished_at?: string | null;
    status: string;
  } | null;
  database_inventory?: {
    stock_profiles: number;
    daily_bars: number;
    alert_events: number;
  };
  universe_inventory?: {
    listed_symbols: number;
    watchlist_symbols: number;
    hydrated_symbols: number;
    known_sector_symbols: number;
    unknown_sector_symbols?: number;
    sector_coverage_pct?: number;
  };
  notifications?: {
    webhook_enabled: boolean;
    webhook_type: string;
    min_severity: string;
  };
};

export type EtlHealthRun = {
  run_id: string;
  trading_date: string;
  started_at?: string | null;
  finished_at?: string | null;
  status: string;
  inserted_rows?: number | null;
  aggregate_rows?: number | null;
  notes?: Record<string, unknown> | null;
};

export type SystemScaleResponse = {
  actual: {
    operational: Record<string, number>;
    warehouse: Record<string, number>;
    streaming: Record<string, number | null>;
    operational_total_rows: number;
    warehouse_total_rows: number;
    streaming_total_rows: number;
    materialized_total_rows: number;
  };
  coverage: {
    listed_symbols: number;
    watchlist_symbols: number;
    hydrated_symbols: number;
    first_daily_date?: string | null;
    last_daily_date?: string | null;
    daily_trading_days_loaded: number;
    intraday_symbols_loaded: number;
    intraday_trading_days_loaded: number;
    first_intraday_date?: string | null;
    last_intraday_date?: string | null;
    trading_days_loaded: number;
  };
  projection: {
    session_minutes: number;
    trading_days_per_year: number;
    listed_symbols: number;
    intraday_symbols_loaded: number;
    intraday_trading_days_loaded: number;
    minute_rows_per_trading_day: number;
    minute_rows_for_loaded_window: number;
    minute_rows_per_year: number;
    tick_and_anomaly_rows_for_loaded_window: number;
    tick_and_anomaly_rows_per_year: number;
    five_year_tick_and_anomaly_rows: number;
    crosses_crore_in_loaded_window: boolean;
    crosses_crore_annually: boolean;
    actual_capture_vs_loaded_window_pct: number;
    current_scope_share_of_listed_universe_pct: number;
  };
};

export type SystemRun = Record<string, unknown>;

export type SystemRunsResponse = {
  ingestion_runs: SystemRun[];
  etl_runs: SystemRun[];
};

export type ReplayStatusResponse = {
  run_id?: string;
  mode?: string;
  status?: string;
  started_at?: string | null;
  finished_at?: string | null;
  symbol_count?: number;
  records_seen?: number;
  records_published?: number;
  notes?: {
    fixture?: string;
    speed?: number;
    trading_date?: string;
  };
};

export type MethodologyResponse = {
  market: {
    timezone: string;
    session_open: string;
    session_close: string;
    session_minutes: number;
    scope: string;
  };
  anomaly: {
    warmup_minutes: number;
    ewma_alpha: number;
    price_z_threshold: number;
    volume_z_threshold: number;
    composite_threshold: number;
    composite_weights: {
      price_z: number;
      volume_z: number;
    };
    threshold_rationale: string;
    formulas: Array<{
      name: string;
      formula: string;
      meaning: string;
    }>;
    flag_rule: string;
    severity_bands: Array<{
      severity: string;
      rule: string;
    }>;
  };
  alerts: {
    cooldown_minutes: number;
    notification_min_severity: string;
    logic: string;
  };
  contagion: {
    window_minutes: number;
    trigger_rule: string;
    peer_rule: string;
    risk_score_formula: string;
    why: string;
  };
  warehouse: {
    facts: string[];
    why: string;
  };
};

export type AlertsLiveResponse = {
  items: AlertItem[];
  open_count: number;
  active_open_count?: number;
  stale_open_count?: number;
  display_scope?: string;
  current_trading_date?: string | null;
  scope_reference_date?: string | null;
};

export async function fetchOverview() {
  return getJson<OverviewResponse>("/api/overview");
}

export async function fetchReferenceStocks(params?: {
  q?: string;
  limit?: number;
  offset?: number;
  watchlistOnly?: boolean;
  historyState?: "all" | "hydrated" | "unhydrated";
  sectorState?: "all" | "known" | "unknown";
  sector?: string;
}) {
  const search = new URLSearchParams();
  if (params?.q) {
    search.set("q", params.q);
  }
  if (params?.limit !== undefined) {
    search.set("limit", String(params.limit));
  }
  if (params?.offset !== undefined) {
    search.set("offset", String(params.offset));
  }
  if (params?.watchlistOnly) {
    search.set("watchlist_only", "true");
  }
  if (params?.historyState && params.historyState !== "all") {
    search.set("history_state", params.historyState);
  }
  if (params?.sectorState && params.sectorState !== "all") {
    search.set("sector_state", params.sectorState);
  }
  if (params?.sector) {
    search.set("sector", params.sector);
  }
  const suffix = search.toString() ? `?${search.toString()}` : "";
  return getJson<ReferenceStocksResponse>(`/api/reference/stocks${suffix}`);
}

export async function fetchScreener(days = DEFAULT_STOCK_HISTORY_DAYS, limit = 100) {
  return getJson<ScreenerResponse>(`/api/stocks/screener?days=${days}&limit=${limit}`);
}

export async function fetchStockWorkspace(symbol: string, days = DEFAULT_STOCK_HISTORY_DAYS) {
  return getJson<StockWorkspaceResponse>(`/api/stocks/${encodeURIComponent(symbol)}/workspace?days=${days}`);
}

export async function fetchContagion() {
  return getJson<ContagionItem[]>("/api/contagion");
}

export async function fetchWarehouseRollups() {
  return getJson<WarehouseDailyRollup[]>("/api/warehouse/sector-rollups");
}

export async function fetchWarehouseSummary() {
  return getJson<WarehouseSummary>("/api/warehouse/summary");
}

export async function fetchWarehouseMonthly() {
  return getJson<WarehouseMonthlyRollup[]>("/api/warehouse/monthly-rollups");
}

export async function fetchWarehouseSectorRegimes(limit = 20) {
  return getJson<WarehouseSectorRegime[]>(`/api/warehouse/sector-regimes?limit=${limit}`);
}

export async function fetchWarehouseStockOutliers() {
  return getJson<WarehouseStockOutlier[]>("/api/warehouse/stock-outliers");
}

export async function fetchWarehouseStockLeaders(limit = 50) {
  return getJson<WarehouseStockLeader[]>(`/api/warehouse/stock-leaders?limit=${limit}`);
}

export async function fetchWarehouseSectorMomentum(limit = 25) {
  return getJson<WarehouseSectorMomentum[]>(`/api/warehouse/sector-momentum?limit=${limit}`);
}

export async function fetchWarehouseStockPersistence(limit = 50) {
  return getJson<WarehouseStockPersistence[]>(`/api/warehouse/stock-persistence?limit=${limit}`);
}

export async function fetchWarehouseIntradayProfile(limit = 375) {
  return getJson<WarehouseIntradayProfilePoint[]>(`/api/warehouse/intraday-profile?limit=${limit}`);
}

export async function fetchWarehouseQueryMetadata() {
  return getJson<WarehouseQueryMetadataResponse>("/api/warehouse/query-metadata");
}

export async function fetchWarehouseQuery(request: WarehouseQueryRequest) {
  return postJson<WarehouseQueryResponse>("/api/warehouse/query", request);
}

export async function fetchSystemHealth() {
  return getJson<SystemHealthResponse>("/api/system/health");
}

export async function fetchSystemScale() {
  return getJson<SystemScaleResponse>("/api/system/scale");
}

export async function fetchSystemRuns() {
  return getJson<SystemRunsResponse>("/api/system/runs");
}

export async function fetchReplayStatus() {
  return getJson<ReplayStatusResponse>("/api/replay/status");
}

export async function fetchMethodology() {
  return getJson<MethodologyResponse>("/api/methodology");
}
