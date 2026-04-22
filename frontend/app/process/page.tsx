import Link from "next/link";

import { ExplainerCards } from "../../components/explainer-cards";
import { IntensityBars } from "../../components/intensity-bars";
import { LineChart } from "../../components/line-chart";
import { StatCard } from "../../components/stat-card";
import {
  fetchMethodology,
  fetchReplayStatus,
  fetchStockWorkspace,
  fetchSystemHealth,
  fetchSystemRuns,
  fetchSystemScale,
  fetchWarehouseIntradayProfile,
  fetchWarehouseSummary,
  type SystemRun,
} from "../../lib/api";
import {
  compactPath,
  formatCompactIndian,
  formatDate,
  formatDateTime,
  formatPercent,
  formatNumber,
  shortId,
} from "../../lib/format";

type UnknownRecord = Record<string, unknown>;

function asRecord(value: unknown): UnknownRecord | null {
  return typeof value === "object" && value !== null ? (value as UnknownRecord) : null;
}

function readString(record: UnknownRecord | null, key: string) {
  const value = record?.[key];
  if (typeof value === "string") {
    return value;
  }
  if (typeof value === "number") {
    return String(value);
  }
  return null;
}

function readNumber(record: UnknownRecord | null, key: string) {
  const value = record?.[key];
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string" && value.trim()) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

function durationLabel(start: string | null | undefined, end: string | null | undefined) {
  if (!start || !end) {
    return "N/A";
  }
  const startMs = new Date(start).getTime();
  const endMs = new Date(end).getTime();
  if (!Number.isFinite(startMs) || !Number.isFinite(endMs) || endMs < startMs) {
    return "N/A";
  }
  const totalMinutes = Math.round((endMs - startMs) / 60_000);
  const hours = Math.floor(totalMinutes / 60);
  const minutes = totalMinutes % 60;
  if (hours <= 0) {
    return `${minutes}m`;
  }
  return `${hours}h ${minutes}m`;
}

function jsonPreview(value: unknown) {
  return JSON.stringify(value, null, 2);
}

function runField(run: SystemRun | undefined, key: string) {
  return asRecord(run)?.[key];
}

export default async function ProcessPage() {
  const exampleSymbol = "AXISBANK.NS";
  const [health, scale, summary, replay, runs, methodology, intradayProfile, exampleWorkspace] = await Promise.all([
    fetchSystemHealth(),
    fetchSystemScale(),
    fetchWarehouseSummary(),
    fetchReplayStatus(),
    fetchSystemRuns(),
    fetchMethodology(),
    fetchWarehouseIntradayProfile(375),
    fetchStockWorkspace(exampleSymbol, 20).catch(() => null),
  ]);

  const ingestionRuns = Array.isArray(runs?.ingestion_runs) ? runs.ingestion_runs : [];
  const etlRuns = Array.isArray(runs?.etl_runs) ? runs.etl_runs : [];
  const latestBackfillRun = ingestionRuns.find(
    (run) => readString(asRecord(run), "mode") === "minute_backfill" && readString(asRecord(run), "status") === "completed",
  );
  const latestReplayRun =
    ingestionRuns.find(
      (run) => readString(asRecord(run), "mode") === "replay" && readString(asRecord(run), "status") === "completed",
    ) ?? undefined;
  const latestSuccessfulEtlRun = etlRuns.find((run) => readString(asRecord(run), "status") === "completed");
  const latestEtlAttempt = health?.latest_etl_attempt;

  const backfillNotes = asRecord(runField(latestBackfillRun, "notes"));
  const replayNotes = asRecord(runField(latestReplayRun, "notes")) ?? asRecord(replay?.notes);

  const listedSymbols = health?.universe_inventory?.listed_symbols ?? scale?.coverage.listed_symbols ?? 0;
  const hydratedSymbols = health?.universe_inventory?.hydrated_symbols ?? scale?.coverage.hydrated_symbols ?? 0;
  const sectorCoveragePct = health?.universe_inventory?.sector_coverage_pct ?? 0;
  const materializedRows = scale?.actual.materialized_total_rows ?? 0;
  const warehouseMinuteFacts = summary?.anomaly_minute_rows ?? 0;
  const loadedWindowProjection = scale?.projection.tick_and_anomaly_rows_for_loaded_window ?? 0;
  const annualProjection = scale?.projection.tick_and_anomaly_rows_per_year ?? 0;
  const fiveYearProjection = scale?.projection.five_year_tick_and_anomaly_rows ?? 0;
  const backfillTickRows = readNumber(backfillNotes, "tick_rows_written") ?? 0;
  const backfillAnomalyRows = readNumber(backfillNotes, "anomaly_rows_written") ?? 0;
  const backfillStateRows = readNumber(backfillNotes, "latest_state_rows_written") ?? 0;
  const backfillTradingDays = readNumber(backfillNotes, "trading_day_count") ?? 0;
  const backfillWindowStart = readString(backfillNotes, "window_start");
  const backfillWindowEnd = readString(backfillNotes, "window_end");
  const backfillProvider = readString(backfillNotes, "provider") ?? "unknown";
  const backfillPartitions = readNumber(backfillNotes, "partition_count") ?? 0;
  const latestReplayRows = readNumber(asRecord(latestReplayRun), "records_published") ?? replay?.records_published ?? 0;
  const latestReplaySymbols = readNumber(asRecord(latestReplayRun), "symbol_count") ?? replay?.symbol_count ?? 0;
  const latestReplaySpeed = readNumber(replayNotes, "speed");
  const latestReplayFixture = readString(replayNotes, "fixture");
  const latestReplayTradingDate = readString(replayNotes, "trading_date");
  const latestCompletedEtlRows = readNumber(asRecord(latestSuccessfulEtlRun), "inserted_rows") ?? 0;
  const latestCompletedEtlDate = readString(asRecord(latestSuccessfulEtlRun), "trading_date");

  const intradayRows = intradayProfile ?? [];
  const intradayLabels = intradayRows.map((row) => row.time_label);
  const intradayValues = intradayRows.map((row) => row.avg_composite_score);
  const intradayPeak = intradayRows.reduce(
    (best, row) => (row.peak_composite_score > best.peak_composite_score ? row : best),
    intradayRows[0] ?? {
      time_sk: 0,
      time_label: "N/A",
      hour: 0,
      minute: 0,
      anomaly_minutes: 0,
      distinct_stocks: 0,
      sessions_covered: 0,
      avg_composite_score: 0,
      peak_composite_score: 0,
      contagion_minutes: 0,
    },
  );
  const averageDistinctStocks = intradayRows.length
    ? intradayRows.reduce((sum, row) => sum + row.distinct_stocks, 0) / intradayRows.length
    : 0;

  const latestEtlFailed = latestEtlAttempt?.status === "failed";

  const quickLinks = [
    {
      href: "/",
      title: "Live operations",
      eyebrow: "Observe",
      metric: health?.last_tick ? formatDateTime(health.last_tick) : "No tick",
      hint: "Jump to the operator console and current live tape.",
    },
    {
      href: "/methodology",
      title: "Signal formulas",
      eyebrow: "Understand",
      metric: methodology ? `${methodology.anomaly.warmup_minutes}m warmup` : "Methodology",
      hint: "See the anomaly math, thresholds, and contagion scoring rules.",
    },
    {
      href: "/warehouse",
      title: "Warehouse analytics",
      eyebrow: "Analyze",
      metric: `${summary?.stocks_covered ?? 0} stocks covered`,
      hint: "Inspect historical rollups, regimes, leaders, and persistence views.",
    },
    {
      href: "/system",
      title: "Infrastructure health",
      eyebrow: "Operate",
      metric: `${etlRuns.length} ETL runs`,
      hint: "Review run history, storage footprint, and projected scale.",
    },
  ];

  const volumeBars = [
    {
      label: "Latest full-NSE backfill ticks",
      value: backfillTickRows,
      detail: backfillWindowStart && backfillWindowEnd ? `${formatDate(backfillWindowStart)} to ${formatDate(backfillWindowEnd)}` : "Latest completed bulk ingestion",
      tone: "accent" as const,
    },
    {
      label: "Latest full-NSE anomaly rows",
      value: backfillAnomalyRows,
      detail: "Streaming anomaly engine output from the same run",
      tone: "warning" as const,
    },
    {
      label: "Warehouse minute facts",
      value: warehouseMinuteFacts,
      detail: `${summary?.trading_days_loaded ?? 0} warehouse trading days loaded`,
      tone: "critical" as const,
    },
    {
      label: "Loaded-window projection",
      value: loadedWindowProjection,
      detail: `${scale?.coverage.trading_days_loaded ?? 0} hydrated daily sessions at full-NSE minute grain`,
      tone: "warning" as const,
    },
    {
      label: "Annual tick + anomaly projection",
      value: annualProjection,
      detail: `${scale?.projection.trading_days_per_year ?? 250} trading sessions`,
      tone: "success" as const,
    },
  ];

  const storeBars = [
    {
      label: "Cassandra market_ticks",
      value: Number(scale?.actual.streaming.market_ticks ?? 0),
      detail: "Operational intraday bars for symbol/time-range reads",
      tone: "accent" as const,
    },
    {
      label: "Cassandra anomaly_metrics",
      value: Number(scale?.actual.streaming.anomaly_metrics ?? 0),
      detail: "Streaming anomaly scores and explainability fields",
      tone: "warning" as const,
    },
    {
      label: "PostgreSQL operational layer",
      value: scale?.actual.operational_total_rows ?? 0,
      detail: "Profiles, daily bars, alerts, contagion events, and run history",
      tone: "critical" as const,
    },
    {
      label: "PostgreSQL warehouse layer",
      value: scale?.actual.warehouse_total_rows ?? 0,
      detail: "Facts and materialized analytical views",
      tone: "success" as const,
    },
  ];

  const pipelineBands = [
    {
      label: "Capture and publish",
      meta: `${listedSymbols.toLocaleString("en-IN")} listed names | ${hydratedSymbols.toLocaleString("en-IN")} hydrated histories`,
      nodes: [
        {
          step: "1",
          title: "Market source",
          text: "Minute and daily bars arrive from the ingestion source as OHLCV rows, preserving dividends and stock-split fields so downstream loaders never silently discard corporate-action columns.",
          meta: methodology?.market.scope ?? "Indian equities minute market scope",
        },
        {
          step: "2",
          title: "Collector + normalization",
          text: "The collector resolves symbol form, exchange, UTC and IST timestamps, trading date, interval, and deterministic dedupe identity before any event is published.",
          meta: `Latest bulk provider ${backfillProvider}`,
        },
        {
          step: "3",
          title: "Kafka topics",
          text: "Canonical tick events are keyed by symbol and published into the event bus so storage, anomaly detection, replay, and recovery all consume the same ordered stream.",
          meta: `${formatCompactIndian(backfillTickRows, 2)} rows published in latest bulk run`,
        },
      ],
    },
    {
      label: "Operational analytics path",
      meta: `${formatCompactIndian(Number(scale?.actual.streaming.market_ticks ?? 0), 2)} ticks | ${formatCompactIndian(Number(scale?.actual.streaming.anomaly_metrics ?? 0), 2)} anomaly rows`,
      nodes: [
        {
          step: "4",
          title: "Storage consumer",
          text: "Kafka tick messages are schema-checked and written into Cassandra for append-heavy session reads, while latest-state tables are refreshed for low-latency operational APIs.",
          meta: `${formatCompactIndian(backfillStateRows, 2)} latest-state rows refreshed in latest bulk run`,
        },
        {
          step: "5",
          title: "Anomaly engine",
          text: "Redis-backed streaming state computes return percentage, EWMA mean and variance, price z, volume z, and the composite score after the warmup horizon.",
          meta: `${methodology?.anomaly.warmup_minutes ?? 5}m warmup | threshold ${formatNumber(methodology?.anomaly.composite_threshold, 2)}`,
        },
        {
          step: "6",
          title: "Alerts + contagion",
          text: "Persisted alert events and five-minute sector contagion windows turn raw anomaly points into operator-ready events with severity, rationale, and relational queryability.",
          meta: `${scale?.actual.operational.alert_events?.toLocaleString("en-IN") ?? "0"} alerts | ${scale?.actual.operational.contagion_events?.toLocaleString("en-IN") ?? "0"} contagion rows`,
        },
      ],
    },
    {
      label: "Historical analytics path",
      meta: `${summary?.trading_days_loaded ?? 0} warehouse days | ${formatCompactIndian(warehouseMinuteFacts, 2)} minute facts`,
      nodes: [
        {
          step: "7",
          title: "ETL staging and keying",
          text: "Operational minute and contagion records are extracted by trading date, staged in PostgreSQL, validated, keyed into dimensions, and loaded into fact tables by grain.",
          meta: latestCompletedEtlDate ? `${formatCompactIndian(latestCompletedEtlRows, 2)} rows inserted for ${formatDate(latestCompletedEtlDate)}` : "Latest successful ETL load available",
        },
        {
          step: "8",
          title: "Warehouse facts + views",
          text: "Minute facts, market-day facts, contagion facts, and coverage facts support sector-day, month, regime, persistence, intraday-pressure, and stock-leader views.",
          meta: `${Object.keys(scale?.actual.warehouse ?? {}).length} warehouse assets exposed`,
        },
        {
          step: "9",
          title: "API + operator console",
          text: "The API surfaces both operational and warehouse contracts to the UI so the same product can monitor the live tape and defend historical analytical claims in demos or viva.",
          meta: `${formatCompactIndian(materializedRows, 2)} materialized rows currently queryable`,
        },
      ],
    },
  ];

  const stepCards = [
    {
      step: "01",
      title: "Acquire raw market bars",
      body: "The ingestion side starts with minute and daily market bars for Indian equities. The system keeps the raw market structure intact, including open, high, low, close, volume, dividends, and stock splits, so the operational and warehouse layers can defend their lineage.",
      details: [
        { label: "Input", value: "Minute OHLCV + corporate actions" },
        { label: "Scope", value: methodology?.market.scope ?? "Indian equities" },
        { label: "Session", value: `${methodology?.market.session_open ?? "09:15"} to ${methodology?.market.session_close ?? "15:30"} IST` },
        { label: "Universe", value: `${listedSymbols.toLocaleString("en-IN")} listed symbols` },
      ],
      tags: ["collector", "market-hours", "timezone", "dedupe"],
    },
    {
      step: "02",
      title: "Normalize and canonicalize each event",
      body: "Before anything is published, the collector standardizes symbol names, exchange-local and UTC timestamps, trading date, interval, source metadata, and row identity. That is what lets live collection, deterministic replay, and full backfill all feed the same downstream contracts.",
      details: [
        { label: "Canonical fields", value: "symbol, exchange, UTC/IST timestamps, trading date, interval" },
        { label: "Data quality", value: "market-hours filtering, null checks, duplicate suppression" },
        { label: "Hydrated symbols", value: `${hydratedSymbols.toLocaleString("en-IN")}` },
        { label: "Sector coverage", value: `${formatNumber(sectorCoveragePct, 2)}%` },
      ],
      tags: ["schema", "validation", "IST", "replay-safe"],
    },
    {
      step: "03",
      title: "Publish the event stream through Kafka",
      body: "Kafka is the transport backbone. Every normalized tick is emitted as an ordered per-symbol event so storage and analytics consumers can scale independently, recover by replay, and stay deterministic.",
      details: [
        { label: "Latest bulk run", value: shortId(readString(asRecord(latestBackfillRun), "run_id")) },
        { label: "Rows published", value: formatCompactIndian(backfillTickRows, 2) },
        { label: "Partitions processed", value: formatCompactIndian(backfillPartitions, 0) },
        { label: "Trading-day window", value: `${backfillTradingDays} sessions` },
      ],
      tags: ["market_ticks", "ordered-by-symbol", "decoupling", "replay"],
    },
    {
      step: "04",
      title: "Persist operational minute data for low-latency reads",
      body: "The storage consumer writes append-heavy tick rows into Cassandra and updates latest-state records for operational reads. This is the layer that powers the live tape, the per-stock session timeline, and session-range intraday retrieval.",
      details: [
        { label: "Cassandra ticks", value: formatCompactIndian(Number(scale?.actual.streaming.market_ticks ?? 0), 2) },
        { label: "Latest-state rows", value: formatCompactIndian(backfillStateRows, 2) },
        { label: "Operational rows", value: formatCompactIndian(scale?.actual.operational_total_rows, 2) },
        { label: "Read pattern", value: "symbol + trading date + minute range" },
      ],
      tags: ["cassandra", "latest_market_state", "append-only", "time-range"],
    },
    {
      step: "05",
      title: "Score streaming anomalies in Redis-backed state",
      body: "The anomaly engine consumes ordered ticks, keeps per-symbol rolling state in Redis, and computes price return surprise, volume surprise, and the weighted composite score. Warmup and cooldown rules stop the detector from firing before a baseline exists or spamming the queue during bursts.",
      details: [
        { label: "Warmup", value: `${methodology?.anomaly.warmup_minutes ?? 5} minutes` },
        { label: "Composite rule", value: methodology ? `0.6 price + 0.4 volume | threshold ${formatNumber(methodology.anomaly.composite_threshold, 2)}` : "Composite scoring" },
        { label: "Anomaly rows", value: formatCompactIndian(Number(scale?.actual.streaming.anomaly_metrics ?? 0), 2) },
        { label: "Alert cooldown", value: `${methodology?.alerts.cooldown_minutes ?? 10} minutes` },
      ],
      tags: ["ewma", "price-z", "volume-z", "redis-state"],
    },
    {
      step: "06",
      title: "Convert raw signals into operator events",
      body: "The platform persists two operator-facing event types: alerts for symbol-level abnormality, and contagion windows for sector-level spread. The contagion engine watches anomaly events, opens a bounded five-minute peer window, and scores propagation risk only when same-sector peers confirm the trigger.",
      details: [
        { label: "Alert rows", value: scale?.actual.operational.alert_events?.toLocaleString("en-IN") ?? "0" },
        { label: "Contagion rows", value: scale?.actual.operational.contagion_events?.toLocaleString("en-IN") ?? "0" },
        { label: "Contagion window", value: `${methodology?.contagion.window_minutes ?? 5} minutes` },
        { label: "Risk formula", value: methodology?.contagion.risk_score_formula ?? "trigger + peers + spread" },
      ],
      tags: ["alerts", "severity", "contagion", "postgres-operational"],
    },
    {
      step: "07",
      title: "Extract, stage, and load into the warehouse",
      body: "Historical analytics do not query Cassandra directly. Instead, ETL extracts operational rows by trading date, stages them in PostgreSQL, assigns dimension keys, and loads the facts by grain so minute analysis, daily analysis, contagion analysis, and monitoring coverage remain analytically clean.",
      details: [
        { label: "Facts", value: methodology?.warehouse.facts.length ? `${methodology.warehouse.facts.length} separated fact families` : "Separated by grain" },
        { label: "Warehouse days", value: `${summary?.trading_days_loaded ?? 0}` },
        { label: "Latest successful ETL", value: latestCompletedEtlDate ? formatDate(latestCompletedEtlDate) : "N/A" },
        { label: "Rows inserted", value: formatCompactIndian(latestCompletedEtlRows, 2) },
      ],
      tags: ["staging", "surrogate-keys", "facts", "materialized-views"],
    },
    {
      step: "08",
      title: "Answer historical surveillance questions",
      body: "Once the warehouse is loaded, the system can answer cross-session questions that the live operational path cannot answer alone: which sectors are accelerating, which stocks persistently misbehave, when pressure peaks intraday, and how contagion evolves over day, month, and regime windows.",
      details: [
        { label: "Minute facts", value: formatCompactIndian(summary?.anomaly_minute_rows, 2) },
        { label: "Market-day facts", value: (summary?.market_day_rows ?? 0).toLocaleString("en-IN") },
        { label: "Contagion facts", value: (summary?.contagion_event_rows ?? 0).toLocaleString("en-IN") },
        { label: "Coverage facts", value: (summary?.coverage_rows ?? 0).toLocaleString("en-IN") },
      ],
      tags: ["rollups", "regimes", "leaders", "persistence"],
    },
  ];

  const storageCards = [
    {
      title: "Kafka",
      value: "Event spine",
      description:
        "Carries normalized market ticks and derived anomaly streams. Replay uses the same contract, which is what makes the demo deterministic and the consumers decoupled.",
      tone: "accent" as const,
    },
    {
      title: "Cassandra",
      value: "Operational minute store",
      description:
        "Holds append-heavy intraday ticks and anomaly metrics keyed for symbol/date/time retrieval rather than ad hoc OLAP.",
      tone: "warning" as const,
    },
    {
      title: "Redis",
      value: "Hot state and caches",
      description:
        "Keeps streaming anomaly state, latest snapshots, and low-latency read models so operational pages do not wait on heavyweight recomputation.",
      tone: "critical" as const,
    },
    {
      title: "PostgreSQL operational",
      value: "Relational event layer",
      description:
        "Stores alert events, contagion windows, daily bars, stock profiles, and run history where relational integrity and filterability matter.",
    },
    {
      title: "PostgreSQL warehouse",
      value: "Historical analytics",
      description:
        "Stores minute, daily, contagion, and coverage facts plus the materialized views that drive regime, persistence, sector, and intraday analytics.",
      tone: "accent" as const,
    },
  ];

  const warehouseCapabilities = [
    {
      title: "Sector-day stress map",
      value: `${(scale?.actual.warehouse.mv_sector_daily_summary ?? 0).toLocaleString("en-IN")} rows`,
      description:
        "Answers which sector was most stressed on a given day, how many active anomaly minutes it had, and how high the peak daily score reached.",
      tone: "accent" as const,
    },
    {
      title: "Month and quarter trends",
      value: `${(scale?.actual.warehouse.mv_sector_monthly_summary ?? 0).toLocaleString("en-IN")} rows`,
      description:
        "Aggregates sector pressure and contagion by month and quarter so the system can show longer-horizon regime shifts, not just one-session bursts.",
    },
    {
      title: "Sector regime and momentum",
      value: `${(scale?.actual.warehouse.mv_sector_regime_summary ?? 0).toLocaleString("en-IN")} + ${(scale?.actual.warehouse.mv_sector_momentum_summary ?? 0).toLocaleString("en-IN")} rows`,
      description:
        "Shows whether recent anomaly load is accelerating or fading compared with a prior window, sector by sector.",
      tone: "warning" as const,
    },
    {
      title: "Stock leaders and persistence",
      value: `${(scale?.actual.warehouse.mv_stock_signal_leaders ?? 0).toLocaleString("en-IN")} + ${(scale?.actual.warehouse.mv_stock_persistence_summary ?? 0).toLocaleString("en-IN")} rows`,
      description:
        "Separates one-off names from repeat offenders by counting anomaly days, latest peaks, and anomaly-day ratios across sessions.",
      tone: "critical" as const,
    },
    {
      title: "Intraday pressure profile",
      value: `${(scale?.actual.warehouse.mv_intraday_pressure_profile ?? 0).toLocaleString("en-IN")} minute buckets`,
      description:
        "Shows where the market session tends to concentrate anomaly pressure and contagion minutes at a time-of-day level.",
      tone: "accent" as const,
    },
    {
      title: "Coverage audit trail",
      value: `${(summary?.coverage_rows ?? 0).toLocaleString("en-IN")} factless rows`,
      description:
        "Tracks whether a stock was monitored on a session even if no anomaly triggered, which is important for completeness and viva defense.",
    },
  ];

  const warehouseAssets = [
    {
      asset: "fact_anomaly_minute",
      rows: scale?.actual.warehouse.fact_anomaly_minute ?? 0,
      purpose: "Minute-grain surveillance measurements for historical intraday analysis.",
    },
    {
      asset: "fact_market_day",
      rows: scale?.actual.warehouse.fact_market_day ?? 0,
      purpose: "Daily stock summaries used for persistence, leaders, and session rollups.",
    },
    {
      asset: "fact_contagion_event",
      rows: scale?.actual.warehouse.fact_contagion_event ?? 0,
      purpose: "Persisted propagation windows with historical analytical joins.",
    },
    {
      asset: "fact_surveillance_coverage",
      rows: scale?.actual.warehouse.fact_surveillance_coverage ?? 0,
      purpose: "Factless monitoring ledger proving session coverage by stock and date.",
    },
    {
      asset: "mv_sector_daily_summary",
      rows: scale?.actual.warehouse.mv_sector_daily_summary ?? 0,
      purpose: "Sector-day rollups for stress ranking and daily comparison.",
    },
    {
      asset: "mv_sector_monthly_summary",
      rows: scale?.actual.warehouse.mv_sector_monthly_summary ?? 0,
      purpose: "Month and quarter rollups for long-horizon sector analysis.",
    },
    {
      asset: "mv_sector_regime_summary",
      rows: scale?.actual.warehouse.mv_sector_regime_summary ?? 0,
      purpose: "Cross-session regime map by sector.",
    },
    {
      asset: "mv_sector_momentum_summary",
      rows: scale?.actual.warehouse.mv_sector_momentum_summary ?? 0,
      purpose: "Recent-versus-prior sector acceleration measure.",
    },
    {
      asset: "mv_stock_signal_leaders",
      rows: scale?.actual.warehouse.mv_stock_signal_leaders ?? 0,
      purpose: "Stock-level anomaly leaders with recent and peak signals.",
    },
    {
      asset: "mv_stock_persistence_summary",
      rows: scale?.actual.warehouse.mv_stock_persistence_summary ?? 0,
      purpose: "Signal durability and anomaly-day ratio by stock.",
    },
    {
      asset: "mv_intraday_pressure_profile",
      rows: scale?.actual.warehouse.mv_intraday_pressure_profile ?? 0,
      purpose: "Time-of-day anomaly and contagion pressure profile.",
    },
  ];

  const exampleTickIndex =
    exampleWorkspace?.ticks.findIndex((tick) => tick.timestamp_ist === "2026-04-20T15:12:00+05:30") ?? -1;
  const exampleTick =
    exampleTickIndex >= 0 ? exampleWorkspace?.ticks[exampleTickIndex] : (exampleWorkspace?.ticks.at(-1) ?? null);
  const examplePrevTick =
    exampleTickIndex > 0
      ? exampleWorkspace?.ticks[exampleTickIndex - 1]
      : (exampleWorkspace?.ticks.at(-2) ?? null);
  const exampleAnomaly =
    (exampleTick
      ? exampleWorkspace?.anomalies.find((point) => point.timestamp_ist === exampleTick.timestamp_ist)
      : null) ??
    exampleWorkspace?.anomalies.find((point) => point.is_anomalous) ??
    null;
  const exampleReturnPct =
    exampleTick && examplePrevTick ? ((exampleTick.close - examplePrevTick.close) / examplePrevTick.close) * 100 : null;
  const priceWeight = methodology?.anomaly.composite_weights.price_z ?? 0.6;
  const volumeWeight = methodology?.anomaly.composite_weights.volume_z ?? 0.4;
  const exampleCompositeRebuild = exampleAnomaly
    ? priceWeight * Math.abs(exampleAnomaly.price_z_score) + volumeWeight * Math.abs(exampleAnomaly.volume_z_score)
    : null;
  const exampleFlagReasons: string[] = [];
  if (exampleAnomaly) {
    if (Math.abs(exampleAnomaly.price_z_score) >= (methodology?.anomaly.price_z_threshold ?? Infinity)) {
      exampleFlagReasons.push(`|price z| >= ${formatNumber(methodology?.anomaly.price_z_threshold, 2)}`);
    }
    if (Math.abs(exampleAnomaly.volume_z_score) >= (methodology?.anomaly.volume_z_threshold ?? Infinity)) {
      exampleFlagReasons.push(`|volume z| >= ${formatNumber(methodology?.anomaly.volume_z_threshold, 2)}`);
    }
    if ((exampleCompositeRebuild ?? 0) >= (methodology?.anomaly.composite_threshold ?? Infinity)) {
      exampleFlagReasons.push(`composite >= ${formatNumber(methodology?.anomaly.composite_threshold, 2)}`);
    }
  }
  const exampleFlagReason = exampleFlagReasons.length
    ? exampleFlagReasons.join(" | ")
    : "No threshold breach persisted for the selected minute";
  const exampleContagionAlert =
    exampleWorkspace?.alerts.find((alert) => alert.event_category === "contagion" && !alert.is_stale) ?? null;
  const exampleAlertRecord = asRecord(exampleContagionAlert);
  const exampleContagionPayload = asRecord(exampleAlertRecord ? exampleAlertRecord["event_payload"] : null);
  const exampleContagionSector = readString(exampleContagionPayload, "sector");
  const exampleContagionPeers = Array.isArray(exampleContagionPayload?.affected_symbols)
    ? exampleContagionPayload.affected_symbols.filter((value): value is string => typeof value === "string")
    : [];
  const exampleNormalizedEvent = exampleTick
    ? {
        symbol: exampleWorkspace?.reference.symbol ?? exampleSymbol,
        company_name: exampleWorkspace?.reference.company_name ?? "Axis Bank",
        exchange: exampleWorkspace?.reference.exchange ?? "NSE",
        sector: exampleWorkspace?.reference.sector ?? "Banking",
        timestamp_utc: exampleTick.timestamp_utc,
        timestamp_ist: exampleTick.timestamp_ist,
        trading_date: exampleWorkspace?.latest_market?.trading_date ?? exampleTick.timestamp_ist.slice(0, 10),
        interval: "1m",
        open: exampleTick.open,
        high: exampleTick.high,
        low: exampleTick.low,
        close: exampleTick.close,
        volume: exampleTick.volume,
        dividends: exampleTick.dividends,
        stock_splits: exampleTick.stock_splits,
        source: backfillProvider,
        ingestion_run_id: readString(asRecord(latestBackfillRun), "run_id"),
        dedupe_key: `${exampleWorkspace?.reference.symbol ?? exampleSymbol}|${exampleTick.timestamp_utc}|1m`,
      }
    : null;
  const exampleAnomalyPayload = exampleAnomaly
    ? {
        symbol: exampleWorkspace?.reference.symbol ?? exampleSymbol,
        timestamp_ist: exampleAnomaly.timestamp_ist,
        return_pct: exampleReturnPct,
        price_z_score: exampleAnomaly.price_z_score,
        volume_z_score: exampleAnomaly.volume_z_score,
        composite_score: exampleAnomaly.composite_score,
        composite_rebuilt: exampleCompositeRebuild,
        thresholds: {
          price_z: methodology?.anomaly.price_z_threshold,
          volume_z: methodology?.anomaly.volume_z_threshold,
          composite: methodology?.anomaly.composite_threshold,
        },
        is_anomalous: exampleAnomaly.is_anomalous,
        breach_reason: exampleFlagReason,
        explainability: exampleAnomaly.explainability,
        writes: ["cassandra.anomaly_metrics", "redis.latest_anomaly_snapshot"],
      }
    : null;
  const exampleWarehousePayload = {
    fact_anomaly_minute: exampleAnomaly
      ? {
          stock: exampleWorkspace?.reference.symbol ?? exampleSymbol,
          calendar_date: exampleWorkspace?.latest_market?.trading_date ?? exampleAnomaly.timestamp_ist.slice(0, 10),
          time_label: "15:12",
          composite_score: exampleAnomaly.composite_score,
          price_z_score: exampleAnomaly.price_z_score,
          volume_z_score: exampleAnomaly.volume_z_score,
          is_anomalous: exampleAnomaly.is_anomalous,
        }
      : null,
    fact_market_day: {
      stock: exampleWorkspace?.reference.symbol ?? exampleSymbol,
      calendar_date: exampleWorkspace?.latest_market?.trading_date ?? null,
      close: exampleWorkspace?.latest_market?.close ?? null,
      anomaly_count: exampleWorkspace?.anomaly_summary.flagged_count ?? 0,
      peak_composite_score: exampleWorkspace?.anomaly_summary.peak_composite_score ?? null,
      volume_ratio_20d: exampleWorkspace?.indicators.volume_ratio_20d ?? null,
    },
    fact_surveillance_coverage: {
      stock: exampleWorkspace?.reference.symbol ?? exampleSymbol,
      calendar_date: exampleWorkspace?.latest_market?.trading_date ?? null,
      monitored: true,
      session_minutes_observed: methodology?.market.session_minutes ?? 375,
    },
    downstream_views: [
      "mv_stock_signal_leaders",
      "mv_stock_persistence_summary",
      "mv_sector_daily_summary",
      "mv_intraday_pressure_profile",
    ],
  };
  const exampleLineage = [
    {
      step: "A1",
      title: "Raw provider minute bar",
      meta: exampleTick?.timestamp_ist ? `${formatDateTime(exampleTick.timestamp_ist)} IST` : "Example minute",
      detail: "Provider output still looks like a market row: OHLCV plus dividends and stock-split columns.",
      facts: [
        `${exampleWorkspace?.reference.symbol ?? exampleSymbol} | ${exampleWorkspace?.reference.company_name ?? "Axis Bank"}`,
        `Close ${formatNumber(exampleTick?.close, 2)} | Volume ${(exampleTick?.volume ?? 0).toLocaleString("en-IN")}`,
        `Corporate actions preserved: dividends ${formatNumber(exampleTick?.dividends, 2)} | splits ${formatNumber(exampleTick?.stock_splits, 2)}`,
      ],
    },
    {
      step: "A2",
      title: "Canonical Kafka event",
      meta: shortId(readString(asRecord(latestBackfillRun), "run_id")),
      detail: "Normalization adds symbol metadata, UTC and IST timestamps, trading date, interval, source, and dedupe identity.",
      facts: [
        `Trading date ${(exampleWorkspace?.latest_market?.trading_date ?? "N/A")}`,
        `Dedupe key ${(exampleNormalizedEvent?.dedupe_key ?? "N/A")}`,
        `Source ${backfillProvider} | exchange ${exampleWorkspace?.reference.exchange ?? "NSE"}`,
      ],
    },
    {
      step: "A3",
      title: "Streaming anomaly decision",
      meta: exampleAnomaly?.is_anomalous ? "Flagged minute" : "Normal minute",
      detail: "Redis-backed state compares the new minute against the rolling baseline using EWMA-derived z-scores and the composite rule.",
      facts: [
        `Return ${formatPercent(exampleReturnPct, 5)}`,
        `Price z ${formatNumber(exampleAnomaly?.price_z_score, 3)} | volume z ${formatNumber(exampleAnomaly?.volume_z_score, 3)}`,
        exampleFlagReason,
      ],
    },
    {
      step: "A4",
      title: "Operational persistence",
      meta: `${formatCompactIndian(Number(scale?.actual.streaming.anomaly_metrics ?? 0), 2)} anomaly rows`,
      detail: "The minute-level signal lands in Cassandra and updates live API state; if severity and cooldown rules allow, an operator alert row is also persisted.",
      facts: [
        "Writes: cassandra.anomaly_metrics + redis latest snapshot",
        `Alert cooldown ${methodology?.alerts.cooldown_minutes ?? 10} minutes`,
        `Notification floor ${methodology?.alerts.notification_min_severity ?? "high"}`,
      ],
    },
    {
      step: "A5",
      title: "Warehouse lineage",
      meta: `${summary?.trading_days_loaded ?? 0} trading days loaded`,
      detail: "ETL turns the operational minute into keyed fact rows and refreshes the historical views that power analyst-facing reports.",
      facts: [
        `fact_anomaly_minute + fact_market_day + fact_surveillance_coverage`,
        `${formatCompactIndian(latestCompletedEtlRows, 2)} rows inserted in latest successful ETL`,
        `Queryable through /warehouse and Analyst Studio`,
      ],
    },
  ];
  const analystQuestions = [
    {
      question: "Which sector accelerated its stress profile most recently?",
      source: "mv_sector_momentum_summary",
      grain: "sector x recent window versus prior window",
      outcome: "Ranks sectors by anomaly, score-intensity, and contagion acceleration.",
    },
    {
      question: "Which stocks keep reappearing as surveillance outliers?",
      source: "mv_stock_persistence_summary + mv_stock_signal_leaders",
      grain: "stock x cross-session persistence",
      outcome: "Surfaces repeat offenders, anomaly-day ratios, and latest peak scores.",
    },
    {
      question: "What part of the trading day usually concentrates pressure?",
      source: "mv_intraday_pressure_profile",
      grain: "IST minute bucket across sessions",
      outcome: "Shows hotspot minutes, average composite load, and contagion density.",
    },
    {
      question: "Which propagation windows were strongest in Banking on 20 Apr?",
      source: "fact_contagion_event + mv_sector_regime_summary",
      grain: "event-level contagion with sector regime context",
      outcome: "Explains trigger symbol, peer confirmations, risk score, and sector pressure context.",
    },
    {
      question: "Was a stock monitored even when nothing triggered?",
      source: "fact_surveillance_coverage",
      grain: "stock x session monitoring ledger",
      outcome: "Proves coverage and avoids confusing lack of alerts with lack of observation.",
    },
    {
      question: "How does an analyst export this story?",
      source: "Warehouse Analyst Studio",
      grain: "curated report over allowlisted warehouse datasets",
      outcome: "Builds charts, result tables, and a printable PDF-ready report from the same facts.",
    },
  ];

  return (
    <>
      <section className="heroPanel">
        <div className="pageHeader">
          <div>
            <p className="pageKicker">Process</p>
            <h2 className="pageTitle">End-to-end system flow</h2>
          </div>
          <div className="pageMetaGroup">
            <span className="metaTag">{health?.last_tick ? formatDateTime(health.last_tick) : "No live tick"}</span>
            <span className="metaTag">{formatCompactIndian(materializedRows, 2)} materialized rows</span>
          </div>
        </div>
        <div className="statusNote">
          The platform currently covers {listedSymbols.toLocaleString("en-IN")} listed NSE symbols, {hydratedSymbols.toLocaleString("en-IN")} hydrated histories,{" "}
          {formatCompactIndian(materializedRows, 2)} materialized rows across the operational and analytical layers, and a loaded-window projection of{" "}
          {formatCompactIndian(loadedWindowProjection, 2)} tick-plus-anomaly rows.
        </div>
        <div className="statsGrid">
          <StatCard
            label="Listed universe"
            value={listedSymbols.toLocaleString("en-IN")}
            info="Total symbols currently tracked in the listed NSE reference universe."
            hint={`${formatNumber(sectorCoveragePct, 2)}% sector coverage`}
          />
          <StatCard
            label="Hydrated histories"
            value={hydratedSymbols.toLocaleString("en-IN")}
            info="Symbols whose historical bars and metadata are already loaded and available for analytics."
            hint={`${(scale?.coverage.trading_days_loaded ?? 0).toLocaleString("en-IN")} daily sessions loaded`}
            tone="accent"
          />
          <StatCard
            label="Latest bulk ticks"
            value={formatCompactIndian(backfillTickRows, 2)}
            info="Tick rows published by the latest full-NSE minute backfill run."
            hint={backfillTradingDays ? `${backfillTradingDays} trading days | ${backfillProvider}` : "Latest completed backfill"}
          />
          <StatCard
            label="Latest bulk anomalies"
            value={formatCompactIndian(backfillAnomalyRows, 2)}
            info="Anomaly rows produced from the latest full-NSE streaming backfill."
            hint={`${formatCompactIndian(backfillStateRows, 2)} latest-state rows refreshed`}
            tone="warning"
          />
          <StatCard
            label="Warehouse minute facts"
            value={formatCompactIndian(warehouseMinuteFacts, 2)}
            info="Minute-grain warehouse rows available for historical intraday analytics."
            hint={`${summary?.trading_days_loaded ?? 0} warehouse days | ${summary?.stocks_covered ?? 0} stocks`}
            tone="critical"
          />
          <StatCard
            label="Loaded-window projection"
            value={formatCompactIndian(loadedWindowProjection, 2)}
            info="Projected tick-plus-anomaly rows if the currently loaded historical window is fully materialized at full-NSE minute grain."
            hint={`${formatNumber(scale?.projection.actual_materialized_vs_loaded_window_pct, 1)}% already materialized`}
          />
          <StatCard
            label="Annual projection"
            value={formatCompactIndian(annualProjection, 2)}
            info="Projected tick-plus-anomaly rows across one full trading year at full-NSE minute scope."
            hint={`${scale?.projection.trading_days_per_year ?? 250} sessions`}
            tone="warning"
          />
          <StatCard
            label="Five-year runway"
            value={formatCompactIndian(fiveYearProjection, 2)}
            info="Five-year projection used to defend the platform's large-scale data story."
            hint="Crore-scale archive path"
            tone="critical"
          />
        </div>
      </section>

      <section className="contentGrid quickActionsGrid">
        {quickLinks.map((item) => (
          <Link key={item.href} href={item.href} className="shortcutCard">
            <p className="panelEyebrow">{item.eyebrow}</p>
            <h3 className="shortcutTitle">{item.title}</h3>
            <div className="shortcutMetric">{item.metric}</div>
            <div className="shortcutHint">{item.hint}</div>
          </Link>
        ))}
      </section>

      <section className="surface">
        <div className="panelHeader">
          <div>
            <p className="panelEyebrow">Figure 1</p>
            <h3 className="panelTitle">Pipeline architecture by stage</h3>
          </div>
          <span className="panelMeta">Operational path and historical path on one page</span>
        </div>
        <div className="pipelineBands">
          {pipelineBands.map((band) => (
            <div key={band.label} className="pipelineBand">
              <div className="pipelineBandHeader">
                <div>
                  <div className="pipelineBandLabel">{band.label}</div>
                </div>
                <div className="panelMeta">{band.meta}</div>
              </div>
              <div className="pipelineTrack">
                {band.nodes.map((node) => (
                  <div key={`${band.label}-${node.step}`} className="pipelineNode">
                    <div className="pipelineNodeHeader">
                      <span className="pipelineNodeStep">{node.step}</span>
                    </div>
                    <h4 className="pipelineNodeTitle">{node.title}</h4>
                    <div className="pipelineNodeText">{node.text}</div>
                    <div className="pipelineNodeMeta">{node.meta}</div>
                  </div>
                ))}
              </div>
            </div>
          ))}
        </div>
      </section>

      <section className="surface">
        <div className="panelHeader">
          <div>
            <p className="panelEyebrow">Figure 1A</p>
            <h3 className="panelTitle">One real market minute as it travels through the stack</h3>
          </div>
          <span className="panelMeta">
            {exampleWorkspace?.reference.company_name ?? exampleSymbol} | {exampleTick?.timestamp_ist ? formatDateTime(exampleTick.timestamp_ist) : "Example minute"}
          </span>
        </div>
        <div className="statusNote">
          This specimen uses a real AXISBANK minute from {exampleTick?.timestamp_ist ? formatDate(exampleTick.timestamp_ist) : "the loaded session"} to show
          the exact shape changes across the system: provider row, canonical event, anomaly decision, operational persistence, and warehouse lineage.
        </div>
        <div className="stageDiagram">
          {exampleLineage.map((stage, index) => (
            <div key={stage.step} className="stageDiagramItem">
              <article className="stageNode">
                <div className="stageNodeTop">
                  <span className="stageNodeStep">{stage.step}</span>
                  <span className="stageNodeMeta">{stage.meta}</span>
                </div>
                <h4 className="stageNodeTitle">{stage.title}</h4>
                <p className="stageNodeDetail">{stage.detail}</p>
                <ul className="stageFactList">
                  {stage.facts.map((fact) => (
                    <li key={`${stage.step}-${fact}`}>{fact}</li>
                  ))}
                </ul>
              </article>
              {index < exampleLineage.length - 1 ? (
                <div className="stageConnector" aria-hidden="true">
                  <span>→</span>
                </div>
              ) : null}
            </div>
          ))}
        </div>
      </section>

      <section className="contentGrid twoUp">
        <article className="surface">
          <div className="panelHeader">
            <div>
              <p className="panelEyebrow">Figure 1B</p>
              <h3 className="panelTitle">Specimen record transformation</h3>
            </div>
            <span className="panelMeta">Raw row versus canonical event contract</span>
          </div>
          <div className="codeCompareGrid">
            <div className="codeCard">
              <div className="codeCardHeader">
                <strong>Provider row</strong>
                <span>Unnormalized minute bar</span>
              </div>
              <pre className="codeBlock">{jsonPreview(exampleTick)}</pre>
            </div>
            <div className="codeCard">
              <div className="codeCardHeader">
                <strong>Canonical event</strong>
                <span>Published to Kafka</span>
              </div>
              <pre className="codeBlock">{jsonPreview(exampleNormalizedEvent)}</pre>
            </div>
          </div>
          <div className="figureCaption">
            The collector adds the business identity and audit fields that downstream consumers need: symbol, exchange, sector, UTC and IST timestamps, trading date,
            interval, source, run ID, and a deterministic dedupe key.
          </div>
        </article>

        <article className="surface">
          <div className="panelHeader">
            <div>
              <p className="panelEyebrow">Figure 1C</p>
              <h3 className="panelTitle">Why this minute was flagged</h3>
            </div>
            <span className="panelMeta">Anomaly math on the same specimen</span>
          </div>
          <div className="mathCardGrid">
            <div className="mathCard">
              <span className="mathLabel">Prev close</span>
              <strong>{formatNumber(examplePrevTick?.close, 4)}</strong>
              <small>{examplePrevTick?.timestamp_ist ? formatDateTime(examplePrevTick.timestamp_ist) : "Previous minute"}</small>
            </div>
            <div className="mathCard accent">
              <span className="mathLabel">Return %</span>
              <strong>{formatPercent(exampleReturnPct, 5)}</strong>
              <small>(({formatNumber(exampleTick?.close, 4)} - {formatNumber(examplePrevTick?.close, 4)}) / prev close) * 100</small>
            </div>
            <div className="mathCard">
              <span className="mathLabel">Price z</span>
              <strong>{formatNumber(exampleAnomaly?.price_z_score, 3)}</strong>
              <small>Threshold {formatNumber(methodology?.anomaly.price_z_threshold, 2)}</small>
            </div>
            <div className="mathCard warning">
              <span className="mathLabel">Volume z</span>
              <strong>{formatNumber(exampleAnomaly?.volume_z_score, 3)}</strong>
              <small>Threshold {formatNumber(methodology?.anomaly.volume_z_threshold, 2)}</small>
            </div>
            <div className="mathCard critical">
              <span className="mathLabel">Composite</span>
              <strong>{formatNumber(exampleCompositeRebuild, 3)}</strong>
              <small>
                {formatNumber(priceWeight, 1)}*|price z| + {formatNumber(volumeWeight, 1)}*|volume z|
              </small>
            </div>
            <div className={`mathCard ${exampleAnomaly?.is_anomalous ? "success" : ""}`}>
              <span className="mathLabel">Decision</span>
              <strong>{exampleAnomaly?.is_anomalous ? "Persisted anomaly" : "No persisted anomaly"}</strong>
              <small>{exampleFlagReason}</small>
            </div>
          </div>
          <div className="codeCard compact">
            <div className="codeCardHeader">
              <strong>Anomaly payload</strong>
              <span>Written to Cassandra + Redis</span>
            </div>
            <pre className="codeBlock">{jsonPreview(exampleAnomalyPayload)}</pre>
          </div>
        </article>
      </section>

      <section className="contentGrid twoUp">
        <article className="surface">
          <div className="panelHeader">
            <div>
              <p className="panelEyebrow">Figure 1D</p>
              <h3 className="panelTitle">How contagion becomes an operator event</h3>
            </div>
            <span className="panelMeta">
              {exampleContagionAlert?.symbol ?? exampleSymbol} | {exampleContagionAlert?.detected_at ? formatDateTime(exampleContagionAlert.detected_at) : "Example window"}
            </span>
          </div>
          <div className="contagionWindow">
            <div className="contagionNode">
              <span className="mathLabel">Trigger</span>
              <strong>{exampleContagionAlert?.symbol ?? exampleSymbol}</strong>
              <small>{exampleContagionSector ? `${exampleContagionSector} sector` : "Sector peer set required"}</small>
            </div>
            <div className="contagionArrow" aria-hidden="true">
              <span>→</span>
            </div>
            <div className="contagionNode warning">
              <span className="mathLabel">Peer window</span>
              <strong>{formatNumber(exampleContagionPeers.length, 0)} confirmations</strong>
              <small>
                {readString(exampleContagionPayload, "observation_window_start")
                  ? `${formatDateTime(readString(exampleContagionPayload, "observation_window_start"))} to ${formatDateTime(readString(exampleContagionPayload, "observation_window_end"))}`
                  : `${methodology?.contagion.window_minutes ?? 5}-minute bounded window`}
              </small>
            </div>
            <div className="contagionArrow" aria-hidden="true">
              <span>→</span>
            </div>
            <div className="contagionNode critical">
              <span className="mathLabel">Risk score</span>
              <strong>{formatNumber(exampleContagionAlert?.composite_score, 3)}</strong>
              <small>{methodology?.contagion.risk_score_formula ?? "trigger + peers + spread"}</small>
            </div>
          </div>
          <div className="keyValueGrid">
            <div className="keyValueCard">
              <span>Peer average score</span>
              <strong>{formatNumber(readNumber(exampleContagionPayload, "peer_average_score"), 3)}</strong>
            </div>
            <div className="keyValueCard">
              <span>Affected symbols</span>
              <strong>{exampleContagionPeers.join(", ") || "N/A"}</strong>
            </div>
            <div className="keyValueCard">
              <span>Severity</span>
              <strong>{exampleContagionAlert?.severity ?? "N/A"}</strong>
            </div>
            <div className="keyValueCard">
              <span>Status</span>
              <strong>{exampleContagionAlert?.status ?? "N/A"}</strong>
            </div>
          </div>
          <div className="figureCaption">
            Contagion does not scan the full market graph. It opens a bounded same-sector window, waits for peer confirmations, scores the propagation risk, and then
            persists a relational event that operators and analysts can filter later.
          </div>
        </article>

        <article className="surface">
          <div className="panelHeader">
            <div>
              <p className="panelEyebrow">Figure 1E</p>
              <h3 className="panelTitle">What ETL writes into the warehouse</h3>
            </div>
            <span className="panelMeta">Minute fact, day fact, coverage fact, and downstream views</span>
          </div>
          <div className="codeCompareGrid singleColumn">
            <div className="codeCard">
              <div className="codeCardHeader">
                <strong>Warehouse lineage payload</strong>
                <span>Analytical rows derived from the operational specimen</span>
              </div>
              <pre className="codeBlock">{jsonPreview(exampleWarehousePayload)}</pre>
            </div>
          </div>
          <div className="figureCaption">
            The same operational minute contributes to multiple analytical grains. Minute-level analysis stays in <span className="dataMono">fact_anomaly_minute</span>,
            session-level context lands in <span className="dataMono">fact_market_day</span>, and coverage completeness is tracked separately in{" "}
            <span className="dataMono">fact_surveillance_coverage</span>.
          </div>
        </article>
      </section>

      <section className="contentGrid twoUp">
        <article className="surface">
          <div className="panelHeader">
            <div>
              <p className="panelEyebrow">Figure 2</p>
              <h3 className="panelTitle">Current row-volume story</h3>
            </div>
            <span className="panelMeta">From latest bulk run to annual projection</span>
          </div>
          <IntensityBars
            items={volumeBars}
            valueFormatter={(value) => formatCompactIndian(value, 2)}
            emptyMessage="Volume figures are unavailable."
          />
        </article>

        <article className="surface">
          <div className="panelHeader">
            <div>
              <p className="panelEyebrow">Figure 3</p>
              <h3 className="panelTitle">Where the rows live</h3>
            </div>
            <span className="panelMeta">Operational versus analytical storage</span>
          </div>
          <IntensityBars
            items={storeBars}
            valueFormatter={(value) => formatCompactIndian(value, 2)}
            emptyMessage="Store footprint is unavailable."
          />
        </article>
      </section>

      <section className="contentGrid twoUp">
        <article className="surface">
          <div className="panelHeader">
            <div>
              <p className="panelEyebrow">Run figure</p>
              <h3 className="panelTitle">Latest full-NSE backfill</h3>
            </div>
            <span className="panelMeta">{shortId(readString(asRecord(latestBackfillRun), "run_id"))}</span>
          </div>
          <div className="keyValueGrid">
            <div className="keyValueCard">
              <span>Window</span>
              <strong>{backfillWindowStart && backfillWindowEnd ? `${formatDate(backfillWindowStart)} to ${formatDate(backfillWindowEnd)}` : "N/A"}</strong>
            </div>
            <div className="keyValueCard">
              <span>Duration</span>
              <strong>{durationLabel(readString(asRecord(latestBackfillRun), "started_at"), readString(asRecord(latestBackfillRun), "finished_at"))}</strong>
            </div>
            <div className="keyValueCard">
              <span>Requested symbols</span>
              <strong>{(readNumber(backfillNotes, "requested_symbol_count") ?? 0).toLocaleString("en-IN")}</strong>
            </div>
            <div className="keyValueCard">
              <span>Published ticks</span>
              <strong>{formatCompactIndian(backfillTickRows, 2)}</strong>
            </div>
            <div className="keyValueCard">
              <span>Anomaly rows</span>
              <strong>{formatCompactIndian(backfillAnomalyRows, 2)}</strong>
            </div>
            <div className="keyValueCard">
              <span>Latest-state rows</span>
              <strong>{formatCompactIndian(backfillStateRows, 2)}</strong>
            </div>
          </div>
        </article>

        <article className="surface">
          <div className="panelHeader">
            <div>
              <p className="panelEyebrow">Run figure</p>
              <h3 className="panelTitle">Deterministic replay path</h3>
            </div>
            <span className="panelMeta">{shortId(readString(asRecord(latestReplayRun), "run_id") ?? replay?.run_id)}</span>
          </div>
          <div className="keyValueGrid">
            <div className="keyValueCard">
              <span>Fixture</span>
              <strong>{compactPath(latestReplayFixture)}</strong>
            </div>
            <div className="keyValueCard">
              <span>Trading date</span>
              <strong>{latestReplayTradingDate ? formatDate(latestReplayTradingDate) : "N/A"}</strong>
            </div>
            <div className="keyValueCard">
              <span>Symbols</span>
              <strong>{(latestReplaySymbols ?? 0).toLocaleString("en-IN")}</strong>
            </div>
            <div className="keyValueCard">
              <span>Rows published</span>
              <strong>{(latestReplayRows ?? 0).toLocaleString("en-IN")}</strong>
            </div>
            <div className="keyValueCard">
              <span>Replay speed</span>
              <strong>{latestReplaySpeed ? `${latestReplaySpeed}x` : "N/A"}</strong>
            </div>
            <div className="keyValueCard">
              <span>Purpose</span>
              <strong>Closed-market demo and deterministic validation</strong>
            </div>
          </div>
        </article>
      </section>

      <section className="surface">
        <div className="panelHeader">
          <div>
            <p className="panelEyebrow">Step-by-step</p>
            <h3 className="panelTitle">The data journey from ingestion to analytics</h3>
          </div>
          <span className="panelMeta">Every major transformation stage</span>
        </div>
        <div className="processSteps">
          {stepCards.map((item) => (
            <article key={item.step} className="processStepCard">
              <div className="processStepIndex">{item.step}</div>
              <div className="processStepBody">
                <div className="processStepTitleRow">
                  <h4 className="processStepTitle">{item.title}</h4>
                </div>
                <p className="processStepText">{item.body}</p>
                <div className="processTagRow">
                  {item.tags.map((tag) => (
                    <span key={`${item.step}-${tag}`} className="metaChip">
                      {tag}
                    </span>
                  ))}
                </div>
                <div className="keyValueGrid processStepGrid">
                  {item.details.map((detail) => (
                    <div key={`${item.step}-${detail.label}`} className="keyValueCard">
                      <span>{detail.label}</span>
                      <strong>{detail.value}</strong>
                    </div>
                  ))}
                </div>
              </div>
            </article>
          ))}
        </div>
      </section>

      <section className="contentGrid twoUp">
        <article className="surface">
          <div className="panelHeader">
            <div>
              <p className="panelEyebrow">Operational roles</p>
              <h3 className="panelTitle">What each storage layer is responsible for</h3>
            </div>
            <span className="panelMeta">Separation by workload, not by habit</span>
          </div>
          <div className="explainerGrid">
            {storageCards.map((card) => (
              <div key={card.title} className={`explainerCard ${card.tone ?? "default"}`}>
                <div className="explainerTitleRow">
                  <strong>{card.title}</strong>
                  <span className="explainerValue">{card.value}</span>
                </div>
                <div className="explainerText">{card.description}</div>
              </div>
            ))}
          </div>
        </article>

        <article className="surface">
          <div className="panelHeader">
            <div>
              <p className="panelEyebrow">Warehouse capabilities</p>
              <h3 className="panelTitle">What the historical layer can answer</h3>
            </div>
            <span className="panelMeta">Fact grain and view design in practice</span>
          </div>
          <div className="explainerGrid">
            {warehouseCapabilities.map((card) => (
              <div key={card.title} className={`explainerCard ${card.tone ?? "default"}`}>
                <div className="explainerTitleRow">
                  <strong>{card.title}</strong>
                  <span className="explainerValue">{card.value}</span>
                </div>
                <div className="explainerText">{card.description}</div>
              </div>
            ))}
          </div>
        </article>
      </section>

      <section className="contentGrid twoUp">
        <article className="surface">
          <div className="panelHeader">
            <div>
              <p className="panelEyebrow">Figure 4</p>
              <h3 className="panelTitle">Warehouse intraday pressure profile</h3>
            </div>
            <span className="panelMeta">
              {intradayRows.length ? `${intradayRows[0]?.sessions_covered ?? 0} sessions | peak ${intradayPeak.time_label}` : "No profile"}
            </span>
          </div>
          {intradayRows.length ? (
            <>
              <LineChart
                values={intradayValues}
                labels={intradayLabels}
                color="var(--accent)"
                height={220}
                valueDigits={3}
                seriesLabel="average warehouse composite score"
              />
              <div className="keyValueGrid">
                <div className="keyValueCard">
                  <span>Minute buckets</span>
                  <strong>{intradayRows.length.toLocaleString("en-IN")}</strong>
                </div>
                <div className="keyValueCard">
                  <span>Peak minute</span>
                  <strong>{intradayPeak.time_label}</strong>
                </div>
                <div className="keyValueCard">
                  <span>Peak score</span>
                  <strong>{formatNumber(intradayPeak.peak_composite_score, 3)}</strong>
                </div>
                <div className="keyValueCard">
                  <span>Avg distinct stocks</span>
                  <strong>{formatNumber(averageDistinctStocks, 0)}</strong>
                </div>
              </div>
            </>
          ) : (
            <div className="emptyState">Warehouse intraday profile is not available.</div>
          )}
        </article>

        <article className="surface">
          <div className="panelHeader">
            <div>
              <p className="panelEyebrow">Warehouse assets</p>
              <h3 className="panelTitle">Facts and views currently materialized</h3>
            </div>
            <span className="panelMeta">{warehouseAssets.length} assets</span>
          </div>
          <div className="tableWrap">
            <table className="dataTable">
              <thead>
                <tr>
                  <th>Asset</th>
                  <th>Rows</th>
                  <th>What it answers</th>
                </tr>
              </thead>
              <tbody>
                {warehouseAssets.map((asset) => (
                  <tr key={asset.asset}>
                    <td className="dataMono">{asset.asset}</td>
                    <td>{formatCompactIndian(asset.rows, 2)}</td>
                    <td>{asset.purpose}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </article>
      </section>

      <section className="surface">
        <div className="panelHeader">
          <div>
            <p className="panelEyebrow">Figure 5</p>
            <h3 className="panelTitle">How an analyst turns warehouse data into reports</h3>
          </div>
          <span className="panelMeta">Question {"->"} fact/view {"->"} result set {"->"} report</span>
        </div>
        <div className="queryBlueprintGrid">
          {analystQuestions.map((item) => (
            <article key={item.question} className="queryBlueprintCard">
              <div className="queryBlueprintHeader">
                <span className="queryBlueprintLabel">Analyst question</span>
                <span className="queryBlueprintSource">{item.source}</span>
              </div>
              <h4 className="queryBlueprintTitle">{item.question}</h4>
              <p className="queryBlueprintText">{item.outcome}</p>
              <div className="queryBlueprintFoot">
                <span>Analytical grain</span>
                <strong>{item.grain}</strong>
              </div>
            </article>
          ))}
        </div>
        <div className="statusNote">
          The warehouse is not just a historical archive. It is the analyst layer: facts stay separated by grain, materialized views precompute the expensive summaries,
          the Analyst Studio exposes a safe visual query builder, and the same result sets can be exported to CSV or printed to PDF for review packs and viva reports.
        </div>
      </section>

      <ExplainerCards
        eyebrow="Warehouse interpretation"
        title="Why this warehouse can support serious analysis"
        meta="Modeling and audit rationale"
        footerHref="/methodology"
        footerLabel="Open formulas and thresholds"
        items={[
          {
            title: "Separated grains",
            value: "Minute, day, contagion, coverage",
            description:
              "The warehouse does not collapse minute facts, daily summaries, contagion windows, and monitoring coverage into one ambiguous table. That is why rollups stay defensible.",
            tone: "accent",
          },
          {
            title: "Operational lineage",
            value: "Cassandra -> staging -> facts",
            description:
              "Every warehouse fact originates from operational records first, then passes through ETL staging and key assignment before it becomes analytical.",
            tone: "warning",
          },
          {
            title: "Crore-scale story",
            value: formatCompactIndian(loadedWindowProjection, 2),
            description:
              "The loaded historical window already projects into multi-crore tick-plus-anomaly rows, and the annual path scales far beyond that without changing the analytical model.",
            tone: "critical",
          },
          {
            title: "Deterministic demos",
            value: `${latestReplaySymbols} symbols / ${latestReplayRows} rows`,
            description:
              "Replay reuses the same pipeline as ingestion, so the architecture remains demonstrable and testable even when the market is closed.",
          },
        ]}
      />

      {latestEtlFailed ? (
        <section className="surface">
          <div className="statusNote warning">
            The latest ETL attempt recorded in system health is a failed maintenance rerun for{" "}
            {latestEtlAttempt?.trading_date ? formatDate(latestEtlAttempt.trading_date) : "an unknown date"}.
            The warehouse itself remains populated through{" "}
            {summary?.last_calendar_date ? formatDate(summary.last_calendar_date) : "the current loaded horizon"}, and the most recent successful ETL load inserted{" "}
            {formatCompactIndian(latestCompletedEtlRows, 2)} rows for{" "}
            {latestCompletedEtlDate ? formatDate(latestCompletedEtlDate) : "the last completed run"}.
          </div>
        </section>
      ) : null}
    </>
  );
}
