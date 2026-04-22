"use client";

import Link from "next/link";
import { useDeferredValue, useMemo, useState } from "react";

import { InfoHint } from "./info-hint";
import type { ScreenerItem } from "../lib/api";
import { formatNumber, formatPercent, severityLabel } from "../lib/format";

type StocksScreenerProps = {
  items: ScreenerItem[];
};

const DEFAULT_LIMIT = "25";

function severityClass(value: string | null | undefined) {
  const severity = severityLabel(value);
  if (severity === "critical") {
    return "critical";
  }
  if (severity === "high") {
    return "high";
  }
  if (severity === "medium") {
    return "medium";
  }
  return "low";
}

function numeric(value: number | null | undefined, fallback = -9999) {
  return value === null || value === undefined || Number.isNaN(value) ? fallback : value;
}

export function StocksScreener({ items }: StocksScreenerProps) {
  const [query, setQuery] = useState("");
  const [sector, setSector] = useState("all");
  const [status, setStatus] = useState("all");
  const [sort, setSort] = useState("priority");
  const [limit, setLimit] = useState(DEFAULT_LIMIT);
  const deferredQuery = useDeferredValue(query.trim().toLowerCase());

  const sectors = useMemo(() => {
    const values = new Set<string>();
    for (const item of items) {
      if (item.sector) {
        values.add(item.sector);
      }
    }
    return [...values].sort((left, right) => left.localeCompare(right));
  }, [items]);

  const filtered = useMemo(() => {
    const severityRank: Record<string, number> = { critical: 0, high: 1, medium: 2, low: 3 };
    const next = items.filter((item) => {
      const haystack = [
        item.symbol,
        item.company_name,
        item.sector ?? "",
        item.exchange ?? "",
      ]
        .join(" ")
        .toLowerCase();

      if (deferredQuery && !haystack.includes(deferredQuery)) {
        return false;
      }
      if (sector !== "all" && item.sector !== sector) {
        return false;
      }
      if (status === "open" && item.latest_alert?.status !== "open") {
        return false;
      }
      if (status === "anomalous" && !item.latest_anomaly?.is_anomalous) {
        return false;
      }
      if (status === "active" && item.latest_alert?.status !== "open" && !item.latest_anomaly?.is_anomalous) {
        return false;
      }
      if (status === "quiet" && (item.latest_alert?.status === "open" || item.latest_anomaly?.is_anomalous)) {
        return false;
      }
      return true;
    });

    next.sort((left, right) => {
      if (sort === "return") {
        return numeric(right.indicators.return_20d_pct) - numeric(left.indicators.return_20d_pct);
      }
      if (sort === "rsi") {
        return numeric(right.indicators.rsi_14) - numeric(left.indicators.rsi_14);
      }
      if (sort === "volume") {
        return numeric(right.indicators.volume_ratio_20d) - numeric(left.indicators.volume_ratio_20d);
      }
      if (sort === "name") {
        return left.symbol.localeCompare(right.symbol);
      }
      return (
        (severityRank[severityLabel(left.latest_alert?.severity)] ?? 99) -
          (severityRank[severityLabel(right.latest_alert?.severity)] ?? 99) ||
        (right.latest_anomaly?.is_anomalous ? 1 : 0) - (left.latest_anomaly?.is_anomalous ? 1 : 0) ||
        numeric(right.latest_anomaly?.composite_score) - numeric(left.latest_anomaly?.composite_score) ||
        numeric(right.indicators.return_20d_pct) - numeric(left.indicators.return_20d_pct) ||
        left.symbol.localeCompare(right.symbol)
      );
    });

    return next;
  }, [deferredQuery, items, sector, sort, status]);

  const visible = useMemo(() => {
    if (limit === "all") {
      return filtered;
    }
    const parsed = Number(limit);
    return filtered.slice(0, Number.isFinite(parsed) ? parsed : 25);
  }, [filtered, limit]);

  const openCount = filtered.filter((item) => item.latest_alert?.status === "open").length;
  const anomalousCount = filtered.filter((item) => item.latest_anomaly?.is_anomalous).length;
  const hasFilters = Boolean(query.trim()) || sector !== "all" || status !== "all" || sort !== "priority" || limit !== DEFAULT_LIMIT;
  const activeSummary = [
    deferredQuery ? `matching "${query.trim()}"` : "across hydrated names",
    sector !== "all" ? sector : `${sectors.length || 0} sectors`,
    status === "open"
      ? "open alerts only"
      : status === "anomalous"
        ? "live anomalies only"
        : status === "active"
          ? "active signals only"
          : status === "quiet"
            ? "quiet names only"
            : "all states",
    sort === "return"
      ? "sorted by 20D return"
      : sort === "rsi"
        ? "sorted by RSI"
        : sort === "volume"
          ? "sorted by volume ratio"
          : sort === "name"
            ? "sorted alphabetically"
            : "priority sorted",
    limit === "all" ? "full list" : `top ${limit}`,
  ].join(" | ");

  function resetFilters() {
    setQuery("");
    setSector("all");
    setStatus("all");
    setSort("priority");
    setLimit(DEFAULT_LIMIT);
  }

  function applyPreset(preset: "priority" | "open" | "anomalous" | "quiet" | "return") {
    setQuery("");
    setSector("all");
    setLimit(DEFAULT_LIMIT);
    if (preset === "priority") {
      setStatus("all");
      setSort("priority");
      return;
    }
    if (preset === "return") {
      setStatus("all");
      setSort("return");
      return;
    }
    setStatus(preset);
    setSort(preset === "quiet" ? "name" : "priority");
  }

  return (
    <div className="stackList">
      <div className="toolbarRow">
        <div className="toolbarGroup grow">
          <input
            className="toolbarInput"
            value={query}
            onChange={(event) => setQuery(event.target.value)}
            placeholder="Filter symbol, company, or sector"
            aria-label="Filter stocks"
            title="Search across hydrated stock workspaces by symbol, company, exchange, or sector."
          />
        </div>
        <div className="toolbarGroup">
          <select
            className="toolbarSelect"
            value={sector}
            onChange={(event) => setSector(event.target.value)}
            title="Restrict the screener to a single sector."
          >
            <option value="all">All sectors</option>
            {sectors.map((item) => (
              <option key={item} value={item}>
                {item}
              </option>
            ))}
          </select>
          <select
            className="toolbarSelect"
            value={status}
            onChange={(event) => setStatus(event.target.value)}
            title="Filter by live surveillance state: open alerts, anomalous names, active names, or quiet names."
          >
            <option value="all">All states</option>
            <option value="open">Open alerts</option>
            <option value="anomalous">Live anomalies</option>
            <option value="active">Any live signal</option>
            <option value="quiet">Quiet names</option>
          </select>
          <select
            className="toolbarSelect"
            value={sort}
            onChange={(event) => setSort(event.target.value)}
            title="Choose whether the table is ordered by surveillance priority, return, RSI, volume ratio, or symbol."
          >
            <option value="priority">Priority</option>
            <option value="return">20D return</option>
            <option value="rsi">RSI 14</option>
            <option value="volume">Volume ratio</option>
            <option value="name">Alphabetical</option>
          </select>
          <select
            className="toolbarSelect"
            value={limit}
            onChange={(event) => setLimit(event.target.value)}
            title="Choose how many screener rows to show at once."
          >
            <option value="25">Top 25</option>
            <option value="50">Top 50</option>
            <option value="100">Top 100</option>
            <option value="all">All rows</option>
          </select>
          <button
            type="button"
            className="actionButton"
            onClick={resetFilters}
            disabled={!hasFilters}
            title="Reset screener search, state filters, sorting, and row count."
          >
            Reset
          </button>
        </div>
      </div>

      <div className="filterPills">
        <div className="filterPillCluster">
          <button
            type="button"
            className={`filterPill ${status === "all" && sort === "priority" && !query.trim() && sector === "all" ? "active" : ""}`}
            onClick={() => applyPreset("priority")}
            title="Default screening order: severity, anomaly state, score, and return."
          >
            Priority view
          </button>
          <InfoHint content="The default ordering: open-alert severity first, then live anomaly state, then composite score and recent return." label="Priority view definition" />
        </div>
        <div className="filterPillCluster">
          <button
            type="button"
            className={`filterPill ${status === "open" ? "active" : ""}`}
            onClick={() => applyPreset("open")}
            title="Only symbols with unresolved operator alerts."
          >
            Open alerts
          </button>
          <InfoHint content="Names with a persisted operator alert that is still unresolved in the current filter context." label="Open alerts definition" />
        </div>
        <div className="filterPillCluster">
          <button
            type="button"
            className={`filterPill ${status === "anomalous" ? "active" : ""}`}
            onClick={() => applyPreset("anomalous")}
            title="Only symbols whose latest intraday signal is anomalous."
          >
            Live anomalies
          </button>
          <InfoHint content="Names whose latest intraday signal is currently crossing at least one anomaly threshold." label="Live anomalies definition" />
        </div>
        <div className="filterPillCluster">
          <button
            type="button"
            className={`filterPill ${status === "quiet" ? "active" : ""}`}
            onClick={() => applyPreset("quiet")}
            title="Only symbols with no current alert and no live anomaly."
          >
            Quiet names
          </button>
          <InfoHint content="Hydrated symbols with no current open alert and no latest-minute anomaly flag." label="Quiet names definition" />
        </div>
        <div className="filterPillCluster">
          <button
            type="button"
            className={`filterPill ${status === "all" && sort === "return" ? "active" : ""}`}
            onClick={() => applyPreset("return")}
            title="Rank hydrated symbols by 20-session percentage return."
          >
            Top 20D return
          </button>
          <InfoHint content="Ranks names by their percentage move over the last 20 loaded trading sessions." label="Top 20D return definition" align="end" />
        </div>
      </div>

      <div className="resultMeta">
        <span>{visible.length} shown</span>
        <span>{filtered.length} matched</span>
        <span>{items.length} tracked</span>
        <span>{openCount} open alerts</span>
        <span>{anomalousCount} live anomalies</span>
      </div>

      <div className="resultSummary">{activeSummary}</div>

      {visible.length ? (
        <div className="tableWrap tableWrapScrollY">
          <table className="dataTable stickyHeaderTable">
            <thead>
              <tr>
                <th>Symbol</th>
                <th>Company</th>
                <th>Sector</th>
                <th>Last close</th>
                <th>1D</th>
                <th>20D</th>
                <th title="Relative Strength Index over 14 sessions. Higher values indicate stronger recent upside momentum.">RSI</th>
                <th title="Current volume relative to the recent baseline. Values above 1 mean participation is running hotter than usual.">Vol ratio</th>
                <th title="The strongest current surveillance state for the symbol: alert severity if present, otherwise live anomaly or normal.">Signal</th>
              </tr>
            </thead>
            <tbody>
              {visible.map((item) => (
                <tr key={item.symbol}>
                  <td>
                    <Link href={`/stocks/${encodeURIComponent(item.symbol)}`} className="tableLink">
                      {item.symbol}
                    </Link>
                  </td>
                  <td>{item.company_name}</td>
                  <td>{item.sector ?? "Unknown"}</td>
                  <td>{formatNumber(item.latest_market?.close ?? item.indicators.last_close)}</td>
                  <td>{formatPercent(item.indicators.day_change_pct)}</td>
                  <td>{formatPercent(item.indicators.return_20d_pct)}</td>
                  <td>{formatNumber(item.indicators.rsi_14, 1)}</td>
                  <td>{formatNumber(item.indicators.volume_ratio_20d, 2)}</td>
                  <td>
                    <span className={`severityTag ${severityClass(item.latest_alert?.severity)}`}>
                      {item.latest_alert?.severity ?? (item.latest_anomaly?.is_anomalous ? "live" : "normal")}
                    </span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        <div className="emptyState">No stocks match the current filters.</div>
      )}
    </div>
  );
}
