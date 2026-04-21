"use client";

import Link from "next/link";
import { useDeferredValue, useMemo, useState } from "react";

import type { ScreenerItem } from "../lib/api";
import { formatNumber, formatPercent, severityLabel } from "../lib/format";

type StocksScreenerProps = {
  items: ScreenerItem[];
};

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
          />
        </div>
        <div className="toolbarGroup">
          <select className="toolbarSelect" value={sector} onChange={(event) => setSector(event.target.value)}>
            <option value="all">All sectors</option>
            {sectors.map((item) => (
              <option key={item} value={item}>
                {item}
              </option>
            ))}
          </select>
          <select className="toolbarSelect" value={status} onChange={(event) => setStatus(event.target.value)}>
            <option value="all">All states</option>
            <option value="open">Open alerts</option>
            <option value="anomalous">Live anomalies</option>
            <option value="active">Any live signal</option>
            <option value="quiet">Quiet names</option>
          </select>
          <select className="toolbarSelect" value={sort} onChange={(event) => setSort(event.target.value)}>
            <option value="priority">Priority</option>
            <option value="return">20D return</option>
            <option value="rsi">RSI 14</option>
            <option value="volume">Volume ratio</option>
            <option value="name">Alphabetical</option>
          </select>
        </div>
      </div>

      <div className="resultMeta">
        <span>{filtered.length} visible</span>
        <span>{items.length} tracked</span>
      </div>

      {filtered.length ? (
        <div className="tableWrap">
          <table className="dataTable">
            <thead>
              <tr>
                <th>Symbol</th>
                <th>Company</th>
                <th>Sector</th>
                <th>Last close</th>
                <th>1D</th>
                <th>20D</th>
                <th>RSI</th>
                <th>Vol ratio</th>
                <th>Signal</th>
              </tr>
            </thead>
            <tbody>
              {filtered.map((item) => (
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
