"use client";

import Link from "next/link";
import { useDeferredValue, useMemo, useState } from "react";

import { InfoHint } from "./info-hint";
import type { LatestMarket } from "../lib/api";
import { formatCompactIndian, formatDate, formatDateTime, formatNumber, formatTime } from "../lib/format";

type LiveTapePanelProps = {
  items: LatestMarket[];
};

const DEFAULT_LIMIT = "25";

function numeric(value: number | null | undefined, fallback = -9999) {
  return value === null || value === undefined || Number.isNaN(value) ? fallback : value;
}

function timestampMs(value: string | null | undefined) {
  if (!value) {
    return null;
  }
  const parsed = new Date(value).getTime();
  return Number.isFinite(parsed) ? parsed : null;
}

export function LiveTapePanel({ items }: LiveTapePanelProps) {
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

  const snapshot = useMemo(() => {
    const counts = new Map<string, number>();
    let latestLabel: string | null = null;
    let latestMs: number | null = null;

    for (const item of items) {
      const label = item.timestamp_ist;
      if (!label) {
        continue;
      }
      const parsedMs = timestampMs(label);
      if (parsedMs === null) {
        continue;
      }
      counts.set(label, (counts.get(label) ?? 0) + 1);
      if (latestMs === null || parsedMs > latestMs) {
        latestMs = parsedMs;
        latestLabel = label;
      }
    }

    const alignedCount = latestLabel ? (counts.get(latestLabel) ?? 0) : 0;
    return {
      latestLabel,
      latestMs,
      uniqueCount: counts.size,
      alignedCount,
      laggingCount: Math.max(items.length - alignedCount, 0),
    };
  }, [items]);

  const filtered = useMemo(() => {
    const next = items.filter((item) => {
      const companyName = item.company_name ?? "";
      const haystack = [item.symbol, companyName, item.sector, item.exchange].join(" ").toLowerCase();
      if (deferredQuery && !haystack.includes(deferredQuery)) {
        return false;
      }
      if (sector !== "all" && item.sector !== sector) {
        return false;
      }
      if (status === "flagged" && !item.is_anomalous) {
        return false;
      }
      if (status === "normal" && item.is_anomalous) {
        return false;
      }
      return true;
    });

    next.sort((left, right) => {
      if (sort === "score") {
        return numeric(right.composite_score) - numeric(left.composite_score) || left.symbol.localeCompare(right.symbol);
      }
      if (sort === "volume") {
        return numeric(right.volume) - numeric(left.volume) || left.symbol.localeCompare(right.symbol);
      }
      if (sort === "symbol") {
        return left.symbol.localeCompare(right.symbol);
      }
      return (
        Number(Boolean(right.is_anomalous)) - Number(Boolean(left.is_anomalous)) ||
        numeric(right.composite_score) - numeric(left.composite_score) ||
        left.sector.localeCompare(right.sector) ||
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

  const flaggedVisible = filtered.filter((item) => item.is_anomalous).length;
  const latestTradingDate = filtered[0]?.trading_date ?? items[0]?.trading_date;
  const strongestSymbol = filtered[0]?.symbol ?? "N/A";
  const hasFilters = Boolean(query.trim()) || sector !== "all" || status !== "all" || sort !== "priority" || limit !== DEFAULT_LIMIT;
  const snapshotIsUniform = snapshot.uniqueCount <= 1;
  const activeSummary = [
    deferredQuery ? `matching "${query.trim()}"` : "across the live universe",
    sector !== "all" ? sector : `${sectors.length || 0} sectors`,
    status === "flagged" ? "flagged only" : status === "normal" ? "normal only" : "all states",
    sort === "score" ? "sorted by score" : sort === "volume" ? "sorted by volume" : sort === "symbol" ? "sorted alphabetically" : "priority sorted",
    limit === "all" ? "full list" : `top ${limit}`,
  ].join(" | ");

  function renderFreshness(item: LatestMarket) {
    const rowMs = timestampMs(item.timestamp_ist);
    if (rowMs === null || snapshot.latestMs === null) {
      return (
        <div className="tapeFreshness">
          <span className="tapeFreshnessLabel">N/A</span>
        </div>
      );
    }

    const lagMinutes = Math.max(Math.round((snapshot.latestMs - rowMs) / 60_000), 0);
    if (lagMinutes === 0) {
      return (
        <div className="tapeFreshness">
          <span className="tapeFreshnessLabel">Latest</span>
          {!snapshotIsUniform ? <span className="tapeFreshnessMeta">{formatTime(item.timestamp_ist)}</span> : null}
        </div>
      );
    }

    return (
      <div className="tapeFreshness">
        <span className="tapeFreshnessLabel">{lagMinutes}m behind</span>
        <span className="tapeFreshnessMeta">{formatTime(item.timestamp_ist)}</span>
      </div>
    );
  }

  function resetFilters() {
    setQuery("");
    setSector("all");
    setStatus("all");
    setSort("priority");
    setLimit(DEFAULT_LIMIT);
  }

  function applyPreset(preset: "all" | "flagged" | "score" | "volume") {
    if (preset === "all") {
      resetFilters();
      return;
    }
    setQuery("");
    setSector("all");
    setLimit(DEFAULT_LIMIT);
    if (preset === "flagged") {
      setStatus("flagged");
      setSort("score");
      return;
    }
    setStatus("all");
    setSort(preset === "score" ? "score" : "volume");
  }

  return (
    <div className="stackList">
      <div className="toolbarRow">
        <div className="toolbarGroup grow">
          <input
            className="toolbarInput"
            value={query}
            onChange={(event) => setQuery(event.target.value)}
            placeholder="Filter symbol, company, sector, or exchange"
            aria-label="Filter latest tape"
            title="Search the latest market snapshot by symbol, company, sector, or exchange."
          />
        </div>
        <div className="toolbarGroup">
          <select
            className="toolbarSelect"
            value={sector}
            onChange={(event) => setSector(event.target.value)}
            title="Restrict the snapshot to one sector."
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
            title="Filter the tape by live signal state."
          >
            <option value="all">All states</option>
            <option value="flagged">Flagged only</option>
            <option value="normal">Normal only</option>
          </select>
          <select
            className="toolbarSelect"
            value={sort}
            onChange={(event) => setSort(event.target.value)}
            title="Sort the latest snapshot by surveillance priority, score, volume, or symbol."
          >
            <option value="priority">Priority</option>
            <option value="score">Composite score</option>
            <option value="volume">Volume</option>
            <option value="symbol">Alphabetical</option>
          </select>
          <select
            className="toolbarSelect"
            value={limit}
            onChange={(event) => setLimit(event.target.value)}
            title="Choose how many snapshot rows to render."
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
            title="Reset the live-tape search, state filters, sorting, and row count."
          >
            Reset
          </button>
        </div>
      </div>

      <div className="filterPills">
        <div className="filterPillCluster">
          <button
            type="button"
            className={`filterPill ${!hasFilters ? "active" : ""}`}
            onClick={() => applyPreset("all")}
            title="Default market snapshot view with priority ordering and no extra filters."
          >
            Live default
          </button>
          <InfoHint content="The default market snapshot ordering: flagged symbols first, then higher composite scores, with no extra filters applied." label="Live default definition" />
        </div>
        <div className="filterPillCluster">
          <button
            type="button"
            className={`filterPill ${status === "flagged" ? "active" : ""}`}
            onClick={() => applyPreset("flagged")}
            title="Only symbols whose latest minute is flagged by the anomaly engine."
          >
            Flagged only
          </button>
          <InfoHint content="Shows only symbols whose latest minute is currently crossing anomaly thresholds." label="Flagged only definition" />
        </div>
        <div className="filterPillCluster">
          <button
            type="button"
            className={`filterPill ${status === "all" && sort === "score" ? "active" : ""}`}
            onClick={() => applyPreset("score")}
            title="Sort the latest snapshot by composite anomaly score."
          >
            Highest score
          </button>
          <InfoHint content="Ranks the snapshot by composite anomaly score, which blends price surprise and volume surprise." label="Highest score definition" />
        </div>
        <div className="filterPillCluster">
          <button
            type="button"
            className={`filterPill ${status === "all" && sort === "volume" ? "active" : ""}`}
            onClick={() => applyPreset("volume")}
            title="Sort the latest snapshot by raw traded volume."
          >
            Highest volume
          </button>
          <InfoHint content="Ranks the latest snapshot by raw traded volume so you can inspect the heaviest names first." label="Highest volume definition" align="end" />
        </div>
      </div>

      <div className="resultMeta">
        <span>{visible.length} shown</span>
        <span>{filtered.length} matched</span>
        <span>{flaggedVisible} flagged</span>
        <span>{latestTradingDate ? formatDate(latestTradingDate) : "No trading date"}</span>
        <span>{snapshot.latestLabel ? `Snapshot ${formatTime(snapshot.latestLabel)}` : "No snapshot minute"}</span>
        <span>Lead symbol {strongestSymbol}</span>
      </div>

      <div className="resultSummary">{activeSummary}</div>

      {snapshot.latestLabel ? (
        <div className="statusNote">
          {snapshotIsUniform
            ? `This tape is a synchronized market snapshot. All ${items.length.toLocaleString("en-IN")} rows are showing the same last available minute, ${formatDateTime(snapshot.latestLabel)}.`
            : `This tape is a synchronized market snapshot anchored to ${formatDateTime(snapshot.latestLabel)}. ${snapshot.alignedCount.toLocaleString("en-IN")} symbols are on the latest minute and ${snapshot.laggingCount.toLocaleString("en-IN")} are behind it.`}
        </div>
      ) : null}

      {visible.length ? (
        <div className="tableWrap tableWrapScrollY">
          <table className="dataTable stickyHeaderTable liveTapeTable">
            <thead>
              <tr>
                <th>Symbol</th>
                <th>Company</th>
                <th>Sector</th>
                <th>Last</th>
                <th title="Composite anomaly score for the latest minute. Higher values mean the bar is more unusual relative to recent behavior.">Score</th>
                <th title="Flagged means the latest minute crossed an anomaly rule. Normal means it stayed below all configured thresholds.">Status</th>
                <th>Volume</th>
                <th title={snapshotIsUniform ? "When the whole tape is aligned to one minute, this column confirms whether a symbol is on the latest shared snapshot." : "Shows the timestamp of the latest bar contributing to that symbol's current snapshot row."}>
                  {snapshotIsUniform ? "Freshness" : "Bar time"}
                </th>
              </tr>
            </thead>
            <tbody>
              {visible.map((item) => (
                <tr key={`${item.symbol}-${item.timestamp_ist}`} className={item.is_anomalous ? "dataRowAlert" : undefined}>
                  <td>
                    <Link href={`/stocks/${encodeURIComponent(item.symbol)}`} className="tableLink">
                      {item.symbol}
                    </Link>
                  </td>
                  <td>{item.company_name ?? item.symbol}</td>
                  <td>{item.sector}</td>
                  <td>{formatNumber(item.close)}</td>
                  <td>{formatNumber(item.composite_score, 3)}</td>
                  <td>
                    <span className={`severityTag ${item.is_anomalous ? "critical" : "low"}`}>
                      {item.is_anomalous ? "flagged" : "normal"}
                    </span>
                  </td>
                  <td>{formatCompactIndian(item.volume, 1)}</td>
                  <td>{renderFreshness(item)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        <div className="emptyState">No live rows match the current tape filters.</div>
      )}
    </div>
  );
}
