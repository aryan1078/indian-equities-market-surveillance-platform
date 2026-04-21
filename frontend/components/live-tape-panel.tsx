"use client";

import Link from "next/link";
import { useDeferredValue, useMemo, useState } from "react";

import type { LatestMarket, StockReference } from "../lib/api";
import { formatCompactIndian, formatDate, formatNumber, formatTime } from "../lib/format";

type LiveTapePanelProps = {
  items: LatestMarket[];
  referenceStocks: StockReference[];
};

function numeric(value: number | null | undefined, fallback = -9999) {
  return value === null || value === undefined || Number.isNaN(value) ? fallback : value;
}

export function LiveTapePanel({ items, referenceStocks }: LiveTapePanelProps) {
  const [query, setQuery] = useState("");
  const [sector, setSector] = useState("all");
  const [status, setStatus] = useState("all");
  const [sort, setSort] = useState("priority");
  const deferredQuery = useDeferredValue(query.trim().toLowerCase());

  const referenceMap = useMemo(
    () => new Map(referenceStocks.map((stock) => [stock.symbol, stock])),
    [referenceStocks],
  );

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
    const next = items.filter((item) => {
      const companyName = referenceMap.get(item.symbol)?.company_name ?? "";
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
  }, [deferredQuery, items, referenceMap, sector, sort, status]);

  const flaggedVisible = filtered.filter((item) => item.is_anomalous).length;
  const latestTradingDate = filtered[0]?.trading_date ?? items[0]?.trading_date;
  const strongestSymbol = filtered[0]?.symbol ?? "N/A";

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
            <option value="flagged">Flagged only</option>
            <option value="normal">Normal only</option>
          </select>
          <select className="toolbarSelect" value={sort} onChange={(event) => setSort(event.target.value)}>
            <option value="priority">Priority</option>
            <option value="score">Composite score</option>
            <option value="volume">Volume</option>
            <option value="symbol">Alphabetical</option>
          </select>
        </div>
      </div>

      <div className="resultMeta">
        <span>{filtered.length} visible</span>
        <span>{flaggedVisible} flagged</span>
        <span>{latestTradingDate ? formatDate(latestTradingDate) : "No trading date"}</span>
        <span>Lead symbol {strongestSymbol}</span>
      </div>

      <div className="tableWrap tableWrapScrollY">
        <table className="dataTable stickyHeaderTable liveTapeTable">
          <thead>
            <tr>
              <th>Symbol</th>
              <th>Company</th>
              <th>Sector</th>
              <th>Last</th>
              <th>Score</th>
              <th>Status</th>
              <th>Volume</th>
              <th>Time</th>
            </tr>
          </thead>
          <tbody>
            {filtered.map((item) => (
              <tr key={`${item.symbol}-${item.timestamp_ist}`} className={item.is_anomalous ? "dataRowAlert" : undefined}>
                <td>
                  <Link href={`/stocks/${encodeURIComponent(item.symbol)}`} className="tableLink">
                    {item.symbol}
                  </Link>
                </td>
                <td>{referenceMap.get(item.symbol)?.company_name ?? item.symbol}</td>
                <td>{item.sector}</td>
                <td>{formatNumber(item.close)}</td>
                <td>{formatNumber(item.composite_score, 3)}</td>
                <td>
                  <span className={`severityTag ${item.is_anomalous ? "critical" : "low"}`}>
                    {item.is_anomalous ? "flagged" : "normal"}
                  </span>
                </td>
                <td>{formatCompactIndian(item.volume, 1)}</td>
                <td>{formatTime(item.timestamp_ist)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
