"use client";

import Link from "next/link";
import { useDeferredValue, useEffect, useMemo, useState } from "react";

import { apiUrl, type ReferenceStocksResponse, type SectorOption, type StockReference } from "../lib/api";
import { formatDate } from "../lib/format";

type UniverseDirectoryProps = {
  initialStocks: StockReference[];
  initialTotalCount: number;
  initialFilteredCount: number;
  watchlistCount: number;
  hydratedCount: number;
  initialKnownSectorCount: number;
  initialUnknownSectorCount: number;
  initialSectorOptions: SectorOption[];
};

const PAGE_SIZE = 100;

function buildPath(query: string, filter: string, sectorState: string, sector: string, page: number) {
  const params = new URLSearchParams({
    limit: String(PAGE_SIZE),
    offset: String(page * PAGE_SIZE),
  });
  if (query) {
    params.set("q", query);
  }
  if (filter === "watchlist") {
    params.set("watchlist_only", "true");
  }
  if (filter === "hydrated") {
    params.set("history_state", "hydrated");
  }
  if (filter === "unhydrated") {
    params.set("history_state", "unhydrated");
  }
  if (sectorState !== "all") {
    params.set("sector_state", sectorState);
  }
  if (sector) {
    params.set("sector", sector);
  }
  return apiUrl(`/api/reference/stocks?${params.toString()}`);
}

export function UniverseDirectory({
  initialStocks,
  initialTotalCount,
  initialFilteredCount,
  watchlistCount,
  hydratedCount,
  initialKnownSectorCount,
  initialUnknownSectorCount,
  initialSectorOptions,
}: UniverseDirectoryProps) {
  const [query, setQuery] = useState("");
  const [filter, setFilter] = useState("all");
  const [sectorState, setSectorState] = useState("all");
  const [sector, setSector] = useState("");
  const [page, setPage] = useState(0);
  const [rows, setRows] = useState<StockReference[]>(initialStocks);
  const [filteredCount, setFilteredCount] = useState(initialFilteredCount);
  const [knownSectorCount, setKnownSectorCount] = useState(initialKnownSectorCount);
  const [unknownSectorCount, setUnknownSectorCount] = useState(initialUnknownSectorCount);
  const [sectorOptions, setSectorOptions] = useState<SectorOption[]>(initialSectorOptions);
  const [loading, setLoading] = useState(false);
  const deferredQuery = useDeferredValue(query.trim());

  useEffect(() => {
    setPage(0);
  }, [deferredQuery, filter, sectorState, sector]);

  useEffect(() => {
    if (sectorState === "unknown" && sector) {
      setSector("");
    }
  }, [sectorState, sector]);

  useEffect(() => {
    let active = true;
    const controller = new AbortController();

    async function load() {
      setLoading(true);
      try {
        const response = await fetch(buildPath(deferredQuery, filter, sectorState, sector, page), {
          signal: controller.signal,
          cache: "no-store",
        });
        if (!response.ok) {
          return;
        }
        const payload = (await response.json()) as ReferenceStocksResponse;
        if (!active) {
          return;
        }
        setRows(payload.stocks ?? []);
        setFilteredCount(payload.filtered_count ?? 0);
        setKnownSectorCount(payload.known_sector_count ?? 0);
        setUnknownSectorCount(payload.unknown_sector_count ?? 0);
        setSectorOptions(payload.sector_options ?? []);
      } catch {}
      if (active) {
        setLoading(false);
      }
    }

    void load();

    return () => {
      active = false;
      controller.abort();
    };
  }, [deferredQuery, filter, sectorState, sector, page]);

  const totalPages = useMemo(() => Math.max(Math.ceil(filteredCount / PAGE_SIZE), 1), [filteredCount]);
  const startRow = filteredCount ? page * PAGE_SIZE + 1 : 0;
  const endRow = Math.min((page + 1) * PAGE_SIZE, filteredCount);
  const classifiedOptions = useMemo(
    () => sectorOptions.filter((option) => option.known && option.sector !== "Unknown"),
    [sectorOptions],
  );

  return (
    <div className="stackList">
      <div className="toolbarRow">
        <div className="toolbarGroup grow">
          <input
            className="toolbarInput"
            value={query}
            onChange={(event) => setQuery(event.target.value)}
            placeholder="Search the NSE universe"
            aria-label="Search NSE universe"
          />
        </div>
        <div className="toolbarGroup">
          <select className="toolbarSelect" value={filter} onChange={(event) => setFilter(event.target.value)}>
            <option value="all">All listed</option>
            <option value="watchlist">Watchlist</option>
            <option value="hydrated">Hydrated</option>
            <option value="unhydrated">Not hydrated yet</option>
          </select>
        </div>
        <div className="toolbarGroup">
          <select className="toolbarSelect" value={sectorState} onChange={(event) => setSectorState(event.target.value)}>
            <option value="all">All sectors</option>
            <option value="known">Classified only</option>
            <option value="unknown">Unknown only</option>
          </select>
        </div>
        <div className="toolbarGroup">
          <select
            className="toolbarSelect"
            value={sector}
            onChange={(event) => setSector(event.target.value)}
            disabled={sectorState === "unknown" || !classifiedOptions.length}
          >
            <option value="">All classified sectors</option>
            {classifiedOptions.map((option) => (
              <option key={option.sector} value={option.sector}>
                {option.sector} ({option.count})
              </option>
            ))}
          </select>
        </div>
      </div>

      <div className="resultMeta">
        <span>{initialTotalCount} listed</span>
        <span>{hydratedCount} with history</span>
        <span>{watchlistCount} watchlist</span>
        <span>{knownSectorCount} classified</span>
        <span>{unknownSectorCount} unknown</span>
        <span>
          {startRow}-{endRow} of {filteredCount || 0}
        </span>
        {loading ? <span>Refreshing...</span> : null}
      </div>

      {rows.length ? (
        <div className="tableWrap">
          <table className="dataTable">
            <thead>
              <tr>
                <th>Symbol</th>
                <th>Company</th>
                <th>Sector</th>
                <th>Series</th>
                <th>History</th>
                <th>Last session</th>
                <th>Mode</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((stock) => (
                <tr key={stock.symbol}>
                  <td>
                    <Link href={`/stocks/${encodeURIComponent(stock.symbol)}`} className="tableLink">
                      {stock.symbol}
                    </Link>
                  </td>
                  <td>{stock.company_name}</td>
                  <td>{stock.sector ?? "Unknown"}</td>
                  <td>{String(stock.metadata?.series ?? "EQ")}</td>
                  <td>{stock.has_history ? String(stock.daily_bar_count ?? 0) : "Pending"}</td>
                  <td>{stock.last_daily_date ? formatDate(stock.last_daily_date) : "Not loaded"}</td>
                  <td>
                    <span className={`severityTag ${stock.watchlist ? "medium" : "low"}`}>
                      {stock.watchlist ? "watchlist" : "directory"}
                    </span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        <div className="emptyState">No listed symbols match the current universe filters.</div>
      )}

      <div className="toolbarRow">
        <div className="resultMeta">
          <span>
            Page {Math.min(page + 1, totalPages)} of {totalPages}
          </span>
          <span>{classifiedOptions.length} classified sectors available</span>
        </div>
        <div className="toolbarGroup">
          <button
            type="button"
            className="actionButton"
            disabled={page <= 0 || loading}
            onClick={() => setPage((current) => Math.max(current - 1, 0))}
          >
            Previous
          </button>
          <button
            type="button"
            className="actionButton"
            disabled={page + 1 >= totalPages || loading}
            onClick={() => setPage((current) => current + 1)}
          >
            Next
          </button>
        </div>
      </div>
    </div>
  );
}
