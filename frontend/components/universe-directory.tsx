"use client";

import Link from "next/link";
import { useDeferredValue, useEffect, useMemo, useState } from "react";

import { InfoHint } from "./info-hint";
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
  const hasFilters = Boolean(query.trim()) || filter !== "all" || sectorState !== "all" || Boolean(sector);
  const activeSummary = [
    query.trim() ? `matching "${query.trim()}"` : "across the listed NSE directory",
    filter === "watchlist"
      ? "watchlist only"
      : filter === "hydrated"
        ? "hydrated only"
        : filter === "unhydrated"
          ? "needs hydration"
          : "all listing states",
    sectorState === "known" ? "classified sectors only" : sectorState === "unknown" ? "unknown sector only" : "all sector states",
    sector ? `sector ${sector}` : "all sector buckets",
  ].join(" | ");

  function resetFilters() {
    setQuery("");
    setFilter("all");
    setSectorState("all");
    setSector("");
    setPage(0);
  }

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
            title="Search across all listed NSE symbols, company names, sectors, and aliases."
          />
        </div>
        <div className="toolbarGroup">
          <select
            className="toolbarSelect"
            value={filter}
            onChange={(event) => setFilter(event.target.value)}
            title="Filter the listed universe by watchlist status or whether daily history has already been loaded."
          >
            <option value="all">All listed</option>
            <option value="watchlist">Watchlist</option>
            <option value="hydrated">Hydrated</option>
            <option value="unhydrated">Not hydrated yet</option>
          </select>
        </div>
        <div className="toolbarGroup">
          <select
            className="toolbarSelect"
            value={sectorState}
            onChange={(event) => setSectorState(event.target.value)}
            title="Filter by whether a symbol already has a resolved sector classification."
          >
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
            title="Restrict the directory to one resolved sector bucket."
          >
            <option value="">All classified sectors</option>
            {classifiedOptions.map((option) => (
              <option key={option.sector} value={option.sector}>
                {option.sector} ({option.count})
              </option>
            ))}
          </select>
          <button
            type="button"
            className="actionButton"
            onClick={resetFilters}
            disabled={!hasFilters}
            title="Reset search, listing-state, and sector filters."
          >
            Reset
          </button>
        </div>
      </div>

      <div className="filterPills">
        <div className="filterPillCluster">
          <button
            type="button"
            className={`filterPill ${filter === "all" && sectorState === "all" && !query.trim() && !sector ? "active" : ""}`}
            onClick={resetFilters}
            title="Every symbol currently present in the NSE reference directory."
          >
            All listed
          </button>
          <InfoHint content="Every symbol currently present in the NSE reference directory, whether or not history has been loaded yet." label="All listed definition" />
        </div>
        <div className="filterPillCluster">
          <button
            type="button"
            className={`filterPill ${filter === "watchlist" ? "active" : ""}`}
            onClick={() => setFilter("watchlist")}
            title="The curated subset prioritized for live demos and replay."
          >
            Watchlist
          </button>
          <InfoHint content="The curated subset used first for live demos, replay stories, and presentation-friendly monitoring." label="Watchlist definition" />
        </div>
        <div className="filterPillCluster">
          <button
            type="button"
            className={`filterPill ${filter === "hydrated" ? "active" : ""}`}
            onClick={() => setFilter("hydrated")}
            title="Symbols whose historical bars and metadata are already loaded."
          >
            Hydrated
          </button>
          <InfoHint content="Symbols whose historical daily bars and metadata are already loaded, so indicators, drill-down pages, and ETL can query them immediately." label="Hydrated definition" />
        </div>
        <div className="filterPillCluster">
          <button
            type="button"
            className={`filterPill ${filter === "unhydrated" ? "active" : ""}`}
            onClick={() => setFilter("unhydrated")}
            title="Listed symbols that still need a history load before analytics can use them."
          >
            Needs hydration
          </button>
          <InfoHint content="Listed names that exist in the directory but still need a history load before stock analytics and warehouse jobs can use them." label="Needs hydration definition" />
        </div>
        <div className="filterPillCluster">
          <button
            type="button"
            className={`filterPill ${sectorState === "unknown" ? "active" : ""}`}
            onClick={() => {
              setSector("");
              setSectorState("unknown");
            }}
            title="Symbols whose sector mapping is still unresolved."
          >
            Unknown sectors
          </button>
          <InfoHint content="Symbols whose sector mapping has not been confidently resolved yet, so peer analysis and contagion grouping stay conservative." label="Unknown sectors definition" align="end" />
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

      <div className="resultSummary">{activeSummary}</div>

      {rows.length ? (
        <div className="tableWrap tableWrapScrollY">
          <table className="dataTable stickyHeaderTable">
            <thead>
              <tr>
                <th>Symbol</th>
                <th>Company</th>
                <th>Sector</th>
                <th>Series</th>
                <th title="How many daily bars are already loaded for this symbol. Pending means the symbol has not been hydrated yet.">History</th>
                <th title="The latest trading date currently available in the loaded daily history for that symbol.">Last session</th>
                <th title="Watchlist means the symbol is in the curated live/demo subset. Directory means it is present in the broader reference universe only.">Mode</th>
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
            onClick={() => setPage(0)}
          >
            First
          </button>
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
          <button
            type="button"
            className="actionButton"
            disabled={page + 1 >= totalPages || loading}
            onClick={() => setPage(Math.max(totalPages - 1, 0))}
          >
            Last
          </button>
        </div>
      </div>
    </div>
  );
}
