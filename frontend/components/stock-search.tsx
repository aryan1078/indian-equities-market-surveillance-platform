"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { useRouter } from "next/navigation";

import { apiUrl, type StockReference } from "../lib/api";

type SearchResponse = {
  matches: StockReference[];
};

export function StockSearch() {
  const router = useRouter();
  const shellRef = useRef<HTMLDivElement | null>(null);
  const [query, setQuery] = useState("");
  const [results, setResults] = useState<StockReference[]>([]);
  const [open, setOpen] = useState(false);

  const trimmed = query.trim();
  const endpoint = useMemo(() => {
    if (trimmed.length < 1) {
      return null;
    }
    return apiUrl(`/api/reference/search?q=${encodeURIComponent(trimmed)}`);
  }, [trimmed]);

  useEffect(() => {
    if (!endpoint || trimmed.length < 2) {
      setResults([]);
      return;
    }
    const controller = new AbortController();
    const handle = window.setTimeout(async () => {
      try {
        const response = await fetch(endpoint, { signal: controller.signal });
        if (!response.ok) {
          return;
        }
        const payload = (await response.json()) as SearchResponse;
        setResults(payload.matches ?? []);
        setOpen(true);
      } catch {
        setResults([]);
      }
    }, 180);

    return () => {
      controller.abort();
      window.clearTimeout(handle);
    };
  }, [endpoint, trimmed]);

  useEffect(() => {
    function handlePointerDown(event: MouseEvent) {
      if (!shellRef.current?.contains(event.target as Node)) {
        setOpen(false);
      }
    }

    function handleEscape(event: KeyboardEvent) {
      if (event.key === "Escape") {
        setOpen(false);
      }
    }

    document.addEventListener("mousedown", handlePointerDown);
    document.addEventListener("keydown", handleEscape);
    return () => {
      document.removeEventListener("mousedown", handlePointerDown);
      document.removeEventListener("keydown", handleEscape);
    };
  }, []);

  function navigate(symbol: string) {
    setOpen(false);
    setQuery("");
    router.push(`/stocks/${encodeURIComponent(symbol)}`);
  }

  function onSubmit(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (results.length) {
      navigate(results[0].symbol);
      return;
    }
    if (trimmed) {
      navigate(trimmed);
    }
  }

  return (
    <div className="searchShell" ref={shellRef}>
      <form className="searchBar" onSubmit={onSubmit}>
        <input
          value={query}
          onChange={(event) => setQuery(event.target.value)}
          onFocus={() => setOpen(true)}
          placeholder="Search symbol or company"
          aria-label="Search stock"
        />
      </form>
      {open && (trimmed.length >= 2 || results.length > 0) ? (
        <div className="searchResults">
          <div className="searchResultsHeader">
            <span>{results.length ? `${results.length} matches` : "Direct workspace open"}</span>
            <small>{trimmed ? `Enter ${trimmed.toUpperCase()}` : "Type a symbol or company"}</small>
          </div>
          {results.length ? (
            results.slice(0, 8).map((item) => (
              <button key={item.symbol} type="button" className="searchResult" onClick={() => navigate(item.symbol)}>
                <span>{item.symbol}</span>
                <small>
                  {item.company_name}
                  {item.exchange ? ` | ${item.exchange}` : ""}
                  {item.sector ? ` | ${item.sector}` : ""}
                  {item.watchlist ? " | Watchlist" : ""}
                </small>
              </button>
            ))
          ) : (
            <div className="searchEmpty">Press Enter to open {trimmed.toUpperCase()} directly.</div>
          )}
        </div>
      ) : null}
    </div>
  );
}
