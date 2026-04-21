"use client";

import { useEffect, useMemo, useRef, useState } from "react";

import { apiUrl, type AlertItem } from "../lib/api";
import { formatDate, formatDateTime, severityLabel } from "../lib/format";

type AlertResponse = {
  items: AlertItem[];
  open_count: number;
  active_open_count?: number;
  stale_open_count?: number;
  display_scope?: string;
  current_trading_date?: string | null;
  scope_reference_date?: string | null;
};

const STORAGE_KEY = "market-surveillance-seen-alerts";

function loadSeen(): Set<string> {
  if (typeof window === "undefined") {
    return new Set();
  }
  const raw = window.localStorage.getItem(STORAGE_KEY);
  if (!raw) {
    return new Set();
  }
  try {
    return new Set(JSON.parse(raw) as string[]);
  } catch {
    return new Set();
  }
}

function saveSeen(seen: Set<string>) {
  window.localStorage.setItem(STORAGE_KEY, JSON.stringify([...seen]));
}

export function AlertsBell() {
  const [open, setOpen] = useState(false);
  const [alerts, setAlerts] = useState<AlertItem[]>([]);
  const [openCount, setOpenCount] = useState(0);
  const [activeOpenCount, setActiveOpenCount] = useState(0);
  const [staleOpenCount, setStaleOpenCount] = useState(0);
  const [displayScope, setDisplayScope] = useState("current");
  const [scopeReferenceDate, setScopeReferenceDate] = useState<string | null>(null);
  const seenRef = useRef<Set<string>>(new Set());

  async function refresh() {
    try {
      const response = await fetch(apiUrl("/api/alerts/live?limit=12"));
      if (!response.ok) {
        return;
      }
      const payload = (await response.json()) as AlertResponse;
      setAlerts(payload.items ?? []);
      setOpenCount(payload.open_count ?? 0);
      setActiveOpenCount(payload.active_open_count ?? 0);
      setStaleOpenCount(payload.stale_open_count ?? 0);
      setDisplayScope(payload.display_scope ?? "current");
      setScopeReferenceDate(payload.scope_reference_date ?? null);
    } catch {}
  }

  useEffect(() => {
    seenRef.current = loadSeen();
    void refresh();
    const handle = window.setInterval(() => void refresh(), 20000);
    return () => window.clearInterval(handle);
  }, []);

  useEffect(() => {
    if (!("Notification" in window) || Notification.permission !== "granted") {
      return;
    }
    let changed = false;
    for (const alert of alerts) {
      if (alert.is_stale) {
        continue;
      }
      if (seenRef.current.has(alert.event_id)) {
        continue;
      }
      seenRef.current.add(alert.event_id);
      changed = true;
      new Notification(alert.title, {
        body: `${alert.symbol} | ${alert.message}`,
      });
    }
    if (changed) {
      saveSeen(seenRef.current);
    }
  }, [alerts]);

  async function requestNotifications() {
    if (!("Notification" in window)) {
      return;
    }
    if (Notification.permission === "default") {
      await Notification.requestPermission();
    }
  }

  async function acknowledge(eventId: string) {
    try {
      await fetch(apiUrl(`/api/alerts/${eventId}/ack`), { method: "POST" });
      void refresh();
    } catch {}
  }

  const label = useMemo(() => (openCount > 99 ? "99+" : String(openCount)), [openCount]);
  const summaryText =
    displayScope === "stale"
      ? `No current-session open alerts. Showing ${staleOpenCount} unresolved historical alerts from ${formatDate(scopeReferenceDate)}.`
      : activeOpenCount
        ? `${activeOpenCount} current-session alerts${staleOpenCount ? ` | ${staleOpenCount} historical unresolved` : ""}`
        : "No current-session alerts.";

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

  return (
    <div className="alertsShell">
      <button type="button" className="alertsButton" onClick={() => setOpen((value) => !value)}>
        Alerts
        <span className="alertCount">{label}</span>
      </button>
      {open ? (
        <div className="alertsPanel">
          <div className="alertsHeader">
            <strong>Open alerts</strong>
            <button type="button" className="linkButton" onClick={() => void requestNotifications()}>
              Desktop
            </button>
          </div>
          <div className="alertsSummary">{summaryText}</div>
          {alerts.length ? (
            <div className="alertsList">
              {alerts.map((alert) => (
                <div key={alert.event_id} className={`alertRow ${alert.severity}`}>
                  <div className="alertBody">
                    <div className="alertTitleRow">
                      <strong>{alert.symbol}</strong>
                      <div className="toolbarGroup">
                        {alert.is_stale ? <span className="severityBadge stale">stale</span> : null}
                        <span className={`severityBadge ${severityClass(alert.severity)}`}>{alert.severity}</span>
                      </div>
                    </div>
                    <p>{alert.message}</p>
                    <small>{formatDateTime(alert.detected_at)}</small>
                  </div>
                  <button type="button" className="iconAction" onClick={() => void acknowledge(alert.event_id)}>
                    Ack
                  </button>
                </div>
              ))}
            </div>
          ) : (
            <div className="alertsEmpty">No open alerts</div>
          )}
        </div>
      ) : null}
    </div>
  );
}
