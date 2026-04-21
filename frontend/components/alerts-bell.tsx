"use client";

import { useEffect, useMemo, useRef, useState } from "react";

import { apiUrl, type AlertItem } from "../lib/api";
import { formatDateTime, severityLabel } from "../lib/format";

type AlertResponse = {
  items: AlertItem[];
  open_count: number;
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
          {alerts.length ? (
            <div className="alertsList">
              {alerts.map((alert) => (
                <div key={alert.event_id} className={`alertRow ${alert.severity}`}>
                  <div className="alertBody">
                    <div className="alertTitleRow">
                      <strong>{alert.symbol}</strong>
                      <span className={`severityBadge ${severityClass(alert.severity)}`}>{alert.severity}</span>
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
