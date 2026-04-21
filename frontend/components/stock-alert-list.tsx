"use client";

import { useState, useTransition } from "react";

import { apiUrl, type AlertItem } from "../lib/api";
import { formatDateTime, severityLabel } from "../lib/format";

type StockAlertListProps = {
  initialAlerts: AlertItem[];
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

export function StockAlertList({ initialAlerts }: StockAlertListProps) {
  const [alerts, setAlerts] = useState(initialAlerts);
  const [pending, startTransition] = useTransition();

  function acknowledge(eventId: string) {
    startTransition(async () => {
      try {
        const response = await fetch(apiUrl(`/api/alerts/${eventId}/ack`), { method: "POST" });
        if (!response.ok) {
          return;
        }
        const payload = (await response.json()) as Pick<AlertItem, "event_id" | "status" | "acknowledged_at">;
        setAlerts((current) =>
          current.map((alert) =>
            alert.event_id === eventId
              ? { ...alert, status: payload.status, acknowledged_at: payload.acknowledged_at }
              : alert,
          ),
        );
      } catch {}
    });
  }

  if (!alerts.length) {
    return <div className="emptyState">No alerts have been recorded for this stock.</div>;
  }

  return (
    <div className="stackList">
      {alerts.slice(0, 10).map((alert) => (
        <div key={alert.event_id} className="alertCard">
          <div className="rowBetween">
            <strong>{alert.title}</strong>
            <div className="toolbarGroup">
              {alert.is_stale ? <span className="severityTag stale">stale</span> : null}
              <span className={`severityTag ${severityClass(alert.severity)}`}>{alert.severity}</span>
            </div>
          </div>
          <div className="alertText">{alert.message}</div>
          <div className="metaRow">
            <span>{alert.event_category}</span>
            <span>{formatDateTime(alert.detected_at)}</span>
          </div>
          <div className="alertFooter">
            <span className="metricSubtext">
              {alert.status === "acknowledged"
                ? `Acknowledged ${formatDateTime(alert.acknowledged_at)}`
                : "Open"}
            </span>
            {alert.status === "open" ? (
              <button
                type="button"
                className="actionButton"
                onClick={() => acknowledge(alert.event_id)}
                disabled={pending}
              >
                Acknowledge
              </button>
            ) : null}
          </div>
        </div>
      ))}
    </div>
  );
}
