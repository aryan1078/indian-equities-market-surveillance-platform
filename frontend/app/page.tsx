import Link from "next/link";

import { ExplainerCards } from "../components/explainer-cards";
import { LiveTapePanel } from "../components/live-tape-panel";
import { StatCard } from "../components/stat-card";
import { fetchOverview } from "../lib/api";
import {
  formatDate,
  formatDateTime,
  formatNumber,
  formatPercent,
  formatTime,
  severityLabel,
} from "../lib/format";

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

function feedModeLabel(mode: string | null | undefined) {
  if (mode === "replay") {
    return "captured session";
  }
  if (mode === "live") {
    return "live polling";
  }
  if (mode === "backfill") {
    return "historical backfill";
  }
  if (mode === "capture_replay") {
    return "capturing session";
  }
  if (mode === "hydrate_daily") {
    return "daily hydration";
  }
  return mode ?? "idle";
}

export default async function OverviewPage() {
  const overview = await fetchOverview();

  const liveMarket = overview?.live_market ?? [];
  const sectorHeatmap = overview?.sector_heatmap ?? [];
  const contagion = overview?.recent_contagion_events ?? [];
  const alerts = overview?.recent_alerts ?? [];
  const movers = overview?.top_movers ?? [];
  const anomalyAlerts = alerts.filter((alert) => alert.event_category === "anomaly");
  const highlightRows = anomalyAlerts.length ? anomalyAlerts : alerts;
  const sectorSummary = new Map<string, { sector: string; count: number; score: number }>();

  for (const alert of alerts) {
    const sector =
      typeof alert.event_payload?.sector === "string" ? String(alert.event_payload.sector) : "Unknown";
    const score = Number(alert.composite_score ?? 0);
    const current = sectorSummary.get(sector) ?? { sector, count: 0, score: 0 };
    current.count += 1;
    current.score = Math.max(current.score, score);
    sectorSummary.set(sector, current);
  }

  const sectorCards = sectorSummary.size
    ? [...sectorSummary.values()].sort((left, right) => right.score - left.score)
    : sectorHeatmap.map((item) => ({
        sector: item.sector,
        count: item.active_anomalies,
        score: item.avg_composite_score,
      }));
  const liveFlaggedCount = liveMarket.filter((item) => item.is_anomalous).length;
  const topSector = sectorCards[0];
  const staleAlertCount = overview?.stale_open_alert_count ?? 0;
  const currentAlertDate = overview?.current_alert_trading_date;
  const latestStaleAlertDate = overview?.latest_stale_alert_date;
  const shortcuts = [
    {
      href: "/stocks",
      eyebrow: "Monitor",
      title: "Stock workspace",
      metric: `${liveFlaggedCount} live flagged`,
      hint: `${overview?.hydrated_symbol_count ?? 0} hydrated names ready for drill-down`,
    },
    {
      href: "/contagion",
      eyebrow: "Investigate",
      title: "Contagion explorer",
      metric: `${contagion.length} recent events`,
      hint: topSector ? `${topSector.sector} is leading current pressure` : "No active contagion cluster",
    },
    {
      href: "/warehouse",
      eyebrow: "Analyze",
      title: "Warehouse analytics",
      metric: `${movers.length} movers highlighted`,
      hint: "Cross-session rollups and sector regime views",
    },
    {
      href: "/replay",
      eyebrow: "Operate",
      title: "Session replay",
      metric: feedModeLabel(overview?.market_mode),
      hint: overview?.as_of ? `Latest live bar ${formatDateTime(overview.as_of)}` : "Ready for captured-session replay",
    },
  ];

  return (
    <>
      <section className="heroPanel">
        <div className="pageHeader">
          <div>
            <p className="pageKicker">Overview</p>
            <h2 className="pageTitle">Market overview</h2>
          </div>
          <div className="pageMetaGroup">
            <span className="metaTag">{feedModeLabel(overview?.market_mode)}</span>
            <span className="metaTag">{overview?.as_of ? formatDateTime(overview.as_of) : "No fresh minute"}</span>
          </div>
        </div>
        <div className="statsGrid">
          <StatCard
            label="Tracked universe"
            value={String(overview?.tracked_symbol_count ?? 0)}
            info="All symbols currently present in the monitored reference universe, whether or not they are actively flagged."
            hint={`${overview?.hydrated_symbol_count ?? 0} hydrated | ${overview?.live_symbol_count ?? liveMarket.length} intraday loaded`}
          />
          <StatCard
            label="Intraday symbols loaded"
            value={String(overview?.live_symbol_count ?? liveMarket.length)}
            info="Symbols that currently have a loaded intraday minute snapshot available in the tape, whether it came from captured replay or live polling."
            hint={`${overview?.live_sector_count ?? 0} sectors active`}
            tone="accent"
          />
          <StatCard
            label="Current feed mode"
            value={feedModeLabel(overview?.market_mode)}
            info="The source mode behind the intraday tape, such as captured session replay, historical backfill, or live polling."
            hint={overview?.as_of ? formatDateTime(overview.as_of) : "No current market bar"}
          />
          <StatCard
            label="Active alerts"
            value={String(overview?.open_alert_count ?? alerts.length)}
            info="Current-session operator alerts that are still unresolved. Older unresolved rows are tracked separately as stale."
            hint={
              staleAlertCount
                ? `${staleAlertCount} historical unresolved`
                : currentAlertDate
                  ? `Current queue ${formatDate(currentAlertDate)}`
                  : `${alerts.length} recent`
            }
            tone="warning"
          />
          <StatCard
            label="Market pressure"
            value={topSector?.sector ?? "Quiet"}
            info="The sector with the strongest current concentration of alert activity or anomaly intensity."
            hint={topSector ? `${formatNumber(topSector.score, 3)} peak score | ${topSector.count} alerts` : "No sector concentration"}
            tone={topSector ? "critical" : "default"}
          />
          <StatCard
            label="Contagion events"
            value={String(contagion.length)}
            info="Persisted sector-spread events where anomalous peer names confirmed a trigger inside the contagion window."
            hint="Recent operational events"
            tone="critical"
          />
        </div>
      </section>

      <section className="contentGrid quickActionsGrid">
        {shortcuts.map((shortcut) => (
          <Link key={shortcut.href} href={shortcut.href} className="shortcutCard">
            <p className="panelEyebrow">{shortcut.eyebrow}</p>
            <h3 className="shortcutTitle">{shortcut.title}</h3>
            <div className="shortcutMetric">{shortcut.metric}</div>
            <div className="shortcutHint">{shortcut.hint}</div>
          </Link>
        ))}
      </section>

      <ExplainerCards
        eyebrow="Reading guide"
        title="What the live terms mean"
        meta="Live operators’ vocabulary"
        footerHref="/methodology"
        footerLabel="Open methodology"
        items={[
          {
            title: "Signal",
            value: "Latest state",
            description:
              "A symbol’s newest anomaly measurement. The signal turns flagged when price z, volume z, or the composite score crosses the configured threshold.",
            tone: "accent",
          },
          {
            title: "Alert",
            value: "Persisted event",
            description:
              "An operator-facing record written after the signal logic and cooldown rules run. The overview queue shows current-session alerts first and separates stale unresolved items.",
            tone: "warning",
          },
          {
            title: "Contagion",
            value: "Sector spread",
            description:
              "A five-minute sector observation window where a trigger stock is confirmed by anomalous peers, producing a propagation event with a risk score.",
            tone: "critical",
          },
          {
            title: "Warehouse",
            value: "Historical lens",
            description:
              "The analytical layer that rolls minute facts into daily, monthly, persistence, and regime views for cross-session investigation.",
          },
        ]}
      />

      <section className="contentGrid twoUp">
        <article className="surface">
          <div className="panelHeader">
            <div>
              <p className="panelEyebrow">Live market</p>
              <h3 className="panelTitle">Latest tape</h3>
            </div>
            <span className="panelMeta">
              {liveMarket.length} rows | {liveFlaggedCount} flagged
            </span>
          </div>
          {liveMarket.length ? (
            <LiveTapePanel items={liveMarket} />
          ) : (
            <div className="emptyState">No live snapshot is active. Historical analytics remain available.</div>
          )}
        </article>

        <article className="surface">
          <div className="panelHeader">
            <div>
              <p className="panelEyebrow">Alerts</p>
              <h3 className="panelTitle">Open queue</h3>
            </div>
            <span className="panelMeta">{alerts.length} current-session rows</span>
          </div>
          {alerts.length ? (
            <div className="stackList">
              {alerts.map((alert) => (
                <div key={alert.event_id} className="alertCard">
                  <div className="rowBetween">
                    <strong>{alert.symbol}</strong>
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
                </div>
              ))}
            </div>
          ) : staleAlertCount ? (
            <div className="statusNote warning">
              No current-session open alerts are active. {staleAlertCount} unresolved historical alerts remain from{" "}
              {formatDate(latestStaleAlertDate)} and are available from the alerts bell for review or acknowledgement.
            </div>
          ) : (
            <div className="emptyState">No open alerts.</div>
          )}
        </article>
      </section>

      <section className="contentGrid twoUp">
        <article className="surface">
          <div className="panelHeader">
            <div>
              <p className="panelEyebrow">Flagged stocks</p>
              <h3 className="panelTitle">Recent anomaly alerts</h3>
            </div>
            <span className="panelMeta">{highlightRows.length} rows</span>
          </div>
          {highlightRows.length ? (
            <div className="stackList">
              {highlightRows.map((item) => {
                const sector =
                  typeof item.event_payload?.sector === "string" ? String(item.event_payload.sector) : "Unknown";
                return (
                  <Link
                    key={`${item.symbol}-${item.event_id}`}
                    href={`/stocks/${encodeURIComponent(item.symbol)}`}
                    className="metricRowLink"
                  >
                    <div>
                      <strong>{item.symbol}</strong>
                      <div className="metricSubtext">{sector}</div>
                    </div>
                    <div className="metricPack">
                      <div className="metricValue">{formatNumber(item.composite_score, 3)}</div>
                      <div className="metricSubtext">{formatDateTime(item.detected_at)}</div>
                    </div>
                  </Link>
                );
              })}
            </div>
          ) : (
            <div className="emptyState">No symbols are above the configured thresholds.</div>
          )}
        </article>

        <article className="surface">
          <div className="panelHeader">
            <div>
              <p className="panelEyebrow">Sector pressure</p>
              <h3 className="panelTitle">Alert concentration</h3>
            </div>
            <span className="panelMeta">{sectorCards.length} sectors</span>
          </div>
          {sectorCards.length ? (
            <div className="cardGrid compact">
              {sectorCards.map((item) => (
                <div key={item.sector} className="miniCard">
                  <div className="rowBetween">
                    <strong>{item.sector}</strong>
                    <span className="miniTag">{item.count}</span>
                  </div>
                  <div className="cardNumber">{formatNumber(item.score, 3)}</div>
                  <div className="metricSubtext">Peak active score</div>
                </div>
              ))}
            </div>
          ) : (
            <div className="emptyState">No sector cluster is active right now.</div>
          )}
        </article>
      </section>

      <section className="contentGrid twoUp">
        <article className="surface">
          <div className="panelHeader">
            <div>
              <p className="panelEyebrow">Historical strength</p>
              <h3 className="panelTitle">20-day movers</h3>
            </div>
            <span className="panelMeta">{movers.length} names</span>
          </div>
          {movers.length ? (
            <div className="tableWrap">
              <table className="dataTable">
                <thead>
                  <tr>
                    <th>Symbol</th>
                    <th>Sector</th>
                    <th>20D</th>
                    <th>RSI</th>
                    <th>Vol ratio</th>
                    <th>Status</th>
                  </tr>
                </thead>
                <tbody>
                  {movers.map((item) => (
                    <tr key={item.symbol}>
                      <td>
                        <Link href={`/stocks/${encodeURIComponent(item.symbol)}`} className="tableLink">
                          {item.symbol}
                        </Link>
                      </td>
                      <td>{item.sector ?? "Unknown"}</td>
                      <td>{formatPercent(item.indicators.return_20d_pct)}</td>
                      <td>{formatNumber(item.indicators.rsi_14, 1)}</td>
                      <td>{formatNumber(item.indicators.volume_ratio_20d, 2)}</td>
                      <td>
                        <span className={`severityTag ${severityClass(item.latest_alert?.severity)}`}>
                          {item.latest_alert?.severity ?? "normal"}
                        </span>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <div className="emptyState">Daily history is not hydrated yet.</div>
          )}
        </article>

        <article className="surface">
          <div className="panelHeader">
            <div>
              <p className="panelEyebrow">Contagion</p>
              <h3 className="panelTitle">Recent events</h3>
            </div>
            <span className="panelMeta">{contagion.length} events</span>
          </div>
          {contagion.length ? (
            <div className="stackList">
              {contagion.map((item) => (
                <div key={item.event_id} className="metricRow">
                  <div>
                    <strong>{item.trigger_symbol}</strong>
                    <div className="metricSubtext">{item.trigger_sector}</div>
                  </div>
                  <div className="metricPack">
                    <div className="metricValue">{formatNumber(item.risk_score, 3)}</div>
                    <div className="metricSubtext">
                      {item.affected_count} peers | {formatTime(item.event_timestamp)}
                    </div>
                  </div>
                </div>
              ))}
            </div>
          ) : (
            <div className="emptyState">No contagion events have been written.</div>
          )}
        </article>
      </section>
    </>
  );
}
