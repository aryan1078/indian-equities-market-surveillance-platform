import Link from "next/link";

import { AnomalyScoreChart } from "../../../components/anomaly-score-chart";
import { ExplainerCards } from "../../../components/explainer-cards";
import { LineChart } from "../../../components/line-chart";
import { StockAlertList } from "../../../components/stock-alert-list";
import { StatCard } from "../../../components/stat-card";
import { fetchStockWorkspace } from "../../../lib/api";
import {
  formatDate,
  formatDateTime,
  formatNumber,
  formatPercent,
  formatTime,
  severityLabel,
} from "../../../lib/format";

type PageProps = {
  params: Promise<{ symbol: string }>;
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

export default async function StockDetailPage({ params }: PageProps) {
  const { symbol } = await params;
  const workspace = await fetchStockWorkspace(symbol);

  if (!workspace) {
    return (
      <section className="surface">
        <div className="emptyState">No stock workspace is available for {symbol}.</div>
      </section>
    );
  }

  const historyValues = workspace.history.map((row) => Number(row.close));
  const historyLabels = workspace.history.map((row) => formatDate(row.trading_date));
  const intradayValues = workspace.ticks.map((row) => Number(row.close));
  const intradayLabels = workspace.ticks.map((row) => formatTime(row.timestamp_ist));
  const anomalyTrend = workspace.anomalies.map((row) => ({
    timestamp: formatTime(row.timestamp_ist),
    composite: Number(row.composite_score),
    priceZ: Number(row.price_z_score),
    volumeZ: Number(row.volume_z_score),
    flagged: Boolean(row.is_anomalous),
  }));
  const recentAlerts = workspace.alerts.slice(0, 8);
  const latestDailyBar = workspace.history.at(-1);
  const latestAnomaly = workspace.latest_anomaly;
  const historySummary = workspace.history_summary;
  const alertSummary = workspace.alert_summary;
  const anomalySummary = workspace.anomaly_summary;
  const listingDate =
    typeof workspace.reference.metadata?.listing_date === "string"
      ? workspace.reference.metadata.listing_date
      : "N/A";
  const isin = typeof workspace.reference.metadata?.isin === "string" ? workspace.reference.metadata.isin : "N/A";
  const series = typeof workspace.reference.metadata?.series === "string" ? workspace.reference.metadata.series : "N/A";
  const faceValue =
    typeof workspace.reference.metadata?.face_value === "string"
      ? workspace.reference.metadata.face_value
      : "N/A";
  const marketLot =
    typeof workspace.reference.metadata?.market_lot === "string"
      ? workspace.reference.metadata.market_lot
      : "N/A";

  return (
    <>
      <section className="heroPanel">
        <div className="pageHeader">
          <div>
            <p className="pageKicker">Stock workspace</p>
            <h2 className="pageTitle">{workspace.reference.company_name}</h2>
            <div className="inlineMeta">
              <span>{workspace.resolved_symbol}</span>
              <span>{workspace.reference.exchange ?? "Unknown exchange"}</span>
              <span>{workspace.reference.sector ?? "Unknown sector"}</span>
            </div>
          </div>
          <div className="pageMetaGroup">
            <span className="metaTag">{workspace.history.length} sessions</span>
            <span className="metaTag">{workspace.ticks.length} ticks</span>
          </div>
        </div>
        <div className="statsGrid">
          <StatCard label="Last close" value={formatNumber(workspace.indicators.last_close)} info="The most recent loaded close price for this stock." />
          <StatCard
            label="1D move"
            value={formatPercent(workspace.indicators.day_change_pct)}
            info="Percentage move from the prior loaded daily close to the latest daily close."
            tone={(workspace.indicators.day_change_pct ?? 0) >= 0 ? "accent" : "warning"}
          />
          <StatCard label="20D move" value={formatPercent(workspace.indicators.return_20d_pct)} info="Percentage return over the last 20 loaded trading sessions." />
          <StatCard label="45D move" value={formatPercent(historySummary.return_45d_pct)} info="Percentage return across the loaded 45-session window used for the stock workspace." />
          <StatCard
            label="Live anomaly"
            value={latestAnomaly ? formatNumber(latestAnomaly.composite_score, 3) : "Normal"}
            info="The latest intraday composite anomaly score for this symbol. Normal means no current active anomaly row is present."
            hint={latestAnomaly ? formatTime(latestAnomaly.timestamp_ist) : "No active flag"}
            tone={latestAnomaly ? "critical" : "default"}
          />
          <StatCard
            label="Range position"
            value={
              historySummary.range_position_pct === null || historySummary.range_position_pct === undefined
                ? "N/A"
                : `${formatNumber(historySummary.range_position_pct, 1)}%`
            }
            info="Where the latest close sits between the period low and period high for the loaded historical range."
            hint={`${formatNumber(historySummary.period_low)} to ${formatNumber(historySummary.period_high)}`}
          />
          <StatCard
            label="Open alerts"
            value={String(alertSummary.open_count)}
            info="Persisted operator alerts for this symbol that have not yet been acknowledged."
            hint={`${alertSummary.acknowledged_count} acknowledged`}
            tone={alertSummary.open_count ? "warning" : "default"}
          />
          <StatCard
            label="Flagged points"
            value={String(anomalySummary.flagged_count)}
            info="Count of intraday anomaly points in the loaded latest session that crossed surveillance thresholds."
            hint={
              anomalySummary.latest_flagged_at ? formatTime(anomalySummary.latest_flagged_at) : "No recent flag"
            }
            tone={anomalySummary.flagged_count ? "critical" : "default"}
          />
        </div>
      </section>

      <ExplainerCards
        eyebrow="Reading guide"
        title="Signal terms for this stock"
        meta="Local signal legend"
        footerHref="/methodology"
        footerLabel="Open full methodology"
        items={[
          {
            title: "Price z",
            value: formatNumber(latestAnomaly?.price_z_score, 2),
            description:
              "Standardized one-minute return. Larger absolute values mean the latest move is far from the recent return baseline.",
            tone: "accent",
          },
          {
            title: "Volume z",
            value: formatNumber(latestAnomaly?.volume_z_score, 2),
            description:
              "Standardized participation surprise. It tells you whether the move is happening on unusual volume.",
            tone: "warning",
          },
          {
            title: "Composite",
            value: formatNumber(latestAnomaly?.composite_score, 3),
            description:
              "Weighted score used to rank the signal: 60% price z and 40% volume z.",
            tone: "critical",
          },
          {
            title: "Alert",
            value: alertSummary.open_count ? `${alertSummary.open_count} open` : "None open",
            description:
              "A persisted operator event. Historical rows may be marked stale when they belong to an older monitored session.",
          },
        ]}
      />

      <section className="surface">
        <div className="panelHeader">
          <div>
            <p className="panelEyebrow">Profile</p>
            <h3 className="panelTitle">Listing and coverage</h3>
          </div>
          <span className="panelMeta">{workspace.reference.watchlist ? "Watchlist" : "Directory"}</span>
        </div>
        <div className="keyValueGrid">
          <div className="keyValueCard">
            <span>Series</span>
            <strong>{series}</strong>
          </div>
          <div className="keyValueCard">
            <span>Listing date</span>
            <strong>{listingDate}</strong>
          </div>
          <div className="keyValueCard">
            <span>ISIN</span>
            <strong className="dataMono">{isin}</strong>
          </div>
          <div className="keyValueCard">
            <span>Face value</span>
            <strong>{faceValue}</strong>
          </div>
          <div className="keyValueCard">
            <span>Market lot</span>
            <strong>{marketLot}</strong>
          </div>
          <div className="keyValueCard">
            <span>Coverage</span>
            <strong>{historySummary.session_count} sessions</strong>
          </div>
        </div>
      </section>

      <section className="contentGrid twoUp">
        <article className="surface">
          <div className="panelHeader">
            <div>
              <p className="panelEyebrow">Historical trend</p>
              <h3 className="panelTitle">Daily close</h3>
            </div>
            <span className="panelMeta">{latestDailyBar ? formatDate(latestDailyBar.trading_date) : "No history"}</span>
          </div>
          {historyValues.length ? (
            <>
              <LineChart values={historyValues} labels={historyLabels} seriesLabel="daily close" />
              <div className="statsGrid compactStats">
                <StatCard label="RSI 14" value={formatNumber(workspace.indicators.rsi_14, 1)} info="Relative Strength Index over 14 sessions, a momentum indicator bounded between 0 and 100." />
                <StatCard label="ATR 14" value={formatNumber(workspace.indicators.atr_14, 2)} info="Average True Range over 14 sessions, used as a recent volatility gauge in price units." />
                <StatCard label="20D vol" value={formatPercent(workspace.indicators.volatility_20d_pct)} info="Recent 20-session realized volatility expressed as a percentage." />
                <StatCard label="Vol ratio" value={formatNumber(workspace.indicators.volume_ratio_20d, 2)} info="Latest volume relative to the recent 20-session average volume baseline." />
                <StatCard label="5D move" value={formatPercent(historySummary.return_5d_pct)} info="Percentage move over the most recent 5 loaded trading sessions." />
                <StatCard label="20D avg vol" value={formatNumber(historySummary.avg_volume_20d, 0)} info="Average daily traded volume across the recent 20-session historical window." />
              </div>
            </>
          ) : (
            <div className="emptyState">Historical bars are not available.</div>
          )}
        </article>

        <article className="surface">
          <div className="panelHeader">
            <div>
              <p className="panelEyebrow">Intraday state</p>
              <h3 className="panelTitle">Latest session</h3>
            </div>
            <span className="panelMeta">
              {workspace.latest_market?.timestamp_ist ? formatTime(workspace.latest_market.timestamp_ist) : "Offline"}
            </span>
          </div>
          {intradayValues.length ? (
            <>
              <LineChart
                values={intradayValues}
                labels={intradayLabels}
                color="var(--accent-strong)"
                height={190}
                seriesLabel="intraday close"
              />
              <div className="keyValueGrid">
                <div className="keyValueCard">
                  <span>Close</span>
                  <strong>{formatNumber(workspace.latest_market?.close)}</strong>
                </div>
                <div className="keyValueCard">
                  <span>Volume</span>
                  <strong>{formatNumber(workspace.latest_market?.volume, 0)}</strong>
                </div>
                <div className="keyValueCard">
                  <span>Price z</span>
                  <strong>{formatNumber(latestAnomaly?.price_z_score, 2)}</strong>
                </div>
                <div className="keyValueCard">
                  <span>Volume z</span>
                  <strong>{formatNumber(latestAnomaly?.volume_z_score, 2)}</strong>
                </div>
              </div>
            </>
          ) : (
            <div className="emptyState">No intraday tape is currently available for this symbol.</div>
          )}
        </article>
      </section>

      <section className="contentGrid twoUp">
        <article className="surface">
          <div className="panelHeader">
            <div>
              <p className="panelEyebrow">Alerts</p>
              <h3 className="panelTitle">Recent events</h3>
            </div>
            <span className="panelMeta">{recentAlerts.length} recent</span>
          </div>
          <StockAlertList initialAlerts={recentAlerts} />
        </article>

        <article className="surface">
          <div className="panelHeader">
            <div>
              <p className="panelEyebrow">Signal log</p>
              <h3 className="panelTitle">Recent anomaly points</h3>
            </div>
            <span className="panelMeta">{workspace.anomalies.length} rows</span>
          </div>
          {workspace.anomalies.length ? (
            <div className="tableWrap">
              <table className="dataTable">
                <thead>
                  <tr>
                    <th>Time</th>
                    <th>Composite</th>
                    <th>Price z</th>
                    <th>Volume z</th>
                    <th>Status</th>
                  </tr>
                </thead>
                <tbody>
                  {workspace.anomalies.slice(-12).reverse().map((row) => (
                    <tr key={row.timestamp_utc}>
                      <td>{formatTime(row.timestamp_ist)}</td>
                      <td>{formatNumber(row.composite_score, 3)}</td>
                      <td>{formatNumber(row.price_z_score, 2)}</td>
                      <td>{formatNumber(row.volume_z_score, 2)}</td>
                      <td>
                        <span className={`severityTag ${row.is_anomalous ? "critical" : "low"}`}>
                          {row.is_anomalous ? "flagged" : "normal"}
                        </span>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <div className="emptyState">No anomaly metrics are available for the latest session.</div>
          )}
        </article>
      </section>

      <section className="contentGrid twoUp">
        <article className="surface">
          <div className="panelHeader">
            <div>
              <p className="panelEyebrow">Sector view</p>
              <h3 className="panelTitle">Peer comparison</h3>
            </div>
            <span className="panelMeta">{workspace.peer_comparison.length} peers</span>
          </div>
          {workspace.peer_comparison.length ? (
            <div className="tableWrap">
              <table className="dataTable">
                <thead>
                  <tr>
                    <th>Symbol</th>
                    <th>Company</th>
                    <th>Last</th>
                    <th>20D</th>
                    <th>RSI</th>
                    <th>Vol ratio</th>
                    <th>Signal</th>
                  </tr>
                </thead>
                <tbody>
                  {workspace.peer_comparison.map((peer) => (
                    <tr key={peer.symbol}>
                      <td>
                        <Link href={`/stocks/${encodeURIComponent(peer.symbol)}`} className="tableLink">
                          {peer.symbol}
                        </Link>
                      </td>
                      <td>{peer.company_name ?? peer.symbol}</td>
                      <td>{formatNumber(peer.latest_market_close ?? peer.last_close)}</td>
                      <td>{formatPercent(peer.return_20d_pct)}</td>
                      <td>{formatNumber(peer.rsi_14, 1)}</td>
                      <td>{formatNumber(peer.volume_ratio_20d, 2)}</td>
                      <td>
                        <span className={`severityTag ${severityClass(peer.latest_alert_severity)}`}>
                          {peer.latest_alert_severity ?? (peer.is_anomalous ? "live" : "normal")}
                        </span>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <div className="emptyState">No peer set is available for this sector.</div>
          )}
        </article>

        <article className="surface">
          <div className="panelHeader">
            <div>
              <p className="panelEyebrow">Contagion</p>
              <h3 className="panelTitle">Related events</h3>
            </div>
            <span className="panelMeta">{workspace.related_contagion.length} events</span>
          </div>
          {workspace.related_contagion.length ? (
            <div className="stackList">
              {workspace.related_contagion.map((event) => (
                <div key={event.event_id} className="metricRow">
                  <div>
                    <strong>{event.trigger_symbol}</strong>
                    <div className="metricSubtext">{event.trigger_sector}</div>
                  </div>
                  <div className="metricPack">
                    <div className="metricValue">{formatNumber(event.risk_score, 3)}</div>
                    <div className="metricSubtext">
                      {event.affected_count} peers | {formatDateTime(event.event_timestamp)}
                    </div>
                  </div>
                </div>
              ))}
            </div>
          ) : (
            <div className="emptyState">No related contagion event is stored for this stock yet.</div>
          )}
        </article>
      </section>

      <section className="surface">
        <div className="panelHeader">
          <div>
            <p className="panelEyebrow">Signal curve</p>
            <h3 className="panelTitle">Composite, price z, and volume z</h3>
          </div>
          <span className="panelMeta">
            {anomalySummary.peak_composite_score ? formatNumber(anomalySummary.peak_composite_score, 3) : "No peak"}
          </span>
        </div>
        {anomalyTrend.length ? (
          <AnomalyScoreChart points={anomalyTrend} height={210} />
        ) : (
          <div className="emptyState">No anomaly curve is available for the latest session.</div>
        )}
      </section>
    </>
  );
}
