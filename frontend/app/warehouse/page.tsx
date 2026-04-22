import Link from "next/link";

import { ExplainerCards } from "../../components/explainer-cards";
import { LineChart } from "../../components/line-chart";
import { StatCard } from "../../components/stat-card";
import {
  fetchWarehouseIntradayProfile,
  fetchWarehouseMonthly,
  fetchWarehouseRollups,
  fetchWarehouseSectorMomentum,
  fetchWarehouseSectorRegimes,
  fetchWarehouseStockLeaders,
  fetchWarehouseStockOutliers,
  fetchWarehouseStockPersistence,
  fetchWarehouseSummary,
} from "../../lib/api";
import { formatCompactIndian, formatDate, formatNumber } from "../../lib/format";

export default async function WarehousePage() {
  const [
    summary,
    daily,
    monthly,
    outliers,
    sectorRegimes,
    stockLeaders,
    sectorMomentum,
    stockPersistence,
    intradayProfile,
  ] = await Promise.all([
    fetchWarehouseSummary(),
    fetchWarehouseRollups(),
    fetchWarehouseMonthly(),
    fetchWarehouseStockOutliers(),
    fetchWarehouseSectorRegimes(20),
    fetchWarehouseStockLeaders(30),
    fetchWarehouseSectorMomentum(18),
    fetchWarehouseStockPersistence(20),
    fetchWarehouseIntradayProfile(375),
  ]);

  const dailyRows = daily ?? [];
  const monthlyRows = monthly ?? [];
  const outlierRows = outliers ?? [];
  const regimeRows = sectorRegimes ?? [];
  const leaderRows = stockLeaders ?? [];
  const momentumRows = sectorMomentum ?? [];
  const persistenceRows = stockPersistence ?? [];
  const intradayRows = intradayProfile ?? [];
  const latestDate = summary?.last_calendar_date ?? dailyRows[0]?.calendar_date ?? null;
  const hottestSector = [...dailyRows].sort((left, right) => right.max_composite_score - left.max_composite_score)[0];
  const momentumLeader = [...momentumRows].sort(
    (left, right) => right.anomaly_delta - left.anomaly_delta || right.score_delta - left.score_delta,
  )[0];
  const persistenceLeader = [...persistenceRows].sort(
    (left, right) => right.anomaly_day_ratio - left.anomaly_day_ratio || right.total_anomalies - left.total_anomalies,
  )[0];
  const intradayLabels = intradayRows.map((row) => row.time_label);
  const intradayValues = intradayRows.map((row) => row.avg_composite_score);
  const hotspotRows = [...intradayRows]
    .sort(
      (left, right) =>
        right.peak_composite_score - left.peak_composite_score || right.contagion_minutes - left.contagion_minutes,
    )
    .slice(0, 12);

  return (
    <>
      <section className="heroPanel">
        <div className="pageHeader">
          <div>
            <p className="pageKicker">Warehouse</p>
            <h2 className="pageTitle">Warehouse analytics</h2>
          </div>
          <div className="pageMetaGroup">
            <span className="metaTag">{summary?.trading_days_loaded ?? 0} trading days</span>
            <span className="metaTag">{latestDate ? formatDate(latestDate) : "No ETL date"}</span>
          </div>
        </div>
        <div className="statsGrid">
          <StatCard
            label="Minute facts"
            value={formatCompactIndian(summary?.anomaly_minute_rows, 2)}
            info="Minute-grain warehouse rows storing surveillance metrics for historical analysis."
            hint={summary ? `${summary.anomaly_minute_rows.toLocaleString("en-IN")} warehouse anomaly rows` : "No warehouse load yet"}
          />
          <StatCard
            label="Market-day facts"
            value={String(summary?.market_day_rows ?? 0)}
            info="Daily stock-level warehouse rows used for rollups, persistence metrics, and cross-session queries."
            hint={`${summary?.stocks_covered ?? 0} stocks | ${summary?.sectors_covered ?? 0} sectors`}
            tone="accent"
          />
          <StatCard
            label="Coverage facts"
            value={formatCompactIndian(summary?.coverage_rows, 2)}
            info="Factless warehouse rows recording whether surveillance coverage existed for a stock on a session."
            hint="Factless surveillance coverage grain"
          />
          <StatCard
            label="Contagion facts"
            value={String(summary?.contagion_event_rows ?? 0)}
            info="Warehouse rows representing persisted contagion windows and their analytical linkages."
            hint={`${summary?.total_contagion_events ?? 0} linked market-day contagion flags`}
            tone="warning"
          />
          <StatCard
            label="Peak sector"
            value={hottestSector?.sector_name ?? "N/A"}
            info="The sector with the highest peak daily composite score in the current warehouse view."
            hint={hottestSector ? formatNumber(hottestSector.max_composite_score, 3) : "No aggregate"}
            tone="critical"
          />
          <StatCard
            label="Momentum sectors"
            value={String(summary?.sector_momentum_rows ?? momentumRows.length)}
            info="Sector regime rows comparing recent sessions against a prior window to show acceleration or cooling."
            hint={momentumLeader ? `${momentumLeader.sector_name} leads with ${momentumLeader.anomaly_delta} anomaly delta` : "Recent-vs-prior regime scan"}
          />
          <StatCard
            label="Persistent names"
            value={String(summary?.stock_persistence_rows ?? persistenceRows.length)}
            info="Stocks whose anomaly activity is evaluated across many sessions to distinguish one-offs from repeat stress."
            hint={
              persistenceLeader
                ? `${persistenceLeader.symbol} active on ${formatNumber(persistenceLeader.anomaly_day_ratio * 100, 1)}% of sessions`
                : "Cross-session persistence profile"
            }
            tone="warning"
          />
          <StatCard
            label="Total anomalies"
            value={formatCompactIndian(summary?.total_anomalies, 2)}
            info="The total count of anomalous minute points loaded into the warehouse over the current date range."
            hint={
              summary
                ? `${summary.first_calendar_date ? formatDate(summary.first_calendar_date) : "N/A"} to ${summary.last_calendar_date ? formatDate(summary.last_calendar_date) : "N/A"}`
                : "Warehouse range unavailable"
            }
          />
        </div>
      </section>

      <section className="contentGrid quickActionsGrid">
        <Link href="/warehouse/analyst" className="shortcutCard">
          <p className="panelEyebrow">Advanced</p>
          <h3 className="shortcutTitle">Analyst studio</h3>
          <div className="shortcutMetric">{summary?.stocks_covered ?? 0} stocks</div>
          <div className="shortcutHint">
            Build grouped warehouse queries visually, inspect the result set, and export reports as CSV or printable PDF.
          </div>
        </Link>
      </section>

      <ExplainerCards
        eyebrow="Reading guide"
        title="What the warehouse stats mean"
        meta="Analytical terms"
        footerHref="/methodology"
        footerLabel="Open warehouse methodology"
        items={[
          {
            title: "Minute fact",
            value: "fact_anomaly_minute",
            description:
              "The warehouse keeps minute-grain anomaly observations separate so intraday pressure analysis stays mathematically clean.",
            tone: "accent",
          },
          {
            title: "Market-day fact",
            value: "Daily stock rollup",
            description:
              "This is the daily grain used for stock-level summaries such as anomaly days, peak score, and contagion-event counts.",
            tone: "warning",
          },
          {
            title: "Momentum",
            value: "Recent vs prior",
            description:
              "Sector momentum compares the recent session window with the prior window to show whether anomaly activity is accelerating or fading.",
          },
          {
            title: "Persistence",
            value: "Signal durability",
            description:
              "Persistence tracks how often a stock appears across sessions, which helps distinguish one-off bursts from repeatedly stressed names.",
            tone: "critical",
          },
        ]}
      />

      <section className="contentGrid twoUp">
        <article className="surface">
          <div className="panelHeader">
            <div>
              <p className="panelEyebrow">Intraday pressure</p>
              <h3 className="panelTitle">Warehouse session profile</h3>
            </div>
            <span className="panelMeta">{intradayRows.length} minute buckets</span>
          </div>
          {intradayRows.length ? (
            <LineChart
              values={intradayValues}
              labels={intradayLabels}
              color="var(--accent)"
              height={220}
              valueDigits={3}
              seriesLabel="Average warehouse composite score"
            />
          ) : (
            <div className="emptyState">Intraday warehouse profile will appear after ETL refresh.</div>
          )}
        </article>

        <article className="surface">
          <div className="panelHeader">
            <div>
              <p className="panelEyebrow">Regime shifts</p>
              <h3 className="panelTitle">Recent versus prior window</h3>
            </div>
            <span className="panelMeta">{momentumRows.length} sectors</span>
          </div>
          {momentumRows.length ? (
            <div className="tableWrap">
              <table className="dataTable">
                <thead>
                  <tr>
                    <th>Sector</th>
                    <th>Recent</th>
                    <th>Prior</th>
                    <th>Delta</th>
                    <th>Score delta</th>
                    <th>Contagion delta</th>
                  </tr>
                </thead>
                <tbody>
                  {momentumRows.map((row) => (
                    <tr key={row.sector_name}>
                      <td>{row.sector_name}</td>
                      <td>{String(row.recent_total_anomalies)}</td>
                      <td>{String(row.prior_total_anomalies)}</td>
                      <td>{formatNumber(row.anomaly_delta, 0)}</td>
                      <td>{formatNumber(row.score_delta, 3)}</td>
                      <td>{formatNumber(row.contagion_delta, 0)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <div className="emptyState">Momentum windows require several loaded trading sessions.</div>
          )}
        </article>
      </section>

      <section className="contentGrid twoUp">
        <article className="surface">
          <div className="panelHeader">
            <div>
              <p className="panelEyebrow">Persistent leaders</p>
              <h3 className="panelTitle">Cross-session signal durability</h3>
            </div>
            <span className="panelMeta">{persistenceRows.length} stocks</span>
          </div>
          {persistenceRows.length ? (
            <div className="tableWrap">
              <table className="dataTable">
                <thead>
                  <tr>
                    <th>Symbol</th>
                    <th>Sector</th>
                    <th>Active days</th>
                    <th>Ratio</th>
                    <th>Recent 5D</th>
                    <th>Days since</th>
                  </tr>
                </thead>
                <tbody>
                  {persistenceRows.map((row) => (
                    <tr key={row.symbol}>
                      <td>{row.symbol}</td>
                      <td>{row.sector_name}</td>
                      <td>{String(row.anomaly_days)}</td>
                      <td>{formatNumber(row.anomaly_day_ratio * 100, 1)}%</td>
                      <td>{String(row.recent_5_session_anomalies)}</td>
                      <td>{row.days_since_last_anomaly ?? "-"}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <div className="emptyState">Persistence profiles will appear after warehouse refresh.</div>
          )}
        </article>

        <article className="surface">
          <div className="panelHeader">
            <div>
              <p className="panelEyebrow">Sector regimes</p>
              <h3 className="panelTitle">Cross-session sector leaders</h3>
            </div>
            <span className="panelMeta">{regimeRows.length} sectors</span>
          </div>
          {regimeRows.length ? (
            <div className="tableWrap">
              <table className="dataTable">
                <thead>
                  <tr>
                    <th>Sector</th>
                    <th>Sessions</th>
                    <th>Stocks</th>
                    <th>Minute facts</th>
                    <th>Total anomalies</th>
                    <th>Peak score</th>
                    <th>Contagion</th>
                    <th>Latest day</th>
                  </tr>
                </thead>
                <tbody>
                  {regimeRows.map((row) => (
                    <tr key={row.sector_name}>
                      <td>{row.sector_name}</td>
                      <td>{String(row.sessions_covered)}</td>
                      <td>{String(row.symbols_covered)}</td>
                      <td>{String(row.anomaly_minutes)}</td>
                      <td>{String(row.total_anomalies)}</td>
                      <td>{formatNumber(row.peak_daily_composite_score, 3)}</td>
                      <td>{String(row.contagion_event_count)}</td>
                      <td>{formatDate(row.latest_calendar_date)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <div className="emptyState">Sector regime rollups will appear after ETL refresh.</div>
          )}
        </article>
      </section>

      <section className="contentGrid twoUp">
        <article className="surface">
          <div className="panelHeader">
            <div>
              <p className="panelEyebrow">Daily summary</p>
              <h3 className="panelTitle">Sector session rollups</h3>
            </div>
            <span className="panelMeta">{dailyRows.length} rows</span>
          </div>
          {dailyRows.length ? (
            <div className="tableWrap">
              <table className="dataTable">
                <thead>
                  <tr>
                    <th>Date</th>
                    <th>Sector</th>
                    <th>Active min</th>
                    <th>Avg score</th>
                    <th>Max score</th>
                    <th>Contagion min</th>
                  </tr>
                </thead>
                <tbody>
                  {dailyRows.map((row) => (
                    <tr key={`${row.calendar_date}-${row.sector_name}`}>
                      <td>{row.calendar_date}</td>
                      <td>{row.sector_name}</td>
                      <td>{String(row.active_minutes ?? 0)}</td>
                      <td>{formatNumber(row.avg_composite_score, 3)}</td>
                      <td>{formatNumber(row.max_composite_score, 3)}</td>
                      <td>{String(row.contagion_minutes)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <div className="emptyState">Run ETL to materialize warehouse aggregates.</div>
          )}
        </article>

        <article className="surface">
          <div className="panelHeader">
            <div>
              <p className="panelEyebrow">Session hotspots</p>
              <h3 className="panelTitle">Intraday peak buckets</h3>
            </div>
            <span className="panelMeta">{hotspotRows.length} rows</span>
          </div>
          {hotspotRows.length ? (
            <div className="tableWrap">
              <table className="dataTable">
                <thead>
                  <tr>
                    <th>Time</th>
                    <th>Avg score</th>
                    <th>Peak score</th>
                    <th>Stocks</th>
                    <th>Contagion min</th>
                  </tr>
                </thead>
                <tbody>
                  {hotspotRows.map((row) => (
                    <tr key={row.time_label}>
                      <td>{row.time_label}</td>
                      <td>{formatNumber(row.avg_composite_score, 3)}</td>
                      <td>{formatNumber(row.peak_composite_score, 3)}</td>
                      <td>{String(row.distinct_stocks)}</td>
                      <td>{String(row.contagion_minutes)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <div className="emptyState">Intraday hotspot ranking will appear after ETL refresh.</div>
          )}
        </article>
      </section>

      <section className="contentGrid twoUp">
        <article className="surface">
          <div className="panelHeader">
            <div>
              <p className="panelEyebrow">Monthly summary</p>
              <h3 className="panelTitle">Sector rollups</h3>
            </div>
            <span className="panelMeta">{monthlyRows.length} rows</span>
          </div>
          {monthlyRows.length ? (
            <div className="tableWrap">
              <table className="dataTable">
                <thead>
                  <tr>
                    <th>Year</th>
                    <th>Quarter</th>
                    <th>Month</th>
                    <th>Sector</th>
                    <th>Avg daily</th>
                    <th>Max daily</th>
                    <th>Contagion</th>
                  </tr>
                </thead>
                <tbody>
                  {monthlyRows.map((row) => (
                    <tr key={`${row.year}-${row.month}-${row.sector_name}`}>
                      <td>{String(row.year)}</td>
                      <td>{String(row.quarter)}</td>
                      <td>{String(row.month)}</td>
                      <td>{row.sector_name}</td>
                      <td>{formatNumber(row.avg_daily_composite_score, 3)}</td>
                      <td>{formatNumber(row.max_daily_composite_score, 3)}</td>
                      <td>{String(row.contagion_event_count)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <div className="emptyState">Monthly rollups will appear after warehouse refresh.</div>
          )}
        </article>

        <article className="surface">
          <div className="panelHeader">
            <div>
              <p className="panelEyebrow">Stock leaders</p>
              <h3 className="panelTitle">Warehouse stock leaderboard</h3>
            </div>
            <span className="panelMeta">{leaderRows.length} stocks</span>
          </div>
          {leaderRows.length ? (
            <div className="tableWrap">
              <table className="dataTable">
                <thead>
                  <tr>
                    <th>Symbol</th>
                    <th>Company</th>
                    <th>Sector</th>
                    <th>Anomaly days</th>
                    <th>Total anomalies</th>
                    <th>Latest peak</th>
                    <th>Peak score</th>
                    <th>Contagion</th>
                  </tr>
                </thead>
                <tbody>
                  {leaderRows.map((row) => (
                    <tr key={row.symbol}>
                      <td>{row.symbol}</td>
                      <td>{row.company_name}</td>
                      <td>{row.sector_name}</td>
                      <td>{String(row.anomaly_days)}</td>
                      <td>{String(row.total_anomalies)}</td>
                      <td>{formatNumber(row.latest_peak_score, 3)}</td>
                      <td>{formatNumber(row.peak_daily_composite_score, 3)}</td>
                      <td>{String(row.contagion_event_count)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <div className="emptyState">Stock leader rollups will appear after ETL refresh.</div>
          )}
        </article>
      </section>

      <section className="surface">
        <div className="panelHeader">
          <div>
            <p className="panelEyebrow">Stock outliers</p>
            <h3 className="panelTitle">Warehouse day leaders</h3>
          </div>
          <span className="panelMeta">{outlierRows.length} rows</span>
        </div>
        {outlierRows.length ? (
          <div className="tableWrap">
            <table className="dataTable">
              <thead>
                <tr>
                  <th>Date</th>
                  <th>Symbol</th>
                  <th>Company</th>
                  <th>Sector</th>
                  <th>Anomalies</th>
                  <th>Max score</th>
                  <th>Avg score</th>
                  <th>Avg vol z</th>
                  <th>Contagion</th>
                </tr>
              </thead>
              <tbody>
                {outlierRows.map((row) => (
                  <tr key={`${row.calendar_date}-${row.symbol}`}>
                    <td>{row.calendar_date}</td>
                    <td>{row.symbol}</td>
                    <td>{row.company_name}</td>
                    <td>{row.sector_name}</td>
                    <td>{String(row.anomaly_count)}</td>
                    <td>{formatNumber(row.max_composite_score, 3)}</td>
                    <td>{formatNumber(row.avg_composite_score, 3)}</td>
                    <td>{formatNumber(row.avg_volume_z_score, 3)}</td>
                    <td>{String(row.contagion_event_count)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <div className="emptyState">No warehouse stock outliers are available yet.</div>
        )}
      </section>
    </>
  );
}
