import { StatCard } from "../../components/stat-card";
import {
  fetchWarehouseMonthly,
  fetchWarehouseRollups,
  fetchWarehouseSectorRegimes,
  fetchWarehouseStockLeaders,
  fetchWarehouseStockOutliers,
  fetchWarehouseSummary,
} from "../../lib/api";
import { formatCompactIndian, formatDate, formatNumber } from "../../lib/format";

export default async function WarehousePage() {
  const [summary, daily, monthly, outliers, sectorRegimes, stockLeaders] = await Promise.all([
    fetchWarehouseSummary(),
    fetchWarehouseRollups(),
    fetchWarehouseMonthly(),
    fetchWarehouseStockOutliers(),
    fetchWarehouseSectorRegimes(20),
    fetchWarehouseStockLeaders(30),
  ]);
  const dailyRows = daily ?? [];
  const monthlyRows = monthly ?? [];
  const outlierRows = outliers ?? [];
  const regimeRows = sectorRegimes ?? [];
  const leaderRows = stockLeaders ?? [];
  const latestDate = summary?.last_calendar_date ?? dailyRows[0]?.calendar_date ?? null;
  const hottestSector = [...dailyRows].sort((left, right) => right.max_composite_score - left.max_composite_score)[0];

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
            hint={summary ? `${summary.anomaly_minute_rows.toLocaleString("en-IN")} warehouse anomaly rows` : "No warehouse load yet"}
          />
          <StatCard
            label="Market-day facts"
            value={String(summary?.market_day_rows ?? 0)}
            hint={`${summary?.stocks_covered ?? 0} stocks | ${summary?.sectors_covered ?? 0} sectors`}
            tone="accent"
          />
          <StatCard
            label="Coverage facts"
            value={formatCompactIndian(summary?.coverage_rows, 2)}
            hint="Factless surveillance coverage grain"
          />
          <StatCard
            label="Contagion facts"
            value={String(summary?.contagion_event_rows ?? 0)}
            hint={`${summary?.total_contagion_events ?? 0} linked market-day contagion flags`}
            tone="warning"
          />
          <StatCard
            label="Peak sector"
            value={hottestSector?.sector_name ?? "N/A"}
            hint={hottestSector ? formatNumber(hottestSector.max_composite_score, 3) : "No aggregate"}
            tone="critical"
          />
          <StatCard
            label="Total anomalies"
            value={formatCompactIndian(summary?.total_anomalies, 2)}
            hint={
              summary
                ? `${summary.first_calendar_date ? formatDate(summary.first_calendar_date) : "N/A"} to ${summary.last_calendar_date ? formatDate(summary.last_calendar_date) : "N/A"}`
                : "Warehouse range unavailable"
            }
          />
        </div>
      </section>

      <section className="contentGrid twoUp">
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
