import { StatCard } from "../../components/stat-card";
import { fetchWarehouseMonthly, fetchWarehouseRollups, fetchWarehouseStockOutliers } from "../../lib/api";
import { formatNumber } from "../../lib/format";

export default async function WarehousePage() {
  const [daily, monthly, outliers] = await Promise.all([
    fetchWarehouseRollups(),
    fetchWarehouseMonthly(),
    fetchWarehouseStockOutliers(),
  ]);
  const dailyRows = daily ?? [];
  const monthlyRows = monthly ?? [];
  const outlierRows = outliers ?? [];
  const latestDate = dailyRows[0]?.calendar_date ?? null;
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
            <span className="metaTag">{latestDate ?? "No ETL date"}</span>
          </div>
        </div>
        <div className="statsGrid">
          <StatCard label="Daily rows" value={String(dailyRows.length)} />
          <StatCard label="Monthly rows" value={String(monthlyRows.length)} />
          <StatCard
            label="Peak sector"
            value={hottestSector?.sector_name ?? "N/A"}
            hint={hottestSector ? formatNumber(hottestSector.max_composite_score, 3) : "No aggregate"}
            tone="warning"
          />
          <StatCard label="Outlier rows" value={String(outlierRows.length)} hint="Warehouse fact_market_day" tone="accent" />
        </div>
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
