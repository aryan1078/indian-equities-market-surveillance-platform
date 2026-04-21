import { StatCard } from "../../components/stat-card";
import { IntensityBars } from "../../components/intensity-bars";
import { fetchSystemHealth, fetchSystemRuns, fetchSystemScale } from "../../lib/api";
import { formatCompactIndian, formatDateTime, shortId } from "../../lib/format";

function displayValue(value: unknown) {
  if (value === null || value === undefined || value === "") {
    return "N/A";
  }
  return String(value);
}

export default async function SystemPage() {
  const [health, runs, scale] = await Promise.all([fetchSystemHealth(), fetchSystemRuns(), fetchSystemScale()]);
  const ingestionRuns = Array.isArray(runs?.ingestion_runs) ? runs.ingestion_runs : [];
  const etlRuns = Array.isArray(runs?.etl_runs) ? runs.etl_runs : [];
  const listedCount = health?.universe_inventory?.listed_symbols ?? health?.database_inventory?.stock_profiles ?? 0;
  const hydratedCount = health?.universe_inventory?.hydrated_symbols ?? 0;
  const knownSectorCount = health?.universe_inventory?.known_sector_symbols ?? 0;
  const unknownSectorCount = health?.universe_inventory?.unknown_sector_symbols ?? 0;
  const sectorCoveragePct = health?.universe_inventory?.sector_coverage_pct ?? 0;
  const coveragePct = listedCount ? `${Math.round((hydratedCount / listedCount) * 100)}%` : "0%";
  const inflightTicks = Number(scale?.actual.streaming.inflight_market_ticks ?? 0);
  const inflightAnomalies = Number(scale?.actual.streaming.inflight_anomaly_metrics ?? 0);
  const storeFootprint = scale
    ? [
        {
          label: "Operational PostgreSQL",
          value: scale.actual.operational_total_rows,
          detail: "profiles, daily bars, alerts, ingestion audit",
          tone: "accent" as const,
        },
        {
          label: "Warehouse PostgreSQL",
          value: scale.actual.warehouse_total_rows,
          detail: "facts and materialized rollups",
          tone: "warning" as const,
        },
        {
          label: "Cassandra stream",
          value: scale.actual.streaming_total_rows,
          detail:
            inflightTicks || inflightAnomalies
              ? `${scale.actual.streaming.market_ticks ?? 0} ticks | ${scale.actual.streaming.anomaly_metrics ?? 0} anomaly points | ${inflightTicks + inflightAnomalies} inflight`
              : `${scale.actual.streaming.market_ticks ?? 0} ticks | ${scale.actual.streaming.anomaly_metrics ?? 0} anomaly points`,
          tone: "critical" as const,
        },
        {
          label: "Redis hot keys",
          value: Number(scale.actual.streaming.redis_keys ?? 0),
          detail: "latest-state and notification cache",
          tone: "success" as const,
        },
      ]
    : [];
  const projectionBars = scale
    ? [
        {
          label: "Minute rows per trading day",
          value: scale.projection.minute_rows_per_trading_day,
          detail: `${scale.projection.listed_symbols} symbols x ${scale.projection.session_minutes} minutes`,
          tone: "accent" as const,
        },
        {
          label: "Current loaded window",
          value: scale.projection.tick_and_anomaly_rows_for_loaded_window,
          detail: `${scale.projection.hydrated_trading_days} trading days with tick + anomaly materialization`,
          tone: "warning" as const,
        },
        {
          label: "One trading year",
          value: scale.projection.tick_and_anomaly_rows_per_year,
          detail: `${scale.projection.trading_days_per_year} sessions of tick + anomaly rows`,
          tone: "critical" as const,
        },
        {
          label: "Five-year runway",
          value: scale.projection.five_year_tick_and_anomaly_rows,
          detail: "full-NSE minute stream and anomaly archive",
          tone: "success" as const,
        },
      ]
    : [];

  return (
    <>
      <section className="heroPanel">
        <div className="pageHeader">
          <div>
            <p className="pageKicker">System</p>
            <h2 className="pageTitle">Infrastructure</h2>
          </div>
          <div className="pageMetaGroup">
            <span className="metaTag">{health?.api ?? "offline"}</span>
            <span className="metaTag">{health?.redis ? "redis online" : "redis offline"}</span>
            <span className="metaTag">{sectorCoveragePct}% sector coverage</span>
          </div>
        </div>
        <div className="statsGrid">
          <StatCard label="Last tick" value={formatDateTime(health?.last_tick)} />
          <StatCard
            label="Materialized rows"
            value={formatCompactIndian(scale?.actual.materialized_total_rows, 2)}
            hint={scale ? `${scale.actual.materialized_total_rows.toLocaleString("en-IN")} across stores` : "Inventory snapshot"}
          />
          <StatCard
            label="Loaded-window scale"
            value={formatCompactIndian(scale?.projection.tick_and_anomaly_rows_for_loaded_window, 2)}
            hint={scale ? `${scale.coverage.trading_days_loaded} trading days at full-NSE minute resolution` : "Projection unavailable"}
            tone="warning"
          />
          <StatCard
            label="Annual scale"
            value={formatCompactIndian(scale?.projection.tick_and_anomaly_rows_per_year, 2)}
            hint="Tick plus anomaly rows per trading year"
            tone="critical"
          />
          <StatCard
            label="Hydrated"
            value={String(hydratedCount)}
            hint={`${health?.database_inventory?.daily_bars ?? 0} daily bars | ${coveragePct} coverage`}
          />
          <StatCard
            label="Sector coverage"
            value={`${sectorCoveragePct}%`}
            hint={`${knownSectorCount} classified | ${unknownSectorCount} unresolved`}
            tone="accent"
          />
          <StatCard
            label="In-flight load"
            value={formatCompactIndian(inflightTicks + inflightAnomalies, 2)}
            hint={inflightTicks + inflightAnomalies ? `${formatCompactIndian(inflightTicks, 2)} ticks | ${formatCompactIndian(inflightAnomalies, 2)} anomalies` : "No active bulk load"}
            tone={inflightTicks + inflightAnomalies ? "warning" : "default"}
          />
          <StatCard
            label="Webhook"
            value={health?.notifications?.webhook_enabled ? "Configured" : "Off"}
            hint={
              health?.notifications?.webhook_enabled
                ? `${health.notifications.webhook_type} | ${health.notifications.min_severity}+`
                : "Optional outbound alert sink"
            }
            tone={health?.notifications?.webhook_enabled ? "accent" : "default"}
          />
        </div>
      </section>

      <section className="contentGrid twoUp">
        <article className="surface">
          <div className="panelHeader">
            <div>
              <p className="panelEyebrow">Database footprint</p>
              <h3 className="panelTitle">Store inventory</h3>
            </div>
            <span className="panelMeta">
              {scale ? `${scale.actual.materialized_total_rows.toLocaleString("en-IN")} rows` : "Unavailable"}
            </span>
          </div>
          <IntensityBars
            items={storeFootprint}
            valueFormatter={(value) => formatCompactIndian(value, 2)}
            emptyMessage="Store inventory is unavailable."
          />
        </article>

        <article className="surface">
          <div className="panelHeader">
            <div>
              <p className="panelEyebrow">Scale story</p>
              <h3 className="panelTitle">Crore-level projection</h3>
            </div>
            <span className="panelMeta">
              {scale?.projection.crosses_crore_annually ? "crore-ready" : "below crore"}
            </span>
          </div>
          <IntensityBars
            items={projectionBars}
            valueFormatter={(value) => formatCompactIndian(value, 2)}
            emptyMessage="Projection is unavailable."
          />
        </article>
      </section>

      <section className="contentGrid twoUp">
        <article className="surface">
          <div className="panelHeader">
            <div>
              <p className="panelEyebrow">Streaming history</p>
              <h3 className="panelTitle">Ingestion runs</h3>
            </div>
            <span className="panelMeta">{ingestionRuns.length} runs</span>
          </div>
          {ingestionRuns.length ? (
            <div className="tableWrap">
              <table className="dataTable">
                <thead>
                  <tr>
                    <th>Run</th>
                    <th>Mode</th>
                    <th>Status</th>
                    <th>Seen</th>
                    <th>Published</th>
                    <th>Finished</th>
                  </tr>
                </thead>
                <tbody>
                  {ingestionRuns.map((run) => (
                    <tr key={displayValue(run.run_id)}>
                      <td>{shortId(displayValue(run.run_id))}</td>
                      <td>{displayValue(run.mode)}</td>
                      <td>{displayValue(run.status)}</td>
                      <td>{displayValue(run.records_seen)}</td>
                      <td>{displayValue(run.records_published)}</td>
                      <td>{formatDateTime(typeof run.finished_at === "string" ? run.finished_at : undefined)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <div className="emptyState">No ingestion runs have been recorded.</div>
          )}
        </article>

        <article className="surface">
          <div className="panelHeader">
            <div>
              <p className="panelEyebrow">Warehouse history</p>
              <h3 className="panelTitle">ETL runs</h3>
            </div>
            <span className="panelMeta">{etlRuns.length} runs</span>
          </div>
          {etlRuns.length ? (
            <div className="tableWrap">
              <table className="dataTable">
                <thead>
                  <tr>
                    <th>Run</th>
                    <th>Date</th>
                    <th>Status</th>
                    <th>Extracted</th>
                    <th>Inserted</th>
                    <th>Finished</th>
                  </tr>
                </thead>
                <tbody>
                  {etlRuns.map((run) => (
                    <tr key={displayValue(run.run_id)}>
                      <td>{shortId(displayValue(run.run_id))}</td>
                      <td>{displayValue(run.trading_date)}</td>
                      <td>{displayValue(run.status)}</td>
                      <td>{displayValue(run.extracted_rows)}</td>
                      <td>{displayValue(run.inserted_rows)}</td>
                      <td>{formatDateTime(typeof run.finished_at === "string" ? run.finished_at : undefined)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <div className="emptyState">No ETL runs have been recorded.</div>
          )}
        </article>
      </section>
    </>
  );
}
