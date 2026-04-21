import { StatCard } from "../../components/stat-card";
import { StocksScreener } from "../../components/stocks-screener";
import { UniverseDirectory } from "../../components/universe-directory";
import { fetchReferenceStocks, fetchScreener } from "../../lib/api";
import { formatPercent } from "../../lib/format";

export default async function StocksPage() {
  const [screener, reference] = await Promise.all([
    fetchScreener(45, 80),
    fetchReferenceStocks({ limit: 100 }),
  ]);
  const items = screener?.items ?? [];
  const listed = reference?.stocks ?? [];
  const alerted = items.filter((item) => item.latest_alert?.status === "open");
  const anomalous = items.filter((item) => item.latest_anomaly?.is_anomalous);
  const historical = items.filter((item) => item.daily_points >= 20);
  const hydratedCount = reference?.hydrated_count ?? historical.length;
  const pendingCount = Math.max((reference?.total_count ?? listed.length) - hydratedCount, 0);
  const topPerformer = [...items]
    .sort((left, right) => (right.indicators.return_20d_pct ?? -9999) - (left.indicators.return_20d_pct ?? -9999))
    .at(0);

  return (
    <>
      <section className="heroPanel">
        <div className="pageHeader">
          <div>
            <p className="pageKicker">Stocks</p>
            <h2 className="pageTitle">Equity directory</h2>
          </div>
          <div className="pageMetaGroup">
            <span className="metaTag">{reference?.total_count ?? listed.length} listed</span>
            <span className="metaTag">{hydratedCount} with history</span>
          </div>
        </div>
        <div className="statsGrid">
          <StatCard label="Open alerts" value={String(alerted.length)} tone="warning" />
          <StatCard label="Live anomalies" value={String(anomalous.length)} tone="critical" />
          <StatCard label="Pending hydration" value={String(pendingCount)} hint="Loads on demand or by batch job" />
          <StatCard
            label="Watchlist"
            value={String(reference?.watchlist_count ?? listed.filter((item) => item.watchlist).length)}
            hint="Default live and replay universe"
          />
          <StatCard
            label="Top 20D move"
            value={topPerformer?.symbol ?? "N/A"}
            hint={topPerformer ? formatPercent(topPerformer.indicators.return_20d_pct) : "No ranking"}
            tone="accent"
          />
        </div>
      </section>

      <section className="surface">
        <div className="panelHeader">
          <div>
            <p className="panelEyebrow">Signal monitor</p>
            <h3 className="panelTitle">Hydrated names and active signals</h3>
          </div>
          <span className="panelMeta">{items.length} rows</span>
        </div>
        {items.length ? <StocksScreener items={items} /> : <div className="emptyState">No stock history is available yet.</div>}
      </section>

      <section className="surface">
        <div className="panelHeader">
          <div>
            <p className="panelEyebrow">NSE universe</p>
            <h3 className="panelTitle">Listed symbol directory</h3>
          </div>
          <span className="panelMeta">{reference?.total_count ?? listed.length} symbols</span>
        </div>
        {listed.length ? (
          <UniverseDirectory
            initialStocks={listed}
            initialTotalCount={reference?.total_count ?? listed.length}
            initialFilteredCount={reference?.filtered_count ?? listed.length}
            watchlistCount={reference?.watchlist_count ?? 0}
            hydratedCount={reference?.hydrated_count ?? 0}
          />
        ) : (
          <div className="emptyState">The listed universe has not been synchronized yet.</div>
        )}
      </section>
    </>
  );
}
