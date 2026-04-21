import { StatCard } from "../../components/stat-card";
import { ContagionNetwork } from "../../components/contagion-network";
import { IntensityBars } from "../../components/intensity-bars";
import { fetchContagion } from "../../lib/api";
import { formatDateTime, formatNumber } from "../../lib/format";

function affectedLabel(symbols: string[] | undefined, count: number) {
  if (!Array.isArray(symbols) || symbols.length === 0) {
    return String(count);
  }
  const preview = symbols.slice(0, 4).join(", ");
  if (symbols.length <= 4) {
    return preview;
  }
  return `${preview}, +${symbols.length - 4} more`;
}

export default async function ContagionPage() {
  const events = (await fetchContagion()) ?? [];
  const averageRisk =
    events.length > 0 ? events.reduce((sum, item) => sum + (item.risk_score ?? 0), 0) / events.length : 0;
  const largestSpread = events.reduce((max, item) => Math.max(max, item.affected_count ?? 0), 0);
  const latestEvent = events[0] ?? null;
  const sectorPressureMap = new Map<
    string,
    { label: string; value: number; eventCount: number; latestSpread: number; tone: "critical" | "warning" }
  >();

  for (const event of events) {
    const sector = event.trigger_sector || "Unknown";
    const current = sectorPressureMap.get(sector) ?? {
      label: sector,
      value: 0,
      eventCount: 0,
      latestSpread: 0,
      tone: "critical" as const,
    };
    current.eventCount += 1;
    current.value += Number(event.risk_score ?? 0);
    current.latestSpread = Number(event.affected_count ?? current.latestSpread);
    sectorPressureMap.set(sector, current);
  }

  const sectorPressure = [...sectorPressureMap.values()]
    .sort((left, right) => right.value - left.value)
    .slice(0, 8)
    .map((item) => ({
      label: item.label,
      value: item.value,
      detail: `${item.eventCount} events | ${item.latestSpread} peers in latest burst`,
      tone: item.tone,
    }));

  return (
    <>
      <section className="heroPanel">
        <div className="pageHeader">
          <div>
            <p className="pageKicker">Contagion</p>
            <h2 className="pageTitle">Contagion monitor</h2>
          </div>
          <div className="pageMetaGroup">
            <span className="metaTag">{events.length} events</span>
          </div>
        </div>
        <div className="statsGrid">
          <StatCard label="Events" value={String(events.length)} tone="critical" />
          <StatCard label="Average risk" value={formatNumber(averageRisk, 3)} />
          <StatCard label="Largest spread" value={String(largestSpread)} hint="Affected peers" />
          <StatCard label="Window" value="5 min" hint="Sector peer horizon" tone="accent" />
        </div>
      </section>

      <section className="contentGrid twoUp">
        <article className="surface">
          <div className="panelHeader">
            <div>
              <p className="panelEyebrow">Latest cascade</p>
              <h3 className="panelTitle">Propagation network</h3>
            </div>
            <span className="panelMeta">{latestEvent ? formatDateTime(latestEvent.event_timestamp) : "No event"}</span>
          </div>
          <ContagionNetwork event={latestEvent} />
        </article>

        <article className="surface">
          <div className="panelHeader">
            <div>
              <p className="panelEyebrow">Sector pressure</p>
              <h3 className="panelTitle">Cascade concentration</h3>
            </div>
            <span className="panelMeta">{sectorPressure.length} sectors</span>
          </div>
          <IntensityBars
            items={sectorPressure}
            valueFormatter={(value) => formatNumber(value, 3)}
            emptyMessage="No sector contagion clusters are active."
          />
        </article>
      </section>

      <section className="surface">
        <div className="panelHeader">
          <div>
            <p className="panelEyebrow">Operational events</p>
            <h3 className="panelTitle">Persisted contagion windows</h3>
          </div>
          <span className="panelMeta">{events.length} rows</span>
        </div>
        {events.length ? (
          <div className="tableWrap">
            <table className="dataTable">
              <thead>
                <tr>
                  <th>Trigger</th>
                  <th>Sector</th>
                  <th>Affected</th>
                  <th>Peer avg</th>
                  <th>Risk</th>
                  <th>Timestamp</th>
                </tr>
              </thead>
              <tbody>
                {events.map((event) => (
                  <tr key={event.event_id}>
                    <td>{event.trigger_symbol}</td>
                    <td>{event.trigger_sector}</td>
                    <td>{affectedLabel(event.affected_symbols, event.affected_count)}</td>
                    <td>{formatNumber(event.peer_average_score, 3)}</td>
                    <td>{formatNumber(event.risk_score, 3)}</td>
                    <td>{formatDateTime(event.event_timestamp)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <div className="emptyState">No contagion events have been persisted yet.</div>
        )}
      </section>
    </>
  );
}
