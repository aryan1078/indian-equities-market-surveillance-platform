import { StatCard } from "../../components/stat-card";
import { fetchReplayStatus } from "../../lib/api";
import { compactPath, fixtureLabel, formatDateTime, shortId } from "../../lib/format";

export default async function ReplayPage() {
  const replay = await fetchReplayStatus();
  const fixture = replay?.notes?.fixture ?? null;

  return (
    <>
      <section className="heroPanel">
        <div className="pageHeader">
          <div>
            <p className="pageKicker">Replay</p>
            <h2 className="pageTitle">Replay control</h2>
          </div>
          <div className="pageMetaGroup">
            <span className="metaTag">{replay?.status ?? "idle"}</span>
            <span className="metaTag">{replay?.notes?.trading_date ?? "No trading date"}</span>
          </div>
        </div>
        <div className="statsGrid">
          <StatCard label="Run" value={shortId(replay?.run_id)} hint={replay?.run_id ?? "No replay run"} />
          <StatCard label="Fixture" value={fixtureLabel(fixture)} hint={fixture ?? "No fixture"} />
          <StatCard label="Symbols" value={String(replay?.symbol_count ?? 0)} />
          <StatCard label="Rows published" value={String(replay?.records_published ?? 0)} tone="accent" />
        </div>
      </section>

      <section className="contentGrid twoUp">
        <article className="surface">
          <div className="panelHeader">
            <div>
              <p className="panelEyebrow">Run metadata</p>
              <h3 className="panelTitle">Latest replay</h3>
            </div>
          </div>
          <div className="stackList">
            <div className="metricRow">
              <div>
                <strong>Started</strong>
                <div className="metricSubtext">{formatDateTime(replay?.started_at)}</div>
              </div>
            </div>
            <div className="metricRow">
              <div>
                <strong>Finished</strong>
                <div className="metricSubtext">{formatDateTime(replay?.finished_at)}</div>
              </div>
            </div>
            <div className="metricRow">
              <div>
                <strong>Rows seen</strong>
                <div className="metricSubtext">{String(replay?.records_seen ?? 0)}</div>
              </div>
            </div>
            <div className="metricRow">
              <div>
                <strong>Speed</strong>
                <div className="metricSubtext">
                  {replay?.notes?.speed ? `${replay.notes.speed}x` : "Not recorded"}
                </div>
              </div>
            </div>
          </div>
        </article>

        <article className="surface">
          <div className="panelHeader">
            <div>
              <p className="panelEyebrow">Fixture</p>
              <h3 className="panelTitle">Replay source</h3>
            </div>
          </div>
          <div className="stackList">
            <div className="metricRow">
              <div>
                <strong>Path</strong>
                <div className="metricSubtext">{compactPath(fixture)}</div>
              </div>
            </div>
            <div className="metricRow">
              <div>
                <strong>Trading date</strong>
                <div className="metricSubtext">{replay?.notes?.trading_date ?? "N/A"}</div>
              </div>
            </div>
            <div className="metricRow">
              <div>
                <strong>Status</strong>
                <div className="metricSubtext">{replay?.status ?? "idle"}</div>
              </div>
            </div>
          </div>
        </article>
      </section>
    </>
  );
}
