import { ExplainerCards } from "../../components/explainer-cards";
import { StatCard } from "../../components/stat-card";
import { fetchMethodology } from "../../lib/api";
import { formatNumber } from "../../lib/format";

export default async function MethodologyPage() {
  const methodology = await fetchMethodology();

  if (!methodology) {
    return (
      <section className="surface">
        <div className="emptyState">Methodology data is not available right now.</div>
      </section>
    );
  }

  return (
    <>
      <section className="heroPanel">
        <div className="pageHeader">
          <div>
            <p className="pageKicker">Methodology</p>
            <h2 className="pageTitle">Signal definitions and thresholds</h2>
          </div>
          <div className="pageMetaGroup">
            <span className="metaTag">{methodology.market.timezone}</span>
            <span className="metaTag">
              {methodology.market.session_open} - {methodology.market.session_close}
            </span>
          </div>
        </div>
        <div className="statsGrid">
          <StatCard
            label="Warmup"
            value={`${methodology.anomaly.warmup_minutes} min`}
            hint="Streaming baseline before scoring begins"
            tone="accent"
          />
          <StatCard
            label="EWMA alpha"
            value={formatNumber(methodology.anomaly.ewma_alpha, 4)}
            hint="Decay factor for mean and variance"
          />
          <StatCard
            label="Price z threshold"
            value={formatNumber(methodology.anomaly.price_z_threshold, 2)}
            hint="Absolute threshold"
            tone="warning"
          />
          <StatCard
            label="Volume z threshold"
            value={formatNumber(methodology.anomaly.volume_z_threshold, 2)}
            hint="Absolute threshold"
            tone="warning"
          />
          <StatCard
            label="Composite threshold"
            value={formatNumber(methodology.anomaly.composite_threshold, 2)}
            hint="0.6 price | 0.4 volume"
            tone="critical"
          />
          <StatCard
            label="Contagion window"
            value={`${methodology.contagion.window_minutes} min`}
            hint="Sector peer confirmation horizon"
          />
        </div>
      </section>

      <ExplainerCards
        eyebrow="Why these numbers"
        title="How the current configuration should be interpreted"
        meta="Threshold rationale"
        items={[
          {
            title: "Sensitive by design",
            description: methodology.anomaly.threshold_rationale,
            tone: "warning",
          },
          {
            title: "Flag rule",
            description: methodology.anomaly.flag_rule,
            tone: "critical",
          },
          {
            title: "Alert cooldown",
            value: `${methodology.alerts.cooldown_minutes} min`,
            description: methodology.alerts.logic,
            tone: "accent",
          },
          {
            title: "Market scope",
            value: `${methodology.market.session_minutes} min`,
            description: methodology.market.scope,
          },
        ]}
      />

      <section className="surface">
        <div className="panelHeader">
          <div>
            <p className="panelEyebrow">Anomaly engine</p>
            <h3 className="panelTitle">Core formulas</h3>
          </div>
          <span className="panelMeta">Streaming math used in the live detector</span>
        </div>
        <div className="tableWrap">
          <table className="dataTable">
            <thead>
              <tr>
                <th>Metric</th>
                <th>Formula</th>
                <th>Meaning</th>
              </tr>
            </thead>
            <tbody>
              {methodology.anomaly.formulas.map((row) => (
                <tr key={row.name}>
                  <td>{row.name}</td>
                  <td className="dataMono">{row.formula}</td>
                  <td>{row.meaning}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      <section className="contentGrid twoUp">
        <article className="surface">
          <div className="panelHeader">
            <div>
              <p className="panelEyebrow">Alerting</p>
              <h3 className="panelTitle">Severity bands</h3>
            </div>
            <span className="panelMeta">{methodology.alerts.notification_min_severity}+ for webhooks</span>
          </div>
          <div className="tableWrap">
            <table className="dataTable">
              <thead>
                <tr>
                  <th>Severity</th>
                  <th>Rule</th>
                </tr>
              </thead>
              <tbody>
                {methodology.anomaly.severity_bands.map((band) => (
                  <tr key={band.severity}>
                    <td>
                      <span className={`severityTag ${band.severity}`}>{band.severity}</span>
                    </td>
                    <td>{band.rule}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </article>

        <article className="surface">
          <div className="panelHeader">
            <div>
              <p className="panelEyebrow">Contagion</p>
              <h3 className="panelTitle">Propagation logic</h3>
            </div>
            <span className="panelMeta">Sector-bounded v1 model</span>
          </div>
          <div className="stackList">
            <div className="explainerCard accent">
              <div className="explainerTitleRow">
                <strong>Trigger rule</strong>
              </div>
              <div className="explainerText">{methodology.contagion.trigger_rule}</div>
            </div>
            <div className="explainerCard warning">
              <div className="explainerTitleRow">
                <strong>Peer rule</strong>
              </div>
              <div className="explainerText">{methodology.contagion.peer_rule}</div>
            </div>
            <div className="explainerCard critical">
              <div className="explainerTitleRow">
                <strong>Risk score</strong>
                <span className="explainerValue dataMono">{methodology.contagion.risk_score_formula}</span>
              </div>
              <div className="explainerText">{methodology.contagion.why}</div>
            </div>
          </div>
        </article>
      </section>

      <section className="contentGrid twoUp">
        <article className="surface">
          <div className="panelHeader">
            <div>
              <p className="panelEyebrow">Glossary</p>
              <h3 className="panelTitle">Terms used across the console</h3>
            </div>
            <span className="panelMeta">UI vocabulary</span>
          </div>
          <div className="stackList">
            <div className="explainerCard">
              <div className="explainerTitleRow">
                <strong>Signal</strong>
              </div>
              <div className="explainerText">The latest anomaly measurement for one stock at one minute.</div>
            </div>
            <div className="explainerCard">
              <div className="explainerTitleRow">
                <strong>Alert</strong>
              </div>
              <div className="explainerText">A persisted operator event created from a signal or contagion event after the alert rules run.</div>
            </div>
            <div className="explainerCard">
              <div className="explainerTitleRow">
                <strong>Stale alert</strong>
              </div>
              <div className="explainerText">An unresolved alert from an older monitored trading session, shown separately from the current session queue.</div>
            </div>
            <div className="explainerCard">
              <div className="explainerTitleRow">
                <strong>Regime</strong>
              </div>
              <div className="explainerText">A recurring cross-session pressure pattern captured in warehouse rollups such as sector momentum and stock persistence.</div>
            </div>
          </div>
        </article>

        <article className="surface">
          <div className="panelHeader">
            <div>
              <p className="panelEyebrow">Warehouse</p>
              <h3 className="panelTitle">Why the facts are separated</h3>
            </div>
            <span className="panelMeta">Modeling rationale</span>
          </div>
          <div className="stackList">
            {methodology.warehouse.facts.map((fact) => (
              <div key={fact} className="explainerCard">
                <div className="explainerText">{fact}</div>
              </div>
            ))}
            <div className="statusNote">{methodology.warehouse.why}</div>
          </div>
        </article>
      </section>
    </>
  );
}
