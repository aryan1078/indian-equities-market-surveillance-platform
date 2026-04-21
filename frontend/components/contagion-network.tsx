import type { ContagionItem } from "../lib/api";
import { formatNumber } from "../lib/format";

type ContagionNetworkProps = {
  event: ContagionItem | null | undefined;
};

function compactSymbol(symbol: string) {
  const normalized = symbol.replace(".NS", "").replace(".BO", "");
  return normalized.length > 10 ? `${normalized.slice(0, 10)}…` : normalized;
}

export function ContagionNetwork({ event }: ContagionNetworkProps) {
  if (!event) {
    return <div className="emptyState">No contagion network is available.</div>;
  }

  const peers = (event.affected_symbols ?? []).slice(0, 8);
  const width = 720;
  const height = 320;
  const centerX = width / 2;
  const centerY = height / 2;
  const radius = 114;

  return (
    <div className="networkSurface">
      <div className="networkHeader">
        <div>
          <strong>{event.trigger_symbol}</strong>
          <div className="metricSubtext">{event.trigger_sector}</div>
        </div>
        <div className="networkMeta">
          <span>{event.affected_count} peers</span>
          <span>{formatNumber(event.risk_score, 3)} risk</span>
        </div>
      </div>
      <svg viewBox={`0 0 ${width} ${height}`} className="contagionNetwork" role="img" aria-label="contagion network">
        <defs>
          <radialGradient id="triggerGlow" cx="50%" cy="50%" r="60%">
            <stop offset="0%" stopColor="rgba(255, 106, 122, 0.40)" />
            <stop offset="100%" stopColor="rgba(255, 106, 122, 0.06)" />
          </radialGradient>
        </defs>
        <circle cx={centerX} cy={centerY} r="64" fill="url(#triggerGlow)" />
        {peers.map((symbol, index) => {
          const angle = (Math.PI * 2 * index) / Math.max(peers.length, 1) - Math.PI / 2;
          const x = centerX + radius * Math.cos(angle);
          const y = centerY + radius * Math.sin(angle);
          return (
            <g key={symbol}>
              <line
                x1={centerX}
                y1={centerY}
                x2={x}
                y2={y}
                className="networkEdge"
                style={{ strokeWidth: `${Math.max(1.6, (event.risk_score ?? 1) * 0.65)}` }}
              />
              <circle cx={x} cy={y} r="24" className="networkPeerNode" />
              <text x={x} y={y + 4} className="networkPeerLabel" textAnchor="middle">
                {compactSymbol(symbol)}
              </text>
            </g>
          );
        })}
        <circle cx={centerX} cy={centerY} r="36" className="networkTriggerNode" />
        <text x={centerX} y={centerY - 2} className="networkTriggerLabel" textAnchor="middle">
          {compactSymbol(event.trigger_symbol)}
        </text>
        <text x={centerX} y={centerY + 18} className="networkTriggerSubLabel" textAnchor="middle">
          {event.affected_count} links
        </text>
      </svg>
      <div className="networkFooter">
        <span>{event.rationale ?? "Sector-linked contagion event."}</span>
      </div>
    </div>
  );
}
