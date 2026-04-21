"use client";

import { useId, useMemo, useState, type PointerEvent } from "react";

type AnomalyScorePoint = {
  timestamp: string;
  composite: number;
  priceZ: number;
  volumeZ: number;
  flagged: boolean;
};

type AnomalyScoreChartProps = {
  points: AnomalyScorePoint[];
  height?: number;
};

type SeriesConfig = {
  key: "composite" | "priceZ" | "volumeZ";
  label: string;
  color: string;
};

type PreparedPoint = AnomalyScorePoint & {
  x: number;
  yComposite: number;
  yPriceZ: number;
  yVolumeZ: number;
};

const SERIES: SeriesConfig[] = [
  { key: "composite", label: "Composite", color: "var(--critical)" },
  { key: "priceZ", label: "Price z", color: "var(--accent-strong)" },
  { key: "volumeZ", label: "Volume z", color: "var(--warning)" },
];

function formatValue(value: number) {
  return value.toLocaleString("en-IN", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 3,
  });
}

function tooltipAlignment(ratio: number) {
  if (ratio <= 0.18) {
    return "left";
  }
  if (ratio >= 0.82) {
    return "right";
  }
  return "center";
}

export function AnomalyScoreChart({ points, height = 220 }: AnomalyScoreChartProps) {
  const [activeIndex, setActiveIndex] = useState<number | null>(null);
  const width = 760;
  const fillId = useId().replace(/:/g, "");
  const allValues = points.flatMap((point) => [point.composite, point.priceZ, point.volumeZ]);
  const min = allValues.length ? Math.min(...allValues) : 0;
  const max = allValues.length ? Math.max(...allValues) : 0;
  const span = Math.max(max - min, 1);
  const flagFloor = Math.min(
    ...points.filter((point) => point.flagged).map((point) => point.composite),
    Number.POSITIVE_INFINITY,
  );

  function pointY(value: number) {
    return height - ((value - min) / span) * (height - 28) - 14;
  }

  function pointX(index: number) {
    return points.length === 1 ? width / 2 : (index / Math.max(points.length - 1, 1)) * width;
  }

  const preparedPoints = useMemo<PreparedPoint[]>(() => {
    return points.map((point, index) => ({
      ...point,
      x: pointX(index),
      yComposite: pointY(point.composite),
      yPriceZ: pointY(point.priceZ),
      yVolumeZ: pointY(point.volumeZ),
    }));
  }, [points, width, height, min, span]);

  if (!points.length) {
    return <div className="emptyState">No anomaly curve is available for the latest session.</div>;
  }

  function pathFor(key: SeriesConfig["key"]) {
    return preparedPoints
      .map((point) => {
        if (key === "composite") {
          return `${point.x},${point.yComposite}`;
        }
        if (key === "priceZ") {
          return `${point.x},${point.yPriceZ}`;
        }
        return `${point.x},${point.yVolumeZ}`;
      })
      .join(" ");
  }

  function handlePointerMove(event: PointerEvent<SVGRectElement>) {
    const bounds = event.currentTarget.getBoundingClientRect();
    const ratio = Math.min(Math.max((event.clientX - bounds.left) / Math.max(bounds.width, 1), 0), 1);
    const index = Math.round(ratio * Math.max(points.length - 1, 0));
    setActiveIndex(index);
  }

  const compositeArea = `0,${height} ${pathFor("composite")} ${width},${height}`;
  const activePoint = activeIndex === null ? null : preparedPoints[activeIndex];
  const activeRatio = activePoint ? activePoint.x / width : 0;
  const alignment = tooltipAlignment(activeRatio);

  return (
    <div className="signalChartShell">
      <div className="signalLegend">
        {SERIES.map((series) => (
          <span key={series.key} className="signalLegendItem">
            <span className="signalLegendSwatch" style={{ background: series.color }} />
            {series.label}
          </span>
        ))}
      </div>
      <div className="signalChartFrame">
        <svg viewBox={`0 0 ${width} ${height}`} className="signalChart" role="img" aria-label="anomaly score chart">
          <defs>
            <linearGradient id={`signalFill-${fillId}`} x1="0" x2="0" y1="0" y2="1">
              <stop offset="0%" stopColor="rgba(255, 106, 122, 0.26)" />
              <stop offset="100%" stopColor="rgba(255, 106, 122, 0.03)" />
            </linearGradient>
          </defs>
          {[0.2, 0.4, 0.6, 0.8].map((ratio) => (
            <line
              key={ratio}
              x1="0"
              x2={width}
              y1={height * ratio}
              y2={height * ratio}
              className="signalGridLine"
            />
          ))}
          {Number.isFinite(flagFloor) ? (
            <line
              x1="0"
              x2={width}
              y1={pointY(flagFloor)}
              y2={pointY(flagFloor)}
              className="signalThresholdLine"
            />
          ) : null}
          <polyline points={compositeArea} fill={`url(#signalFill-${fillId})`} stroke="none" />
          {SERIES.map((series) => (
            <polyline
              key={series.key}
              points={pathFor(series.key)}
              fill="none"
              stroke={series.color}
              strokeWidth={series.key === "composite" ? "3" : "2.2"}
              strokeLinejoin="round"
              strokeLinecap="round"
            />
          ))}
          {preparedPoints
            .filter((point) => point.flagged)
            .map((point) => (
              <circle
                key={`${point.timestamp}-${point.x}`}
                cx={point.x}
                cy={point.yComposite}
                r="4.5"
                className="signalFlagPoint"
              />
            ))}
          {activePoint ? (
            <>
              <line x1={activePoint.x} x2={activePoint.x} y1="0" y2={height} className="signalHoverGuide" />
              <circle cx={activePoint.x} cy={activePoint.yComposite} r="5" className="signalActivePoint composite" />
              <circle cx={activePoint.x} cy={activePoint.yPriceZ} r="4.5" className="signalActivePoint priceZ" />
              <circle cx={activePoint.x} cy={activePoint.yVolumeZ} r="4.5" className="signalActivePoint volumeZ" />
            </>
          ) : null}
          <rect
            x="0"
            y="0"
            width={width}
            height={height}
            fill="transparent"
            className="chartInteractionLayer"
            onPointerMove={handlePointerMove}
            onPointerLeave={() => setActiveIndex(null)}
            onPointerDown={handlePointerMove}
          />
        </svg>
        {activePoint ? (
          <div
            className={`signalTooltip ${alignment}`}
            style={{
              left: `${activeRatio * 100}%`,
              top: `${(activePoint.yComposite / height) * 100}%`,
            }}
          >
            <span className="chartTooltipLabel">{activePoint.timestamp}</span>
            <strong>{activePoint.flagged ? "Flagged point" : "Observed point"}</strong>
            <div className="signalTooltipMetrics">
              <span>Composite {formatValue(activePoint.composite)}</span>
              <span>Price z {formatValue(activePoint.priceZ)}</span>
              <span>Volume z {formatValue(activePoint.volumeZ)}</span>
            </div>
          </div>
        ) : null}
      </div>
      <div className="signalAxisLabels">
        <span>{points[0]?.timestamp ?? ""}</span>
        <span>{points[Math.floor(points.length / 2)]?.timestamp ?? ""}</span>
        <span>{points.at(-1)?.timestamp ?? ""}</span>
      </div>
    </div>
  );
}
