"use client";

import { useId, useMemo, useState, type PointerEvent } from "react";

type LineChartProps = {
  values: number[];
  labels?: string[];
  color?: string;
  height?: number;
  valueDigits?: number;
  seriesLabel?: string;
};

type ChartPoint = {
  x: number;
  y: number;
  value: number;
  label: string;
};

function formatValue(value: number, digits: number) {
  return value.toLocaleString("en-IN", {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
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

export function LineChart({
  values,
  labels = [],
  color = "var(--accent)",
  height = 220,
  valueDigits = 2,
  seriesLabel = "series",
}: LineChartProps) {
  const [activeIndex, setActiveIndex] = useState<number | null>(null);
  const width = 720;
  const gradientId = useId().replace(/:/g, "");

  const min = values.length ? Math.min(...values) : 0;
  const max = values.length ? Math.max(...values) : 0;
  const span = Math.max(max - min, 1);

  const chartPoints = useMemo<ChartPoint[]>(() => {
    return values.map((value, index) => {
      const x =
        values.length === 1
          ? width / 2
          : (index / Math.max(values.length - 1, 1)) * width;
      const y = height - ((value - min) / span) * (height - 18) - 9;
      return {
        x,
        y,
        value,
        label: labels[index] ?? `Point ${index + 1}`,
      };
    });
  }, [height, labels, max, min, span, values]);

  if (!values.length) {
    return <div className="chartEmpty">No data</div>;
  }

  const linePoints = chartPoints.map((point) => `${point.x},${point.y}`).join(" ");
  const area = `0,${height} ${linePoints} ${width},${height}`;
  const activePoint = activeIndex === null ? null : chartPoints[activeIndex];
  const activeRatio = activePoint ? activePoint.x / width : 0;
  const alignment = tooltipAlignment(activeRatio);
  const latestPoint = chartPoints.at(-1) ?? chartPoints[0];
  const minPoint = chartPoints.reduce((lowest, point) => (point.value < lowest.value ? point : lowest), chartPoints[0]);
  const maxPoint = chartPoints.reduce((highest, point) => (point.value > highest.value ? point : highest), chartPoints[0]);

  function handlePointerMove(event: PointerEvent<SVGRectElement>) {
    const bounds = event.currentTarget.getBoundingClientRect();
    const ratio = Math.min(Math.max((event.clientX - bounds.left) / Math.max(bounds.width, 1), 0), 1);
    const index = Math.round(ratio * Math.max(values.length - 1, 0));
    setActiveIndex(index);
  }

  return (
    <div className="lineChartShell">
      <svg viewBox={`0 0 ${width} ${height}`} className="lineChart" role="img" aria-label={`${seriesLabel} chart`}>
        <defs>
          <linearGradient id={`chartFill-${gradientId}`} x1="0" x2="0" y1="0" y2="1">
            <stop offset="0%" stopColor={color} stopOpacity="0.32" />
            <stop offset="100%" stopColor={color} stopOpacity="0.02" />
          </linearGradient>
        </defs>
        <polyline points={area} fill={`url(#chartFill-${gradientId})`} stroke="none" />
        <polyline
          points={linePoints}
          fill="none"
          stroke={color}
          strokeWidth="3"
          strokeLinejoin="round"
          strokeLinecap="round"
        />
        {activePoint ? (
          <>
            <line x1={activePoint.x} x2={activePoint.x} y1="0" y2={height} className="chartCrosshair" />
            <circle cx={activePoint.x} cy={activePoint.y} r="5" fill={color} className="chartActivePoint" />
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
          className={`chartTooltip ${alignment}`}
          style={{
            left: `${activeRatio * 100}%`,
            top: `${(activePoint.y / height) * 100}%`,
          }}
        >
          <span className="chartTooltipLabel">{activePoint.label}</span>
          <strong>{formatValue(activePoint.value, valueDigits)}</strong>
        </div>
      ) : null}
      <div className="chartSummaryRow" aria-label={`${seriesLabel} summary`}>
        <div className="chartSummaryCard">
          <span>Min</span>
          <strong>{formatValue(minPoint.value, valueDigits)}</strong>
          <small>{minPoint.label}</small>
        </div>
        <div className="chartSummaryCard">
          <span>Max</span>
          <strong>{formatValue(maxPoint.value, valueDigits)}</strong>
          <small>{maxPoint.label}</small>
        </div>
        <div className="chartSummaryCard">
          <span>Last</span>
          <strong>{formatValue(latestPoint.value, valueDigits)}</strong>
          <small>{latestPoint.label}</small>
        </div>
      </div>
    </div>
  );
}
