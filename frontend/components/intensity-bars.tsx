type IntensityBarItem = {
  label: string;
  value: number;
  detail?: string;
  tone?: "accent" | "warning" | "critical" | "success";
};

type IntensityBarsProps = {
  items: IntensityBarItem[];
  valueFormatter?: (value: number) => string;
  emptyMessage?: string;
};

export function IntensityBars({
  items,
  valueFormatter = (value) => value.toLocaleString("en-IN"),
  emptyMessage = "No data available.",
}: IntensityBarsProps) {
  if (!items.length) {
    return <div className="emptyState">{emptyMessage}</div>;
  }

  const peak = Math.max(...items.map((item) => item.value), 1);

  return (
    <div className="intensityList">
      {items.map((item) => {
        const width = Math.max((item.value / peak) * 100, item.value > 0 ? 8 : 0);
        return (
          <div key={`${item.label}-${item.detail ?? "bar"}`} className="intensityRow">
            <div className="intensityHeader">
              <div className="intensityCopy">
                <strong>{item.label}</strong>
                {item.detail ? <span className="metricSubtext">{item.detail}</span> : null}
              </div>
              <span className="intensityValue">{valueFormatter(item.value)}</span>
            </div>
            <div className="intensityTrack" aria-hidden="true">
              <div className={`intensityFill ${item.tone ?? "accent"}`} style={{ width: `${width}%` }} />
            </div>
          </div>
        );
      })}
    </div>
  );
}
