import type { ReactNode } from "react";

import { InfoHint } from "./info-hint";

type StatCardProps = {
  label: string;
  value: ReactNode;
  hint?: ReactNode;
  info?: ReactNode;
  tone?: "default" | "accent" | "warning" | "critical";
};

export function StatCard({ label, value, hint, info, tone = "default" }: StatCardProps) {
  return (
    <article className={`statCard ${tone}`}>
      <span className="statLabel">
        <span className="labelWithHint">
          <span>{label}</span>
          {info ? <InfoHint content={info} label={`${label} definition`} /> : null}
        </span>
      </span>
      <strong className="statValue">{value}</strong>
      {hint ? <span className="statHint">{hint}</span> : null}
    </article>
  );
}
