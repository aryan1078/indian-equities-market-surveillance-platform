import type { ReactNode } from "react";

type StatCardProps = {
  label: string;
  value: ReactNode;
  hint?: ReactNode;
  tone?: "default" | "accent" | "warning" | "critical";
};

export function StatCard({ label, value, hint, tone = "default" }: StatCardProps) {
  return (
    <article className={`statCard ${tone}`}>
      <span className="statLabel">{label}</span>
      <strong className="statValue">{value}</strong>
      {hint ? <span className="statHint">{hint}</span> : null}
    </article>
  );
}
