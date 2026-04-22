"use client";

import type { ReactNode } from "react";

type InfoHintProps = {
  content: ReactNode;
  label?: string;
  align?: "start" | "end";
};

export function InfoHint({ content, label = "More information", align = "start" }: InfoHintProps) {
  return (
    <span className={`infoHint ${align}`}>
      <button type="button" className="infoHintTrigger" aria-label={label}>
        i
      </button>
      <span className={`infoHintTooltip ${align}`} role="tooltip">
        {content}
      </span>
    </span>
  );
}
