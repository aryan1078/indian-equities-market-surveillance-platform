export function shortId(value: string | null | undefined, size = 8) {
  if (!value) {
    return "N/A";
  }
  return value.length <= size ? value : `${value.slice(0, size)}...`;
}

const MARKET_TIMEZONE = "Asia/Kolkata";

function toDate(value: string | null | undefined) {
  if (!value) {
    return null;
  }
  const normalized =
    /^\d{4}-\d{2}-\d{2}$/.test(value)
      ? `${value}T00:00:00+05:30`
      : value;
  const parsed = new Date(normalized);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
}

export function formatDateTime(value: string | null | undefined) {
  const parsed = toDate(value);
  if (!parsed) {
    return "N/A";
  }
  return parsed.toLocaleString("en-IN", {
    day: "2-digit",
    month: "short",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
    timeZone: MARKET_TIMEZONE,
  });
}

export function formatDate(value: string | null | undefined) {
  const parsed = toDate(value);
  if (!parsed) {
    return "N/A";
  }
  return parsed.toLocaleDateString("en-IN", {
    day: "2-digit",
    month: "short",
    year: "numeric",
    timeZone: MARKET_TIMEZONE,
  });
}

export function formatMonth(value: string | null | undefined) {
  const parsed = toDate(value);
  if (!parsed) {
    return "N/A";
  }
  return parsed.toLocaleDateString("en-IN", {
    month: "short",
    year: "numeric",
    timeZone: MARKET_TIMEZONE,
  });
}

export function formatTime(value: string | null | undefined) {
  const parsed = toDate(value);
  if (!parsed) {
    return "N/A";
  }
  return parsed.toLocaleTimeString("en-IN", {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
    timeZone: MARKET_TIMEZONE,
  });
}

export function formatNumber(value: number | null | undefined, digits = 2) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "N/A";
  }
  return value.toLocaleString("en-IN", {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
}

export function formatInteger(value: number | null | undefined) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "N/A";
  }
  return value.toLocaleString("en-IN");
}

export function formatCompactIndian(value: number | null | undefined, digits = 2) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "N/A";
  }
  const absolute = Math.abs(value);
  if (absolute >= 10_000_000) {
    return `${formatNumber(value / 10_000_000, digits)} cr`;
  }
  if (absolute >= 100_000) {
    return `${formatNumber(value / 100_000, digits)} lakh`;
  }
  if (absolute >= 1_000) {
    return `${formatNumber(value / 1_000, digits)}k`;
  }
  return formatInteger(value);
}

export function formatPercent(value: number | null | undefined, digits = 2) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "N/A";
  }
  const prefix = value > 0 ? "+" : "";
  return `${prefix}${formatNumber(value, digits)}%`;
}

export function fixtureLabel(path: string | null | undefined) {
  if (!path) {
    return "N/A";
  }
  const parts = path.split(/[\\/]/).filter(Boolean);
  return parts.at(-1) ?? path;
}

export function compactPath(path: string | null | undefined, segments = 3) {
  if (!path) {
    return "N/A";
  }
  const parts = path.split(/[\\/]/).filter(Boolean);
  if (parts.length <= segments) {
    return path;
  }
  return `.../${parts.slice(-segments).join("/")}`;
}

export function severityLabel(value: string | null | undefined) {
  if (!value) {
    return "normal";
  }
  return value.toLowerCase();
}
