"use client";

import { useMemo, useState, useTransition } from "react";

import { InfoHint } from "./info-hint";
import { IntensityBars } from "./intensity-bars";
import { LineChart } from "./line-chart";
import { StatCard } from "./stat-card";
import {
  fetchWarehouseQuery,
  type WarehouseQueryDataset,
  type WarehouseQueryField,
  type WarehouseQueryMetadataResponse,
  type WarehouseQueryRequest,
  type WarehouseQueryResponse,
} from "../lib/api";
import { formatCompactIndian, formatDate, formatDateTime, formatNumber } from "../lib/format";

type WarehouseAnalystStudioProps = {
  metadata: WarehouseQueryMetadataResponse;
  initialRequest: WarehouseQueryRequest;
  initialResult: WarehouseQueryResponse;
};

const LIMIT_OPTIONS = [25, 50, 100, 250, 500];

function defaultsForDataset(dataset: WarehouseQueryDataset): WarehouseQueryRequest {
  return {
    dataset: dataset.key,
    dimensions: [...dataset.defaults.dimensions],
    measures: [...dataset.defaults.measures],
    date_from: dataset.defaults.date_from ?? null,
    date_to: dataset.defaults.date_to ?? null,
    sector: null,
    exchange: null,
    symbol_search: null,
    min_signal: null,
    sort_field: dataset.defaults.sort_field,
    sort_direction: dataset.defaults.sort_direction,
    limit: dataset.defaults.limit,
  };
}

function fieldMap(fields: WarehouseQueryField[]) {
  return new Map(fields.map((field) => [field.key, field]));
}

function formatCell(value: string | number | null | undefined, kind: WarehouseQueryField["kind"]) {
  if (value === null || value === undefined || value === "") {
    return "N/A";
  }
  if ((kind === "number" || kind === "integer") && typeof value === "number") {
    return kind === "integer" ? Math.round(value).toLocaleString("en-IN") : formatNumber(value, 3);
  }
  if (kind === "date" && typeof value === "string") {
    return formatDate(value);
  }
  if (kind === "datetime" && typeof value === "string") {
    return formatDateTime(value);
  }
  return String(value);
}

function csvEscape(value: string) {
  return `"${value.replaceAll('"', '""')}"`;
}

function escapeHtml(value: string) {
  return value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function printReportDocument(title: string, result: WarehouseQueryResponse) {
  if (typeof window === "undefined") {
    return;
  }
  const opened = window.open("", "_blank", "noopener,noreferrer");
  if (!opened) {
    return;
  }

  const headerCells = result.columns.map((column) => `<th>${escapeHtml(column.label)}</th>`).join("");
  const bodyRows = result.rows
    .map(
      (row) =>
        `<tr>${result.columns
          .map((column) => `<td>${escapeHtml(formatCell(row[column.key], column.kind))}</td>`)
          .join("")}</tr>`,
    )
    .join("");
  const findings = result.report.findings.map((finding) => `<li>${escapeHtml(finding)}</li>`).join("");
  const highlights = result.report.highlights
    .map(
      (highlight) =>
        `<div class="highlight"><span>${escapeHtml(highlight.label)}</span><strong>${escapeHtml(highlight.value)}</strong></div>`,
    )
    .join("");

  opened.document.write(`<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <title>${escapeHtml(title)}</title>
    <style>
      body { font-family: "Segoe UI", Arial, sans-serif; margin: 32px; color: #10151d; }
      h1 { margin: 0 0 8px; font-size: 28px; }
      h2 { margin: 28px 0 12px; font-size: 18px; }
      p, li { line-height: 1.6; }
      .meta { color: #5a6777; margin-bottom: 18px; }
      .grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 12px; margin: 16px 0 24px; }
      .highlight { border: 1px solid #d7deea; border-radius: 12px; padding: 12px 14px; }
      .highlight span { display: block; color: #5a6777; font-size: 12px; text-transform: uppercase; letter-spacing: 0.08em; }
      .highlight strong { display: block; margin-top: 6px; font-size: 18px; }
      table { width: 100%; border-collapse: collapse; margin-top: 12px; font-size: 13px; }
      th, td { border: 1px solid #d7deea; padding: 8px 10px; text-align: left; vertical-align: top; }
      th { background: #eef3f8; text-transform: uppercase; font-size: 11px; letter-spacing: 0.08em; }
      @media print {
        body { margin: 18px; }
      }
    </style>
  </head>
  <body>
    <h1>${escapeHtml(title)}</h1>
    <div class="meta">${escapeHtml(result.dataset.label)} | ${escapeHtml(result.query.preview)} | ${escapeHtml(result.generated_at)}</div>
    <div class="grid">${highlights}</div>
    <h2>Analyst Findings</h2>
    <ul>${findings}</ul>
    <h2>Result Set</h2>
    <table>
      <thead><tr>${headerCells}</tr></thead>
      <tbody>${bodyRows}</tbody>
    </table>
  </body>
</html>`);
  opened.document.close();
  opened.focus();
  opened.print();
}

export function WarehouseAnalystStudio({
  metadata,
  initialRequest,
  initialResult,
}: WarehouseAnalystStudioProps) {
  const datasetMap = useMemo(() => new Map(metadata.datasets.map((dataset) => [dataset.key, dataset])), [metadata.datasets]);
  const [draft, setDraft] = useState<WarehouseQueryRequest>(initialRequest);
  const [result, setResult] = useState<WarehouseQueryResponse>(initialResult);
  const [reportTitle, setReportTitle] = useState(`${initialResult.dataset.label} report`);
  const [error, setError] = useState<string | null>(null);
  const [isPending, startTransition] = useTransition();

  const currentDataset = datasetMap.get(draft.dataset) ?? metadata.datasets[0];
  const currentDimensionMap = useMemo(() => fieldMap(currentDataset.dimensions), [currentDataset.dimensions]);
  const currentMeasureMap = useMemo(() => fieldMap(currentDataset.measures), [currentDataset.measures]);
  const selectedDimensions = (draft.dimensions?.filter((value) => currentDimensionMap.has(value)) ?? currentDataset.defaults.dimensions).slice(0, 4);
  const selectedMeasures = (draft.measures?.filter((value) => currentMeasureMap.has(value)) ?? currentDataset.defaults.measures).slice(0, 5);
  const sortOptions = [...selectedDimensions, ...selectedMeasures];
  const chart = result.chart;
  const chartColumn = chart ? result.columns.find((column) => column.key === chart.value_key) : undefined;
  const chartRows = chart
    ? result.rows.filter(
        (row) =>
          row[chart.label_key] !== null &&
          row[chart.label_key] !== undefined &&
          typeof row[chart.value_key] === "number",
      )
    : [];
  const chartValues = chartRows.map((row) => Number(row[chart!.value_key] ?? 0));
  const chartLabels = chartRows.map((row) => String(row[chart!.label_key] ?? ""));
  const hasLimitClip = result.row_count >= (draft.limit ?? currentDataset.defaults.limit);

  function setDataset(datasetKey: string) {
    const nextDataset = datasetMap.get(datasetKey);
    if (!nextDataset) {
      return;
    }
    setDraft(defaultsForDataset(nextDataset));
    setError(null);
  }

  function toggleDimension(key: string) {
    if (!currentDimensionMap.has(key)) {
      return;
    }
    setDraft((previous) => {
      const existing = previous.dimensions?.filter((value) => currentDimensionMap.has(value)) ?? [...currentDataset.defaults.dimensions];
      const hasKey = existing.includes(key);
      const next = hasKey ? existing.filter((value) => value !== key) : [...existing, key].slice(0, 4);
      const safeDimensions = next.length ? next : existing;
      const safeSort = previous.sort_field && [...safeDimensions, ...(previous.measures ?? [])].includes(previous.sort_field)
        ? previous.sort_field
        : (previous.measures?.[0] ?? safeDimensions[0]);
      return { ...previous, dimensions: safeDimensions, sort_field: safeSort };
    });
  }

  function toggleMeasure(key: string) {
    if (!currentMeasureMap.has(key)) {
      return;
    }
    setDraft((previous) => {
      const existing = previous.measures?.filter((value) => currentMeasureMap.has(value)) ?? [...currentDataset.defaults.measures];
      const hasKey = existing.includes(key);
      const next = hasKey ? existing.filter((value) => value !== key) : [...existing, key].slice(0, 5);
      const safeMeasures = next.length ? next : existing;
      const safeSort = previous.sort_field && [...(previous.dimensions ?? []), ...safeMeasures].includes(previous.sort_field)
        ? previous.sort_field
        : safeMeasures[0];
      return { ...previous, measures: safeMeasures, sort_field: safeSort };
    });
  }

  function applyPreset(request: WarehouseQueryRequest, label: string) {
    setDraft({
      dataset: request.dataset,
      dimensions: [...(request.dimensions ?? [])],
      measures: [...(request.measures ?? [])],
      date_from: request.date_from ?? null,
      date_to: request.date_to ?? null,
      sector: request.sector ?? null,
      exchange: request.exchange ?? null,
      symbol_search: request.symbol_search ?? null,
      min_signal: request.min_signal ?? null,
      sort_field: request.sort_field ?? null,
      sort_direction: request.sort_direction ?? "desc",
      limit: request.limit ?? 100,
    });
    setReportTitle(`${label} report`);
    setError(null);
  }

  function resetCurrentDataset() {
    setDraft(defaultsForDataset(currentDataset));
    setError(null);
  }

  function runQuery() {
    setError(null);
    const request: WarehouseQueryRequest = {
      dataset: currentDataset.key,
      dimensions: selectedDimensions,
      measures: selectedMeasures,
      date_from: currentDataset.supports.date ? draft.date_from ?? null : null,
      date_to: currentDataset.supports.date ? draft.date_to ?? null : null,
      sector: currentDataset.supports.sector ? draft.sector ?? null : null,
      exchange: currentDataset.supports.exchange ? draft.exchange ?? null : null,
      symbol_search: currentDataset.supports.symbol_search ? draft.symbol_search?.trim() || null : null,
      min_signal: currentDataset.supports.min_signal ? draft.min_signal ?? null : null,
      sort_field: sortOptions.includes(draft.sort_field ?? "") ? draft.sort_field : sortOptions[0],
      sort_direction: draft.sort_direction ?? "desc",
      limit: draft.limit ?? currentDataset.defaults.limit,
    };

    startTransition(async () => {
      const next = await fetchWarehouseQuery(request);
      if (!next) {
        setError("The warehouse query could not be completed. Try narrowing the filters or rerunning the request.");
        return;
      }
      setResult(next);
      setDraft(request);
      if (!reportTitle.trim()) {
        setReportTitle(`${next.dataset.label} report`);
      }
    });
  }

  function exportCsv() {
    if (typeof window === "undefined" || !result.rows.length) {
      return;
    }
    const header = result.columns.map((column) => csvEscape(column.label)).join(",");
    const lines = result.rows.map((row) =>
      result.columns.map((column) => csvEscape(formatCell(row[column.key], column.kind))).join(","),
    );
    const csv = [header, ...lines].join("\n");
    const blob = new Blob([csv], { type: "text/csv;charset=utf-8" });
    const url = URL.createObjectURL(blob);
    const anchor = document.createElement("a");
    anchor.href = url;
    anchor.download = `${result.dataset.key}-warehouse-report.csv`;
    anchor.click();
    URL.revokeObjectURL(url);
  }

  return (
    <div className="stackList">
      <section className="surface">
        <div className="panelHeader">
          <div>
            <p className="panelEyebrow">Analyst studio</p>
            <h3 className="panelTitle">Visual warehouse query builder</h3>
          </div>
          <span className="panelMeta">
            {metadata.date_window.first_calendar_date ? formatDate(metadata.date_window.first_calendar_date) : "N/A"} to{" "}
            {metadata.date_window.last_calendar_date ? formatDate(metadata.date_window.last_calendar_date) : "N/A"}
          </span>
        </div>
        <div className="statusNote">
          Query curated warehouse datasets, change dimensions and measures visually, and export the resulting report as CSV or printable PDF without writing SQL.
        </div>
        <div className="statsGrid compactStats">
          <StatCard
            label="Datasets"
            value={String(metadata.datasets.length)}
            info="Curated warehouse surfaces exposed to the analyst studio."
            hint="Facts and materialized views"
          />
          <StatCard
            label="Latest result"
            value={String(result.row_count)}
            info="Rows returned by the most recent warehouse query."
            hint={result.dataset.label}
            tone="accent"
          />
          <StatCard
            label="Query time"
            value={`${result.query_time_ms} ms`}
            info="Server-side execution time for the most recent warehouse query."
            hint={hasLimitClip ? "Result is clipped at the active row limit" : "Within active row limit"}
            tone="warning"
          />
          <StatCard
            label="Available rows"
            value={formatCompactIndian(result.dataset.available_rows, 2)}
            info="Physical rows currently available in the selected warehouse dataset."
            hint={result.dataset.grain}
            tone="critical"
          />
        </div>
      </section>

      <section className="surface">
        <div className="panelHeader">
          <div>
            <p className="panelEyebrow">Quick starts</p>
            <h3 className="panelTitle">Analyst presets</h3>
          </div>
          <span className="panelMeta">{metadata.presets.length} presets</span>
        </div>
        <div className="datasetGrid">
          {metadata.presets.map((preset) => (
            <button
              key={preset.id}
              type="button"
              className={`datasetCard ${draft.dataset === preset.request.dataset ? "active" : ""}`}
              onClick={() => applyPreset(preset.request, preset.label)}
              title={preset.description}
            >
              <span className="datasetCardKicker">{preset.request.dataset.replaceAll("_", " ")}</span>
              <strong>{preset.label}</strong>
              <span>{preset.description}</span>
            </button>
          ))}
        </div>
      </section>

      <section className="contentGrid twoUp">
        <article className="surface">
          <div className="panelHeader">
            <div>
              <p className="panelEyebrow">Step 1</p>
              <h3 className="panelTitle">Choose the warehouse surface</h3>
            </div>
            <span className="panelMeta">{currentDataset.row_count.toLocaleString("en-IN")} rows</span>
          </div>
          <div className="stackList">
            <select
              className="toolbarSelect"
              value={currentDataset.key}
              onChange={(event) => setDataset(event.target.value)}
              title="Choose which warehouse fact or materialized view you want to query."
            >
              {metadata.datasets.map((dataset) => (
                <option key={dataset.key} value={dataset.key}>
                  {dataset.label}
                </option>
              ))}
            </select>
            <div className="querySynopsis">{currentDataset.description}</div>
            <div className="resultMeta">
              <span>{currentDataset.grain}</span>
              <span>{formatCompactIndian(currentDataset.row_count, 2)} rows available</span>
              {currentDataset.defaults.suggested_window_days ? <span>{currentDataset.defaults.suggested_window_days}D recommended default window</span> : null}
            </div>
          </div>
        </article>

        <article className="surface">
          <div className="panelHeader">
            <div>
              <p className="panelEyebrow">Step 2</p>
              <h3 className="panelTitle">Pick dimensions and measures</h3>
            </div>
            <span className="panelMeta">{selectedDimensions.length} dimensions | {selectedMeasures.length} measures</span>
          </div>
          <div className="querySection">
            <div>
              <div className="querySectionTitle">Dimensions</div>
              <div className="fieldChipGrid">
                {currentDataset.dimensions.map((field) => {
                  const active = selectedDimensions.includes(field.key);
                  return (
                    <div key={field.key} className="fieldChipWrap">
                      <button
                        type="button"
                        className={`fieldChip ${active ? "active" : ""}`}
                        onClick={() => toggleDimension(field.key)}
                        title={field.description}
                      >
                        <span>{field.label}</span>
                      </button>
                      <InfoHint content={field.description} label={`${field.label} definition`} align="end" />
                    </div>
                  );
                })}
              </div>
            </div>
            <div>
              <div className="querySectionTitle">Measures</div>
              <div className="fieldChipGrid">
                {currentDataset.measures.map((field) => {
                  const active = selectedMeasures.includes(field.key);
                  return (
                    <div key={field.key} className="fieldChipWrap">
                      <button
                        type="button"
                        className={`fieldChip ${active ? "active" : ""}`}
                        onClick={() => toggleMeasure(field.key)}
                        title={field.description}
                      >
                        <span>{field.label}</span>
                      </button>
                      <InfoHint content={field.description} label={`${field.label} definition`} align="end" />
                    </div>
                  );
                })}
              </div>
            </div>
          </div>
        </article>
      </section>

      <section className="surface">
        <div className="panelHeader">
          <div>
            <p className="panelEyebrow">Step 3</p>
            <h3 className="panelTitle">Filter, sort, and run</h3>
          </div>
          <span className="panelMeta">BI-style control layer</span>
        </div>
        <div className="studioFiltersGrid">
          {currentDataset.supports.date ? (
            <>
              <label className="queryField">
                <span>Date from</span>
                <input
                  className="toolbarInput"
                  type="date"
                  value={draft.date_from ?? ""}
                  onChange={(event) => setDraft((previous) => ({ ...previous, date_from: event.target.value || null }))}
                />
              </label>
              <label className="queryField">
                <span>Date to</span>
                <input
                  className="toolbarInput"
                  type="date"
                  value={draft.date_to ?? ""}
                  onChange={(event) => setDraft((previous) => ({ ...previous, date_to: event.target.value || null }))}
                />
              </label>
            </>
          ) : null}
          {currentDataset.supports.sector ? (
            <label className="queryField">
              <span>Sector</span>
              <select
                className="toolbarSelect"
                value={draft.sector ?? ""}
                onChange={(event) => setDraft((previous) => ({ ...previous, sector: event.target.value || null }))}
              >
                <option value="">All sectors</option>
                {metadata.sectors.map((sector) => (
                  <option key={sector} value={sector}>
                    {sector}
                  </option>
                ))}
              </select>
            </label>
          ) : null}
          {currentDataset.supports.exchange ? (
            <label className="queryField">
              <span>Exchange</span>
              <select
                className="toolbarSelect"
                value={draft.exchange ?? ""}
                onChange={(event) => setDraft((previous) => ({ ...previous, exchange: event.target.value || null }))}
              >
                <option value="">All exchanges</option>
                {metadata.exchanges.map((exchange) => (
                  <option key={exchange} value={exchange}>
                    {exchange}
                  </option>
                ))}
              </select>
            </label>
          ) : null}
          {currentDataset.supports.symbol_search ? (
            <label className="queryField queryFieldWide">
              <span>Symbol or company</span>
              <input
                className="toolbarInput"
                value={draft.symbol_search ?? ""}
                onChange={(event) => setDraft((previous) => ({ ...previous, symbol_search: event.target.value }))}
                placeholder="Type a symbol or company fragment"
              />
            </label>
          ) : null}
          {currentDataset.supports.min_signal ? (
            <label className="queryField">
              <span>Min signal</span>
              <input
                className="toolbarInput"
                type="number"
                step="0.1"
                value={draft.min_signal ?? ""}
                onChange={(event) =>
                  setDraft((previous) => ({
                    ...previous,
                    min_signal: event.target.value ? Number(event.target.value) : null,
                  }))
                }
                placeholder="e.g. 2.2"
              />
            </label>
          ) : null}
          <label className="queryField">
            <span>Sort by</span>
            <select
              className="toolbarSelect"
              value={draft.sort_field ?? sortOptions[0]}
              onChange={(event) => setDraft((previous) => ({ ...previous, sort_field: event.target.value }))}
            >
              {sortOptions.map((option) => (
                <option key={option} value={option}>
                  {(currentDimensionMap.get(option) ?? currentMeasureMap.get(option))?.label ?? option}
                </option>
              ))}
            </select>
          </label>
          <label className="queryField">
            <span>Direction</span>
            <select
              className="toolbarSelect"
              value={draft.sort_direction ?? "desc"}
              onChange={(event) =>
                setDraft((previous) => ({
                  ...previous,
                  sort_direction: event.target.value === "asc" ? "asc" : "desc",
                }))
              }
            >
              <option value="desc">Descending</option>
              <option value="asc">Ascending</option>
            </select>
          </label>
          <label className="queryField">
            <span>Row limit</span>
            <select
              className="toolbarSelect"
              value={String(draft.limit ?? currentDataset.defaults.limit)}
              onChange={(event) =>
                setDraft((previous) => ({
                  ...previous,
                  limit: Number(event.target.value),
                }))
              }
            >
              {LIMIT_OPTIONS.map((value) => (
                <option key={value} value={String(value)}>
                  {value} rows
                </option>
              ))}
            </select>
          </label>
        </div>
        <div className="toolbarRow queryActionRow">
          <div className="querySynopsis">{result.query.preview}</div>
          <div className="toolbarGroup">
            <button type="button" className="actionButton" onClick={resetCurrentDataset} disabled={isPending}>
              Reset
            </button>
            <button type="button" className="actionButton primaryAction" onClick={runQuery} disabled={isPending}>
              {isPending ? "Running..." : "Run warehouse query"}
            </button>
          </div>
        </div>
        {error ? <div className="statusNote critical">{error}</div> : null}
      </section>

      <section className="contentGrid twoUp">
        <article className="surface">
          <div className="panelHeader">
            <div>
              <p className="panelEyebrow">Result visual</p>
              <h3 className="panelTitle">{chart?.title ?? "Result chart"}</h3>
            </div>
            <span className="panelMeta">{result.row_count} rows | {result.query_time_ms} ms</span>
          </div>
          {chart && chartRows.length ? (
            chart.kind === "line" ? (
              <LineChart
                values={chartValues}
                labels={chartLabels}
                color="var(--accent)"
                height={220}
                valueDigits={chartColumn?.kind === "integer" ? 0 : 3}
                seriesLabel={chart.title}
              />
            ) : (
              <IntensityBars
                items={chartRows.slice(0, 12).map((row) => ({
                  label: String(row[chart.label_key] ?? "N/A"),
                  value: Number(row[chart.value_key] ?? 0),
                  detail: chartColumn ? chartColumn.label : chart.value_key,
                  tone: "accent",
                }))}
                valueFormatter={(value) => (chartColumn?.kind === "integer" ? Math.round(value).toLocaleString("en-IN") : formatNumber(value, 3))}
              />
            )
          ) : (
            <div className="emptyState">Run a query with at least one dimension and one numeric measure to visualize the result.</div>
          )}
        </article>

        <article className="surface">
          <div className="panelHeader">
            <div>
              <p className="panelEyebrow">Generated report</p>
              <h3 className="panelTitle">Analyst narrative and export</h3>
            </div>
            <span className="panelMeta">CSV and PDF-ready</span>
          </div>
          <div className="stackList">
            <label className="queryField">
              <span>Report title</span>
              <input
                className="toolbarInput"
                value={reportTitle}
                onChange={(event) => setReportTitle(event.target.value)}
                placeholder="Report title"
              />
            </label>
            <div className="keyValueGrid">
              {result.report.highlights.map((highlight) => (
                <div key={highlight.label} className="keyValueCard">
                  <span>{highlight.label}</span>
                  <strong>{highlight.value}</strong>
                </div>
              ))}
            </div>
            <div className="reportFindings">
              {result.report.findings.map((finding) => (
                <p key={finding}>{finding}</p>
              ))}
            </div>
            <div className="toolbarGroup">
              <button type="button" className="actionButton" onClick={exportCsv} disabled={!result.rows.length}>
                Export CSV
              </button>
              <button
                type="button"
                className="actionButton primaryAction"
                onClick={() => printReportDocument(reportTitle || result.report.headline, result)}
                disabled={!result.rows.length}
              >
                Print / Save PDF
              </button>
            </div>
          </div>
        </article>
      </section>

      <section className="surface">
        <div className="panelHeader">
          <div>
            <p className="panelEyebrow">Query output</p>
            <h3 className="panelTitle">Warehouse result set</h3>
          </div>
          <span className="panelMeta">{result.row_count} rows returned</span>
        </div>
        {hasLimitClip ? (
          <div className="statusNote warning">The current result set has reached the active row limit. Increase the limit or narrow the filters if you need a broader scan.</div>
        ) : null}
        <div className="tableWrap tableWrapScrollY">
          <table className="dataTable stickyHeaderTable">
            <thead>
              <tr>
                {result.columns.map((column) => (
                  <th key={column.key} title={column.description}>
                    {column.label}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {result.rows.map((row, index) => (
                <tr key={`${index}-${String(row[result.columns[0]?.key ?? "row"] ?? index)}`}>
                  {result.columns.map((column) => (
                    <td key={`${index}-${column.key}`}>{formatCell(row[column.key], column.kind)}</td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>
    </div>
  );
}
