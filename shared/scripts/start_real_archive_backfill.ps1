param(
  [int]$ChunkSize = 20,
  [int]$MaxChunks = 0,
  [string]$Period = "max",
  [string]$StateFile = "data/archive-backfill-state.json",
  [switch]$Restart
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$statePath = Join-Path $repoRoot $StateFile
$metadataPath = Join-Path $repoRoot "shared\\metadata\\stocks.json"

function Get-ActiveSymbols {
  $rows = Get-Content -Path $metadataPath -Raw | ConvertFrom-Json
  return @($rows | Where-Object { $_.is_active } | Select-Object -ExpandProperty symbol)
}

function Invoke-InContainerPython([string]$python) {
  $python | docker compose exec -T api python -
}

function Get-ArchiveStats {
  $python = @'
import json
import sys
sys.path.insert(0, '/app/shared/contracts/src')
from market_surveillance.db import pg_connection
with pg_connection() as conn:
    row = conn.execute(
        """
        select count(*) as bars,
               count(distinct symbol) as symbols,
               min(trading_date) as first_trading_date,
               max(trading_date) as last_trading_date
        from operational.stock_daily_bars
        """
    ).fetchone()
    print(json.dumps({
        "bars": int(row["bars"] or 0),
        "symbols": int(row["symbols"] or 0),
        "first_trading_date": row["first_trading_date"].isoformat() if row["first_trading_date"] else None,
        "last_trading_date": row["last_trading_date"].isoformat() if row["last_trading_date"] else None,
    }))
'@
  Invoke-InContainerPython $python | ConvertFrom-Json
}

function Get-LatestHydrateRun {
  $python = @'
import json
import sys
sys.path.insert(0, '/app/shared/contracts/src')
from market_surveillance.db import pg_connection
with pg_connection() as conn:
    row = conn.execute(
        """
        select run_id, mode, status, symbol_count, records_seen, records_published, started_at, finished_at, notes
        from operational.ingestion_runs
        where mode = 'hydrate_daily'
        order by started_at desc
        limit 1
        """
    ).fetchone()
    if row is None:
        print("null")
    else:
        print(json.dumps({
            "run_id": row["run_id"],
            "mode": row["mode"],
            "status": row["status"],
            "symbol_count": int(row["symbol_count"] or 0),
            "records_seen": int(row["records_seen"] or 0),
            "records_published": int(row["records_published"] or 0),
            "started_at": row["started_at"].isoformat() if row["started_at"] else None,
            "finished_at": row["finished_at"].isoformat() if row["finished_at"] else None,
            "notes": row["notes"] if isinstance(row["notes"], dict) else {},
        }))
'@
  $raw = Invoke-InContainerPython $python
  if ($raw -eq "null") {
    return $null
  }
  return $raw | ConvertFrom-Json
}

function Save-State([hashtable]$state) {
  $stateDir = Split-Path -Parent $statePath
  if (-not (Test-Path $stateDir)) {
    New-Item -ItemType Directory -Path $stateDir -Force | Out-Null
  }
  $state | ConvertTo-Json -Depth 8 | Set-Content -Path $statePath -Encoding utf8
}

$symbols = Get-ActiveSymbols
$totalSymbols = $symbols.Count
$offset = 0

if (-not $Restart -and (Test-Path $statePath)) {
  try {
    $existingState = Get-Content -Path $statePath -Raw | ConvertFrom-Json
    $offset = [int]($existingState.next_offset)
  } catch {
    $offset = 0
  }
}

if ($offset -ge $totalSymbols) {
  Write-Host "Archive backfill already completed for all active symbols."
  Get-ArchiveStats
  exit 0
}

$chunkCounter = 0
for ($index = $offset; $index -lt $totalSymbols; $index += $ChunkSize) {
  if ($MaxChunks -gt 0 -and $chunkCounter -ge $MaxChunks) {
    break
  }

  $lastIndex = [Math]::Min($index + $ChunkSize - 1, $totalSymbols - 1)
  $chunk = @($symbols[$index..$lastIndex])
  $chunkCounter += 1

  Write-Host ("[{0}] Backfilling daily archive for symbols {1}-{2} of {3}" -f (Get-Date).ToString("s"), ($index + 1), ($lastIndex + 1), $totalSymbols)

  $beforeStats = Get-ArchiveStats
  $args = @("compose", "exec", "-T", "api", "python", "-m", "collector.main", "hydrate-daily", "--period", $Period, "--symbols") + $chunk
  & docker @args
  if ($LASTEXITCODE -ne 0) {
    throw "Archive chunk failed at offset $index."
  }

  $afterStats = Get-ArchiveStats
  $latestHydrateRun = Get-LatestHydrateRun
  $notes = if ($latestHydrateRun -and $latestHydrateRun.notes) { $latestHydrateRun.notes } else { $null }
  $hydratedSymbols = @()
  if ($notes -and $notes.PSObject.Properties.Name -contains "hydrated_symbols" -and $notes.hydrated_symbols) {
    $hydratedSymbols = @($notes.hydrated_symbols)
  }
  $dailyBarDelta = [int]$afterStats.bars - [int]$beforeStats.bars
  $symbolDelta = [int]$afterStats.symbols - [int]$beforeStats.symbols

  $advanceCheckpoint = $true
  $chunkStatus = "advanced"
  if ($dailyBarDelta -le 0 -and $hydratedSymbols.Count -eq 0) {
    $advanceCheckpoint = $false
    $chunkStatus = "no_progress_paused"
  } elseif ($dailyBarDelta -le 0) {
    $chunkStatus = "refreshed_without_growth"
  } elseif ($symbolDelta -gt 0) {
    $chunkStatus = "advanced_with_new_symbols"
  }

  $completedSymbols = if ($advanceCheckpoint) { $lastIndex + 1 } else { $index }
  $nextOffset = if ($advanceCheckpoint) { $lastIndex + 1 } else { $index }

  $state = @{
    period = $Period
    chunk_size = $ChunkSize
    total_symbols = $totalSymbols
    completed_symbols = $completedSymbols
    next_offset = $nextOffset
    updated_at = (Get-Date).ToString("o")
    last_chunk_first_symbol = $chunk[0]
    last_chunk_last_symbol = $chunk[-1]
    max_chunks_requested = $MaxChunks
    last_chunk_status = $chunkStatus
    last_chunk_daily_bars_before = [int]$beforeStats.bars
    last_chunk_daily_bars_after = [int]$afterStats.bars
    last_chunk_daily_bar_delta = $dailyBarDelta
    last_chunk_symbol_count_before = [int]$beforeStats.symbols
    last_chunk_symbol_count_after = [int]$afterStats.symbols
    last_chunk_symbol_delta = $symbolDelta
    last_chunk_hydrated_symbol_count = $hydratedSymbols.Count
    last_chunk_hydrated_symbols = $hydratedSymbols
    last_chunk_run_id = if ($latestHydrateRun) { $latestHydrateRun.run_id } else { $null }
    last_chunk_records_seen = if ($latestHydrateRun) { [int]$latestHydrateRun.records_seen } else { 0 }
    last_chunk_records_published = if ($latestHydrateRun) { [int]$latestHydrateRun.records_published } else { 0 }
  }
  Save-State $state

  if (-not $advanceCheckpoint) {
    Write-Host "Archive backfill paused because the chunk produced no new real daily rows and no hydrated symbols."
    Get-ArchiveStats
    break
  }

  Get-ArchiveStats
  Start-Sleep -Seconds 2
}

Write-Host "Archive backfill chunk run finished."
