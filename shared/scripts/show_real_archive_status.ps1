$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$statePath = Join-Path $repoRoot "data\\archive-backfill-state.json"

if (Test-Path $statePath) {
  Write-Host "Archive backfill state:"
  Get-Content -Path $statePath
  Write-Host ""
} else {
  Write-Host "Archive backfill state: none recorded."
  Write-Host ""
}

$python = @'
import sys
sys.path.insert(0, '/app/shared/contracts/src')
from market_surveillance.db import pg_connection
with pg_connection() as conn:
    with conn.cursor() as cur:
        cur.execute("select count(*) as bars, count(distinct symbol) as symbols, min(trading_date), max(trading_date) from operational.stock_daily_bars")
        print('daily_bars', cur.fetchone())
        cur.execute("select run_id, mode, status, symbol_count, started_at, finished_at, notes from operational.ingestion_runs order by started_at desc limit 5")
        for row in cur.fetchall():
            print('ingestion_run', row)
        cur.execute("select run_id, trading_date, status, inserted_rows, aggregate_rows, finished_at from operational.etl_runs order by started_at desc limit 5")
        for row in cur.fetchall():
            print('etl_run', row)
'@

$python | docker compose exec -T api python -
