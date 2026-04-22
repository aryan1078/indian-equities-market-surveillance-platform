param(
  [string]$StartDate = "",
  [string]$EndDate = ""
)

$ErrorActionPreference = "Stop"

if (-not $StartDate -or -not $EndDate) {
  $python = @'
import sys
sys.path.insert(0, '/app/shared/contracts/src')
from market_surveillance.db import pg_connection
with pg_connection() as conn:
    with conn.cursor() as cur:
        cur.execute("select min(trading_date), max(trading_date) from operational.stock_daily_bars")
        row = cur.fetchone()
        print(f'{row[0]}|{row[1]}')
'@
  $range = $python | docker compose exec -T api python -
  $parts = $range.Trim().Split("|")
  if (-not $StartDate) {
    $StartDate = $parts[0]
  }
  if (-not $EndDate) {
    $EndDate = $parts[1]
  }
}

if (-not $StartDate -or -not $EndDate) {
  throw "Could not determine archive trading-date range."
}

Write-Host "Loading warehouse facts from $StartDate to $EndDate"
& docker compose exec -T api python -m etl_service.main run-window --start-date $StartDate --end-date $EndDate
if ($LASTEXITCODE -ne 0) {
  throw "Warehouse archive load failed."
}
