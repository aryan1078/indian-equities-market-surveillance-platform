param(
    [string]$UiBaseUrl = 'http://localhost:3000',
    [string]$ApiBaseUrl = 'http://localhost:8000'
)

$ErrorActionPreference = 'Stop'

function Invoke-SmokeRequest {
    param(
        [string]$Name,
        [string]$Url,
        [string]$Method = 'GET',
        [object]$Body = $null,
        [string]$Contains = $null
    )

    $headers = @{}
    $params = @{
        Uri = $Url
        Method = $Method
        TimeoutSec = 60
        UseBasicParsing = $true
        Headers = $headers
    }

    if ($Body -ne $null) {
        $params.Body = ($Body | ConvertTo-Json -Depth 10)
        $params.ContentType = 'application/json'
    }

    $attempts = 0
    $response = $null
    $durationMs = $null
    do {
        $attempts += 1
        $started = Get-Date
        try {
            $response = Invoke-WebRequest @params
            $durationMs = [math]::Round(((Get-Date) - $started).TotalMilliseconds, 0)
        } catch {
            if ($attempts -ge 3) {
                throw
            }
            Start-Sleep -Seconds 2
        }
    } while ($null -eq $response -and $attempts -lt 3)

    if ($Contains -and -not ($response.Content -like "*$Contains*")) {
        throw "$Name missing expected marker: $Contains"
    }

    return [pscustomobject]@{
        name = $Name
        status = $response.StatusCode
        duration_ms = $durationMs
        bytes = if ($null -ne $response.RawContentLength) { $response.RawContentLength } else { 0 }
        url = $Url
    }
}

$checks = @(
    @{ Name = 'UI Overview'; Url = "$UiBaseUrl/"; Contains = 'Market overview' }
    @{ Name = 'UI Stocks'; Url = "$UiBaseUrl/stocks"; Contains = 'Listed symbol directory' }
    @{ Name = 'UI Contagion'; Url = "$UiBaseUrl/contagion"; Contains = 'Contagion' }
    @{ Name = 'UI Warehouse'; Url = "$UiBaseUrl/warehouse"; Contains = 'Warehouse' }
    @{ Name = 'UI Analyst'; Url = "$UiBaseUrl/warehouse/analyst"; Contains = 'Analyst' }
    @{ Name = 'UI Methodology'; Url = "$UiBaseUrl/methodology"; Contains = 'Methodology' }
    @{ Name = 'UI Process'; Url = "$UiBaseUrl/process"; Contains = 'Process' }
    @{ Name = 'UI Replay'; Url = "$UiBaseUrl/replay"; Contains = 'Replay' }
    @{ Name = 'UI System'; Url = "$UiBaseUrl/system"; Contains = 'System' }
    @{ Name = 'API Health'; Url = "$ApiBaseUrl/api/system/health"; Contains = 'ok' }
    @{ Name = 'API Overview'; Url = "$ApiBaseUrl/api/overview"; Contains = 'live_market' }
    @{ Name = 'API Stocks Directory'; Url = "$ApiBaseUrl/api/reference/stocks?limit=5"; Contains = 'stocks' }
    @{ Name = 'API Screener'; Url = "$ApiBaseUrl/api/stocks/screener?days=45&limit=20"; Contains = 'items' }
    @{ Name = 'API Workspace'; Url = "$ApiBaseUrl/api/stocks/TCS.NS/workspace?days=45"; Contains = 'resolved_symbol' }
    @{ Name = 'API Contagion'; Url = "$ApiBaseUrl/api/contagion"; Contains = 'event_id' }
    @{ Name = 'API Warehouse Summary'; Url = "$ApiBaseUrl/api/warehouse/summary"; Contains = 'market_day_rows' }
    @{ Name = 'API Warehouse Regimes'; Url = "$ApiBaseUrl/api/warehouse/sector-regimes?limit=5"; Contains = 'sector_name' }
    @{ Name = 'API Warehouse Leaders'; Url = "$ApiBaseUrl/api/warehouse/stock-leaders?limit=5"; Contains = 'symbol' }
    @{ Name = 'API Warehouse Metadata'; Url = "$ApiBaseUrl/api/warehouse/query-metadata"; Contains = 'datasets' }
    @{ Name = 'API System Scale'; Url = "$ApiBaseUrl/api/system/scale"; Contains = 'projection' }
    @{ Name = 'API System Runs'; Url = "$ApiBaseUrl/api/system/runs"; Contains = 'ingestion_runs' }
    @{ Name = 'API Replay Status'; Url = "$ApiBaseUrl/api/replay/status"; Contains = 'status' }
)

$results = foreach ($check in $checks) {
    Invoke-SmokeRequest @check
}

$queryResult = Invoke-SmokeRequest `
    -Name 'API Warehouse Query' `
    -Url "$ApiBaseUrl/api/warehouse/query" `
    -Method 'POST' `
    -Body @{
        dataset = 'sector_day'
        dimensions = @('calendar_date', 'sector_name')
        measures = @('active_minutes', 'max_composite_score')
        limit = 5
    } `
    -Contains 'rows'

$results += $queryResult

$results | Format-Table -AutoSize

$summary = [pscustomobject]@{
    checked_at = (Get-Date).ToString('s')
    total_checks = $results.Count
    max_duration_ms = ($results | Measure-Object -Property duration_ms -Maximum).Maximum
    average_duration_ms = [math]::Round(($results | Measure-Object -Property duration_ms -Average).Average, 0)
    slowest_check = ($results | Sort-Object duration_ms -Descending | Select-Object -First 1).name
}

Write-Host ''
Write-Host 'Smoke summary' -ForegroundColor Cyan
$summary | Format-List
