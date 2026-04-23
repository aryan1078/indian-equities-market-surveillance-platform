param(
    [string]$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\\..')).Path,
    [switch]$Rebuild,
    [switch]$NoTunnel,
    [switch]$NoBrowser,
    [switch]$LiveCollector,
    [ValidateSet('auto', 'localhostrun', 'cloudflare')]
    [string]$TunnelProvider = 'auto',
    [int]$UiPort = 3000,
    [int]$ApiPort = 8000,
    [string]$TunnelContainerName = 'market-public-tunnel',
    [string]$TunnelImage = 'cloudflare/cloudflared:latest'
)

$ErrorActionPreference = 'Stop'

function Write-Step {
    param([string]$Message)
    Write-Host "==> $Message" -ForegroundColor Cyan
}

function Invoke-Docker {
    param([string[]]$Arguments)
    & docker @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "Docker command failed: docker $($Arguments -join ' ')"
    }
}

function Wait-HttpOk {
    param(
        [string]$Url,
        [int]$TimeoutSeconds = 600
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        try {
            $response = Invoke-WebRequest -Uri $Url -TimeoutSec 15 -UseBasicParsing
            if ($response.StatusCode -ge 200 -and $response.StatusCode -lt 300) {
                return $response
            }
        } catch {
        }
        Start-Sleep -Seconds 3
    }

    throw "Timed out waiting for $Url"
}

function Invoke-WarmRequest {
    param(
        [string]$Url
    )

    try {
        Invoke-WebRequest -Uri $Url -TimeoutSec 120 -UseBasicParsing | Out-Null
    } catch {
        Write-Host "Warm-up skipped for $Url" -ForegroundColor Yellow
    }
}

function Stop-CloudflareQuickTunnel {
    param(
        [string]$ContainerName
    )

    $existingContainerOutput = & docker ps -aq -f "name=^${ContainerName}$" 2>$null
    $existingContainer = if ($existingContainerOutput) { ($existingContainerOutput | Select-Object -First 1).ToString().Trim() } else { "" }
    if ($existingContainer) {
        $nativeCommandPreference = $PSNativeCommandUseErrorActionPreference
        $PSNativeCommandUseErrorActionPreference = $false
        try {
            & docker rm -f $ContainerName 2>$null | Out-Null
        } finally {
            $PSNativeCommandUseErrorActionPreference = $nativeCommandPreference
        }
    }
}

function Stop-LocalhostRunTunnel {
    param(
        [string]$Root
    )

    $pidPath = Join-Path $Root 'tmp\localhostrun.pid'
    if (-not (Test-Path -LiteralPath $pidPath)) {
        return
    }

    $pidText = (Get-Content -LiteralPath $pidPath -ErrorAction SilentlyContinue | Select-Object -First 1)
    if ($pidText -and $pidText.ToString() -match '^\d+$') {
        Stop-Process -Id ([int]$pidText) -Force -ErrorAction SilentlyContinue
    }
    Remove-Item -LiteralPath $pidPath -Force -ErrorAction SilentlyContinue
}

function Start-CloudflareQuickTunnel {
    param(
        [string]$Root,
        [string]$ContainerName,
        [string]$Image,
        [int]$Port
    )

    $stdoutPath = Join-Path $Root 'tmp\cloudflared.stdout.log'
    $stderrPath = Join-Path $Root 'tmp\cloudflared.stderr.log'
    Remove-Item -LiteralPath $stdoutPath, $stderrPath -ErrorAction SilentlyContinue
    Stop-CloudflareQuickTunnel -ContainerName $ContainerName

    $process = Start-Process -FilePath 'docker' `
        -ArgumentList @('run', '--rm', '--name', $ContainerName, $Image, 'tunnel', '--no-autoupdate', '--protocol', 'http2', '--url', "http://host.docker.internal:$Port") `
        -RedirectStandardOutput $stdoutPath `
        -RedirectStandardError $stderrPath `
        -PassThru `
        -WindowStyle Hidden

    $deadline = (Get-Date).AddMinutes(3)
    while ((Get-Date) -lt $deadline) {
        foreach ($path in @($stdoutPath, $stderrPath)) {
            if (-not (Test-Path $path)) {
                continue
            }
            $content = Get-Content -LiteralPath $path -Raw -ErrorAction SilentlyContinue
            if ($content -match 'https://[-a-z0-9]+\.trycloudflare\.com') {
                return @{
                    Provider = 'cloudflare'
                    Url = $Matches[0]
                    ProcessId = $process.Id
                    Stdout = $stdoutPath
                    Stderr = $stderrPath
                }
            }
        }

        if ($process.HasExited) {
            break
        }

        Start-Sleep -Seconds 2
    }

    return @{
        Provider = 'cloudflare'
        Url = $null
        ProcessId = $process.Id
        Stdout = $stdoutPath
        Stderr = $stderrPath
    }
}

function Start-LocalhostRunTunnel {
    param(
        [string]$Root,
        [int]$Port
    )

    $stdoutPath = Join-Path $Root 'tmp\localhostrun-json.out'
    $stderrPath = Join-Path $Root 'tmp\localhostrun-json.err'
    $pidPath = Join-Path $Root 'tmp\localhostrun.pid'
    Remove-Item -LiteralPath $stdoutPath, $stderrPath -ErrorAction SilentlyContinue
    Stop-LocalhostRunTunnel -Root $Root

    $sshCommand = (Get-Command ssh -ErrorAction Stop).Source
    $process = Start-Process -FilePath $sshCommand `
        -ArgumentList @('-T', '-o', 'StrictHostKeyChecking=no', '-o', 'ServerAliveInterval=30', '-o', 'ExitOnForwardFailure=yes', '-R', "80:localhost:$Port", 'nokey@localhost.run', '--', '--output', 'json') `
        -RedirectStandardOutput $stdoutPath `
        -RedirectStandardError $stderrPath `
        -PassThru `
        -WindowStyle Hidden

    Set-Content -LiteralPath $pidPath -Value $process.Id -Encoding UTF8

    $deadline = (Get-Date).AddMinutes(2)
    while ((Get-Date) -lt $deadline) {
        foreach ($path in @($stdoutPath, $stderrPath)) {
            if (-not (Test-Path $path)) {
                continue
            }
            $content = Get-Content -LiteralPath $path -Raw -ErrorAction SilentlyContinue
            if ($content -match 'https://[a-z0-9.-]+') {
                return @{
                    Provider = 'localhostrun'
                    Url = $Matches[0]
                    ProcessId = $process.Id
                    Stdout = $stdoutPath
                    Stderr = $stderrPath
                }
            }
        }

        if ($process.HasExited) {
            break
        }

        Start-Sleep -Seconds 2
    }

    return @{
        Provider = 'localhostrun'
        Url = $null
        ProcessId = $process.Id
        Stdout = $stdoutPath
        Stderr = $stderrPath
    }
}

function Start-PublicTunnel {
    param(
        [string]$Root,
        [string]$Provider,
        [int]$Port,
        [string]$ContainerName,
        [string]$Image
    )

    Stop-CloudflareQuickTunnel -ContainerName $ContainerName
    Stop-LocalhostRunTunnel -Root $Root

    $providers = switch ($Provider) {
        'localhostrun' { @('localhostrun') }
        'cloudflare' { @('cloudflare') }
        default { @('localhostrun', 'cloudflare') }
    }

    foreach ($candidate in $providers) {
        if ($candidate -eq 'localhostrun') {
            $result = Start-LocalhostRunTunnel -Root $Root -Port $Port
        } else {
            $result = Start-CloudflareQuickTunnel -Root $Root -ContainerName $ContainerName -Image $Image -Port $Port
        }
        if ($result.Url) {
            return $result
        }
    }

    return $result
}

Write-Step 'Ensuring runtime folders exist'
& (Join-Path $ProjectRoot 'shared\scripts\bootstrap.ps1') -ProjectRoot $ProjectRoot

Write-Step 'Checking Docker availability'
& docker info *> $null
if ($LASTEXITCODE -ne 0) {
    throw 'Docker is not available. Start Docker Desktop and try again.'
}

$composeServices = @(
    'kafka',
    'cassandra',
    'cassandra-init',
    'redis',
    'postgres',
    'postgres-migrate',
    'api',
    'storage-consumer',
    'anomaly-engine',
    'contagion-engine',
    'frontend'
)

Push-Location $ProjectRoot
try {
    Write-Step 'Starting the local stack'
    $composeArgs = @('compose', 'up', '-d')
    if ($Rebuild) {
        $composeArgs += '--build'
    }
    $composeArgs += $composeServices
    Invoke-Docker -Arguments $composeArgs

    if ($LiveCollector) {
        Write-Step 'Starting the live collector profile'
        Invoke-Docker -Arguments @('compose', '--profile', 'live', 'up', '-d', 'collector-live')
    }

    Write-Step 'Waiting for the API to become healthy'
    $healthResponse = Wait-HttpOk -Url "http://localhost:$ApiPort/api/system/health"

    Write-Step 'Waiting for the frontend to become available'
    $null = Wait-HttpOk -Url "http://localhost:$UiPort"

    Write-Step 'Prewarming critical API caches'
    foreach ($warmUrl in @(
        "http://localhost:$ApiPort/api/overview",
        "http://localhost:$ApiPort/api/stocks/screener?days=45&limit=80",
        "http://localhost:$ApiPort/api/system/scale",
        "http://localhost:$ApiPort/api/warehouse/summary",
        "http://localhost:$ApiPort/api/warehouse/query-metadata",
        "http://localhost:$ApiPort/api/replay/status"
    )) {
        Invoke-WarmRequest -Url $warmUrl
    }

    $publicUrl = $null
    $tunnelInfo = $null
    if (-not $NoTunnel) {
        Write-Step "Starting a public tunnel ($TunnelProvider)"
        $tunnelInfo = Start-PublicTunnel -Root $ProjectRoot -Provider $TunnelProvider -ContainerName $TunnelContainerName -Image $TunnelImage -Port $UiPort
        $publicUrl = $tunnelInfo.Url
        if ($publicUrl) {
            Set-Content -LiteralPath (Join-Path $ProjectRoot 'tmp\public-url.txt') -Value $publicUrl -Encoding UTF8
        }
    }

    $health = $healthResponse.Content | ConvertFrom-Json
    $summary = [ordered]@{
        launched_at = (Get-Date).ToString('s')
        local_ui = "http://localhost:$UiPort"
        local_api = "http://localhost:$ApiPort/api/system/health"
        public_url = $publicUrl
        tunnel_provider = if ($tunnelInfo) { $tunnelInfo.Provider } else { $null }
        live_collector_enabled = [bool]$LiveCollector
        last_tick = $health.last_tick
        latest_ingestion_mode = if ($health.latest_ingestion_run) { $health.latest_ingestion_run.mode } else { $null }
        latest_etl_status = if ($health.latest_etl_run) { $health.latest_etl_run.status } else { $null }
        tunnel_stdout_log = if ($tunnelInfo) { $tunnelInfo.Stdout } else { $null }
        tunnel_stderr_log = if ($tunnelInfo) { $tunnelInfo.Stderr } else { $null }
    }
    $summaryPath = Join-Path $ProjectRoot 'tmp\launch-summary.json'
    $summary | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath $summaryPath -Encoding UTF8

    Write-Host ''
    Write-Host "Local UI:  http://localhost:$UiPort" -ForegroundColor Green
    Write-Host "Local API: http://localhost:$ApiPort/api/system/health" -ForegroundColor Green
    if ($publicUrl) {
        Write-Host "Public URL: $publicUrl" -ForegroundColor Green
    } elseif (-not $NoTunnel) {
        Write-Host "Public URL not detected yet. Check $($tunnelInfo.Stdout) and $($tunnelInfo.Stderr)." -ForegroundColor Yellow
    }
    Write-Host "Launch summary: $summaryPath" -ForegroundColor DarkGray

    if (-not $NoBrowser) {
        $launchUrl = if ($publicUrl) { $publicUrl } else { "http://localhost:$UiPort" }
        Start-Process $launchUrl
    }
} finally {
    Pop-Location
}
