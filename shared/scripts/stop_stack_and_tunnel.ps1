param(
    [string]$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\\..')).Path,
    [string]$TunnelContainerName = 'market-public-tunnel'
)

$ErrorActionPreference = 'Stop'

Push-Location $ProjectRoot
try {
    & docker rm -f $TunnelContainerName *> $null
    $pidPath = Join-Path $ProjectRoot 'tmp\localhostrun.pid'
    if (Test-Path -LiteralPath $pidPath) {
        $pidText = (Get-Content -LiteralPath $pidPath -ErrorAction SilentlyContinue | Select-Object -First 1)
        if ($pidText -and $pidText.ToString() -match '^\d+$') {
            Stop-Process -Id ([int]$pidText) -Force -ErrorAction SilentlyContinue
        }
        Remove-Item -LiteralPath $pidPath -Force -ErrorAction SilentlyContinue
    }
    & docker compose down --remove-orphans
    if ($LASTEXITCODE -ne 0) {
        throw 'Failed to stop the Docker stack.'
    }
    Write-Host 'Market surveillance stack and public tunnel stopped.' -ForegroundColor Green
} finally {
    Pop-Location
}
