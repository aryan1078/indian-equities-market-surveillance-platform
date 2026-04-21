param(
    [string]$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\\..')).Path
)

$paths = @(
    'data',
    'data\\raw',
    'data\\replay',
    'data\\logs',
    'data\\exports',
    'datasets',
    'tmp'
)

foreach ($relative in $paths) {
    $full = Join-Path $ProjectRoot $relative
    New-Item -ItemType Directory -Force -Path $full | Out-Null
}

Write-Output "Bootstrap complete. Runtime directories ensured under $ProjectRoot."

