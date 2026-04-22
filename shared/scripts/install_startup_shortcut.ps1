param(
    [string]$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\\..')).Path,
    [switch]$Remove
)

$shortcutPath = Join-Path ([Environment]::GetFolderPath('Startup')) 'Market Surveillance Console.lnk'

if ($Remove) {
    Remove-Item -LiteralPath $shortcutPath -Force -ErrorAction SilentlyContinue
    Write-Host "Removed startup shortcut: $shortcutPath" -ForegroundColor Green
    return
}

$launcher = Join-Path $ProjectRoot 'Launch Market Surveillance.cmd'
if (-not (Test-Path -LiteralPath $launcher)) {
    throw "Launcher not found: $launcher"
}

$shell = New-Object -ComObject WScript.Shell
$shortcut = $shell.CreateShortcut($shortcutPath)
$shortcut.TargetPath = $launcher
$shortcut.WorkingDirectory = $ProjectRoot
$shortcut.IconLocation = "$env:SystemRoot\System32\SHELL32.dll,220"
$shortcut.Description = 'Launch the Market Surveillance stack and public tunnel on Windows sign-in.'
$shortcut.Save()

Write-Host "Startup shortcut created: $shortcutPath" -ForegroundColor Green
