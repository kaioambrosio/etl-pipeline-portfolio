param(
    [Parameter(Mandatory = $true)]
    [int]$GeneratorPid
)

$ErrorActionPreference = "SilentlyContinue"

while (Get-Process -Id $GeneratorPid -ErrorAction SilentlyContinue) {
    Start-Sleep -Seconds 30
}

$root = Split-Path -Parent $PSScriptRoot
$python = Join-Path $root "venv\\Scripts\\python.exe"
$etlScript = Join-Path $root "scripts\\main.py"
$logsDir = Join-Path $root "logs"
$stdoutLog = Join-Path $logsDir "etl-run.log"
$stderrLog = Join-Path $logsDir "etl-run.err.log"

if (-not (Test-Path $logsDir)) {
    New-Item -ItemType Directory -Path $logsDir | Out-Null
}

Push-Location $root
& $python $etlScript 1> $stdoutLog 2> $stderrLog
Pop-Location
