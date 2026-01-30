param(
    [ValidateSet('start', 'stop', 'status', 'restart')]
    [string]$Action = 'start',
    [int]$ApiPort = 8000,
    [int]$DashPort = 8080
)

$root = Split-Path -Parent $PSScriptRoot
$logs = Join-Path $root 'logs'
$apiPidPath = Join-Path $logs 'api.pid'
$dashPidPath = Join-Path $logs 'dash.pid'
$apiOut = Join-Path $logs 'api-out.log'
$apiErr = Join-Path $logs 'api-err.log'
$dashOut = Join-Path $logs 'dash-out.log'
$dashErr = Join-Path $logs 'dash-err.log'

function Get-RunningProcess([string]$pidPath) {
    if (Test-Path $pidPath) {
        $pid = (Get-Content $pidPath -ErrorAction SilentlyContinue | Select-Object -First 1)
        if ($pid -match '^\d+$') {
            try { return Get-Process -Id $pid -ErrorAction Stop } catch { return $null }
        }
    }
    return $null
}

function Stop-ByPidFile([string]$pidPath, [string]$label) {
    $proc = Get-RunningProcess $pidPath
    if ($null -ne $proc) {
        Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue
        Remove-Item $pidPath -ErrorAction SilentlyContinue
        Write-Host "$label stopped (PID $($proc.Id))"
    } else {
        Write-Host "$label not running"
    }
}

function Start-Api() {
    if (Get-RunningProcess $apiPidPath) {
        Write-Host "API already running"
        return
    }

    New-Item -ItemType Directory -Force -Path $logs | Out-Null
    $python = Join-Path $root 'venv\\Scripts\\python.exe'
    if (!(Test-Path $python)) { $python = 'python' }
    $api = Start-Process -FilePath $python `
        -ArgumentList "-m uvicorn api.main:app --reload --port $ApiPort" `
        -WorkingDirectory $root `
        -RedirectStandardOutput $apiOut `
        -RedirectStandardError $apiErr `
        -PassThru
    $api.Id | Set-Content $apiPidPath
    Write-Host "API started (PID $($api.Id)) on port $ApiPort"
}

function Start-Dashboard() {
    if (Get-RunningProcess $dashPidPath) {
        Write-Host "Dashboard already running"
        return
    }

    New-Item -ItemType Directory -Force -Path $logs | Out-Null
    $dash = Start-Process -FilePath "cmd.exe" `
        -ArgumentList "/c npm run dev -- --port $DashPort" `
        -WorkingDirectory (Join-Path $root 'dashboard') `
        -RedirectStandardOutput $dashOut `
        -RedirectStandardError $dashErr `
        -PassThru
    $dash.Id | Set-Content $dashPidPath
    Write-Host "Dashboard started (PID $($dash.Id)) on port $DashPort"
}

switch ($Action) {
    'start' {
        Start-Api
        Start-Dashboard
    }
    'stop' {
        Stop-ByPidFile $dashPidPath "Dashboard"
        Stop-ByPidFile $apiPidPath "API"
    }
    'status' {
        $api = Get-RunningProcess $apiPidPath
        $dash = Get-RunningProcess $dashPidPath
        if ($api) { Write-Host "API running (PID $($api.Id))" } else { Write-Host "API not running" }
        if ($dash) { Write-Host "Dashboard running (PID $($dash.Id))" } else { Write-Host "Dashboard not running" }
    }
    'restart' {
        Stop-ByPidFile $dashPidPath "Dashboard"
        Stop-ByPidFile $apiPidPath "API"
        Start-Api
        Start-Dashboard
    }
}
