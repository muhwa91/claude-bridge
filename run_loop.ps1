# claude-bridge supervisor (local / laptop -- PowerShell)
# ASCII-only on purpose: Windows PowerShell 5.1 misreads UTF-8-no-BOM Korean as cp949
# and breaks brace matching (parse error). Keep this file ASCII.
#
# Runs the bot as a synchronous child and starts it again whenever it ends. Normally launched
# hidden + detached by start.ps1 (never by hand in a window: closing the window would kill it).
#   exit 75 (bridge saw its own code change, idle)  -> relaunch now (quiet; bridge skips the On notice)
#   exit 0  ('restart' command / normal)            -> relaunch now
#   other   (crash)                                 -> relaunch after 3s; 5 deaths in a row within
#                                                      30s each -> wait 60s (then 120s, ... max 300s)
#   logs\stop_requested exists                      -> do NOT relaunch; delete the file and exit
#                                                      (stop.ps1 creates it, then kills the bot tree)
# Single instance: logs\run_loop.pid (verified against the live process command line).
#
# Test hooks (env, for verifying the loop without the real bot -- never set in normal use):
#   RUN_LOOP_TARGET     script run with PATH's python instead of bridge.py (skips the discord check)
#   RUN_LOOP_STATE_DIR  directory used for run_loop.pid / run_loop.log / stop_requested
$ErrorActionPreference = 'Continue'
Set-Location -Path $PSScriptRoot

# Host guard (same as start.ps1): only the PC with the git-ignored marker may run the bridge.
if (-not (Test-Path -LiteralPath (Join-Path $PSScriptRoot 'bridge_host.local'))) {
    Write-Output 'NOT_HOST (no bridge_host.local marker in the project root - this PC is not the bridge host; create the file to make it one)'
    exit 0
}

$stateDir = Join-Path $PSScriptRoot 'logs'
if ($env:RUN_LOOP_STATE_DIR) { $stateDir = $env:RUN_LOOP_STATE_DIR }
New-Item -ItemType Directory -Force -Path $stateDir -ErrorAction SilentlyContinue | Out-Null
$pidFile  = Join-Path $stateDir 'run_loop.pid'
$logFile  = Join-Path $stateDir 'run_loop.log'
$stopFile = Join-Path $stateDir 'stop_requested'

function Write-Log([string]$msg) {
    Add-Content -Path $logFile -Value ("[{0}] {1}" -f (Get-Date -Format 'yyyy-MM-dd HH:mm:ss'), $msg) `
        -Encoding ascii -ErrorAction SilentlyContinue
}

# Another supervisor alive? The pid file alone is not enough (a stale pid can be reused), so the
# process command line must also mention run_loop.ps1. Independent of bridge.py's own pid lock
# (logs\bridge.pid) -- that one guards the bot, this one guards the supervisor.
if (Test-Path -LiteralPath $pidFile) {
    $old = 0
    [void][int]::TryParse(((Get-Content -LiteralPath $pidFile -ErrorAction SilentlyContinue) -join '').Trim(), [ref]$old)
    if ($old -gt 0 -and $old -ne $PID) {
        $proc = Get-CimInstance Win32_Process -Filter ("ProcessId={0}" -f $old) -ErrorAction SilentlyContinue
        if ($proc -and $proc.CommandLine -match 'run_loop\.ps1') {
            Write-Output ("SUPERVISOR_ALREADY_RUNNING pid={0}" -f $old)
            exit 0
        }
    }
}
Set-Content -LiteralPath $pidFile -Value $PID -Encoding ascii

# trading-info backend needs PHP 8.4.1+ (Laravel 13 / composer.lock symfony 8.1); inherited by
# python and its `claude` subprocess. Since 2026-07-28 the machine PATH has C:\php84 instead of
# C:\xampp\php (7.4), so usually PATH php is already fine -- only prepend when it is missing or too old.
$phpCmd = Get-Command php.exe -ErrorAction SilentlyContinue
$phpOk = $false
if ($phpCmd -and $phpCmd.Version) { $phpOk = ([version]$phpCmd.Version -ge [version]'8.4.1') }
if (-not $phpOk) {
    if (Test-Path 'C:\php84\php.exe') { $env:PATH = 'C:\php84;' + $env:PATH }
    else { Write-Log 'NO_PHP84 (PATH php missing or < 8.4.1, and no C:\php84\php.exe) - trading-info tasks will fail' }
}
if (-not $env:BRIDGE_PLATFORM) { $env:BRIDGE_PLATFORM = 'discord' }

# Pick the interpreter that actually HAS the deps, not merely the newest one: setup installs with
# `python -m pip install -r requirements.txt` (PATH's python), but a second Python 3.x folder would
# win a "highest version" pick and die later with ModuleNotFoundError. Try PATH first, then the
# folders (highest minor first; sort on the number, 'Python39' sorts above 'Python313' as text) and
# accept the first one that can import discord. That also rejects the Microsoft Store alias stub.
# Full path on purpose: `Start-Process python` + hidden has failed with exit 255.
$target = 'bridge.py'
$py = $null
if ($env:RUN_LOOP_TARGET) {
    $target = $env:RUN_LOOP_TARGET
    $onPath = Get-Command python -ErrorAction SilentlyContinue
    if ($onPath) { $py = $onPath.Source }
} else {
    $pyRoot = Join-Path $env:LOCALAPPDATA 'Programs\Python'
    $cands = @()
    $onPath = Get-Command python -ErrorAction SilentlyContinue
    if ($onPath) { $cands += $onPath.Source }
    $cands += Get-ChildItem $pyRoot -Filter 'Python3*' -Directory -ErrorAction SilentlyContinue |
        Sort-Object { [int]($_.Name -replace '^Python3', '') } -Descending |
        ForEach-Object { Join-Path $_.FullName 'python.exe' }
    foreach ($cand in $cands) {
        if (-not $cand -or -not (Test-Path $cand)) { continue }
        & $cand -c "import discord" 2>$null
        if ($LASTEXITCODE -eq 0) { $py = $cand; break }
    }
}
if (-not $py) {
    Write-Log 'NO_PYTHON_DEPS (no interpreter with discord.py; run: python -m pip install -r requirements.txt)'
    Remove-Item -LiteralPath $pidFile -ErrorAction SilentlyContinue
    exit 1
}

# Sleep in 1s steps so a stop request is noticed during a long backoff. True = stop was requested.
function Wait-OrStop([int]$sec) {
    for ($i = 0; $i -lt $sec; $i++) {
        if (Test-Path -LiteralPath $stopFile) { return $true }
        Start-Sleep -Seconds 1
    }
    return (Test-Path -LiteralPath $stopFile)
}

Write-Log ("supervisor start pid={0} python={1} target={2}" -f $PID, $py, $target)
$fast = 0
while ($true) {
    $t0 = Get-Date
    & $py $target
    $code = $LASTEXITCODE
    $dur = [int]((Get-Date) - $t0).TotalSeconds
    Write-Log ("bot exit code={0} ({1}s)" -f $code, $dur)

    if (Test-Path -LiteralPath $stopFile) {
        Remove-Item -LiteralPath $stopFile -ErrorAction SilentlyContinue
        Write-Log 'stop_requested - supervisor exiting'
        break
    }
    if ($code -eq 75 -or $code -eq 0) {
        # Deliberate exit (code-change reload / 'restart' command): not a crash, no backoff.
        $fast = 0
        Write-Log 'deliberate exit - relaunching'
        if (Wait-OrStop 1) { Remove-Item -LiteralPath $stopFile -ErrorAction SilentlyContinue; Write-Log 'stop_requested - supervisor exiting'; break }
        continue
    }
    if ($dur -lt 30) { $fast++ } else { $fast = 0 }
    $wait = 3
    if ($fast -ge 5) {
        $wait = [Math]::Min(60 * ($fast - 4), 300)
        Write-Log ("{0} fast crashes in a row - backing off {1}s (check logs\bridge.log)" -f $fast, $wait)
    }
    if (Wait-OrStop $wait) { Remove-Item -LiteralPath $stopFile -ErrorAction SilentlyContinue; Write-Log 'stop_requested - supervisor exiting'; break }
}
Remove-Item -LiteralPath $pidFile -ErrorAction SilentlyContinue
