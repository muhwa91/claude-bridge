# claude-bridge start (local / laptop -- PowerShell)
# ASCII-only on purpose: Windows PowerShell 5.1 misreads UTF-8-no-BOM Korean as cp949
# and breaks brace matching (parse error). Keep this file ASCII.
#
# Idempotent "make sure the bot is up". Called at the top of every owner session
# (root CLAUDE.md, 2FA step) and by hand when the bot died.
# The bot runs under the supervisor run_loop.ps1 (hidden + detached), which relaunches it after
# the 'restart' command, a crash, or an automatic reload (bridge saw its own code change).
#   - Supervisor running -> no-op (ALREADY_RUNNING). Restarting would drop a healthy Gateway session.
#   - No supervisor      -> start one (hidden, detached: closing this window does not kill it).
#   - -Force             -> kill the bot tree (taskkill /T /F); the supervisor relaunches it.
#                           With no supervisor yet: kill any bot, then start the supervisor.
#                           Normally NOT needed after editing code -- the bot reloads itself when idle.
#   - -NoPing            -> no-op, kept for compatibility: the startup shortcut still passes it, and
#                           removing it would make parameter binding fail (auto-start dies).
# To really stop the bot: stop.ps1 (a plain kill just makes the supervisor relaunch it).
param([switch]$Force, [switch]$NoPing)
$ErrorActionPreference = 'Stop'
Set-Location -Path $PSScriptRoot

# Host guard: only a PC with the git-ignored marker file bridge_host.local may run the bridge.
# Default is "do not start" (fresh clones / other PCs), so two PCs never share one bot token.
# -Force does not bypass this, and there is no other bypass switch.
if (-not (Test-Path -LiteralPath (Join-Path $PSScriptRoot 'bridge_host.local'))) {
    Write-Output 'NOT_HOST (no bridge_host.local marker in the project root - this PC is not the bridge host; create the file to make it one)'
    exit 0
}

New-Item -ItemType Directory -Force -Path 'logs' -ErrorAction SilentlyContinue | Out-Null

# Test hooks (same env as run_loop.ps1, never set in normal use): state files in RUN_LOOP_STATE_DIR,
# bot = the RUN_LOOP_TARGET script instead of bridge.py.
$stateDir = Join-Path $PSScriptRoot 'logs'
if ($env:RUN_LOOP_STATE_DIR) { $stateDir = $env:RUN_LOOP_STATE_DIR }
$ready    = Join-Path $stateDir 'bridge.ready'
$stopFile = Join-Path $stateDir 'stop_requested'
$loopLog  = Join-Path $stateDir 'run_loop.log'
$loopPid  = Join-Path $stateDir 'run_loop.pid'
$botName  = 'bridge.py'
if ($env:RUN_LOOP_TARGET) { $botName = Split-Path -Leaf $env:RUN_LOOP_TARGET }
$botRe    = '[\\/ "]' + [regex]::Escape($botName) + '"?\s*$'

# Bot processes. Match the real launch only (`python.exe" bridge.py` at the end of the command line):
# a loose `-like '*bridge.py*'` also caught `pytest tests/test_bridge.py` and path strings in
# `python -c`, and reported ALREADY_RUNNING while tests ran (2026-08-16).
function Get-Bots {
    @(Get-CimInstance Win32_Process -Filter "Name='python.exe'" |
        Where-Object { $_.CommandLine -match $botRe })
}

# Supervisor: pid file + the live process command line must mention run_loop.ps1 (a stale pid can
# be reused by an unrelated process). Returns the pid, or 0.
function Get-SupervisorPid {
    if (-not (Test-Path -LiteralPath $loopPid)) { return 0 }
    $n = 0
    [void][int]::TryParse(((Get-Content -LiteralPath $loopPid -ErrorAction SilentlyContinue) -join '').Trim(), [ref]$n)
    if ($n -le 0) { return 0 }
    $p = Get-CimInstance Win32_Process -Filter ("ProcessId={0}" -f $n) -ErrorAction SilentlyContinue
    if ($p -and $p.CommandLine -match 'run_loop\.ps1') { return $n }
    return 0
}

function Get-LogLines([string]$path) {
    if (-not (Test-Path -LiteralPath $path)) { return @() }
    return @(Get-Content -LiteralPath $path -ErrorAction SilentlyContinue)
}

$sup  = Get-SupervisorPid
$bots = @(Get-Bots)

if (-not $Force) {
    if ($sup -gt 0) {
        $shown = $sup
        if ($bots.Count -gt 0) { $shown = $bots[0].ProcessId }
        Write-Output ("ALREADY_RUNNING pid={0}" -f $shown)
        exit 0
    }
    if ($bots.Count -gt 0) {
        # A bot started before the supervisor existed. Killing a healthy session unasked is wrong;
        # -Force puts it under supervision.
        Write-Output ("ALREADY_RUNNING pid={0} (no supervisor - run start.ps1 -Force once to put it under supervision)" -f $bots[0].ProcessId)
        exit 0
    }
}

# Forget the previous bot's ready flag first (taskkill /F skips bridge.py's own cleanup) so the
# wait below cannot be satisfied by a stale file.
Remove-Item -LiteralPath $ready -ErrorAction SilentlyContinue
$base = (Get-LogLines $loopLog).Count   # only look at run_loop.log lines written after this point
# 'Continue' here: under 'Stop', PS 5.1 turns taskkill's stderr text into a terminating error.
$ErrorActionPreference = 'Continue'
foreach ($b in $bots) { & taskkill.exe /PID $b.ProcessId /T /F 2>&1 | Out-Null }
$ErrorActionPreference = 'Stop'

# The supervisor logs the killed bot's exit (code=1) a moment later. That line is the OLD bot dying
# on purpose, not a failed start -- the wait loop below would read it as EXITED_EARLY and stop the
# supervisor (2026-10-09). So let it land, then only look at lines after it. The killed exit also
# bumps the supervisor's fast-crash counter by one; harmless (5 in a row are needed to back off).
if ($sup -gt 0 -and $bots.Count -gt 0) {
    for ($k = 0; $k -lt 50; $k++) {   # up to 10s
        $cur = @(Get-LogLines $loopLog)
        if (@($cur | Select-Object -Skip $base | Where-Object { $_ -match 'bot exit code=' }).Count -gt 0) { $base = $cur.Count; break }
        Start-Sleep -Milliseconds 200
    }
}

$supProc = $null
if ($sup -le 0) {
    Remove-Item -LiteralPath $stopFile -ErrorAction SilentlyContinue   # stale stop request from an old stop.ps1
    $psExe = Join-Path $env:SystemRoot 'System32\WindowsPowerShell\v1.0\powershell.exe'
    $supProc = Start-Process -FilePath $psExe -WindowStyle Hidden -PassThru -WorkingDirectory $PSScriptRoot `
        -ArgumentList '-NoProfile', '-ExecutionPolicy', 'Bypass', '-WindowStyle', 'Hidden', '-File', (Join-Path $PSScriptRoot 'run_loop.ps1')
}

# Do not report STARTED for a process that is merely still alive. A "sleep N then check liveness"
# test cannot answer "did it log in?" (a rejected token only surfaces ~4s in), so wait on the real
# outcomes and take whichever lands first:
#   logs\bridge.ready appears -> bridge reached Gateway on_ready (written by bridge.py)
#   bot dies / supervisor exits -> startup failed; exit code and log say why
$deadline = 300   # 200ms ticks (60s); generous because a cold start pulls the project list first
$failed = $null
for ($i = 0; $i -lt $deadline; $i++) {
    if (Test-Path $ready) { break }
    if ($supProc -and $supProc.HasExited) { $failed = 'supervisor'; break }
    if ($i % 5 -eq 0) {
        $new = @(Get-LogLines $loopLog | Select-Object -Skip $base)
        $dead = @($new | Where-Object { $_ -match 'bot exit code=(\d+)' -and $Matches[1] -ne '0' -and $Matches[1] -ne '75' })
        if ($dead.Count -gt 0) { $failed = $dead[0]; break }
    }
    Start-Sleep -Milliseconds 200
}

$new = @(Get-LogLines $loopLog | Select-Object -Skip $base)
$warn = @($new | Where-Object { $_ -match 'NO_PHP84' })
foreach ($w in $warn) { Write-Output ($w -replace '^\[[^\]]*\]\s*', '') }

if ($failed) {
    Write-Output ("EXITED_EARLY - bridge died on startup ({0}), see logs\bridge.log / logs\run_loop.log" -f $failed)
    # Nothing should stay behind retrying a doomed start: ask a still-living supervisor to stop.
    if ((Get-SupervisorPid) -gt 0) {
        Set-Content -LiteralPath $stopFile -Value 'start.ps1 startup failure' -Encoding ascii -ErrorAction SilentlyContinue
        for ($k = 0; $k -lt 40 -and (Get-SupervisorPid) -gt 0; $k++) { Start-Sleep -Milliseconds 250 }
    }
    # -Encoding UTF8: bridge.log is UTF-8 but PS5.1 reads as ANSI, turning the Korean reason
    # (the whole point of printing it) into mojibake.
    foreach ($f in @('logs\run_loop.log', 'logs\bridge.log')) {
        $path = Join-Path $PSScriptRoot $f
        if (Test-Path $path) { Get-Content $path -Tail 4 -Encoding UTF8 | ForEach-Object { Write-Output ("  " + $_) } }
    }
    exit 1
}

$botPid = 0
$bp = Join-Path $PSScriptRoot 'logs\bridge.pid'
if (Test-Path $bp) { [void][int]::TryParse(((Get-Content -LiteralPath $bp -ErrorAction SilentlyContinue) -join '').Trim(), [ref]$botPid) }
if ($botPid -le 0) {
    $nb = @(Get-Bots)
    if ($nb.Count -gt 0) { $botPid = $nb[0].ProcessId }
}
if (-not (Test-Path $ready)) {
    # Alive but never reached on_ready within the window. Not proof of failure (a slow network can
    # do this), so do not kill it -- but do not claim success either.
    Write-Output ("STARTING pid={0} - alive but no Gateway connect yet, check logs\bridge.log" -f $botPid)
    exit 0
}
Write-Output ("STARTED pid={0}" -f $botPid)
