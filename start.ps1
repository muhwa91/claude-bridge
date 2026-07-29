# claude-bridge start (local / laptop -- PowerShell)
# ASCII-only on purpose: Windows PowerShell 5.1 misreads UTF-8-no-BOM Korean as cp949
# and breaks brace matching (parse error). Keep this file ASCII.
#
# Idempotent "make sure the bot is up". Called at the top of every owner session
# (root CLAUDE.md, 2FA step) and by hand when the bot died.
#   - Already running -> no-op. Restarting would drop a healthy Gateway session for nothing.
#   - -Force          -> kill it first, then start (use after editing bridge.py).
# Starts hidden + detached so closing this window does not kill the bot
# (that is exactly why run_loop.ps1 is not used interactively anymore).
param([switch]$Force)
$ErrorActionPreference = 'Stop'
Set-Location -Path $PSScriptRoot

# Session ping: stamp today's date so the bridge can fire the once-a-day digest
# (schedules/notify.json item with "on": "session"). Written BEFORE the ALREADY_RUNNING
# exit on purpose -- the ping means "a session started", which is independent of whether
# the bot needed starting. SilentlyContinue: a failed log write must never block the bot.
New-Item -ItemType Directory -Force -Path 'logs' -ErrorAction SilentlyContinue | Out-Null
Set-Content -Path 'logs\session_ping' -Value (Get-Date -Format 'yyyy-MM-dd') `
    -Encoding ascii -ErrorAction SilentlyContinue

$running = @(Get-CimInstance Win32_Process -Filter "Name='python.exe'" |
    Where-Object { $_.CommandLine -like '*bridge.py*' })

if ($running.Count -gt 0 -and -not $Force) {
    Write-Output ("ALREADY_RUNNING pid={0}" -f ($running[0].ProcessId))
    exit 0
}
foreach ($p in $running) { Stop-Process -Id $p.ProcessId -Force -ErrorAction SilentlyContinue }

if (-not $env:BRIDGE_PLATFORM) { $env:BRIDGE_PLATFORM = 'discord' }
# trading-info backend needs PHP 8.4.1+ (Laravel 13 / composer.lock symfony 8.1); inherited by the
# bot's `claude` subprocess. Since 2026-07-28 the machine PATH has C:\php84 instead of C:\xampp\php
# (7.4), so usually PATH php is already fine -- only prepend when it is missing or too old (stale
# process env, or a machine that installed 8.4 elsewhere). Same block as run_loop.ps1.
$phpCmd = Get-Command php.exe -ErrorAction SilentlyContinue
$phpOk = $false
if ($phpCmd -and $phpCmd.Version) { $phpOk = ([version]$phpCmd.Version -ge [version]'8.4.1') }
if (-not $phpOk) {
    if (Test-Path 'C:\php84\php.exe') { $env:PATH = 'C:\php84;' + $env:PATH }
    else { Write-Output 'NO_PHP84 (PATH php missing or < 8.4.1, and no C:\php84\php.exe) - trading-info tasks will fail' }
}

# Full path on purpose: `Start-Process python` + -WindowStyle Hidden has failed with exit 255.
# Built from $env:LOCALAPPDATA so no personal path is hardcoded (this file is mirrored publicly).
#
# Pick the interpreter that actually HAS the deps, not merely the newest one. Setup installs with
# `python -m pip install -r requirements.txt` (PATH's python), while this launcher used to take the
# highest-numbered Python3* folder. Those are the same only while one 3.x is installed -- add a
# second and the bridge starts on an interpreter without discord.py and dies at runtime with
# ModuleNotFoundError, far from the cause. So: try PATH first, then folders (highest minor first;
# sort on the number, as 'Python39' sorts above 'Python313' as text), and accept the first one that
# can import discord. That check also rejects the Microsoft Store alias stub.
$pyRoot = Join-Path $env:LOCALAPPDATA 'Programs\Python'
$cands = @()
$onPath = (Get-Command python -ErrorAction SilentlyContinue)
if ($onPath) { $cands += $onPath.Source }
$cands += Get-ChildItem $pyRoot -Filter 'Python3*' -Directory -ErrorAction SilentlyContinue |
    Sort-Object { [int]($_.Name -replace '^Python3', '') } -Descending |
    ForEach-Object { Join-Path $_.FullName 'python.exe' }

$py = $null
foreach ($cand in $cands) {
    if (-not $cand -or -not (Test-Path $cand)) { continue }
    & $cand -c "import discord" 2>$null
    if ($LASTEXITCODE -eq 0) { $py = $cand; break }
}
if (-not $py) {
    Write-Output "NO_PYTHON_DEPS (no interpreter with discord.py; run: python -m pip install -r requirements.txt)"
    exit 1
}

$proc = Start-Process -FilePath $py -ArgumentList 'bridge.py' `
    -WorkingDirectory $PSScriptRoot -WindowStyle Hidden -PassThru

# Do not report STARTED for a process that is merely still alive. A "sleep N then check liveness"
# test cannot answer "did it log in?", because on a rejected token the failure only surfaces at
# roughly python startup (~1.5s) + login refusal (~0.4s) + shutdown (~2s) -- about 4s in. The old
# 3s check therefore sampled the one window where a doomed bridge still looks healthy, and reported
# STARTED for a bridge that was dead moments later (2026-07-28 and again 07-29). Raising the sleep
# would just move the race, so wait on the two real outcomes instead and take whichever lands first:
#   logs\bridge.ready appears -> bridge reached Gateway on_ready (written by bridge.py)
#   process exits             -> startup failed; exit code and log say why
$ready = Join-Path $PSScriptRoot 'logs\bridge.ready'
$deadline = 60   # 200ms ticks; generous because a cold start pulls the project list first
for ($i = 0; $i -lt $deadline; $i++) {
    if (Test-Path $ready) { break }
    if ($proc.HasExited) { break }
    Start-Sleep -Milliseconds 200
}
if ($proc.HasExited) {
    Write-Output ("EXITED_EARLY pid={0} code={1} - bridge died on startup, see logs\bridge.log" -f $proc.Id, $proc.ExitCode)
    $log = Join-Path $PSScriptRoot 'logs\bridge.log'
    # -Encoding UTF8: bridge.log is UTF-8 but PS5.1 reads as ANSI, turning the Korean reason
    # (the whole point of printing it) into mojibake.
    if (Test-Path $log) { Get-Content $log -Tail 4 -Encoding UTF8 | ForEach-Object { Write-Output ("  " + $_) } }
    exit 1
}
if (-not (Test-Path $ready)) {
    # Alive but never reached on_ready within the window. Not proof of failure (a slow network can
    # do this), so do not kill it -- but do not claim success either.
    Write-Output ("STARTING pid={0} - alive but no Gateway connect yet, check logs\bridge.log" -f $proc.Id)
    exit 0
}
Write-Output ("STARTED pid={0}" -f $proc.Id)
