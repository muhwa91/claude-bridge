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
# trading-info backend needs PHP 8.4 (XAMPP 7.4 on PATH breaks vendor/tests); inherited by the
# bot's `claude` subprocess. Same reason as run_loop.ps1.
if (Test-Path 'C:\php84\php.exe') { $env:PATH = 'C:\php84;' + $env:PATH }

# Full path on purpose: `Start-Process python` + -WindowStyle Hidden has failed with exit 255.
# Built from $env:LOCALAPPDATA so no personal path is hardcoded (this file is mirrored publicly).
# Highest installed 3.x wins -- machines differ (desktop 3.13, laptop 3.12). Sort on the numeric
# minor, not the folder name: 'Python39' sorts above 'Python313' as text.
$pyRoot = Join-Path $env:LOCALAPPDATA 'Programs\Python'
$py = Get-ChildItem $pyRoot -Filter 'Python3*' -Directory -ErrorAction SilentlyContinue |
    Sort-Object { [int]($_.Name -replace '^Python3', '') } -Descending |
    ForEach-Object { Join-Path $_.FullName 'python.exe' } |
    Where-Object { Test-Path $_ } | Select-Object -First 1
if (-not $py) { Write-Output "NO_PYTHON (no Python3*\python.exe under $pyRoot)"; exit 1 }

$proc = Start-Process -FilePath $py -ArgumentList 'bridge.py' `
    -WorkingDirectory $PSScriptRoot -WindowStyle Hidden -PassThru
Write-Output ("STARTED pid={0}" -f $proc.Id)
