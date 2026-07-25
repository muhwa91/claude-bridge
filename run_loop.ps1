# claude-bridge restart launcher (local / laptop -- PowerShell)
# ASCII-only on purpose: Windows PowerShell 5.1 misreads UTF-8-no-BOM Korean as cp949
# and breaks brace matching (parse error). Keep this file ASCII.
#
# Auto-restarts on both the '재시작' command (bridge exits 0) and crashes (exit != 0),
# completing the phone self-edit loop.
#   - Local  = this loop handles restart.
#   - Oracle VM = systemd (Restart=always) handles it; this file is not used there.
#   - Crash guard: if it dies within 10s five times in a row, stop.
#   - To stop: close this window.
$ErrorActionPreference = 'Continue'
Set-Location -Path $PSScriptRoot
if (-not $env:BRIDGE_PLATFORM) { $env:BRIDGE_PLATFORM = 'discord' }
# trading-info backend needs PHP 8.4 (XAMPP 7.4 on PATH breaks vendor/tests). Prepend C:\php84 so
# the bot's `php` = 8.4; inherited by python and its `claude` subprocess. Bot itself is python (unaffected).
if (Test-Path 'C:\php84\php.exe') { $env:PATH = 'C:\php84;' + $env:PATH }
$fails = 0
while ($true) {
    $t0 = Get-Date
    & python bridge.py
    $code = $LASTEXITCODE
    $dur = ((Get-Date) - $t0).TotalSeconds
    Write-Host ("[{0}] bridge exit code={1} ({2}s)" -f (Get-Date -Format HH:mm:ss), $code, [int]$dur)
    if ($code -eq 0) { $fails = 0; Write-Host 'restart/normal exit - relaunching now'; continue }
    if ($dur -lt 10) { $fails++ } else { $fails = 0 }
    if ($fails -ge 5) { Write-Host '[WARN] 5 fast crashes in a row - stopping. check logs/bridge.log'; break }
    Write-Host 'relaunch in 3s...'
    Start-Sleep -Seconds 3
}
