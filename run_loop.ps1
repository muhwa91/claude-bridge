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
# trading-info backend needs PHP 8.4.1+ (Laravel 13 / composer.lock symfony 8.1); inherited by
# python and its `claude` subprocess. Bot itself is python (unaffected). Since 2026-07-28 the machine
# PATH has C:\php84 instead of C:\xampp\php (7.4), so usually PATH php is already fine -- only
# prepend when it is missing or too old. Same block as start.ps1.
$phpCmd = Get-Command php.exe -ErrorAction SilentlyContinue
$phpOk = $false
if ($phpCmd -and $phpCmd.Version) { $phpOk = ([version]$phpCmd.Version -ge [version]'8.4.1') }
if (-not $phpOk) {
    if (Test-Path 'C:\php84\php.exe') { $env:PATH = 'C:\php84;' + $env:PATH }
    else { Write-Host 'NO_PHP84 (PATH php missing or < 8.4.1, and no C:\php84\php.exe) - trading-info tasks will fail' }
}
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
