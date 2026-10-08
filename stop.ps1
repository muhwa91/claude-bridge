# claude-bridge stop (local / laptop -- PowerShell)
# ASCII-only on purpose (Windows PowerShell 5.1 misreads UTF-8-no-BOM Korean as cp949). Keep ASCII.
#
# The ONLY way to really turn the bot off. A plain kill just makes the supervisor (run_loop.ps1)
# relaunch it. Creates logs\stop_requested (the supervisor then does not relaunch and exits, deleting
# the file), kills the bot tree (taskkill /T /F, so a running child claude dies with it), and waits for
# the supervisor to leave; if it does not within ~10s it is killed too.
# Start again with start.ps1.
$ErrorActionPreference = 'Continue'
Set-Location -Path $PSScriptRoot
New-Item -ItemType Directory -Force -Path 'logs' -ErrorAction SilentlyContinue | Out-Null
$stopFile = Join-Path $PSScriptRoot 'logs\stop_requested'
$loopPid  = Join-Path $PSScriptRoot 'logs\run_loop.pid'

function Get-SupervisorPid {
    if (-not (Test-Path -LiteralPath $loopPid)) { return 0 }
    $n = 0
    [void][int]::TryParse(((Get-Content -LiteralPath $loopPid -ErrorAction SilentlyContinue) -join '').Trim(), [ref]$n)
    if ($n -le 0) { return 0 }
    $p = Get-CimInstance Win32_Process -Filter ("ProcessId={0}" -f $n) -ErrorAction SilentlyContinue
    if ($p -and $p.CommandLine -match 'run_loop\.ps1') { return $n }
    return 0
}

$sup = Get-SupervisorPid
# The stop file goes first, so the supervisor cannot relaunch in the gap after the bot dies.
Set-Content -LiteralPath $stopFile -Value 'stop.ps1' -Encoding ascii
$bots = @(Get-CimInstance Win32_Process -Filter "Name='python.exe'" |
    Where-Object { $_.CommandLine -match '[\/ ]bridge\.py\s*$' })
foreach ($b in $bots) { & taskkill.exe /PID $b.ProcessId /T /F 2>&1 | Out-Null }
Remove-Item -LiteralPath (Join-Path $PSScriptRoot 'logs\bridge.ready') -ErrorAction SilentlyContinue

if ($sup -gt 0) {
    for ($i = 0; $i -lt 40 -and (Get-SupervisorPid) -gt 0; $i++) { Start-Sleep -Milliseconds 250 }
    if ((Get-SupervisorPid) -gt 0) {
        & taskkill.exe /PID $sup /T /F 2>&1 | Out-Null
        Remove-Item -LiteralPath $loopPid -ErrorAction SilentlyContinue
        Remove-Item -LiteralPath $stopFile -ErrorAction SilentlyContinue
        Write-Output ("STOPPED (supervisor pid={0} did not exit by itself - killed)" -f $sup)
        exit 0
    }
} else {
    # No supervisor to consume the request; leaving the file would block nothing but confuse the next read.
    Remove-Item -LiteralPath $stopFile -ErrorAction SilentlyContinue
}
Write-Output ("STOPPED bots_killed={0} supervisor={1}" -f $bots.Count, $(if ($sup -gt 0) { 'exited' } else { 'none' }))
