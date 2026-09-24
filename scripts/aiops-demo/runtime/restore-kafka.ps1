[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'common.ps1')

$state = Get-DemoState
if ($null -eq $state -or $state.status -ne 'running') {
    throw 'A running Demo state is required. Run start-aiops-demo.ps1 first.'
}
$current = Get-ProcessRecord -State $state -Role 'consumer'
if (Test-ProcessRecordRunning $current) {
    Write-Host "Consumer is already running with PID $($current.pid)."
    return
}

$suffix = Get-Date -Format 'yyyyMMdd-HHmmss'
$record = Start-DemoConsumerProcess -LogDirectory $state.run_directory -Role "consumer-restored-$suffix"
Wait-HttpReady -Uri 'http://127.0.0.1:18082/internal/aiops/mysql-health' `
    -Name 'restored hmdp-consumer MySQL health outlet' -TimeoutSeconds 120
Set-ProcessListenerPid -Record $record -Port 18082

$current.pid = $record.pid
$current.listener_pid = $record.listener_pid
$current.start_time = $record.start_time
$current.stdout = $record.stdout
$current.stderr = $record.stderr
$current.status = 'running'
Set-StateFault -State $state -Name 'kafka_consumer_down' -Status 'restored' `
    -Timestamp ((Get-Date).ToUniversalTime().ToString('o'))
Save-DemoState $state

Write-Host "Kafka Consumer restored with PID $($current.pid)."
Write-Host "Logs: $($current.stdout)"
