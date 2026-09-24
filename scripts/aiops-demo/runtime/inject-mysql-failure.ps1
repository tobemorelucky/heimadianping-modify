[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'common.ps1')

$state = Get-DemoState
if ($null -eq $state -or $state.status -ne 'running') {
    throw 'A running Demo state is required. Run start-aiops-demo.ps1 first.'
}
$consumer = Get-ProcessRecord -State $state -Role 'consumer'
if (-not (Test-ProcessRecordRunning $consumer)) {
    throw 'Consumer JVM must be running before injecting its isolated MySQL path fault.'
}

& (Join-Path $script:RepositoryRoot 'scripts\aiops-demo\inject-consumer-mysql-fault.ps1')
$faultStartedAt = (Get-Date).ToUniversalTime().ToString('o')
Set-StateFault -State $state -Name 'consumer_mysql_path' -Status 'active' -Timestamp $faultStartedAt
Save-DemoState $state

$proxyState = docker inspect --format '{{.State.Status}}' hmdp-aiops-demo-mysql-consumer-proxy 2>$null
Write-Host "MySQL Consumer-path fault started at: $faultStartedAt"
Write-Host "Current proxy state: $proxyState"
Write-Host 'Demo MySQL itself remains healthy; only the Consumer TCP path is interrupted.'
