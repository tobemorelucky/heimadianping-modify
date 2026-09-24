[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'common.ps1')

$state = Get-DemoState
if ($null -eq $state) {
    throw 'Demo runtime state was not found.'
}

& (Join-Path $script:RepositoryRoot 'scripts\aiops-demo\restore-consumer-mysql-path.ps1')
$restoredAt = (Get-Date).ToUniversalTime().ToString('o')
Set-StateFault -State $state -Name 'consumer_mysql_path' -Status 'restored' -Timestamp $restoredAt
Save-DemoState $state

$proxyState = docker inspect --format '{{.State.Status}}/{{.State.Health.Status}}' `
    hmdp-aiops-demo-mysql-consumer-proxy 2>$null
Write-Host "MySQL Consumer path restored at: $restoredAt"
Write-Host "Current proxy state: $proxyState"
