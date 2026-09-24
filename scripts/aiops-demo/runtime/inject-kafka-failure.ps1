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
    throw 'The recorded Consumer JVM is not running; no new Kafka fault was injected.'
}
$web = Get-ProcessRecord -State $state -Role 'web'
if (-not (Test-ProcessRecordRunning $web)) {
    throw 'The Web JVM is not running. Refusing to label this as an isolated Consumer failure.'
}

$faultStartedAt = (Get-Date).ToUniversalTime().ToString('o')
Stop-ProcessRecord $consumer
Set-StateFault -State $state -Name 'kafka_consumer_down' -Status 'active' -Timestamp $faultStartedAt
Save-DemoState $state

Write-Host "Kafka Consumer Down fault started at: $faultStartedAt"
Write-Host "Consumer PID $($consumer.pid) stopped. Web PID $($web.pid) remains running."
Write-Host 'Send a few isolated test orders through the HTTP helper, then wait for two monitoring windows.'
