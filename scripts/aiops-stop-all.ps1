[CmdletBinding()]
param(
    [switch]$KeepDocker
)

$ErrorActionPreference = 'Stop'
$runtimeScript = Join-Path $PSScriptRoot 'aiops-demo\runtime\stop-aiops-demo.ps1'

if (-not (Test-Path -LiteralPath $runtimeScript -PathType Leaf)) {
    throw "AIOps Demo 停止脚本不存在：$runtimeScript"
}

Write-Host '正在安全停止 AIOps Demo...'
Write-Host '停止顺序：Console -> Scheduler -> Agent -> Web -> Consumer -> Docker'

$arguments = @{}
if ($KeepDocker) { $arguments.KeepDocker = $true }
& $runtimeScript @arguments

Write-Host ''
Write-Host 'AIOps Demo 已安全停止。' -ForegroundColor Green
if ($KeepDocker) {
    Write-Host 'Demo Docker 容器按参数要求保持运行。'
}
else {
    Write-Host 'Demo Docker 容器已停止，但容器、Volume、Kafka Topic 和数据库数据均已保留。'
}
