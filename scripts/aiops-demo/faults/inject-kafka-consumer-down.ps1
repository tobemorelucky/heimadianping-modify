[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$runtimeScript = Join-Path $PSScriptRoot '..\runtime\inject-kafka-failure.ps1'

if (-not (Test-Path -LiteralPath $runtimeScript -PathType Leaf)) {
    throw "Kafka 故障注入脚本不存在：$runtimeScript"
}

# 底层脚本根据 Demo state.json 校验受管 PID，只停止 Consumer JVM；
# Web、Kafka、Agent 和 Monitoring Scheduler 均保持运行。
& $runtimeScript

Write-Host ''
Write-Host 'Kafka Consumer故障已注入' -ForegroundColor Yellow
Write-Host 'Web、Kafka、Agent 和 Monitoring Scheduler 保持运行。'
Write-Host '请继续通过真实 HTTP 入口发送少量隔离测试订单。'
Write-Host '等待Monitoring检测...'
