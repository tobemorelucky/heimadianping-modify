[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$runtimeScript = Join-Path $PSScriptRoot '..\runtime\inject-mysql-failure.ps1'

if (-not (Test-Path -LiteralPath $runtimeScript -PathType Leaf)) {
    throw "MySQL 故障注入脚本不存在：$runtimeScript"
}

# 底层脚本只停止 Consumer 专属 TCP Proxy，并在操作前后确认 Demo MySQL
# 本身仍然健康。Web、Kafka、Agent 和持久化 Volume 不受影响。
& $runtimeScript

Write-Host ''
Write-Host 'Consumer MySQL连接超时故障已注入' -ForegroundColor Yellow
Write-Host 'mysql-consumer-proxy 已停止；MySQL 本身、Web、Kafka 和 Agent 保持运行。'
Write-Host '请继续通过真实 HTTP 入口发送少量隔离测试订单。'
Write-Host '等待Monitoring检测...'
