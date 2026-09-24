[CmdletBinding()]
param(
    [ValidateRange(10, 300)][int]$TimeoutSeconds = 120
)

$ErrorActionPreference = 'Stop'
$runtimeScript = Join-Path $PSScriptRoot '..\runtime\restore-mysql.ps1'

if (-not (Test-Path -LiteralPath $runtimeScript -PathType Leaf)) {
    throw "MySQL 路径恢复脚本不存在：$runtimeScript"
}

# 底层脚本只恢复无状态 TCP Proxy，不修改数据库、不重放消息，也不补偿订单。
& $runtimeScript

Write-Host '正在等待 Consumer 数据库连接测试恢复...'
$deadline = (Get-Date).AddSeconds($TimeoutSeconds)
$connectionHealthy = $false
$lastStatus = 'unavailable'

do {
    try {
        $health = Invoke-RestMethod -Uri 'http://127.0.0.1:18082/internal/aiops/mysql-health' `
            -Method Get -TimeoutSec 3
        $lastStatus = "database_reachable=$($health.database_reachable), connection_test_status=$($health.connection_test_status)"
        $connectionHealthy = $health.source_role -eq 'hmdp-consumer' -and
            $health.database_reachable -eq $true -and
            $health.connection_test_status -eq 'valid'
    }
    catch {
        $lastStatus = $_.Exception.Message
    }

    if (-not $connectionHealthy) { Start-Sleep -Seconds 2 }
} while (-not $connectionHealthy -and (Get-Date) -lt $deadline)

if (-not $connectionHealthy) {
    throw "Proxy 已恢复，但 Consumer 数据库健康出口未在 $TimeoutSeconds 秒内恢复。最后状态：$lastStatus"
}

Write-Host ''
Write-Host 'Consumer MySQL连接路径已恢复。' -ForegroundColor Green
Write-Host '数据库、表结构和历史数据均未修改，也未执行订单补偿。'
Write-Host '请等待连续健康检测窗口，直到 Console 中 Incident 变为 RECOVERED。'
