[CmdletBinding()]
param(
    [ValidateRange(10, 300)][int]$TimeoutSeconds = 120
)

$ErrorActionPreference = 'Stop'
$runtimeScript = Join-Path $PSScriptRoot '..\runtime\restore-kafka.ps1'
$repositoryRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..\..')).Path
$composeFile = Join-Path $repositoryRoot 'docker-compose.aiops-demo.yml'
$consumerGroup = 'hmdp-aiops-mysql-demo-consumer-v1'

if (-not (Test-Path -LiteralPath $runtimeScript -PathType Leaf)) {
    throw "Kafka Consumer 恢复脚本不存在：$runtimeScript"
}
if (-not (Test-Path -LiteralPath $composeFile -PathType Leaf)) {
    throw "AIOps Demo Compose 文件不存在：$composeFile"
}

# 底层脚本使用原 Profile 和原日志目录重新启动受管 Consumer JVM，
# 不修改 Topic、offset 或任何业务数据。
& $runtimeScript

Write-Host '正在等待 Consumer 重新加入 Kafka Group...'
$deadline = (Get-Date).AddSeconds($TimeoutSeconds)
$lastGroupOutput = ''
$joined = $false

do {
    $groupOutput = & docker compose --project-name hmdp-aiops-demo --file $composeFile `
        exec --no-TTY kafka-demo `
        /opt/kafka/bin/kafka-consumer-groups.sh `
        --bootstrap-server kafka-demo:19092 `
        --describe --group $consumerGroup --state 2>&1
    $commandSucceeded = $LASTEXITCODE -eq 0
    $lastGroupOutput = (@($groupOutput) -join [Environment]::NewLine).Trim()

    if ($commandSucceeded) {
        foreach ($line in @($groupOutput)) {
            $text = [string]$line
            if (-not $text.TrimStart().StartsWith($consumerGroup, [StringComparison]::Ordinal)) {
                continue
            }
            $columns = @($text.Trim() -split '\s+')
            $membersText = $columns[-1]
            $members = 0
            if ($text -match '\bStable\b' -and
                    [int]::TryParse($membersText, [ref]$members) -and $members -gt 0) {
                $joined = $true
                break
            }
        }
    }

    if (-not $joined) { Start-Sleep -Seconds 2 }
} while (-not $joined -and (Get-Date) -lt $deadline)

if (-not $joined) {
    throw "Consumer 已重新启动，但未在 $TimeoutSeconds 秒内稳定加入 Group。最后状态：$lastGroupOutput"
}

Write-Host ''
Write-Host 'Kafka Consumer已恢复，并重新加入Consumer Group。' -ForegroundColor Green
Write-Host 'Kafka Topic、offset 和业务数据均未重置。'
Write-Host '请等待连续健康检测窗口，直到 Console 中 Incident 变为 RECOVERED。'
