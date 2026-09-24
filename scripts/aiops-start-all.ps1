[CmdletBinding()]
param(
    [switch]$Rebuild,
    [switch]$NoOpen
)

$ErrorActionPreference = 'Stop'
$runtimeScript = Join-Path $PSScriptRoot 'aiops-demo\runtime\start-aiops-demo.ps1'

if (-not (Test-Path -LiteralPath $runtimeScript -PathType Leaf)) {
    throw "AIOps Demo 启动脚本不存在：$runtimeScript"
}

$arguments = @{}
if ($Rebuild) { $arguments.Rebuild = $true }
if ($NoOpen) { $arguments.NoOpen = $true }

Write-Host '正在启动 AIOps Demo 完整环境...'
& $runtimeScript @arguments

Write-Host ''
Write-Host 'AIOps Demo 启动完成。' -ForegroundColor Green
Write-Host 'Console: http://127.0.0.1:5173'
Write-Host 'Agent:   http://127.0.0.1:8010'
Write-Host 'Web:     http://127.0.0.1:8081'
Write-Host ''
Write-Host '故障注入与恢复只允许通过 scripts\aiops-demo\faults 下的后台脚本执行。'
Write-Host 'Console 仅用于查看系统状态、Incident 和 Agent 诊断过程。'
