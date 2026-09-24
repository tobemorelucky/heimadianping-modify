[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$results = [System.Collections.Generic.List[object]]::new()

function Add-CheckResult {
    param(
        [Parameter(Mandatory)][string]$Category,
        [Parameter(Mandatory)][string]$Name,
        [Parameter(Mandatory)][bool]$Healthy,
        [Parameter(Mandatory)][string]$Detail
    )
    $results.Add([pscustomobject]@{
        类别 = $Category
        检查项 = $Name
        状态 = if ($Healthy) { '正常' } else { '异常' }
        详情 = $Detail
    })
}

function Test-HttpEndpoint {
    param(
        [Parameter(Mandatory)][string]$Name,
        [Parameter(Mandatory)][string]$Uri
    )
    try {
        $response = Invoke-WebRequest -Uri $Uri -UseBasicParsing -TimeoutSec 5
        Add-CheckResult -Category '本地服务' -Name $Name -Healthy $true `
            -Detail "HTTP $($response.StatusCode) · $Uri"
    }
    catch {
        $statusCode = $null
        if ($null -ne $_.Exception.Response) {
            try { $statusCode = [int]$_.Exception.Response.StatusCode } catch { }
        }
        if ($null -ne $statusCode -and $statusCode -lt 500) {
            Add-CheckResult -Category '本地服务' -Name $Name -Healthy $true `
                -Detail "HTTP $statusCode · $Uri"
        }
        else {
            Add-CheckResult -Category '本地服务' -Name $Name -Healthy $false `
                -Detail "无法访问 · $Uri"
        }
    }
}

Test-HttpEndpoint -Name 'AIOps Agent（8010）' -Uri 'http://127.0.0.1:8010/health'
Test-HttpEndpoint -Name 'AIOps Console（5173）' -Uri 'http://127.0.0.1:5173'
Test-HttpEndpoint -Name 'Nginx（8080）' -Uri 'http://127.0.0.1:8080'
Test-HttpEndpoint -Name 'Spring Boot Web（8081）' -Uri 'http://127.0.0.1:8081/shop-type/list'

docker version --format '{{.Server.Version}}' 1>$null 2>$null
if ($LASTEXITCODE -ne 0) {
    Add-CheckResult -Category 'Docker' -Name 'Docker Engine' -Healthy $false `
        -Detail 'Docker daemon 不可用'
}
else {
    Add-CheckResult -Category 'Docker' -Name 'Docker Engine' -Healthy $true `
        -Detail 'Docker daemon 可用'
    foreach ($container in @(
        @{ Name = 'Demo MySQL'; Container = 'hmdp-aiops-demo-mysql' },
        @{ Name = 'Demo Redis'; Container = 'hmdp-aiops-demo-redis' },
        @{ Name = 'Demo Kafka'; Container = 'hmdp-aiops-demo-kafka' },
        @{ Name = 'Consumer MySQL Proxy'; Container = 'hmdp-aiops-demo-mysql-consumer-proxy' }
    )) {
        $state = docker inspect --format '{{.State.Status}}/{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' `
            $container.Container 2>$null
        $healthy = $LASTEXITCODE -eq 0 -and $state -match '^running/(healthy|none)$'
        Add-CheckResult -Category 'Docker' -Name $container.Name -Healthy $healthy `
            -Detail $(if ($state) { $state } else { '容器不存在或不可访问' })
    }
}

Write-Host ''
Write-Host 'AIOps Demo 健康检查结果'
Write-Host '========================'
$results | Format-Table -AutoSize

$failed = @($results | Where-Object { $_.状态 -eq '异常' })
if ($failed.Count -gt 0) {
    Write-Host "发现 $($failed.Count) 项异常。" -ForegroundColor Yellow
    exit 1
}

Write-Host '全部检查项正常。' -ForegroundColor Green
exit 0
