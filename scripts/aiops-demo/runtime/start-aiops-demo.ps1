[CmdletBinding()]
param(
    [switch]$Rebuild,
    [switch]$NoOpen
)

$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'common.ps1')

Initialize-DemoRuntimeDirectory
$existing = Get-DemoState
if ($null -ne $existing) {
    $running = @($existing.processes | Where-Object { Test-ProcessRecordRunning $_ })
    if ($running.Count -gt 0) {
        if ($running.Count -eq @($existing.processes).Count) {
            Write-Host 'AIOps Demo is already running.'
            Write-Host 'Console: http://localhost:5173'
            if (-not $NoOpen) { Start-Process 'http://localhost:5173' }
            return
        }
        throw 'A previous Demo is only partially running. Run stop-aiops-demo.ps1 first.'
    }
}

foreach ($port in @(8081, 18081, 18082, 18083, 8010, 5173)) {
    Assert-PortAvailable -Port $port
}

foreach ($command in @('docker', 'java', 'node', 'npm')) {
    if ($null -eq (Get-Command $command -ErrorAction SilentlyContinue)) {
        throw "Required command is not available: $command"
    }
}
docker version --format '{{.Server.Version}}' 1>$null 2>$null
if ($LASTEXITCODE -ne 0) {
    throw 'Docker daemon is unavailable. Start Docker Desktop, wait until it is ready, then run this script again.'
}

if ($Rebuild -or -not (Test-Path -LiteralPath $script:JarPath -PathType Leaf)) {
    if ($null -eq (Get-Command 'mvn.cmd' -ErrorAction SilentlyContinue)) {
        throw 'Maven is required because the application JAR is missing or -Rebuild was requested.'
    }
    Push-Location $script:RepositoryRoot
    try {
        & mvn.cmd -DskipTests package
        if ($LASTEXITCODE -ne 0) { throw 'Spring Boot package failed.' }
    }
    finally { Pop-Location }
}

if (-not (Test-Path -LiteralPath $script:PythonPath -PathType Leaf)) {
    $python = Get-Command 'python' -ErrorAction SilentlyContinue
    if ($null -eq $python) { throw 'Python 3.11+ is required to initialize aiops-agent.' }
    & $python.Source -m venv (Join-Path $script:AgentRoot '.venv')
    & $script:PythonPath -m pip install -r (Join-Path $script:AgentRoot 'requirements.txt')
    if ($LASTEXITCODE -ne 0) { throw 'Unable to install aiops-agent dependencies.' }
}

if (-not (Test-Path -LiteralPath (Join-Path $script:ConsoleRoot 'node_modules') -PathType Container)) {
    Push-Location $script:ConsoleRoot
    try {
        & npm.cmd install
        if ($LASTEXITCODE -ne 0) { throw 'Unable to install aiops-console dependencies.' }
    }
    finally { Pop-Location }
}

& (Join-Path $script:RepositoryRoot 'scripts\aiops-demo\start-demo.ps1')
Wait-DockerHealth -Container 'hmdp-aiops-demo-mysql' -Name 'Demo MySQL'
Wait-DockerHealth -Container 'hmdp-aiops-demo-redis' -Name 'Demo Redis'
Wait-DockerHealth -Container 'hmdp-aiops-demo-kafka' -Name 'Demo Kafka'
Wait-DockerHealth -Container 'hmdp-aiops-demo-mysql-consumer-proxy' -Name 'Consumer MySQL proxy'
Wait-KafkaReady

$runId = Get-Date -Format 'yyyyMMdd-HHmmss'
$runDirectory = Join-Path $script:RuntimeRoot "runs\$runId"
New-Item -ItemType Directory -Force -Path $runDirectory | Out-Null

$state = [pscustomobject]@{
    schema_version = 1
    status = 'starting'
    run_id = $runId
    started_at = (Get-Date).ToUniversalTime().ToString('o')
    repository_root = $script:RepositoryRoot
    run_directory = $runDirectory
    database_path = (Join-Path $runDirectory 'aiops-demo.db')
    console_url = 'http://localhost:5173'
    processes = @()
    faults = [pscustomobject]@{}
}

try {
    $consumer = Start-DemoConsumerProcess -LogDirectory $runDirectory
    $state.processes += $consumer
    Save-DemoState $state
    Wait-HttpReady -Uri 'http://127.0.0.1:18082/internal/aiops/mysql-health' `
        -Name 'hmdp-consumer MySQL health outlet' -TimeoutSeconds 120
    Wait-HttpReady -Uri 'http://127.0.0.1:18083/internal/aiops/business-metrics/consumer' `
        -Name 'hmdp-consumer business metrics outlet' -TimeoutSeconds 30
    Set-ProcessListenerPid -Record $consumer -Port 18082

    $web = Start-LoggedProcess -Role 'web' -FilePath 'java.exe' `
        -ArgumentList @('-jar', $script:JarPath, '--spring.profiles.active=web,web-aiops-demo') `
        -WorkingDirectory $script:RepositoryRoot -LogDirectory $runDirectory -ListenerPort 8081
    $state.processes += $web
    Save-DemoState $state
    Wait-HttpReady -Uri 'http://127.0.0.1:8081/shop-type/list' -Name 'hmdp-web' -TimeoutSeconds 120
    Wait-HttpReady -Uri 'http://127.0.0.1:18081/internal/aiops/business-metrics/web' `
        -Name 'hmdp-web business metrics outlet' -TimeoutSeconds 30
    Set-ProcessListenerPid -Record $web -Port 8081

    $env:AIOPS_DATABASE_PATH = $state.database_path
    $env:AIOPS_REPLAY_DIRECTORY = Join-Path $script:AgentRoot 'data\console-replays'
    $env:HMDP_PROJECT_ROOT = $script:RepositoryRoot
    $env:KAFKA_BOOTSTRAP_SERVERS = '127.0.0.1:29092'
    $env:KAFKA_VOUCHER_ORDER_TOPIC = 'hmdp.aiops.mysql-demo.order.main.v1'
    $env:KAFKA_CONSUMER_GROUP = 'hmdp-aiops-mysql-demo-consumer-v1'
    $env:AIOPS_KAFKA_LAG_THRESHOLD = '1'
    $env:AIOPS_MYSQL_HEALTH_PORT = '18082'
    $env:AIOPS_BUSINESS_METRICS_WEB_PORT = '18081'
    $env:AIOPS_BUSINESS_METRICS_CONSUMER_PORT = '18083'

    & $script:PythonPath -m evaluation.replay_catalog
    if ($LASTEXITCODE -ne 0) { throw 'Unable to import recorded Console replays.' }

    $api = Start-LoggedProcess -Role 'agent-api' -FilePath $script:PythonPath `
        -ArgumentList @('-m', 'uvicorn', 'api.main:app', '--host', '127.0.0.1', '--port', '8010') `
        -WorkingDirectory $script:AgentRoot -LogDirectory $runDirectory -ListenerPort 8010
    $state.processes += $api
    Save-DemoState $state
    Wait-HttpReady -Uri 'http://127.0.0.1:8010/health' -Name 'AIOps Agent API' -TimeoutSeconds 60
    Set-ProcessListenerPid -Record $api -Port 8010

    $scheduler = Start-LoggedProcess -Role 'monitoring' -FilePath $script:PythonPath `
        -ArgumentList @('-m', 'monitoring.scheduler') -WorkingDirectory $script:AgentRoot `
        -LogDirectory $runDirectory
    $state.processes += $scheduler

    $console = Start-LoggedProcess -Role 'console' -FilePath 'npm.cmd' `
        -ArgumentList @('run', 'dev') -WorkingDirectory $script:ConsoleRoot `
        -LogDirectory $runDirectory -ListenerPort 5173
    $state.processes += $console
    Save-DemoState $state
    Wait-HttpReady -Uri 'http://127.0.0.1:5173' -Name 'AIOps Console' -TimeoutSeconds 60
    Set-ProcessListenerPid -Record $console -Port 5173

    $state.status = 'running'
    Save-DemoState $state
}
catch {
    for ($index = @($state.processes).Count - 1; $index -ge 0; $index--) {
        Stop-ProcessRecord $state.processes[$index]
    }
    $state.status = 'failed'
    if ($state.PSObject.Properties.Name -notcontains 'failure') {
        $state | Add-Member -NotePropertyName failure -NotePropertyValue $_.Exception.Message
    }
    Save-DemoState $state
    throw
}

Write-Host ''
Write-Host 'AIOps Demo is ready.'
Write-Host "Console:  $($state.console_url)"
Write-Host 'Web API:  http://127.0.0.1:8081'
Write-Host 'Agent API: http://127.0.0.1:8010'
Write-Host "State:    $script:StatePath"
Write-Host "Logs:     $runDirectory"
Write-Host "Database: $($state.database_path)"
Write-Host ''
$state.processes | Select-Object role, pid, listener_pid, stdout, stderr | Format-Table -AutoSize

if (-not $NoOpen) {
    Start-Process $state.console_url
}
