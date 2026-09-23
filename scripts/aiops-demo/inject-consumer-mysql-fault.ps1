[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$projectName = 'hmdp-aiops-demo'
$proxyService = 'mysql-consumer-proxy'
$proxyContainer = 'hmdp-aiops-demo-mysql-consumer-proxy'
$mysqlContainer = 'hmdp-aiops-demo-mysql'
$scriptRoot = Split-Path -Parent $PSCommandPath
$repositoryRoot = (Resolve-Path (Join-Path $scriptRoot '..\..')).Path
$composeFile = Join-Path $repositoryRoot 'docker-compose.aiops-demo.yml'

if (-not (Test-Path -LiteralPath $composeFile -PathType Leaf)) {
    throw "Demo Compose file not found: $composeFile"
}

$mysqlState = docker inspect --format '{{.State.Status}}/{{.State.Health.Status}}' $mysqlContainer 2>$null
if ($LASTEXITCODE -ne 0 -or $mysqlState -ne 'running/healthy') {
    throw 'Demo MySQL must be running and healthy before injecting the Consumer-only path fault.'
}

$proxyState = docker inspect --format '{{.State.Status}}' $proxyContainer 2>$null
if ($LASTEXITCODE -ne 0 -or $proxyState -ne 'running') {
    throw 'Consumer MySQL proxy is not running; no new fault was injected.'
}

Push-Location $repositoryRoot
try {
    # Stop only the stateless Consumer proxy. MySQL and all persistent volumes remain untouched.
    docker compose --project-name $projectName --file $composeFile stop $proxyService
    if ($LASTEXITCODE -ne 0) {
        throw 'Unable to stop the Consumer MySQL proxy.'
    }

    $mysqlStateAfter = docker inspect --format '{{.State.Status}}/{{.State.Health.Status}}' $mysqlContainer
    if ($LASTEXITCODE -ne 0 -or $mysqlStateAfter -ne 'running/healthy') {
        throw 'Safety check failed: Demo MySQL is not healthy after proxy injection.'
    }

    docker compose --project-name $projectName --file $composeFile ps --all $proxyService mysql-demo
    Write-Host 'Consumer-only MySQL path fault is active.'
    Write-Host 'Demo MySQL remains running and healthy; no volume or data was changed.'
}
finally {
    Pop-Location
}
