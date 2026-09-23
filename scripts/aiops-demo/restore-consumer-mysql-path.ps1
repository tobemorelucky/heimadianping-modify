[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$projectName = 'hmdp-aiops-demo'
$proxyService = 'mysql-consumer-proxy'
$mysqlContainer = 'hmdp-aiops-demo-mysql'
$scriptRoot = Split-Path -Parent $PSCommandPath
$repositoryRoot = (Resolve-Path (Join-Path $scriptRoot '..\..')).Path
$composeFile = Join-Path $repositoryRoot 'docker-compose.aiops-demo.yml'

if (-not (Test-Path -LiteralPath $composeFile -PathType Leaf)) {
    throw "Demo Compose file not found: $composeFile"
}

$mysqlState = docker inspect --format '{{.State.Status}}/{{.State.Health.Status}}' $mysqlContainer 2>$null
if ($LASTEXITCODE -ne 0 -or $mysqlState -ne 'running/healthy') {
    throw 'Demo MySQL must be running and healthy before restoring the Consumer path.'
}

Push-Location $repositoryRoot
try {
    docker compose --project-name $projectName --file $composeFile up --detach --wait $proxyService
    if ($LASTEXITCODE -ne 0) {
        throw 'Unable to restore the Consumer MySQL proxy.'
    }

    docker compose --project-name $projectName --file $composeFile ps $proxyService mysql-demo
    Write-Host 'Consumer MySQL path is restored. No application or database repair was executed.'
}
finally {
    Pop-Location
}
