[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$projectName = 'hmdp-aiops-demo'
$scriptRoot = Split-Path -Parent $PSCommandPath
$repositoryRoot = (Resolve-Path (Join-Path $scriptRoot '..\..')).Path
$composeFile = Join-Path $repositoryRoot 'docker-compose.aiops-demo.yml'

if (-not (Test-Path -LiteralPath $composeFile -PathType Leaf)) {
    throw "Demo Compose file not found: $composeFile"
}

Push-Location $repositoryRoot
try {
    # Stop only: containers and all demo volumes remain recoverable.
    docker compose --project-name $projectName --file $composeFile stop
    if ($LASTEXITCODE -ne 0) {
        throw 'Unable to stop the demo services cleanly.'
    }
    docker compose --project-name $projectName --file $composeFile ps --all
    Write-Host 'AIOps demo services are stopped. Containers, topics, and volumes were retained.'
}
finally {
    Pop-Location
}
