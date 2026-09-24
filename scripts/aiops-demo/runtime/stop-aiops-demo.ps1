[CmdletBinding()]
param([switch]$KeepDocker)

$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'common.ps1')

$state = Get-DemoState
if ($null -ne $state) {
    foreach ($role in @('console', 'monitoring', 'agent-api', 'web', 'consumer')) {
        Stop-ProcessRecord (Get-ProcessRecord -State $state -Role $role)
    }
    $state.status = 'stopped'
    if ($state.PSObject.Properties.Name -contains 'stopped_at') {
        $state.stopped_at = (Get-Date).ToUniversalTime().ToString('o')
    }
    else {
        $state | Add-Member -NotePropertyName stopped_at `
            -NotePropertyValue ((Get-Date).ToUniversalTime().ToString('o'))
    }
    Save-DemoState $state
}
else {
    Write-Host 'No runtime state file was found; no recorded host process was stopped.'
}

if (-not $KeepDocker) {
    docker version --format '{{.Server.Version}}' 1>$null 2>$null
    if ($LASTEXITCODE -eq 0) {
        & (Join-Path $script:RepositoryRoot 'scripts\aiops-demo\stop-demo.ps1')
    }
    else {
        Write-Warning 'Docker daemon is unavailable; host processes were stopped, and Demo containers were left untouched.'
    }
}

Write-Host 'AIOps Demo host processes are stopped. Logs, SQLite files, containers, and volumes were retained.'
