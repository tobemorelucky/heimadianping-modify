[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent $PSScriptRoot
$ComposeFile = Join-Path $ProjectRoot "docker-compose.yml"
$RuntimeDirectory = Join-Path $ProjectRoot "target\runtime"
$NginxRoot = Join-Path $ProjectRoot "nginx-1.18.0"
$NginxExe = Join-Path $NginxRoot "nginx.exe"
$NginxPidFile = Join-Path $RuntimeDirectory "nginx.pid"
$LegacyComposeFiles = @(
    (Join-Path $ProjectRoot "docker\kafka\docker-compose-kafka.yml"),
    (Join-Path $ProjectRoot "docker\elasticsearch\docker-compose-es.yml"),
    (Join-Path $ProjectRoot "docker\canal\docker-compose-canal.yml")
)

function Stop-ManagedProcess {
    param(
        [Parameter(Mandatory = $true)][string]$Name,
        [Parameter(Mandatory = $true)][string]$PidFile
    )

    if (-not (Test-Path -LiteralPath $PidFile)) {
        Write-Host "$Name has no managed PID file."
        return
    }
    $processIdText = (Get-Content -LiteralPath $PidFile -Raw).Trim()
    if ($processIdText -match "^\d+$") {
        $managedProcess = Get-Process -Id ([int]$processIdText) -ErrorAction SilentlyContinue
        if ($null -ne $managedProcess) {
            # Stop the complete process tree so Maven/Java and Uvicorn children do not remain.
            & taskkill.exe /PID $managedProcess.Id /T /F | Out-Host
        } else {
            Write-Host "$Name process is no longer running."
        }
    } else {
        Write-Warning "$Name PID file is invalid; no process was terminated."
    }
    Remove-Item -LiteralPath $PidFile -Force -ErrorAction SilentlyContinue
}

function Stop-ManagedNginx {
    if (-not (Test-Path -LiteralPath $NginxPidFile)) {
        Write-Host "Nginx frontend has no managed PID file."
        return
    }

    $processIdText = (Get-Content -LiteralPath $NginxPidFile -Raw).Trim()
    $managedProcess = if ($processIdText -match "^\d+$") {
        Get-Process -Id ([int]$processIdText) -ErrorAction SilentlyContinue
    } else {
        $null
    }

    if ($null -ne $managedProcess -and (Test-Path -LiteralPath $NginxExe)) {
        # Ask Nginx to drain requests and stop its master/worker processes cleanly.
        Push-Location $NginxRoot
        try {
            & $NginxExe -s quit
        } finally {
            Pop-Location
        }

        $deadline = (Get-Date).AddSeconds(15)
        while ((Get-Date) -lt $deadline -and $null -ne (Get-Process -Id ([int]$processIdText) -ErrorAction SilentlyContinue)) {
            Start-Sleep -Milliseconds 250
        }
        $managedProcess = Get-Process -Id ([int]$processIdText) -ErrorAction SilentlyContinue
        if ($null -ne $managedProcess) {
            # Fall back to terminating only the PID recorded by this project.
            & taskkill.exe /PID $managedProcess.Id /T /F | Out-Host
        }
    } else {
        Write-Host "Nginx frontend process is no longer running."
    }
    Remove-Item -LiteralPath $NginxPidFile -Force -ErrorAction SilentlyContinue
}

Write-Host "[1/4] Stopping Nginx frontend..."
Stop-ManagedNginx

Write-Host "[2/4] Stopping AI service..."
Stop-ManagedProcess -Name "AI service" -PidFile (Join-Path $RuntimeDirectory "ai-assistant.pid")

Write-Host "[3/4] Stopping Spring Boot..."
Stop-ManagedProcess -Name "Spring Boot" -PidFile (Join-Path $RuntimeDirectory "spring-boot.pid")

Write-Host "[4/4] Stopping Docker services while preserving named volumes..."
& docker compose -f $ComposeFile down --remove-orphans
if ($LASTEXITCODE -ne 0) {
    throw "Failed to stop the Docker environment."
}
# Also stop a component started independently through a legacy child Compose.
foreach ($legacyComposeFile in $LegacyComposeFiles) {
    & docker compose -f $legacyComposeFile down --remove-orphans
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to stop legacy Compose project: $legacyComposeFile"
    }
}

Write-Host "All managed services have been stopped. Persistent volumes were preserved."
