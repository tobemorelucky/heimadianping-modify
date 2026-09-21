[CmdletBinding()]
param()

$ProjectRoot = Split-Path -Parent $PSScriptRoot
$ComposeFile = Join-Path $ProjectRoot "docker-compose.yml"
$RuntimeDirectory = Join-Path $ProjectRoot "target\runtime"

function Get-ManagedStatus {
    param(
        [Parameter(Mandatory = $true)][string]$Name,
        [Parameter(Mandatory = $true)][string]$PidFile
    )

    if (-not (Test-Path -LiteralPath $PidFile)) {
        return [PSCustomObject]@{ Service = $Name; Managed = $false; ProcessId = "-"; ProcessState = "not managed" }
    }
    $processIdText = (Get-Content -LiteralPath $PidFile -Raw).Trim()
    if ($processIdText -notmatch "^\d+$") {
        return [PSCustomObject]@{ Service = $Name; Managed = $true; ProcessId = $processIdText; ProcessState = "invalid PID file" }
    }
    $managedProcess = Get-Process -Id ([int]$processIdText) -ErrorAction SilentlyContinue
    $state = if ($null -eq $managedProcess) { "stopped" } else { "running" }
    return [PSCustomObject]@{ Service = $Name; Managed = $true; ProcessId = $processIdText; ProcessState = $state }
}

function Get-PortStatus {
    param(
        [Parameter(Mandatory = $true)][string]$Name,
        [Parameter(Mandatory = $true)][int]$Port
    )

    $listener = Get-NetTCPConnection -State Listen -LocalPort $Port -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($null -eq $listener) {
        return [PSCustomObject]@{ Service = $Name; Port = $Port; State = "closed"; OwningProcess = "-" }
    }
    return [PSCustomObject]@{ Service = $Name; Port = $Port; State = "listening"; OwningProcess = $listener.OwningProcess }
}

Write-Host "Docker services"
& docker compose -f $ComposeFile ps
if ($LASTEXITCODE -ne 0) {
    Write-Warning "Docker status is unavailable. Is Docker Desktop running?"
}

Write-Host "`nManaged application processes"
@(
    Get-ManagedStatus -Name "Nginx Frontend" -PidFile (Join-Path $RuntimeDirectory "nginx.pid")
    Get-ManagedStatus -Name "Spring Boot" -PidFile (Join-Path $RuntimeDirectory "spring-boot.pid")
    Get-ManagedStatus -Name "AI Assistant" -PidFile (Join-Path $RuntimeDirectory "ai-assistant.pid")
) | Format-Table -AutoSize

Write-Host "Application ports"
@(
    Get-PortStatus -Name "Nginx Frontend" -Port 8080
    Get-PortStatus -Name "Spring Boot" -Port 8081
    Get-PortStatus -Name "AI Assistant" -Port 8000
) | Format-Table -AutoSize
