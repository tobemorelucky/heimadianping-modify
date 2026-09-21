[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent $PSScriptRoot
$ComposeFile = Join-Path $ProjectRoot "docker-compose.yml"
$RuntimeDirectory = Join-Path $ProjectRoot "target\runtime"
$SpringPidFile = Join-Path $RuntimeDirectory "spring-boot.pid"
$AiPidFile = Join-Path $RuntimeDirectory "ai-assistant.pid"
$SpringOutLog = Join-Path $RuntimeDirectory "spring-boot.out.log"
$SpringErrorLog = Join-Path $RuntimeDirectory "spring-boot.error.log"
$AiOutLog = Join-Path $RuntimeDirectory "ai-assistant.out.log"
$AiErrorLog = Join-Path $RuntimeDirectory "ai-assistant.error.log"
$MySqlPortFile = Join-Path $RuntimeDirectory "mysql-host-port.txt"
$NginxRoot = Join-Path $ProjectRoot "nginx-1.18.0"
$NginxExe = Join-Path $NginxRoot "nginx.exe"
$NginxPidFile = Join-Path $RuntimeDirectory "nginx.pid"
$NginxNativePidFile = Join-Path $NginxRoot "logs\nginx.pid"
$AiRoot = Join-Path $ProjectRoot "ai-assistant"
$AiEnvFile = Join-Path $AiRoot ".env"
$AiPython = Join-Path $AiRoot ".venv\Scripts\python.exe"
$LegacyComposeFiles = @(
    (Join-Path $ProjectRoot "docker\kafka\docker-compose-kafka.yml"),
    (Join-Path $ProjectRoot "docker\elasticsearch\docker-compose-es.yml"),
    (Join-Path $ProjectRoot "docker\canal\docker-compose-canal.yml")
)

function Test-CommandAvailable {
    param([Parameter(Mandatory = $true)][string]$Name)
    return $null -ne (Get-Command $Name -ErrorAction SilentlyContinue)
}

function Get-ManagedProcess {
    param([Parameter(Mandatory = $true)][string]$PidFile)

    if (-not (Test-Path -LiteralPath $PidFile)) {
        return $null
    }
    $processIdText = (Get-Content -LiteralPath $PidFile -Raw).Trim()
    if ($processIdText -notmatch "^\d+$") {
        return $null
    }
    return Get-Process -Id ([int]$processIdText) -ErrorAction SilentlyContinue
}

function Wait-ContainerHealthy {
    param(
        [Parameter(Mandatory = $true)][string]$ContainerName,
        [int]$TimeoutSeconds = 120
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        $health = & docker inspect --format "{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}" $ContainerName 2>$null
        if ($LASTEXITCODE -eq 0 -and $health -eq "healthy") {
            return
        }
        if ($health -eq "unhealthy" -or $health -eq "exited") {
            throw "Container $ContainerName entered state: $health"
        }
        Start-Sleep -Seconds 2
    }
    throw "Timed out waiting for container $ContainerName to become healthy."
}

function Wait-TcpPort {
    param(
        [Parameter(Mandatory = $true)][int]$Port,
        [int]$TimeoutSeconds = 120
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        $listener = Get-NetTCPConnection -State Listen -LocalPort $Port -ErrorAction SilentlyContinue
        if ($null -ne $listener) {
            return
        }
        Start-Sleep -Seconds 2
    }
    throw "Timed out waiting for local port $Port."
}

if (-not (Test-CommandAvailable "docker")) {
    throw "Docker CLI was not found. Install and start Docker Desktop first."
}
if (-not (Test-CommandAvailable "mvn.cmd") -and -not (Test-CommandAvailable "mvn")) {
    throw "Maven was not found in PATH."
}
if (-not (Test-Path -LiteralPath $AiEnvFile)) {
    throw "Missing ai-assistant/.env. Copy .env.example and configure the LLM before starting."
}
if (-not (Test-Path -LiteralPath $NginxExe)) {
    throw "Missing frontend server: $NginxExe"
}

New-Item -ItemType Directory -Path $RuntimeDirectory -Force | Out-Null

# Keep existing user-provided values; defaults match the local Spring Boot configuration.
if ([string]::IsNullOrWhiteSpace($env:MYSQL_ROOT_PASSWORD)) {
    $env:MYSQL_ROOT_PASSWORD = "123456"
}
# If a native MySQL already owns 3306, place the managed container on 3307.
if ([string]::IsNullOrWhiteSpace($env:MYSQL_HOST_PORT)) {
    $savedMySqlPort = if (Test-Path -LiteralPath $MySqlPortFile) {
        (Get-Content -LiteralPath $MySqlPortFile -Raw).Trim()
    } else {
        ""
    }
    if ($savedMySqlPort -match "^\d+$") {
        $env:MYSQL_HOST_PORT = $savedMySqlPort
    } else {
        $port3306Listener = Get-NetTCPConnection -State Listen -LocalPort 3306 -ErrorAction SilentlyContinue
        if ($null -ne $port3306Listener) {
            $env:MYSQL_HOST_PORT = "3307"
            Write-Warning "Port 3306 is occupied; using 3307 for the managed MySQL container."
        } else {
            $env:MYSQL_HOST_PORT = "3306"
        }
        Set-Content -LiteralPath $MySqlPortFile -Value $env:MYSQL_HOST_PORT
    }
}
if ([string]::IsNullOrWhiteSpace($env:CANAL_DB_USERNAME)) {
    $env:CANAL_DB_USERNAME = "root"
}
if ([string]::IsNullOrWhiteSpace($env:CANAL_DB_PASSWORD)) {
    $env:CANAL_DB_PASSWORD = $env:MYSQL_ROOT_PASSWORD
}
if ([string]::IsNullOrWhiteSpace($env:SPRING_DATASOURCE_PASSWORD)) {
    $env:SPRING_DATASOURCE_PASSWORD = $env:MYSQL_ROOT_PASSWORD
}
$env:CANAL_DB_PORT = $env:MYSQL_HOST_PORT
if ([string]::IsNullOrWhiteSpace($env:SPRING_DATASOURCE_URL)) {
    $env:SPRING_DATASOURCE_URL = "jdbc:mysql://127.0.0.1:$($env:MYSQL_HOST_PORT)/hmdp?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true"
}

Write-Host "[1/7] Migrating legacy component projects without deleting volumes..."
foreach ($legacyComposeFile in $LegacyComposeFiles) {
    & docker compose -f $legacyComposeFile down --remove-orphans
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to stop legacy Compose project: $legacyComposeFile"
    }
}

Write-Host "[2/7] Starting MySQL and Redis..."
& docker compose -f $ComposeFile up -d mysql redis
if ($LASTEXITCODE -ne 0) {
    throw "Failed to start MySQL and Redis."
}
Wait-ContainerHealthy -ContainerName "hmdp-mysql"
Wait-ContainerHealthy -ContainerName "hmdp-redis"

Write-Host "[3/7] Starting Kafka, Elasticsearch, Kibana and Canal..."
& docker compose -f $ComposeFile up -d
if ($LASTEXITCODE -ne 0) {
    throw "Failed to start the complete Docker environment."
}

Write-Host "[4/7] Starting Spring Boot..."
$springProcess = Get-ManagedProcess -PidFile $SpringPidFile
if ($null -eq $springProcess) {
    $springPortOwner = Get-NetTCPConnection -State Listen -LocalPort 8081 -ErrorAction SilentlyContinue
    if ($null -ne $springPortOwner) {
        throw "Port 8081 is already occupied by an unmanaged process. Stop it or manage Spring Boot manually."
    }
    $mavenCommand = Get-Command "mvn.cmd" -ErrorAction SilentlyContinue
    if ($null -eq $mavenCommand) {
        $mavenCommand = Get-Command "mvn"
    }
    $springProcess = Start-Process `
        -FilePath $mavenCommand.Source `
        -ArgumentList "spring-boot:run" `
        -WorkingDirectory $ProjectRoot `
        -RedirectStandardOutput $SpringOutLog `
        -RedirectStandardError $SpringErrorLog `
        -WindowStyle Hidden `
        -PassThru
    Set-Content -LiteralPath $SpringPidFile -Value $springProcess.Id
} else {
    Write-Host "Spring Boot is already managed with PID $($springProcess.Id)."
}
Wait-TcpPort -Port 8081

Write-Host "[5/7] Starting Nginx frontend..."
$nginxPortOwner = Get-NetTCPConnection -State Listen -LocalPort 8080 -ErrorAction SilentlyContinue | Select-Object -First 1
if ($null -eq $nginxPortOwner) {
    # Validate the bundled Nginx configuration before launching the static frontend.
    Push-Location $NginxRoot
    try {
        & $NginxExe -t
        if ($LASTEXITCODE -ne 0) {
            throw "Nginx configuration validation failed."
        }
        $nginxProcess = Start-Process `
            -FilePath $NginxExe `
            -WorkingDirectory $NginxRoot `
            -WindowStyle Hidden `
            -PassThru
    } finally {
        Pop-Location
    }
    Wait-TcpPort -Port 8080

    # Prefer Nginx's own master PID; retain the launched PID as a safe fallback.
    $nginxProcessId = $nginxProcess.Id
    if (Test-Path -LiteralPath $NginxNativePidFile) {
        $nativePid = (Get-Content -LiteralPath $NginxNativePidFile -Raw).Trim()
        if ($nativePid -match "^\d+$") {
            $nginxProcessId = $nativePid
        }
    }
    Set-Content -LiteralPath $NginxPidFile -Value $nginxProcessId
} else {
    $existingNginx = Get-Process -Id $nginxPortOwner.OwningProcess -ErrorAction SilentlyContinue
    if ($null -eq $existingNginx -or $existingNginx.ProcessName -ne "nginx") {
        throw "Port 8080 is already occupied by a non-Nginx process. Stop it before starting the frontend."
    }
    Set-Content -LiteralPath $NginxPidFile -Value $nginxPortOwner.OwningProcess
    Write-Host "Nginx frontend is already listening on port 8080."
}

Write-Host "[6/7] Preparing and starting the AI service..."
if (-not (Test-Path -LiteralPath $AiPython)) {
    $pythonCommand = Get-Command "python" -ErrorAction SilentlyContinue
    if ($null -eq $pythonCommand) {
        throw "Python was not found in PATH and ai-assistant/.venv does not exist."
    }
    & $pythonCommand.Source -m venv (Join-Path $AiRoot ".venv")
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to create the AI virtual environment."
    }
    & $AiPython -m pip install -r (Join-Path $AiRoot "requirements.txt")
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to install AI service dependencies."
    }
}

$aiProcess = Get-ManagedProcess -PidFile $AiPidFile
if ($null -eq $aiProcess) {
    $aiPortOwner = Get-NetTCPConnection -State Listen -LocalPort 8000 -ErrorAction SilentlyContinue
    if ($null -ne $aiPortOwner) {
        throw "Port 8000 is already occupied by an unmanaged process. Stop it or manage FastAPI manually."
    }
    $aiProcess = Start-Process `
        -FilePath $AiPython `
        -ArgumentList "-m", "uvicorn", "app.main:app", "--host", "127.0.0.1", "--port", "8000" `
        -WorkingDirectory $AiRoot `
        -RedirectStandardOutput $AiOutLog `
        -RedirectStandardError $AiErrorLog `
        -WindowStyle Hidden `
        -PassThru
    Set-Content -LiteralPath $AiPidFile -Value $aiProcess.Id
} else {
    Write-Host "AI service is already managed with PID $($aiProcess.Id)."
}
Wait-TcpPort -Port 8000

Write-Host "[7/7] HM-DianPing Plus is ready."
Write-Host "Frontend: http://127.0.0.1:8080"
Write-Host "Spring Boot: http://127.0.0.1:8081"
Write-Host "AI Assistant: http://127.0.0.1:8000/docs"
Write-Host "Kibana: http://127.0.0.1:5601"
Write-Host "Run scripts/status.ps1 for complete status."
