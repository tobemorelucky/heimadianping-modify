Set-StrictMode -Version Latest

$script:RuntimeScriptRoot = $PSScriptRoot
$script:RepositoryRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..\..')).Path
$script:RuntimeRoot = Join-Path $script:RepositoryRoot 'target\runtime\aiops-demo-runtime'
$script:StatePath = Join-Path $script:RuntimeRoot 'state.json'
$script:JarPath = Join-Path $script:RepositoryRoot 'target\hm-dianping-0.0.1-SNAPSHOT.jar'
$script:AgentRoot = Join-Path $script:RepositoryRoot 'aiops-agent'
$script:ConsoleRoot = Join-Path $script:RepositoryRoot 'aiops-console'
$script:PythonPath = Join-Path $script:AgentRoot '.venv\Scripts\python.exe'

function Initialize-DemoRuntimeDirectory {
    New-Item -ItemType Directory -Force -Path $script:RuntimeRoot | Out-Null
}

function Get-DemoState {
    if (-not (Test-Path -LiteralPath $script:StatePath -PathType Leaf)) {
        return $null
    }
    return Get-Content -LiteralPath $script:StatePath -Raw -Encoding UTF8 | ConvertFrom-Json
}

function Save-DemoState {
    param([Parameter(Mandatory)]$State)

    Initialize-DemoRuntimeDirectory
    $temporaryPath = "$($script:StatePath).tmp"
    $State | ConvertTo-Json -Depth 12 | Set-Content -LiteralPath $temporaryPath -Encoding UTF8
    Move-Item -LiteralPath $temporaryPath -Destination $script:StatePath -Force
}

function Get-ProcessRecord {
    param(
        [Parameter(Mandatory)]$State,
        [Parameter(Mandatory)][string]$Role
    )
    return $State.processes | Where-Object { $_.role -eq $Role } | Select-Object -First 1
}

function Test-ProcessRecordRunning {
    param($Record)

    if ($null -eq $Record -or $null -eq $Record.pid) {
        return $false
    }
    $process = Get-Process -Id ([int]$Record.pid) -ErrorAction SilentlyContinue
    if ($null -eq $process) { return $false }
    try {
        $expected = [DateTimeOffset]::Parse([string]$Record.start_time).UtcDateTime
        $actual = $process.StartTime.ToUniversalTime()
        return [Math]::Abs(($actual - $expected).TotalSeconds) -lt 2
    }
    catch {
        return $false
    }
}

function Get-PortOwnerPid {
    param([Parameter(Mandatory)][int]$Port)

    $connection = Get-NetTCPConnection -LocalAddress 127.0.0.1 -LocalPort $Port -State Listen `
        -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($null -eq $connection) {
        $connection = Get-NetTCPConnection -LocalPort $Port -State Listen `
            -ErrorAction SilentlyContinue | Select-Object -First 1
    }
    if ($null -eq $connection) { return $null }
    return [int]$connection.OwningProcess
}

function Assert-PortAvailable {
    param([Parameter(Mandatory)][int]$Port)

    $owner = Get-PortOwnerPid -Port $Port
    if ($null -ne $owner) {
        throw "Required local port $Port is already owned by PID $owner. Stop the conflicting process first."
    }
}

function Start-LoggedProcess {
    param(
        [Parameter(Mandatory)][string]$Role,
        [Parameter(Mandatory)][string]$FilePath,
        [Parameter(Mandatory)][string[]]$ArgumentList,
        [Parameter(Mandatory)][string]$WorkingDirectory,
        [Parameter(Mandatory)][string]$LogDirectory,
        [int]$ListenerPort = 0
    )

    $stdout = Join-Path $LogDirectory "$Role.out.log"
    $stderr = Join-Path $LogDirectory "$Role.error.log"
    $process = Start-Process -FilePath $FilePath -ArgumentList $ArgumentList `
        -WorkingDirectory $WorkingDirectory -WindowStyle Hidden -PassThru `
        -RedirectStandardOutput $stdout -RedirectStandardError $stderr

    return [pscustomobject]@{
        role = $Role
        pid = $process.Id
        listener_pid = $null
        listener_port = $ListenerPort
        process_name = $process.ProcessName
        start_time = $process.StartTime.ToUniversalTime().ToString('o')
        working_directory = $WorkingDirectory
        executable = $FilePath
        arguments = @($ArgumentList)
        stdout = $stdout
        stderr = $stderr
        status = 'running'
    }
}

function Set-ProcessListenerPid {
    param(
        [Parameter(Mandatory)]$Record,
        [Parameter(Mandatory)][int]$Port
    )
    $owner = Get-PortOwnerPid -Port $Port
    if ($null -ne $owner) {
        $Record.listener_pid = $owner
    }
}

function Stop-ProcessRecord {
    param($Record)

    if ($null -eq $Record) { return }
    $ids = @()
    if (Test-ProcessRecordRunning $Record) {
        $ids += [int]$Record.pid
    }
    if ($Record.listener_port -gt 0 -and $null -ne $Record.listener_pid) {
        $currentOwner = Get-PortOwnerPid -Port ([int]$Record.listener_port)
        if ($currentOwner -eq [int]$Record.listener_pid) {
            $ids += $currentOwner
        }
    }
    $ids = @($ids | Select-Object -Unique)
    foreach ($processId in $ids) {
        $process = Get-Process -Id $processId -ErrorAction SilentlyContinue
        if ($null -ne $process) {
            Stop-Process -Id $processId -ErrorAction SilentlyContinue
            try { Wait-Process -Id $processId -Timeout 8 -ErrorAction Stop } catch {
                Stop-Process -Id $processId -Force -ErrorAction SilentlyContinue
            }
        }
    }
    $Record.status = 'stopped'
    if ($Record.PSObject.Properties.Name -contains 'stopped_at') {
        $Record.stopped_at = (Get-Date).ToUniversalTime().ToString('o')
    }
    else {
        $Record | Add-Member -NotePropertyName stopped_at `
            -NotePropertyValue ((Get-Date).ToUniversalTime().ToString('o'))
    }
}

function Wait-HttpReady {
    param(
        [Parameter(Mandatory)][string]$Uri,
        [Parameter(Mandatory)][string]$Name,
        [int]$TimeoutSeconds = 120
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    do {
        try {
            $response = Invoke-WebRequest -Uri $Uri -UseBasicParsing -TimeoutSec 3
            if ($response.StatusCode -ge 200 -and $response.StatusCode -lt 500) {
                return
            }
        }
        catch { }
        Start-Sleep -Seconds 1
    } while ((Get-Date) -lt $deadline)
    throw "$Name did not become ready within $TimeoutSeconds seconds: $Uri"
}

function Wait-DockerHealth {
    param(
        [Parameter(Mandatory)][string]$Container,
        [Parameter(Mandatory)][string]$Name,
        [int]$TimeoutSeconds = 120
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    do {
        $status = docker inspect --format '{{.State.Status}}/{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' $Container 2>$null
        if ($LASTEXITCODE -eq 0 -and $status -eq 'running/healthy') { return }
        Start-Sleep -Seconds 2
    } while ((Get-Date) -lt $deadline)
    throw "$Name did not become healthy within $TimeoutSeconds seconds."
}

function Wait-KafkaReady {
    param([int]$TimeoutSeconds = 120)

    $compose = Join-Path $script:RepositoryRoot 'docker-compose.aiops-demo.yml'
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    do {
        docker compose --project-name hmdp-aiops-demo --file $compose exec --no-TTY kafka-demo `
            /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-demo:19092 --list 1>$null 2>$null
        if ($LASTEXITCODE -eq 0) { return }
        Start-Sleep -Seconds 2
    } while ((Get-Date) -lt $deadline)
    throw 'Kafka did not become ready within the configured timeout.'
}

function Start-DemoConsumerProcess {
    param(
        [Parameter(Mandatory)][string]$LogDirectory,
        [string]$Role = 'consumer'
    )

    $arguments = @(
        '--add-opens=java.base/java.lang.invoke=ALL-UNNAMED',
        '-jar',
        $script:JarPath,
        '--spring.profiles.active=consumer,consumer-aiops-demo'
    )
    return Start-LoggedProcess -Role $Role -FilePath 'java.exe' `
        -ArgumentList $arguments -WorkingDirectory $script:RepositoryRoot `
        -LogDirectory $LogDirectory -ListenerPort 18082
}

function Set-StateFault {
    param(
        [Parameter(Mandatory)]$State,
        [Parameter(Mandatory)][string]$Name,
        [Parameter(Mandatory)][string]$Status,
        [Parameter(Mandatory)][string]$Timestamp
    )

    if ($null -eq $State.faults) {
        $State | Add-Member -NotePropertyName faults -NotePropertyValue ([pscustomobject]@{}) -Force
    }
    $value = [pscustomobject]@{ status = $Status; timestamp = $Timestamp }
    $State.faults | Add-Member -NotePropertyName $Name -NotePropertyValue $value -Force
}
