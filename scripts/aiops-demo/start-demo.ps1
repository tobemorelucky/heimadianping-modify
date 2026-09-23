[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$projectName = 'hmdp-aiops-demo'
$scriptRoot = Split-Path -Parent $PSCommandPath
$repositoryRoot = (Resolve-Path (Join-Path $scriptRoot '..\..')).Path
$composeFile = Join-Path $repositoryRoot 'docker-compose.aiops-demo.yml'
$topics = @(
    'hmdp.aiops.mysql-demo.order.main.v1',
    'hmdp.aiops.mysql-demo.order.retry.v1',
    'hmdp.aiops.mysql-demo.order.dlt.v1'
)
$mysqlPort = if ($env:AIOPS_DEMO_MYSQL_PORT) { $env:AIOPS_DEMO_MYSQL_PORT } else { '13307' }
$consumerMysqlProxyPort = if ($env:AIOPS_DEMO_CONSUMER_MYSQL_PROXY_PORT) { $env:AIOPS_DEMO_CONSUMER_MYSQL_PROXY_PORT } else { '13308' }
$redisPort = if ($env:AIOPS_DEMO_REDIS_PORT) { $env:AIOPS_DEMO_REDIS_PORT } else { '16379' }
$kafkaPort = if ($env:AIOPS_DEMO_KAFKA_PORT) { $env:AIOPS_DEMO_KAFKA_PORT } else { '29092' }

if (-not (Test-Path -LiteralPath $composeFile -PathType Leaf)) {
    throw "Demo Compose file not found: $composeFile"
}

Push-Location $repositoryRoot
try {
    docker compose --project-name $projectName --file $composeFile config --quiet
    if ($LASTEXITCODE -ne 0) {
        throw 'Demo Compose validation failed.'
    }

    docker compose --project-name $projectName --file $composeFile up --detach --wait
    if ($LASTEXITCODE -ne 0) {
        throw 'Demo services did not start successfully.'
    }

    foreach ($topic in $topics) {
        docker compose --project-name $projectName --file $composeFile exec --no-TTY kafka-demo `
            /opt/kafka/bin/kafka-topics.sh `
            --bootstrap-server kafka-demo:19092 `
            --create --if-not-exists `
            --topic $topic `
            --partitions 1 `
            --replication-factor 1
        if ($LASTEXITCODE -ne 0) {
            throw "Unable to create demo Kafka topic: $topic"
        }
    }

    docker compose --project-name $projectName --file $composeFile ps
    Write-Host ''
    Write-Host 'AIOps demo dependencies are ready.'
    Write-Host "MySQL direct (Web):       127.0.0.1:$mysqlPort"
    Write-Host "MySQL Consumer proxy:     127.0.0.1:$consumerMysqlProxyPort"
    Write-Host "Redis:  127.0.0.1:$redisPort"
    Write-Host "Kafka:  127.0.0.1:$kafkaPort"
    Write-Host 'Consumer group: hmdp-aiops-mysql-demo-consumer-v1'
    Write-Host 'No fault has been injected.'
}
finally {
    Pop-Location
}
