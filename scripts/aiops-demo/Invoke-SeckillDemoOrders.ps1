[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)][string]$Phase,
    [Parameter(Mandatory = $true)][string[]]$Phones,
    [long]$VoucherId = 0,
    [int]$Stock = 10,
    [string]$BaseUrl = 'http://127.0.0.1:8081',
    [ValidateSet('default', 'aiops-demo')][string]$Infrastructure = 'default',
    [switch]$IUnderstandLocalTestData
)

$ErrorActionPreference = 'Stop'
if (-not $IUnderstandLocalTestData) {
    throw 'This script creates local test users/orders. Pass -IUnderstandLocalTestData explicitly.'
}
$base = [uri]$BaseUrl
if ($base.Scheme -ne 'http' -or $base.Host -notin @('127.0.0.1', 'localhost') -or $base.Port -ne 8081) {
    throw 'The demo HTTP target must be http://127.0.0.1:8081 or http://localhost:8081.'
}
if ($Phones.Count -lt 1 -or $Phones.Count -gt 10 -or (@($Phones | Select-Object -Unique)).Count -ne $Phones.Count) {
    throw 'Provide 1-10 distinct test phone numbers.'
}
foreach ($phone in $Phones) {
    if ($phone -notmatch '^1([38][0-9]|4[579]|5[0-3,5-9]|66|7[0135678]|9[89])\d{8}$') {
        throw 'A test phone number does not match the HMDP login format.'
    }
}
if ($VoucherId -eq 0 -and $Stock -lt $Phones.Count) {
    throw 'New test voucher stock must cover all requested test orders.'
}

$projectRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$baseUrl = $BaseUrl.TrimEnd('/')
$composeFile = if ($Infrastructure -eq 'aiops-demo') {
    Join-Path $projectRoot 'docker-compose.aiops-demo.yml'
} else {
    Join-Path $projectRoot 'docker-compose.yml'
}
$composeProject = if ($Infrastructure -eq 'aiops-demo') {
    'hmdp-aiops-demo'
} else {
    'hmdp-local'
}
$redisService = if ($Infrastructure -eq 'aiops-demo') { 'redis-demo' } else { 'redis' }

function Assert-HmdpSuccess {
    param([Parameter(Mandatory = $true)]$Response, [Parameter(Mandatory = $true)][string]$Stage)
    if ($null -eq $Response -or $Response.success -ne $true) {
        throw "$Stage failed: $($Response.errorMsg)"
    }
}

if ($VoucherId -eq 0) {
    $now = Get-Date
    $voucher = @{
        shopId = 1
        title = 'AIOPS_P22_' + $now.ToString('yyyyMMdd_HHmmss')
        subTitle = 'isolated local fault demo'
        rules = 'Local test data only'
        payValue = 1
        actualValue = 2
        type = 1
        status = 1
        stock = $Stock
        beginTime = $now.AddMinutes(-5).ToString('yyyy-MM-ddTHH:mm:ss')
        endTime = $now.AddHours(2).ToString('yyyy-MM-ddTHH:mm:ss')
    }
    $created = Invoke-RestMethod -Method Post -Uri "$baseUrl/voucher/seckill" -ContentType 'application/json' -Body ($voucher | ConvertTo-Json -Compress)
    Assert-HmdpSuccess -Response $created -Stage 'Create test voucher'
    $VoucherId = [long]$created.data
}

$orders = @()
Push-Location $projectRoot
try {
    foreach ($phone in $Phones) {
        $sent = Invoke-RestMethod -Method Post -Uri "$baseUrl/user/code?phone=$phone"
        Assert-HmdpSuccess -Response $sent -Stage 'Request local login code'

        # The code is read only to complete the normal HTTP login. Never print it or the token.
        $codeLines = & docker compose --project-name $composeProject --file $composeFile `
            exec --no-TTY $redisService redis-cli --raw GET "login:code:$phone" 2>$null
        if ($LASTEXITCODE -ne 0) {
            throw 'Could not read the local demo login code from the Docker Redis container.'
        }
        $code = [string](@($codeLines | Select-Object -Last 1)[0])
        $code = $code.Trim()
        if ($code -notmatch '^\d{6}$') {
            throw 'Local demo login code was unavailable or malformed.'
        }
        $loginBody = @{ phone = $phone; code = $code } | ConvertTo-Json -Compress
        $login = Invoke-RestMethod -Method Post -Uri "$baseUrl/user/login" -ContentType 'application/json' -Body $loginBody
        Assert-HmdpSuccess -Response $login -Stage 'HTTP login'
        $token = [string]$login.data
        if ([string]::IsNullOrWhiteSpace($token)) {
            throw 'HTTP login returned no token.'
        }

        $accepted = Invoke-RestMethod -Method Post -Uri "$baseUrl/voucher-order/seckill/$VoucherId" -Headers @{ authorization = $token }
        Assert-HmdpSuccess -Response $accepted -Stage 'HTTP seckill request'
        $orders += [long]$accepted.data
        $code = $null
        $token = $null
    }
} finally {
    Pop-Location
}

[pscustomobject]@{
    phase = $Phase
    voucher_id = $VoucherId
    attempted = $Phones.Count
    accepted = $orders.Count
    order_ids = $orders
    observed_at = (Get-Date).ToString('o')
} | ConvertTo-Json -Compress -Depth 4
