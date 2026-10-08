# Requires a running API, consumer, outbox, provider and webhook-sink.
# PowerShell 5.1/7: JSON is read from files to avoid native argument quoting issues.
[CmdletBinding()]
param(
    [string]$BaseUrl = "http://localhost:8000",
    [string]$ApiKey = "dev-api-key"
)

$ErrorActionPreference = "Stop"
$BaseUrl = $BaseUrl.TrimEnd('/')
$curlPath = (Get-Command "curl.exe" -CommandType Application -ErrorAction Stop).Source
$runDirectory = Join-Path ([IO.Path]::GetTempPath()) ("payment-api-smoke-" + [Guid]::NewGuid().ToString("N"))
$null = New-Item -ItemType Directory -Path $runDirectory
$responseFiles = New-Object 'System.Collections.Generic.List[string]'
$requestKey = [Guid]::NewGuid().ToString()
$exitCode = 0

function Invoke-ApiRequest {
    param(
        [string]$Method,
        [string]$Path,
        [string]$RequestFile = "",
        [double]$TimeoutSeconds = 10
    )

    $responseFile = Join-Path $runDirectory ([Guid]::NewGuid().ToString("N") + ".json")
    $responseFiles.Add($responseFile)
    $timeoutText = $TimeoutSeconds.ToString("0.000", [Globalization.CultureInfo]::InvariantCulture)
    $curlArguments = @(
        "--silent", "--show-error",
        "--max-time", $timeoutText,
        "--request", $Method,
        "--header", ("X-API-Key: " + $ApiKey),
        "--output", $responseFile,
        "--write-out", "%{http_code}",
        ($BaseUrl + $Path)
    )
    if ($RequestFile) {
        $curlArguments += @(
            "--header", ("Idempotency-Key: " + $requestKey),
            "--header", "Content-Type: application/json",
            "--data-binary", ("@" + $RequestFile)
        )
    }

    # Do not print the command or headers: ApiKey may be a real secret.
    $statusText = & $curlPath @curlArguments
    if ($LASTEXITCODE -ne 0) {
        throw "curl failed for $Method $Path (exit $LASTEXITCODE)."
    }
    $statusText = ([string]$statusText).Trim()
    if ($statusText -notmatch '^\d{3}$') {
        throw "curl returned an invalid HTTP status for $Method $Path."
    }
    $responseText = Get-Content -LiteralPath $responseFile -Raw -Encoding UTF8
    try {
        $body = ConvertFrom-Json -InputObject $responseText
    }
    catch {
        throw "HTTP $statusText from $Method $Path did not contain valid JSON."
    }
    return [pscustomobject]@{
        StatusCode = [int]$statusText
        Body = $body
        Text = $responseText
    }
}

function Assert-HttpStatus {
    param($Response, [int]$Expected, [string]$Step)
    if ($Response.StatusCode -ne $Expected) {
        throw "$Step expected HTTP $Expected; received HTTP $($Response.StatusCode)."
    }
}

try {
    Write-Host "Idempotency-Key: $requestKey"
    Write-Host "Case 1: create a payment and wait for its final status."
    $created = Invoke-ApiRequest -Method POST -Path '/api/v1/payments' -RequestFile (Join-Path $PSScriptRoot 'payment-create.json')
    Assert-HttpStatus $created 202 "Create"
    $paymentId = [string]$created.Body.payment_id
    if (-not $paymentId -or $created.Body.status -ne 'pending') {
        throw "Create must return payment_id and pending status."
    }
    Write-Host "Create: HTTP 202; payment_id=$paymentId; status=pending"
    Write-Host ("Create JSON: " + $created.Text)
    $paymentPath = '/api/v1/payments/' + $paymentId
    $pollWatch = [Diagnostics.Stopwatch]::StartNew()
    $payment = $null
    while ($pollWatch.Elapsed.TotalSeconds -lt 30) {
        $remaining = 30 - $pollWatch.Elapsed.TotalSeconds
        if ($remaining -le 0.01) { break }
        try {
            $payment = Invoke-ApiRequest -Method GET -Path $paymentPath -TimeoutSeconds ([Math]::Min(5, $remaining))
        }
        catch {
            if ($pollWatch.Elapsed.TotalSeconds -ge 30) { break }
            throw
        }
        Assert-HttpStatus $payment 200 "Poll payment"
        if ($payment.Body.payment_id -ne $paymentId) {
            throw "GET returned a different payment_id."
        }
        if ($payment.Body.status -in @('succeeded', 'failed')) { break }
        if ($payment.Body.status -ne 'pending') {
            throw "GET returned an unknown payment status."
        }
        $remaining = 30 - $pollWatch.Elapsed.TotalSeconds
        if ($remaining -gt 0) {
            Start-Sleep -Milliseconds ([int][Math]::Min(1000, $remaining * 1000))
        }
    }
    $pollWatch.Stop()

    if (-not $payment -or $payment.Body.status -notin @('succeeded', 'failed')) {
        Write-Warning "UNSETTLED: payment_id=$paymentId is still pending or could not be confirmed within 30 seconds. Inspect consumer/outbox/provider logs. No completed system test is claimed."
        $exitCode = 2
    }
    else {
        # The provider has a legitimate final decline outcome: failed is also final.
        $finalStatus = [string]$payment.Body.status
        if ([string]::IsNullOrWhiteSpace([string]$payment.Body.processed_at)) {
            throw "A final payment must include processed_at."
        }
        Write-Host "Processing: HTTP 200; status=$finalStatus"
        Write-Host "Case 2: equivalent replay, then a conflicting request with the same key."
        $replayed = Invoke-ApiRequest -Method POST -Path '/api/v1/payments' -RequestFile (Join-Path $PSScriptRoot 'payment-equivalent.json')
        Assert-HttpStatus $replayed 200 "Equivalent replay"
        if ($replayed.Body.payment_id -ne $paymentId -or $replayed.Body.status -ne $finalStatus) {
            throw "Equivalent replay changed payment_id or did not return the current final status."
        }
        if ($replayed.Body.created_at -ne $created.Body.created_at) {
            throw "Equivalent replay changed created_at."
        }
        Write-Host "Equivalent replay: HTTP 200; same payment_id; current status=$finalStatus"

        $conflict = Invoke-ApiRequest -Method POST -Path '/api/v1/payments' -RequestFile (Join-Path $PSScriptRoot 'payment-conflict.json')
        Assert-HttpStatus $conflict 409 "Changed request"
        Write-Host "Changed request: HTTP 409"
        $unchanged = Invoke-ApiRequest -Method GET -Path $paymentPath
        Assert-HttpStatus $unchanged 200 "Final GET"
        # API serializes details deterministically, so the complete JSON document
        # must match the final snapshot taken before the conflicting request.
        if ($unchanged.Text -cne $payment.Text) {
            throw "Final GET changed the saved payment after replay/conflict."
        }
        Write-Host "Final GET: HTTP 200; complete payment unchanged; status=$finalStatus"
        Write-Host ("Final GET JSON: " + $unchanged.Text)
        Write-Host "PASS: both cases completed; payment_id=$paymentId"
    }
}
catch {
    Write-Host ("FAIL: " + $_.Exception.Message) -ForegroundColor Red
    $exitCode = 1
}
finally {
    # Remove only files created by this invocation, without recursive deletion.
    foreach ($responseFile in $responseFiles) {
        if (Test-Path -LiteralPath $responseFile -PathType Leaf) {
            Remove-Item -LiteralPath $responseFile -Force -ErrorAction SilentlyContinue
        }
    }
    if (Test-Path -LiteralPath $runDirectory -PathType Container) {
        if (@(Get-ChildItem -LiteralPath $runDirectory -Force).Count -eq 0) {
            Remove-Item -LiteralPath $runDirectory -ErrorAction SilentlyContinue
        }
    }
}

exit $exitCode
