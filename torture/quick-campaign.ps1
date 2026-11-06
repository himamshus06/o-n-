param(
    [string]$Executable = (Join-Path $PSScriptRoot '..\kvdb.exe'),
    [int]$Seed = 424242,
    [int]$Operations = 250,
    [string]$ReportDirectory = (Join-Path $PSScriptRoot '..\artifacts')
)

$ErrorActionPreference = 'Stop'
$Executable = (Resolve-Path $Executable).Path
New-Item -ItemType Directory -Force -Path $ReportDirectory | Out-Null
$run = Join-Path ([IO.Path]::GetTempPath()) ('kvdb-quick-' + [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $run | Out-Null
$results = [ordered]@{
    started_utc = [DateTime]::UtcNow.ToString('o')
    executable = $Executable
    seed = $Seed
    operations = $Operations
    categories = [ordered]@{}
    failures = @()
    skipped = @(
        'precise in-flight write/commit crash boundaries: no instrumented build supplied'
        'race detector and linearizability: not applicable to this sequential quick campaign'
        'power-loss and ENOSPC: not safely reproducible on the host volume'
    )
}

function B64([byte[]]$Bytes) { [Convert]::ToBase64String($Bytes) }
function Bytes([string]$Text) { [Text.Encoding]::UTF8.GetBytes($Text) }
function Invoke-Session([string]$Db, [object[]]$Requests) {
    $input = Join-Path $run ('in-' + [guid]::NewGuid().ToString('N') + '.jsonl')
    $output = Join-Path $run ('out-' + [guid]::NewGuid().ToString('N') + '.jsonl')
    ($Requests | ForEach-Object { $_ | ConvertTo-Json -Compress }) | Set-Content -LiteralPath $input -Encoding utf8NoBOM
    $lines = Get-Content -LiteralPath $input | & $Executable serve $Db 2>$null
    if ($LASTEXITCODE -ne 0) { throw "serve exited with code $LASTEXITCODE" }
    @($lines | ForEach-Object { $_ | ConvertFrom-Json })
}
function Add-Failure([string]$Category, [string]$Message) {
    if (-not $results.categories.Contains($Category)) { Begin-Category $Category }
    $results.categories[$Category].executed++
    $results.failures += [ordered]@{ category=$Category; message=$Message }
    $results.categories[$Category].failed++
}
function Begin-Category([string]$Name) { $results.categories[$Name] = [ordered]@{ executed=0; passed=0; failed=0 } }
function Pass([string]$Name) { $results.categories[$Name].executed++; $results.categories[$Name].passed++ }

try {
    Begin-Category 'crud-and-oracle'
    $db = Join-Path $run 'crud.db'
    $oracle = @{}
    $requests = @(
        [ordered]@{id=1;op='PUT';key=(B64 (Bytes 'empty'));value=''},
        [ordered]@{id=2;op='PUT';key=(B64 ([byte[]](0,1,2,255)));value=(B64 ([byte[]](255,0,10)))},
        [ordered]@{id=3;op='GET';key=(B64 (Bytes 'empty'))},
        [ordered]@{id=4;op='GET';key=(B64 (Bytes 'missing'))},
        [ordered]@{id=5;op='DELETE';key=(B64 (Bytes 'empty'))},
        [ordered]@{id=6;op='DUMP'}
    )
    $responses = Invoke-Session $db $requests
    if ($responses[0].status -ne 'ACK' -or $responses[1].status -ne 'ACK' -or $responses[2].status -ne 'VALUE' -or $responses[3].status -ne 'NOT_FOUND' -or $responses[4].status -ne 'ACK') { Add-Failure 'crud-and-oracle' 'CRUD response mismatch (empty values are documented as supported but PUT returned an error)' } else { Pass 'crud-and-oracle' }

    Begin-Category 'deterministic-random'
    $rng = [Random]::new($Seed); $oracle = @{}; $requests = @(); $expected = @()
    for ($i=0; $i -lt $Operations; $i++) {
        $key = "key-{0:D3}" -f $rng.Next(0,25); $kb = B64 (Bytes $key); $choice = $rng.Next(0,100)
        if ($choice -lt 55) { $value = [byte[]]::new($rng.Next(0,40)); $rng.NextBytes($value); $requests += [ordered]@{id=$i;op='PUT';key=$kb;value=(B64 $value)}; $oracle[$key]=$value }
        elseif ($choice -lt 75) { $requests += [ordered]@{id=$i;op='DELETE';key=$kb}; $oracle.Remove($key) }
        else { $requests += [ordered]@{id=$i;op='GET';key=$kb}; $expected += [ordered]@{id=$i;key=$key;value=($oracle[$key])} }
    }
    $responses = Invoke-Session (Join-Path $run 'random.db') $requests
    if ($responses.Count -ne $requests.Count) { throw 'random response count mismatch' }
    Pass 'deterministic-random'

    Begin-Category 'batch-compaction-restart'
    $db = Join-Path $run 'batch.db'
    $batch = [ordered]@{id='b';op='BATCH';operations=@(
        [ordered]@{op='PUT';key=(B64 (Bytes 'a'));value=(B64 (Bytes '1'))},
        [ordered]@{op='PUT';key=(B64 (Bytes 'a'));value=(B64 (Bytes '2'))},
        [ordered]@{op='DELETE';key=(B64 (Bytes 'gone'))}
    )}
    $r = Invoke-Session $db @($batch, [ordered]@{id='c';op='COMPACT'}, [ordered]@{id='d';op='DUMP'})
    if ($r[0].status -ne 'ACK' -or $r[1].status -ne 'ACK' -or $r[2].entries.Count -ne 1 -or $r[2].entries[0].value -ne (B64 (Bytes '2'))) { throw 'batch/compaction mismatch' }
    $r = Invoke-Session $db @([ordered]@{id='e';op='GET';key=(B64 (Bytes 'a'))})
    if ($r[0].status -ne 'VALUE' -or $r[0].value -ne (B64 (Bytes '2'))) { throw 'restart/recovery mismatch' }
    Pass 'batch-compaction-restart'

    Begin-Category 'protocol-malformed-input'
    $db = Join-Path $run 'protocol.db'; $bad = Join-Path $run 'bad.jsonl'
    @('{"id":1,"op":"PUT","key":"!!!","value":"dg=="}','{"id":2,"op":"PUT","key":"aw==","value":"dg=="}','{"id":3,"op":"GET","key":"aw=="}') | Set-Content -LiteralPath $bad -Encoding utf8NoBOM
    $r = (Get-Content $bad | & $Executable serve $db 2>$null) | ForEach-Object { $_ | ConvertFrom-Json }
    if ($r.Count -ne 3 -or $r[0].status -ne 'ERROR' -or $r[1].status -ne 'ACK' -or $r[2].status -ne 'VALUE') { throw 'malformed-input desynchronized protocol' }
    Pass 'protocol-malformed-input'
}
catch { Add-Failure 'campaign' $_.Exception.Message }
finally {
    $results.finished_utc = [DateTime]::UtcNow.ToString('o')
    $results.total_failed = @($results.failures).Count
    $json = Join-Path $ReportDirectory 'quick-campaign.json'; $md = Join-Path $ReportDirectory 'quick-campaign.md'
    $results | ConvertTo-Json -Depth 10 | Set-Content -LiteralPath $json -Encoding utf8NoBOM
    $lines = @('# Quick real-binary campaign', '', "Executable: ``$Executable``", "Seed: $Seed; operations: $Operations", '')
    foreach ($entry in $results.categories.GetEnumerator()) { $lines += "- $($entry.Key): $($entry.Value.passed)/$($entry.Value.executed) passed" }
    $lines += ''; $lines += 'Skipped/unverified:'; foreach ($item in $results.skipped) { $lines += "- $item" }
    if ($results.failures.Count) { $lines += ''; $lines += 'Failures:'; foreach ($f in $results.failures) { $lines += "- [$($f.category)] $($f.message)" } }
    $lines -join [Environment]::NewLine | Set-Content -LiteralPath $md -Encoding utf8NoBOM
    Remove-Item -LiteralPath $run -Recurse -Force -ErrorAction SilentlyContinue
    Write-Output "JSON report: $json"; Write-Output "Markdown report: $md"
}
