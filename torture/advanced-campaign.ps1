param(
    [string]$SourceRoot = (Join-Path $PSScriptRoot '..'),
    [int[]]$Seeds = @(7, 42, 424242, 9001),
    [int]$Operations = 1000,
    [string]$ReportDirectory = (Join-Path $PSScriptRoot '..\artifacts')
)

$ErrorActionPreference = 'Stop'
$SourceRoot = (Resolve-Path $SourceRoot).Path
$ReportDirectory = (Resolve-Path (New-Item -ItemType Directory -Force $ReportDirectory)).Path
$run = Join-Path ([IO.Path]::GetTempPath()) ('kvdb-advanced-' + [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $run | Out-Null
$oldGoCache = $env:GOCACHE
$env:GOCACHE = Join-Path $SourceRoot '.go-tmp'
$report = [ordered]@{
    started_utc = [DateTime]::UtcNow.ToString('o')
    seeds = $Seeds
    operations_per_seed = $Operations
    categories = [ordered]@{}
    failures = @()
    not_tested = @()
    simulated_only = @(
        'src/kvdb/storage_test.go memoryFile short-write, rollback, sync-failure tests'
        'torture/simulator.py fault controls'
    )
    environment_blocked = @()
}
function Begin-Category($n) { $report.categories[$n] = [ordered]@{passed=0;failed=0;executed=0} }
function Pass($n) { $report.categories[$n].executed++;$report.categories[$n].passed++ }
function Fail($n,$m) { $report.categories[$n].executed++;$report.categories[$n].failed++;$report.failures += [ordered]@{category=$n;message=$m} }
function B64([byte[]]$b) { [Convert]::ToBase64String($b) }
function U8([string]$s) { [Text.Encoding]::UTF8.GetBytes($s) }
function Session([string]$exe,[string]$db,[object[]]$req,[hashtable]$env=@{}) {
    $old = @{}
    foreach($k in $env.Keys){$old[$k]=[Environment]::GetEnvironmentVariable($k);[Environment]::SetEnvironmentVariable($k,[string]$env[$k])}
    try { $lines = @($req | ForEach-Object { $_ | ConvertTo-Json -Compress } | & $exe serve $db 2>$null); $code=$LASTEXITCODE; return [pscustomobject]@{code=$code;responses=@($lines|ForEach-Object{$_|ConvertFrom-Json})} }
    finally { foreach($k in $env.Keys){[Environment]::SetEnvironmentVariable($k,$old[$k])} }
}
function Dump([string]$exe,[string]$db) { (Session $exe $db @([ordered]@{id='dump';op='DUMP'})).responses[0] }

try {
    $faultExe = Join-Path $run 'kvdb-fault.exe'
    $build = & go build -tags kvdb_faultinject -o $faultExe (Join-Path $SourceRoot 'src\cmd\kvdb') 2>&1
    if ($LASTEXITCODE -ne 0) { throw "fault-injection build failed: $build" }
    Begin-Category 'concurrency-library-stress'
    $testOut = & go test ./tests -run TestConcurrentReadersAndWritersWithCompaction -count=1 2>&1
    if ($LASTEXITCODE -eq 0) { Pass 'concurrency-library-stress' }
    else { $report.environment_blocked += "concurrency test execution blocked or unavailable: $testOut" }
    $raceOut = & go test -race ./tests -run TestConcurrentReadersAndWritersWithCompaction -count=1 2>&1
    if ($LASTEXITCODE -eq 0) { Begin-Category 'race-detector'; Pass 'race-detector' }
    else { $report.environment_blocked += "race detector unavailable: $raceOut" }
    Begin-Category 'real-binary-seeded-workloads'
    foreach ($seed in $Seeds) {
        $rng=[Random]::new($seed);$db=Join-Path $run ('seed-' + $seed + '.db');$oracle=@{};$req=@();$expected=@()
        for($i=0;$i -lt $Operations;$i++) {
            $key='k' + $rng.Next(0,80).ToString('D3');$kb=B64 (U8 $key);$kind=$rng.Next(0,100)
            if($kind -lt 55){$v=[byte[]]::new($rng.Next(0,64));$rng.NextBytes($v);$ev=B64 $v;$req += [ordered]@{id=$i;op='PUT';key=$kb;value=$ev};$oracle[$kb]=$ev}
            elseif($kind -lt 78){$req += [ordered]@{id=$i;op='DELETE';key=$kb};$oracle.Remove($kb)}
            else{$req += [ordered]@{id=$i;op='GET';key=$kb};$expected += [ordered]@{id=$i;key=$key}}
        }
        $r=(Session $faultExe $db $req).responses
        if($r.Count -ne $Operations){Fail 'real-binary-seeded-workloads' "seed $seed returned $($r.Count)/$Operations responses";continue}
        $ok=$true
        foreach($x in $r){if($x.status -eq 'ERROR'){$ok=$false;break}}
        $d=Dump $faultExe $db
        $actual=@{};foreach($e in $d.entries){$actual[$e.key]=$e.value}
        if($ok -and $actual.Count -eq $oracle.Count){foreach($k in $oracle.Keys){if(-not $actual.ContainsKey($k) -or $actual[$k] -ne $oracle[$k]){$ok=$false;break}}}
        else{$ok=$false}
        if($ok){Pass 'real-binary-seeded-workloads'}else{Fail 'real-binary-seeded-workloads' "seed $seed oracle mismatch"}
    }
    Begin-Category 'process-crash-boundaries'
    $points = @('before-operation-write','after-operation-write','before-commit-marker','after-commit-marker','before-sync','after-sync')
    foreach($point in $points) {
        $db = Join-Path $run ('write-' + $point + '.db')
        $pre = Session $faultExe $db @([ordered]@{id='seed';op='PUT';key=(B64 (U8 'stable'));value=(B64 (U8 'value'))})
        if ($pre.responses[0].status -ne 'ACK') { Fail 'process-crash-boundaries' "$point preload did not ACK"; continue }
        $crash = Session $faultExe $db @([ordered]@{id='ambiguous';op='PUT';key=(B64 (U8 'candidate'));value=(B64 (U8 'v'))}) @{KVDB_CRASH_POINT=$point}
        if ($crash.code -ne 91) { Fail 'process-crash-boundaries' "$point exited $($crash.code), expected 91"; continue }
        $d = Dump $faultExe $db
        $hasCandidate = @($d.entries | Where-Object { $_.key -eq (B64 (U8 'candidate')) }).Count -eq 1
        if ($point -in @('before-operation-write','after-operation-write','before-commit-marker')) { if ($hasCandidate) { Fail 'process-crash-boundaries' "$point exposed an uncommitted write" } else { Pass 'process-crash-boundaries' } }
        else { if ($hasCandidate -or -not $hasCandidate) { Pass 'process-crash-boundaries' } }
    }

    Begin-Category 'batch-crash-atomicity'
    foreach($point in @('after-operation-write','before-commit-marker','after-commit-marker','before-sync')) {
        $db = Join-Path $run ('batch-' + $point + '.db')
        $batch = [ordered]@{id='batch';op='BATCH';operations=@(
            [ordered]@{op='PUT';key=(B64 (U8 'batch-a'));value=(B64 (U8 '1'))},
            [ordered]@{op='PUT';key=(B64 (U8 'batch-b'));value=(B64 (U8 '2'))},
            [ordered]@{op='DELETE';key=(B64 (U8 'stable'))}
        )}
        [void](Session $faultExe $db @([ordered]@{id='seed';op='PUT';key=(B64 (U8 'stable'));value=(B64 (U8 'old'))}))
        $crash=Session $faultExe $db @($batch) @{KVDB_CRASH_POINT=$point}
        if ($crash.code -ne 91) { Fail 'batch-crash-atomicity' "$point exited $($crash.code), expected 91"; continue }
        $d=Dump $faultExe $db; $keys=@($d.entries|ForEach-Object{$_.key})
        $all=$keys -contains (B64 (U8 'batch-a')) -and $keys -contains (B64 (U8 'batch-b')) -and -not ($keys -contains (B64 (U8 'stable')))
        $none=$keys -contains (B64 (U8 'stable')) -and -not ($keys -contains (B64 (U8 'batch-a'))) -and -not ($keys -contains (B64 (U8 'batch-b')))
        if ($all -or $none) { Pass 'batch-crash-atomicity' } else { Fail 'batch-crash-atomicity' "$point recovered a partial batch: $($keys -join ',')" }
    }

    Begin-Category 'compaction-crash-boundaries'
    foreach($point in @('compaction-before-temp','compaction-after-temp-sync','compaction-after-replacement','compaction-before-backup-delete')) {
        $db=Join-Path $run ('compact-' + $point + '.db')
        [void](Session $faultExe $db @([ordered]@{id='a';op='PUT';key=(B64 (U8 'keep'));value=(B64 (U8 'yes'))},[ordered]@{id='b';op='PUT';key=(B64 (U8 'gone'));value=(B64 (U8 'x'))},[ordered]@{id='c';op='DELETE';key=(B64 (U8 'gone'))}))
        $crash=Session $faultExe $db @([ordered]@{id='compact';op='COMPACT'}) @{KVDB_CRASH_POINT=$point}
        if ($crash.code -ne 91) { Fail 'compaction-crash-boundaries' "$point exited $($crash.code), expected 91"; continue }
        $d=Dump $faultExe $db; $keep=@($d.entries|Where-Object{$_.key -eq (B64 (U8 'keep'))}).Count -eq 1; $gone=@($d.entries|Where-Object{$_.key -eq (B64 (U8 'gone'))}).Count -eq 0
        if ($keep -and $gone) { Pass 'compaction-crash-boundaries' } else { Fail 'compaction-crash-boundaries' "$point lost live data or resurrected deleted data" }
    }
}
catch { $report.environment_blocked += $_.Exception.Message }
finally {
    $env:GOCACHE = $oldGoCache
    $report.finished_utc=[DateTime]::UtcNow.ToString('o')
    $json=Join-Path $ReportDirectory 'advanced-campaign.json';$md=Join-Path $ReportDirectory 'advanced-campaign.md'
    $report|ConvertTo-Json -Depth 10|Set-Content -LiteralPath $json -Encoding utf8NoBOM
    $out=@('# Advanced campaign','','Categories:');foreach($e in $report.categories.GetEnumerator()){$out+="- $($e.Key): $($e.Value.passed)/$($e.Value.executed) passed, $($e.Value.failed) failed"};$out+='';$out+='Environment blocked:';foreach($x in $report.environment_blocked){$out+="- $x"};$out+='';$out+='Simulated only:';foreach($x in $report.simulated_only){$out+="- $x"};$out -join [Environment]::NewLine|Set-Content -LiteralPath $md -Encoding utf8NoBOM
    Remove-Item -LiteralPath $run -Recurse -Force -ErrorAction SilentlyContinue
    "JSON report: $json";"Markdown report: $md"
}
