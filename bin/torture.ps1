param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$Arguments
)

$ErrorActionPreference = 'Stop'
$Root = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
Push-Location $Root
try {
    $env:PYTHONPATH = if ($env:PYTHONPATH) { "torture;$env:PYTHONPATH" } else { 'torture' }
    $executable = if ($env:KVDB_EXECUTABLE) { $env:KVDB_EXECUTABLE } else { '.\kvdb.exe' }
    $forwarded = @('--executable', $executable) + $Arguments
    if ($env:KVDB_FAULT_EXECUTABLE) {
        $forwarded += @('--fault-executable', $env:KVDB_FAULT_EXECUTABLE)
    }
    & python -m campaign @forwarded
    exit $LASTEXITCODE
}
finally {
    Pop-Location
}
