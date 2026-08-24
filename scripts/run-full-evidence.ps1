$ErrorActionPreference = "Stop"

function Get-LineCount {
    param([Parameter(Mandatory = $true)][string]$Path)
    [long]$Count = 0
    foreach ($Line in [System.IO.File]::ReadLines($Path)) {
        $Count += 1
    }
    return $Count
}

function Assert-CanonicalSnapshot {
    param([Parameter(Mandatory = $true)][string]$Snapshot)

    $ExpectedSnapshotId = "d4cfb79165bcc88e82f3aae0721d86b9cd5fd008f5567d11d5e6bc98e6dfb4ff"
    $ExpectedEventsHash = "dd790ff824e2c53e9661da9e03a6f0d05eb78fc61b36e10c57ac4b6d83314f8f"
    $ExpectedLineagesHash = "92fe3ae37fadf616e7e9d7e3e043feec792a1d355cf044c87aac03245136daab"
    $ExpectedFiles = @("events.jsonl", "lineages.jsonl", "snapshot_manifest.json")
    if (-not (Test-Path -LiteralPath $Snapshot -PathType Container)) {
        throw "canonical snapshot directory is absent: $Snapshot"
    }
    $ObservedFiles = @(Get-ChildItem -LiteralPath $Snapshot -File | ForEach-Object { $_.Name } | Sort-Object)
    $FileDifference = @(Compare-Object ($ExpectedFiles | Sort-Object) $ObservedFiles)
    if ($FileDifference.Count -ne 0) {
        throw "canonical snapshot must contain exactly: $($ExpectedFiles -join ', ')"
    }
    if ((Get-ChildItem -LiteralPath $Snapshot -Directory | Measure-Object).Count -ne 0) {
        throw "canonical snapshot must not contain subdirectories"
    }

    $ManifestPath = Join-Path $Snapshot "snapshot_manifest.json"
    $EventsPath = Join-Path $Snapshot "events.jsonl"
    $LineagesPath = Join-Path $Snapshot "lineages.jsonl"
    $Manifest = Get-Content -LiteralPath $ManifestPath -Raw | ConvertFrom-Json
    if ($Manifest.schema_version -ne "telemetry-snapshot-v1") {
        throw "canonical snapshot manifest has an unsupported schema_version"
    }
    if ($Manifest.snapshot_id -ne $ExpectedSnapshotId) {
        throw "canonical snapshot identity does not match the frozen full-evidence contract"
    }
    $EventsHash = (Get-FileHash -LiteralPath $EventsPath -Algorithm SHA256).Hash.ToLowerInvariant()
    $LineagesHash = (Get-FileHash -LiteralPath $LineagesPath -Algorithm SHA256).Hash.ToLowerInvariant()
    if ($EventsHash -ne $Manifest.events_sha256) {
        throw "canonical snapshot events SHA-256 does not match its manifest"
    }
    if ($EventsHash -ne $ExpectedEventsHash) {
        throw "canonical snapshot events SHA-256 does not match the frozen contract"
    }
    if ($LineagesHash -ne $Manifest.lineages_sha256) {
        throw "canonical snapshot lineages SHA-256 does not match its manifest"
    }
    if ($LineagesHash -ne $ExpectedLineagesHash) {
        throw "canonical snapshot lineages SHA-256 does not match the frozen contract"
    }
    $EventCount = Get-LineCount -Path $EventsPath
    $LineageCount = Get-LineCount -Path $LineagesPath
    if ($EventCount -ne 225204 -or $EventCount -ne $Manifest.event_count) {
        throw "canonical snapshot event count does not match the frozen contract"
    }
    if ($LineageCount -ne 4 -or $LineageCount -ne $Manifest.lineage_count) {
        throw "canonical snapshot lineage count does not match the frozen contract"
    }
}

$RepositoryRoot = Split-Path -Parent $PSScriptRoot
$Python = Join-Path $RepositoryRoot ".venv\Scripts\python.exe"
if (-not (Test-Path -LiteralPath $Python)) {
    throw "Create the repository .venv before running full evidence"
}
$SourceRoot = $env:TINYML_IDS_EVIDENCE_ROOT
if ([string]::IsNullOrWhiteSpace($SourceRoot) -or -not (Test-Path -LiteralPath $SourceRoot)) {
    throw "TINYML_IDS_EVIDENCE_ROOT must identify the verified evidence source or bundle"
}
$SourceRoot = (Resolve-Path -LiteralPath $SourceRoot).Path
$env:TINYML_IDS_EVIDENCE_ROOT = $SourceRoot

& (Join-Path $RepositoryRoot "scripts\bootstrap.ps1")
Push-Location $RepositoryRoot
try {
    & $Python -m pytest tests\full_evidence\test_cukd_xai.py -q
    if ($LASTEXITCODE -ne 0) { throw "full-evidence verification failed" }

    $Snapshot = Join-Path $RepositoryRoot "artifacts\canonical-snapshot-v1"
    if (-not (Test-Path -LiteralPath $Snapshot)) {
        & $Python -m tinyml_ids_telemetry.cli.main evidence snapshot --evidence-root $SourceRoot --output $Snapshot
        if ($LASTEXITCODE -ne 0) { throw "canonical snapshot creation failed" }
    }
    Assert-CanonicalSnapshot -Snapshot $Snapshot
}
finally {
    Pop-Location
}
