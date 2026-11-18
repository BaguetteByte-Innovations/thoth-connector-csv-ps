# =====================================================================
# Thoth Connector SDK - PowerShell
#
# Infrastructure building blocks for writing a Thoth connector in PowerShell.
# Dot-source it at the top of a connector script:
#     . "$PSScriptRoot\thoth-protocol.ps1"
#
# This file contains NO source-specific parsing logic.
# It only wraps the communication protocol with Thoth Engine.
# Reference: https://thoth-iam.com/docs/develop/reference/powershell-module/
# =====================================================================

# Console encoding: UTF-8 without BOM in all three directions.
# No-BOM is mandatory here so no stray BOM prefix corrupts the NDJSON streams.
$utf8NoBom = New-Object System.Text.UTF8Encoding($false)
[Console]::OutputEncoding = $utf8NoBom
[Console]::InputEncoding  = $utf8NoBom
$OutputEncoding           = $utf8NoBom

# ---------------------------------------------------------------------
# PROTOCOL IO - Emitting prefixed NDJSON lines on stdout
# ---------------------------------------------------------------------

function Emit-Identity($id) {
    [Console]::Out.WriteLine("idy:$($id | ConvertTo-Json -Compress -Depth 10)")
}

# $sourceFields: names of the fields the source exposes to the mapping (for a CSV, its columns).
# Always emitted, even empty - empty for a fixed schema, whose $samples are then keyed by target
# identifiers (ThothRawIdentity property names) instead of source field names.
function Emit-InspectionResult([bool]$success, [bool]$requiresMapping, [string[]]$sourceFields, $samples, [string]$err, [string]$fixedRole, $connectorTargets = $null) {
    $payload = @{
        Success               = $success
        RequiresManualMapping = $requiresMapping
        SourceFields          = @($sourceFields)
        SampleData            = @($samples)
    }
    if ($err)       { $payload.ErrorMessage = $err }
    if ($fixedRole) { $payload.FixedRole    = $fixedRole }
    # ConnectorTargets are the connector's OWN mapping targets, offered next to the engine's. The
    # engine relays them without reading them, and the chosen column comes back in the mapping.
    if ($connectorTargets) { $payload.ConnectorTargets = @($connectorTargets) }
    # ExpectedTargets is engine-owned: the engine fills it from FixedRole at inspection.
    # A connector never produces it.
    [Console]::Out.WriteLine("res:$($payload | ConvertTo-Json -Compress -Depth 10)")
}

function Emit-IngestionResult([bool]$success, [string]$syncId, [int]$students, [int]$teachers, [int]$staff, [string]$err) {
    $payload = @{
        Success           = $success
        SyncId            = $syncId
        StudentsProcessed = $students
        TeachersProcessed = $teachers
        StaffProcessed    = $staff
    }
    if ($err) { $payload.ErrorMessage = $err }
    [Console]::Out.WriteLine("res:$($payload | ConvertTo-Json -Compress -Depth 10)")
}

function Emit-Log([string]$level, [string]$message) {
    [Console]::Out.WriteLine("log:$(@{ Level = $level; Message = $message } | ConvertTo-Json -Compress)")
}

# Emits a failure 'res:' line whose SHAPE matches the current mode, so the engine
# deserializes it correctly: an InspectionResult in 'inspect' mode, an
# IngestionResult otherwise (or when the mode is still unknown - e.g. a crash
# before parsing the request).
function Emit-CrashResult([string]$mode, [string]$err) {
    if ($mode -eq 'inspect') {
        Emit-InspectionResult $false $false @() @() $err $null
    } else {
        Emit-IngestionResult $false '' 0 0 0 $err
    }
}

# ---------------------------------------------------------------------
# GENERIC HELPERS
# ---------------------------------------------------------------------

# Adds an entry to a hashtable only if the value is not $null.
# Useful for building a ThothRawIdentity without polluting the object with null keys.
function Add-IfNotNull([hashtable]$h, [string]$key, $value) {
    if ($null -ne $value) { $h[$key] = $value }
}

# Reads a connector parameter (ConnectorContext.Parameters) by key, returning the
# default when it is absent or empty. Parameters always arrive as strings (cf. the
# manifest), so the caller parses/compares afterwards if needed.
# $default is intentionally UNTYPED: a [string] annotation would coerce a $null default into ""
# (that is what PowerShell does casting $null to [string]), so a caller asking for "absent means
# nothing" would get an empty string back - and that "" then travels into the emitted identity,
# where the contract expects an absent value.
function Get-Param($ctx, [string]$key, $default = $null) {
    $value = $ctx.Parameters.$key
    if ([string]::IsNullOrEmpty($value)) { return $default }
    return $value
}

# Converts a raw value into the contract's date shape (yyyy-MM-dd, a DateOnly? on
# the C# side). This is a TYPE conversion mandated by the contract, not cleaning:
# an empty or unparseable value yields $null (garbage in, null out), so Add-IfNotNull simply skips it.
#
# $formats are the patterns the OPERATOR stated, tried in order. Never TryParse without a format:
# `04/07/2011` is a valid date in dd/MM as in MM/dd, so guessing produces a wrong and plausible
# date. And InvariantCulture is written out IN FULL - this script is not compiled, so it is not
# subject to InvariantGlobalization: naming the culture is what makes it read exactly like the
# native connector. That parity IS the point.
#
# AllowWhiteSpaces, not None: a hand-filled `; 15/03/2010;` must pass. A type conversion is TOTAL,
# nothing of the original string survives in a date, so tolerating padding changes no emitted
# value - it is not a license to trim anything else. A fully blank cell still fails.
function ConvertTo-ThothDate([string]$raw, [string[]]$formats) {
    if ([string]::IsNullOrEmpty($raw)) { return $null }
    if ($null -eq $formats -or $formats.Count -eq 0) { return $null }
    $parsed = New-Object System.DateTime
    if ([datetime]::TryParseExact($raw, [string[]]$formats, [cultureinfo]::InvariantCulture,
                                  [System.Globalization.DateTimeStyles]::AllowWhiteSpaces,
                                  [ref]$parsed)) {
        return $parsed.ToString('yyyy-MM-dd')
    }
    return $null
}

# ---------------------------------------------------------------------
# ENTRY POINT
# Reads the JSON request on stdin, dispatches to the right handler, handles exit codes.
#
# Usage:
#   Invoke-ThothConnector -OnInspect { param($req) ... } -OnIngest { param($req) ... }
#
# The engine sends a single JSON line per invocation.
# The connector answers with NDJSON lines (idy:/log:/res:) and exit 0.
# ---------------------------------------------------------------------
function Invoke-ThothConnector {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [scriptblock] $OnInspect,
        [Parameter(Mandatory)] [scriptblock] $OnIngest
    )
    $ErrorActionPreference = 'Stop'
    # Tracked so the crash handler can emit a result of the right shape (cf. Emit-CrashResult).
    $mode = $null
    try {
        # Read stdin via an explicit UTF-8 (no-BOM) StreamReader rather than
        # [Console]::In: under Windows PowerShell 5.1, having Console.InputEncoding
        # assigned and then reading Console.In on a REDIRECTED stdin (exactly how the
        # engine launches us) injects a phantom BOM (U+FEFF) at the head of the first
        # line, which breaks JSON parsing ("Invalid JSON primitive"). Reading the raw
        # stdin handle ourselves sidesteps that quirk while keeping UTF-8 decoding.
        $stdin = New-Object System.IO.StreamReader([Console]::OpenStandardInput(), (New-Object System.Text.UTF8Encoding($false)), $true)
        $line  = $stdin.ReadLine()
        if ([string]::IsNullOrWhiteSpace($line)) {
            Emit-CrashResult $mode 'No request received on stdin.'
            exit 1
        }

        $req = $line | ConvertFrom-Json
        $mode = $req.Mode

        switch ($req.Mode) {
            'inspect' { & $OnInspect $req }
            'ingest'  { & $OnIngest  $req }
            default   {
                Emit-CrashResult $mode "Unknown mode: '$($req.Mode)'."
                exit 1
            }
        }
        exit 0
    }
    catch {
        Emit-CrashResult $mode "Connector crash: $($_.Exception.Message)"
        exit 1
    }
}
