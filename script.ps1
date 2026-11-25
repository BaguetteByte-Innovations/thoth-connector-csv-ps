# =====================================================================
# Thoth Connector - CSV Import (PowerShell variant)
#
# This file contains ONLY the CSV parsing (including the column→property index
# resolution, which is tabular-source specific).
# The whole Thoth protocol (Emit-*, Get-Param, ConvertTo-ThothDate, Invoke-ThothConnector)
# is imported from thoth-protocol.ps1.
#
# Two reading modes, driven by the `rows_per_person` parameter:
#   one  (default) - one row is one person, rows are streamed straight out.
#   many           - several rows may describe the SAME person (three files, one per school; or a
#                    long-format export with one row per teaching assignment). Rows are
#                    cross-referenced on an anchor cell and folded into a single identity. Rows
#                    fold across every source of one launch: the engine hands every file of an
#                    interactive import over in a single launch, and every source of one
#                    ingestion of an autonomous import.
#
# Merging is NOT cleaning: the anchor is normalized to COMPARE only, lists are concatenated
# without deduplication (the engine owns the Distinct), and values travel raw.
#
# `target_role` is read PER SOURCE (falling back to the call for a single-source or single-role
# batch): one call may carry students, teachers and staff at once, and the IngestionResult counts
# them separately. In `many` mode the fold key is therefore the couple (role, anchor).
# =====================================================================

# Console encoding: UTF-8 without BOM in all three directions.
# No-BOM is mandatory here so no stray BOM prefix corrupts the NDJSON streams.
$utf8NoBom = New-Object System.Text.UTF8Encoding($false)
[Console]::OutputEncoding = $utf8NoBom
[Console]::InputEncoding  = $utf8NoBom
$OutputEncoding           = $utf8NoBom

. "$PSScriptRoot\thoth-protocol.ps1"

# ---------------------------------------------------------------------
# CONTEXT ACCESSORS
# ---------------------------------------------------------------------

# Human-readable name of a source, for error messages.
function Get-SourceLabel($source) {
    if (-not [string]::IsNullOrWhiteSpace($source.OriginalName)) { return $source.OriginalName }
    return (Split-Path -Leaf $source.PhysicalPath)
}

# Effective reading settings of a source: its own when the operator set some for it, the call's
# otherwise. The engine has ALREADY merged them (batch <- source), so this picks one bag or the
# other and never merges anything.
# Returns an object exposing a .Parameters property, so Get-Param reads it unchanged.
function Get-SourceSettings($ctx, $source) {
    if ($null -ne $source.Parameters) { return [pscustomobject]@{ Parameters = $source.Parameters } }
    return $ctx
}

# True when the operator asked for the rows of a same person to be cross-referenced.
function Test-Merging($ctx) {
    return ((Get-Param $ctx 'rows_per_person' 'one').Trim() -ieq 'many')
}

# Minimal hygiene on an operator-typed setting: blank means "not given".
function Get-Cleaned([string]$value) {
    if ([string]::IsNullOrWhiteSpace($value)) { return $null }
    return $value.Trim()
}

# Target on which the rows of a same person are cross-referenced. One reading for the inspection,
# which announces it, and the ingestion, which applies it.
function Get-MergeAnchor($ctx) {
    $mergeAnchor = Get-Cleaned (Get-Param $ctx 'merge_anchor' $null)
    if ($null -eq $mergeAnchor) { return 'SamAccountName' }
    return $mergeAnchor
}

# An operator-facing refusal: its message is surfaced AS-IS in the ingestion result, without the
# "Connector crash:" wrapping of an unexpected error. ApplicationException serves as the marker
# type because the BCL never throws it on its own, so `catch [ApplicationException]` cannot
# swallow a genuine crash - and it needs no `class` declaration, which PowerShell 5.1 would not
# reliably carry across a dot-sourced file.
function New-IngestRefusal([string]$message) {
    return (New-Object System.ApplicationException($message))
}

# ---------------------------------------------------------------------
# CSV PARSING - specific to this connector
# ---------------------------------------------------------------------

# Name of a column in a file without a header row: Column_1, Column_2... in file order. The
# inspection announces these names and a mapping refers to them, so both phases build them the
# same way. The native flavor uses the same names.
function Get-PositionalColumnName([int]$index) {
    return "Column_$($index + 1)"
}

# Index of the column named $columnName, or -1. Without a header row the columns are named
# positionally (Column_1, Column_2...), exactly as the inspection announced them.
function Find-Column([string]$columnName, [string[]]$headers, [bool]$hasHeader) {
    for ($i = 0; $i -lt $headers.Length; $i++) {
        # Trim the headers only: they act as mapping keys and
        # must match exactly between the Inspect phase and the Ingest phase.
        $current = if ($hasHeader) { $headers[$i].Trim() } else { (Get-PositionalColumnName $i) }
        if ($current -ieq $columnName) { return $i }
    }
    return -1
}

# The date patterns stated for ONE source, tried in order. Twin of DateFormats.Resolve in the
# native connector: the two flavors must read the same bytes the same way, so the rules and the
# messages are the SAME on both sides.
# An EMPTY array means the operator picked "custom" without entering anything: that ends in a
# refusal (Find-DateFormatError), never in a silent fallback on the default.
# $DateFormatDefault MUST equal manifest.json -> date_format.DefaultValue: the manifest is read by
# the FRONT only, so an autonomous run lands here.
$script:DateFormatDefault = 'yyyy-MM-dd'

function Resolve-DateFormats($settings) {
    $format = Get-Param $settings 'date_format' $null
    if ([string]::IsNullOrWhiteSpace($format)) { return ,@($script:DateFormatDefault) }
    if ($format -ine 'custom') { return ,@($format) }

    $custom = Get-Param $settings 'date_format_custom' $null
    if ($null -eq $custom) { return ,@() }

    $patterns = New-Object System.Collections.Generic.List[string]
    foreach ($piece in $custom.Split('|')) {
        $trimmed = $piece.Trim()
        if ($trimmed.Length -gt 0) { [void]$patterns.Add($trimmed) }
    }
    return ,@($patterns.ToArray())
}

# The operator-facing refusal when the SETTING itself is unusable, $null when it is sound.
# Same two sentences as the native connector, word for word: an operator switching flavors must
# read the same refusal.
function Find-DateFormatError([string[]]$formats) {
    if ($null -eq $formats -or $formats.Count -eq 0) {
        return "No date format was entered. Write the format the dates are in, e.g. 'dd/MM/yyyy'."
    }

    foreach ($format in $formats) {
        # A ONE-character format string is a standard specifier, not a pattern: 'd' does not mean
        # "the day", it means "the culture's short date" - i.e. MM/dd/yyyy under the invariant
        # culture. An operator typing `d` would get back exactly the defect this setting fixes.
        if ($format.Length -eq 1) {
            return "Date format '$format' is a single-letter standard specifier, not a pattern. Write the full pattern, e.g. 'd/M/yyyy'."
        }
    }

    return $null
}

# The file's encoding, stated for ONE source. Twin of Encodings.ResolveName in the native connector:
# same rules, same messages, so an operator switching flavors reads the same refusal.
# $null means the operator picked "custom" without entering anything: that ends in a refusal
# (Find-EncodingError), never in a silent fallback on the default. A Trim is the only hygiene: this
# is a setting the operator typed, not a value of the file.
# $EncodingDefault MUST equal manifest.json -> encoding.DefaultValue: the manifest is read by the
# FRONT only, so an autonomous run lands here.
$script:EncodingDefault = 'utf-8'

function Resolve-EncodingName($settings) {
    $encoding = Get-Param $settings 'encoding' $null
    if ([string]::IsNullOrWhiteSpace($encoding)) { return $script:EncodingDefault }
    if ($encoding.Trim() -ine 'custom') { return $encoding.Trim() }

    $custom = Get-Param $settings 'encoding_custom' $null
    if ([string]::IsNullOrWhiteSpace($custom)) { return $null }
    return $custom.Trim()
}

# A STRICT encoding for a name Find-EncodingError accepted: invalid bytes throw, never replace.
# Accepts an IANA name or a code page number - GetEncoding('1252') throws, while the field's
# description promises the number. On .NET Framework every code page is native: no provider.
function Get-StrictEncoding([string]$name) {
    $codePage = 0
    if ([int]::TryParse($name, [System.Globalization.NumberStyles]::None, [System.Globalization.CultureInfo]::InvariantCulture, [ref]$codePage)) {
        return [System.Text.Encoding]::GetEncoding($codePage, [System.Text.EncoderFallback]::ExceptionFallback, [System.Text.DecoderFallback]::ExceptionFallback)
    }
    return [System.Text.Encoding]::GetEncoding($name, [System.Text.EncoderFallback]::ExceptionFallback, [System.Text.DecoderFallback]::ExceptionFallback)
}

# The operator-facing refusal when the SETTING itself is unusable (nothing entered, unknown name),
# $null when it is sound. The ONE place Get-StrictEncoding is wrapped in a try/catch: callers only
# call it after a $null answer from here.
# $name is intentionally untyped: a [string] annotation would coerce $null into "".
function Find-EncodingError($name) {
    if ($null -eq $name) {
        return "No encoding was entered. Write the name of the file's encoding, e.g. 'windows-1252'."
    }

    try {
        [void](Get-StrictEncoding $name)
        return $null
    }
    catch [System.ArgumentException], [System.NotSupportedException] {
        # ArgumentException: a name or number no table answers to. NotSupportedException: a code
        # page the framework knows of but ships no table for.
        return "Unknown encoding '$name'. Use an IANA name (e.g. 'windows-1252', 'iso-8859-15') or a code page number (e.g. 850)."
    }
}

# The strict encoding a byte-order mark announces, $null without one. UTF-32 LE is tested BEFORE
# UTF-16 LE - both start with FF FE - which is also the order StreamReader uses.
function Get-ByteOrderMarkEncoding([byte[]]$buffer, [int]$length) {
    if ($length -ge 4 -and $buffer[0] -eq 0xFF -and $buffer[1] -eq 0xFE -and $buffer[2] -eq 0x00 -and $buffer[3] -eq 0x00) {
        return (New-Object System.Text.UTF32Encoding($false, $false, $true))
    }
    if ($length -ge 4 -and $buffer[0] -eq 0x00 -and $buffer[1] -eq 0x00 -and $buffer[2] -eq 0xFE -and $buffer[3] -eq 0xFF) {
        return (New-Object System.Text.UTF32Encoding($true, $false, $true))
    }
    if ($length -ge 3 -and $buffer[0] -eq 0xEF -and $buffer[1] -eq 0xBB -and $buffer[2] -eq 0xBF) {
        return (New-Object System.Text.UTF8Encoding($false, $true))
    }
    if ($length -ge 2 -and $buffer[0] -eq 0xFF -and $buffer[1] -eq 0xFE) {
        return (New-Object System.Text.UnicodeEncoding($false, $false, $true))
    }
    if ($length -ge 2 -and $buffer[0] -eq 0xFE -and $buffer[1] -eq 0xFF) {
        return (New-Object System.Text.UnicodeEncoding($true, $false, $true))
    }
    return $null
}

# Fills the buffer as far as the stream allows: a single Read may return fewer bytes than asked
# without being at the end, and the line arithmetic relies on 64 KiB blocks.
function Read-Block([System.IO.Stream]$stream, [byte[]]$buffer) {
    $total = 0
    while ($total -lt $buffer.Length) {
        $n = $stream.Read($buffer, $total, $buffer.Length - $total)
        if ($n -le 0) { break }
        $total += $n
    }
    return $total
}

# The refusal of guard 1 (see Find-ByteMismatch). The faulty byte is read in BytesUnknown, never in
# $buffer[Index]: the index may be NEGATIVE - a sequence head held back from the previous block, or a
# sequence cut by the end of the file at flush - and then designates a byte of a block no longer
# there. The line count taken BEFORE the block holds for any negative index, since the bytes a
# decoder holds back are sequence heads or continuations (>= 0x80), never a 0x0A. Not clamped to 0:
# that would accuse the first byte of the block, which is innocent.
# $blockText is the current block read through Latin-1 (one char per byte), so the newlines before
# the faulty index are counted by a native Split, never by a PowerShell loop over the bytes.
function Get-Guard1Message([string]$label, [System.Text.Encoding]$effective, $ex, [string]$blockText, [long]$bytesBefore, [long]$newlinesBefore) {
    $faulty = 0
    if ($null -ne $ex.BytesUnknown -and $ex.BytesUnknown.Length -gt 0) { $faulty = [int]$ex.BytesUnknown[0] }
    $index    = [int]$ex.Index
    $position = $bytesBefore + $index

    $line = $newlinesBefore + 1
    if ($index -gt 0 -and $index -le $blockText.Length) {
        $line = $newlinesBefore + $blockText.Substring(0, $index).Split([char]10).Length
    }

    # A line is counted on 0x0A: exact in UTF-8 and in a single-byte encoding, approximate in
    # UTF-16/32 (a 0A may be the low byte of another character) - there, the byte offset alone.
    $where = if ($effective.CodePage -eq 65001 -or $effective.IsSingleByte) { "line $line" } else { "offset $position" }

    # Lower-cased so the two flavors say the same word: .NET Framework answers 'Windows-1252'
    # where .NET answers 'windows-1252'.
    $name = $effective.WebName.ToLowerInvariant()
    $hex  = '{0:X2}' -f $faulty

    # The verdict and nothing else: which byte, where. Which encoding to state instead is the
    # operator's call, helped by the option labels of the setting.
    return "'$label' is not valid ${name}: byte 0x$hex at $where cannot be read."
}

# The byte guards, run over the whole file in 64 KiB blocks. Twin of Encodings.FindByteMismatch in
# the native connector. Returns the operator-facing message, or $null when the bytes are consistent
# with the DECLARED encoding.
#
# The declared encoding is the only judge - the operator states, the connector never guesses, and a
# file is either read as stated or refused. Three refusals, in this order:
#   Guard 0 - the file carries a byte-order mark announcing ANOTHER encoding than the declared one:
#             the file is not what it is said to be, the mark says so. A mark that AGREES with the
#             declaration is just the first character of the file.
#   Guard 1 - at least one byte sequence is invalid in the declared encoding: the file is not what it
#             is said to be. Named with the byte and its place.
#   Guard 2 - the declared encoding is single-byte and the WHOLE file is valid UTF-8 with at least
#             one byte >= 0x80: the file is UTF-8, not what it is said to be.
# A pure-ASCII file trips no guard - any setting reads it right. Every refusal is a VERDICT - which
# file, what it is not, on what evidence - and nothing more: no hint at which encoding to state
# instead. The option labels of the setting carry the only help there is.
#
# Decoder.GetChars, never its count-only sibling: only GetChars carries the decoder's state from
# one block to the next; the count-only call is stateless and would throw on a sequence cut by a
# block boundary.
# NEVER iterate the bytes of a block in a PowerShell loop: 100 MB would be 100 million iterations.
# The newline count and the high-byte flag come from native calls on the block read through
# Latin-1 (28591, one char per byte).
function Find-ByteMismatch([string]$path, [System.Text.Encoding]$declared, [string]$label) {
    $chunkSize = 65536
    $latin1    = [System.Text.Encoding]::GetEncoding(28591)
    $buffer    = [byte[]]::new($chunkSize)

    $stream = [System.IO.File]::OpenRead($path)
    try {
        $read = Read-Block $stream $buffer

        # Guard 0. The mark is RECOGNIZED on the first bytes; when it agrees with the declaration,
        # those bytes then go through the decoder like any other (a UTF-8 mark is valid UTF-8, a
        # UTF-16 mark is a valid U+FEFF - they pass, and the reader strips them).
        $marked = Get-ByteOrderMarkEncoding $buffer $read
        if ($null -ne $marked -and $marked.CodePage -ne $declared.CodePage) {
            $markName = $marked.WebName.ToLowerInvariant()
            $declName = $declared.WebName.ToLowerInvariant()
            return "'$label' is not ${declName}: it carries a $markName byte-order mark."
        }
        $effective = $declared

        $strict = $effective.GetDecoder()
        $chars  = [char[]]::new($effective.GetMaxCharCount($chunkSize))

        # The UTF-8 probe of guard 2 is only in play for a single-byte effective encoding, and leaves
        # play at the first invalid sequence: the file is then not UTF-8, nothing more to ask.
        $utf8Probe = $null
        $utf8Chars = $null
        if ($effective.IsSingleByte) {
            $strictUtf8 = New-Object System.Text.UTF8Encoding($false, $true)
            $utf8Probe  = $strictUtf8.GetDecoder()
            $utf8Chars  = [char[]]::new($strictUtf8.GetMaxCharCount($chunkSize))
        }

        [long]$bytesBefore    = 0   # bytes handed to the decoder before the current block
        [long]$newlinesBefore = 0   # 0x0A seen before the current block
        $sawHighByte = $false
        $text = ''

        while ($read -gt 0) {
            $text            = $latin1.GetString($buffer, 0, $read)
            $newlinesInBlock = $text.Split([char]10).Length - 1
            if (-not $sawHighByte -and $text -match '[^\x00-\x7F]') { $sawHighByte = $true }

            try {
                [void]$strict.GetChars($buffer, 0, $read, $chars, 0, $false)
            }
            catch [System.Text.DecoderFallbackException] {
                # The .NET exception arrives wrapped in a MethodInvocationException.
                $ex = $_.Exception
                if ($ex -isnot [System.Text.DecoderFallbackException]) { $ex = $ex.InnerException }
                return (Get-Guard1Message $label $effective $ex $text $bytesBefore $newlinesBefore)
            }

            if ($null -ne $utf8Probe) {
                try {
                    [void]$utf8Probe.GetChars($buffer, 0, $read, $utf8Chars, 0, $false)
                }
                catch [System.Text.DecoderFallbackException] {
                    $utf8Probe = $null
                }
            }

            $bytesBefore    += $read
            $newlinesBefore += $newlinesInBlock
            $read = Read-Block $stream $buffer
        }

        # A sequence cut by the END of the file throws here and nowhere else.
        try {
            [void]$strict.GetChars($buffer, 0, 0, $chars, 0, $true)
        }
        catch [System.Text.DecoderFallbackException] {
            $ex = $_.Exception
            if ($ex -isnot [System.Text.DecoderFallbackException]) { $ex = $ex.InnerException }
            return (Get-Guard1Message $label $effective $ex '' $bytesBefore $newlinesBefore)
        }

        if ($null -ne $utf8Probe) {
            try {
                [void]$utf8Probe.GetChars($buffer, 0, 0, $utf8Chars, 0, $true)
            }
            catch [System.Text.DecoderFallbackException] {
                $utf8Probe = $null
            }
        }

        if ($null -ne $utf8Probe -and $sawHighByte) {
            $name = $effective.WebName.ToLowerInvariant()
            return "'$label' is not ${name}: its bytes are utf-8."
        }

        return $null
    } finally {
        $stream.Dispose()
    }
}

# Name of the source column mapped to $targetProperty, or $null when unmapped.
function Get-MappedColumnName([string]$targetProperty, $mapping) {
    foreach ($prop in $mapping.PSObject.Properties) {
        if ($prop.Value -eq $targetProperty) { return $prop.Name }
    }
    return $null
}

# Finds the column index matching a ThothRawIdentity property, via the MappingProfile.
# Tabular-source specific (positional columns + header row), hence it lives here and
# not in the protocol plumbing.
# $mapping: PSObject whose properties are (sourceColumn -> ThothProperty)
# Returns -1 if the property is not mapped.
function Get-ColumnIndex([string]$targetProperty, $mapping, [string[]]$headers, [bool]$hasHeader) {
    $columnName = Get-MappedColumnName $targetProperty $mapping
    if ($null -eq $columnName) { return -1 }
    return (Find-Column $columnName $headers $hasHeader)
}

# Column index of the merge anchor. Reuses the indexes already resolved for this file rather than
# walking the mapping again: a second resolution path would diverge on the first awkward case
# (two columns aiming at the same target).
function Resolve-AnchorColumn([string]$anchor, [hashtable]$idx) {
    $index =
        if     ($anchor -ieq 'SourceId')          { $idx.SourceId }
        elseif ($anchor -ieq 'SamAccountName')    { $idx.SamAccountName }
        elseif ($anchor -ieq 'UserPrincipalName') { $idx.UserPrincipalName }
        else { throw (New-IngestRefusal "Unknown cross-referencing key: '$anchor'.") }

    if ($index -lt 0) {
        throw (New-IngestRefusal "Cross-referencing is enabled on '$anchor' but no column is mapped to that target.")
    }
    return $index
}

# Structural check of EVERY source, run as a preliminary pass - before the first identity is built.
# It must stay a pass over the whole call: in `rows_per_person = one` the connector emits as it
# reads, so a check done when opening each file would let the first two sources emit before the
# third is refused - exactly the half-batch this guard exists to forbid.
#
# Three things are checked, all per source and against ITS OWN mapping and settings: a source with
# no mapping at all (a free-schema connector cannot read a column without being told which one), the
# encoding (a sound setting, then bytes that do not contradict it - Find-ByteMismatch - checked
# BEFORE the header line is decoded), and a mapped column the source does not carry (see below).
# Returns the operator-facing message, or $null when every source is readable.
#
# A mapped column the source does not carry is a REFUSAL, not a shrug. Its index would be -1, every
# cell would read as $null, and for the engine `null` = empty list = WIPE of the dimension: the night
# a source renames `Groupes`, an autonomous run would strip every pedagogical group off every student
# without a word. The optional dimensions are precisely the ones that bleed - a missing FirstName
# fails validation loudly, a missing Groupes does not. Failing here demands the human decision
# (re-map) instead of taking it alone.
#
# Checked PER FILE - a sibling carrying the column proves nothing about this one - and BEFORE the
# first identity is built, so a refusal never leaves half a batch behind.
#
# Without a header the mapping is positional (Column_N) and the check reads as a column COUNT; the
# defect is worse there, since a shift produces silently WRONG identities rather than merely empty
# ones.
#
# A column PRESENT but empty on a row is out of scope: that is the source saying "no value for this
# person", and the resulting wipe is wanted. This guard is about the file's structure.
# Returns the operator-facing message, or $null when every mapped column is present.
function Find-SourceStructureError($ctx, $sources) {
    foreach ($source in $sources) {
        if ($null -eq $source.Mapping -or $null -eq $source.Mapping.FieldMappings) {
            return 'The mapping profile is missing.'
        }

        $settings  = Get-SourceSettings $ctx $source
        $delimiter = Get-Param $settings 'delimiter' ';'
        $hasHeader = (Get-Param $settings 'has_header' 'true') -ieq 'true'

        # Same checks as the inspection, because the autonomous path NEVER inspects. They cost
        # nothing and they are the only place an autonomous run can hear about a typo.
        # Encoding first, and the bytes BEFORE the header line is decoded: headers decoded askew
        # would not find the mapped column, and the refusal would say "column missing" instead of
        # "wrong encoding".
        $encodingName  = Resolve-EncodingName $settings
        $encodingError = Find-EncodingError $encodingName
        if ($null -ne $encodingError) { return $encodingError }
        $encoding = Get-StrictEncoding $encodingName

        $byteMismatch = Find-ByteMismatch $source.PhysicalPath $encoding (Get-SourceLabel $source)
        if ($null -ne $byteMismatch) { return $byteMismatch }

        $dateFormats     = Resolve-DateFormats $settings
        $dateFormatError = Find-DateFormatError $dateFormats
        if ($null -ne $dateFormatError) { return $dateFormatError }

        $reader = [System.IO.StreamReader]::new($source.PhysicalPath, $encoding, $true)
        try {
            $firstLine = $reader.ReadLine()

            # An empty source carries no column, but it produces no identity either: Process-File
            # skips it. Refusing here would turn a harmless no-op into a failed run.
            if ([string]::IsNullOrWhiteSpace($firstLine)) { continue }

            $headers = $firstLine.Split($delimiter)

            $columnError = $null
            foreach ($prop in $source.Mapping.FieldMappings.PSObject.Properties) {
                if ([string]::IsNullOrWhiteSpace($prop.Name)) { continue }
                if ((Find-Column $prop.Name $headers $hasHeader) -ge 0) { continue }

                $label = Get-SourceLabel $source
                $columnError = "Mapped column '$($prop.Name)' was not found in '$label'."
                break
            }
            if ($null -ne $columnError) { return $columnError }

            # A WHOLE column of dates the stated format cannot read is an operator mistake, and it
            # is said once, here, instead of one cell at a time over 2 500 lines. An isolated
            # unreadable cell stays a $null: BirthDate is an OPTIONAL target, and in autonomous mode
            # the all-or-nothing rule would make one stray `N/C` cost the whole night.
            $dateIndex = Get-ColumnIndex 'BirthDate' $source.Mapping.FieldMappings $headers $hasHeader
            if ($dateIndex -lt 0) { continue }

            $sample   = New-Object System.Collections.Generic.List[string]
            $nonBlank = 0
            $rowsRead = 0

            # Without a header the first line IS a data row - that is already how inspection counts it.
            if (-not $hasHeader) {
                $cell = Get-RawField $headers $dateIndex
                [void]$sample.Add($cell)
                if (-not [string]::IsNullOrWhiteSpace($cell)) { $nonBlank++ }
                $rowsRead++
            }

            # Two bounds, each earning its keep: 20 non-blank cells are enough to decide, and the
            # 200-row ceiling stops a column that is sparse at the top of the file from making us
            # read all of it - without creating the blind spot of a 20-ROW sample that happens to be
            # empty on this column.
            while ($nonBlank -lt 20 -and $rowsRead -lt 200 -and ($line = $reader.ReadLine()) -ne $null) {
                if ([string]::IsNullOrWhiteSpace($line)) { continue }
                $cell = Get-RawField $line.Split($delimiter) $dateIndex
                [void]$sample.Add($cell)
                if (-not [string]::IsNullOrWhiteSpace($cell)) { $nonBlank++ }
                $rowsRead++
            }

            # Zero non-blank cell is NOT an error: a mapped but empty column is a legitimate file.
            if ($nonBlank -eq 0) { continue }

            $firstValue = ''
            $anyParsed  = $false
            foreach ($cell in $sample) {
                if ([string]::IsNullOrWhiteSpace($cell)) { continue }
                if ($firstValue -eq '') { $firstValue = $cell }
                if ($null -ne (ConvertTo-ThothDate $cell $dateFormats)) { $anyParsed = $true; break }
            }
            if ($anyParsed) { continue }

            $columnName = Get-MappedColumnName 'BirthDate' $source.Mapping.FieldMappings
            $label      = Get-SourceLabel $source
            $joined     = ($dateFormats -join '|')
            return "Column '$columnName' in '$label': none of the $nonBlank dates read matches the format '$joined'. First value read: '$firstValue'."
        } finally {
            $reader.Dispose()
        }
    }

    return $null
}

# Returns the raw value at the index, or $null if the column is not mapped.
function Get-RawField([string[]]$values, [int]$index) {
    if ($index -lt 0 -or $index -ge $values.Length) { return $null }
    return $values[$index]
}

# Splits a cell into a raw list (without filtering empties: that is the engine's job).
# $rawVal is intentionally untyped: a [string] annotation would coerce a $null
# (unmapped column) into "", defeating the null check and emitting a spurious [""].
function Get-RawList($rawVal, [string]$delim) {
    if ($null -eq $rawVal) { return $null }
    return ,@($rawVal.Split($delim))
}

# Splits a cell into ScopedAssignments by breaking down "school#name".
# An item WITHOUT an explicit prefix belongs to this row's school ($lineSchool): a $null there
# would let the engine backfill the GLOBAL default school, which pulls every file of a per-school
# export into a single school.
# $rawVal is intentionally untyped (see Get-RawList) to preserve $null for unmapped columns.
function Get-RawScopedList($rawVal, [string]$listDelim, [string]$scopeDelim, $lineSchool) {
    if ($null -eq $rawVal) { return $null }
    $list = New-Object System.Collections.Generic.List[hashtable]
    foreach ($item in $rawVal.Split($listDelim)) {
        $idx = $item.IndexOf($scopeDelim)
        if ($idx -ge 0) {
            [void]$list.Add(@{ School = $item.Substring(0, $idx); Name = $item.Substring($idx + $scopeDelim.Length) })
        } else {
            [void]$list.Add(@{ School = $lineSchool; Name = $item })
        }
    }
    return ,@($list.ToArray())
}

# True when a list carries nothing usable (absent, empty, or made of blanks only).
function Test-BlankList($values) {
    if ($null -eq $values -or $values.Count -eq 0) { return $true }
    foreach ($value in $values) {
        if (-not [string]::IsNullOrWhiteSpace($value)) { return $false }
    }
    return $true
}

# The one school a row names, or $null when it names none or several - in which case the row does
# not say which school an unprefixed assignment belongs to.
# Blank entries are skipped for the COUNT only: the value returned travels RAW, like everything a
# connector emits.
function Get-SingleSchool($schools) {
    if ($null -eq $schools) { return $null }

    $found = $null
    foreach ($school in $schools) {
        if ([string]::IsNullOrWhiteSpace($school)) { continue }
        if ($null -ne $found) { return $null }
        $found = $school
    }
    return $found
}

# Builds a ThothRawIdentity from a CSV line (values already split).
# $options: reading options shared by every source of the run.
# $layout : what THIS source resolved to - column indexes and school of the file.
function Build-Identity([string[]]$values, $options, $layout) {
    $idx = $layout.Indexes
    $id  = @{ Role = $options.Role }

    Add-IfNotNull $id 'SourceId'          (Get-RawField $values $idx.SourceId)
    Add-IfNotNull $id 'SamAccountName'    (Get-RawField $values $idx.SamAccountName)
    Add-IfNotNull $id 'UserPrincipalName' (Get-RawField $values $idx.UserPrincipalName)
    Add-IfNotNull $id 'FirstName'         (Get-RawField $values $idx.FirstName)
    Add-IfNotNull $id 'LastName'          (Get-RawField $values $idx.LastName)

    # BirthDate: mandatory type conversion (the contract expects DateOnly?).
    Add-IfNotNull $id 'BirthDate' (ConvertTo-ThothDate (Get-RawField $values $idx.BirthDate) $options.DateFormats)

    # Assignment schools, as the row DECLARES them. When the source says nothing, the school the
    # operator attached to the file stands in - not a guessed default: it is an explicit statement
    # ("this file is school X"). A student has no assignment, so nothing is injected.
    $schools = Get-RawList (Get-RawField $values $idx.Schools) $options.ListDelimiter
    if ($options.Role -in @('Teacher', 'Staff') -and (Test-BlankList $schools) -and -not [string]::IsNullOrWhiteSpace($layout.FileScope)) {
        $schools = @($layout.FileScope)
    }

    # The school an UNPREFIXED assignment belongs to. A row naming exactly one school says it
    # without ambiguity; a row naming several does not, and falls back to the file's school.
    # $null lets the engine backfill its default assignment school.
    #
    # ORDER MATTERS: this reads $schools, which the block above may have filled from the file.
    # Computing it first would leave every unprefixed assignment of a per-school export in the
    # engine's default school - silently, and for the whole batch.
    $singleSchool = Get-SingleSchool $schools
    $lineSchool   = if ($null -ne $singleSchool) { $singleSchool } else { $layout.FileScope }

    # Residence: the cascade is stated in Resolve-Residence, and Merge-Residence applies it to a
    # person folded from several rows.
    #
    # The file's school is handed over RAW, for every role alike: the ENGINE interprets it
    # (a student's school is their residence, a teacher's or a staff member's resolves to that
    # school's dedicated district). A connector transports a residence, it does not
    # know what one is.
    #
    # The transverse district is the ONE role-conditioned term, and it is so because the SETTING is:
    # its VisibleWhen restricts it to teachers and staff, while it is asked once for the whole call.
    # A mixed batch would otherwise leak it onto the students.
    $transverseDistrict = if ($options.Role -in @('Teacher', 'Staff')) { $options.ResidenceDistrict } else { $null }
    $residence          = Resolve-Residence (Get-RawField $values $idx.Scope) $transverseDistrict $layout.FileScope

    Add-IfNotNull $id 'Scope'       $residence.Value
    Add-IfNotNull $id 'Class'       (Get-RawField $values $idx.Class)

    Add-IfNotNull $id 'StudentPedagogicalGroups' (Get-RawList (Get-RawField $values $idx.StudentGroups) $options.ListDelimiter)
    Add-IfNotNull $id 'Schools'                  $schools
    Add-IfNotNull $id 'TeacherClasses'           (Get-RawScopedList (Get-RawField $values $idx.TeacherClasses)  $options.ListDelimiter $options.ScopeDelimiter $lineSchool)
    Add-IfNotNull $id 'TeacherPedagogicalGroups' (Get-RawScopedList (Get-RawField $values $idx.TeacherGroups)   $options.ListDelimiter $options.ScopeDelimiter $lineSchool)
    Add-IfNotNull $id 'TeacherSubjects'          (Get-RawScopedList (Get-RawField $values $idx.TeacherSubjects) $options.ListDelimiter $options.ScopeDelimiter $lineSchool)
    Add-IfNotNull $id 'Description'              (Get-RawField $values $idx.Description)
    Add-IfNotNull $id 'Mail'                     (Get-RawField $values $idx.Mail)

    return @{ Identity = $id; ResidenceOrigin = $residence.Origin }
}

# ---------------------------------------------------------------------
# CROSS-REFERENCING - fold the fragments of one person into a single identity
# ---------------------------------------------------------------------

# Scalars: first non-empty wins, a later disagreement is a warning (see Merge-Scalar).
# BirthDate belongs here: the protocol already carries it as a 'yyyy-MM-dd' string, so it
# compares like any other scalar.
#
# 'Scope' is NOT in this list: its fold follows the residence cascade (see Merge-Residence).
$ScalarFields = @('SourceId', 'SamAccountName', 'UserPrincipalName', 'FirstName', 'LastName',
                  'BirthDate', 'Class', 'Description', 'Mail')

# Origin of a residence, from the weakest to the strongest term of the cascade: the numeric order
# IS the precedence. Same values as ResidenceOrigin in the native flavor.
$ResidenceOrigin = @{ None = 0; FileSchool = 1; TransverseDistrict = 2; Column = 3 }

# The residence of ONE row: the mapped column cell > the batch's transverse district > the school
# stated on the file > nothing. Returns the value AND the term it comes from, which the fold needs
# to apply the same cascade to a person read over several files (see Merge-Residence).
# The blank check serves ONLY the choice between the cell and what follows: the retained value
# travels RAW, and the engine sanitizes it.
function Resolve-Residence($cell, $transverseDistrict, $fileSchool) {
    if (-not [string]::IsNullOrWhiteSpace($cell)) { return @{ Value = $cell; Origin = $ResidenceOrigin.Column } }
    if ($null -ne $transverseDistrict) { return @{ Value = $transverseDistrict; Origin = $ResidenceOrigin.TransverseDistrict } }
    if ($null -ne $fileSchool) { return @{ Value = $fileSchool; Origin = $ResidenceOrigin.FileSchool } }
    return @{ Value = $null; Origin = $ResidenceOrigin.None }
}

# Lists: concatenated, NEVER deduplicated - the engine deduplicates, and the contract forbids a
# connector from cleaning what it reads.
$ListFields = @('StudentPedagogicalGroups', 'Schools', 'TeacherClasses',
                'TeacherPedagogicalGroups', 'TeacherSubjects')

# The three counters of an IngestionResult, filled as the sources are read. A call carrying three
# roles fills three of them - that is what a per-source role buys, and it is why the result has
# always had three.
function New-RoleTally {
    return @{ Students = 0; Teachers = 0; Staff = 0 }
}

function Add-RoleCount([hashtable]$tally, [string]$role, [int]$count) {
    switch ($role) {
        'Teacher' { $tally.Teachers += $count }
        'Staff'   { $tally.Staff    += $count }
        default   { $tally.Students += $count }
    }
}

# Accumulates the fragments of a run, keyed by the couple (role, normalized anchor), and folds them.
# O(population of the call) in memory - the price of cross-referencing: nothing can be emitted
# before the last source has been read.
#
# The ROLE is part of the key. Two sources of a same call may describe two different populations
# - students split over two files PLUS a teacher file is a legitimate batch - and folding on the
# anchor alone would merge a teacher into a student who happens to share it. Refusing the
# combination instead would forbid precisely the batch this connector exists to serve.
function New-MergeBuffer {
    return @{
        # Trim + case-insensitive, for COMPARISON only: `SOULARD.N1` and `soulard.n1` are the same
        # person to the engine, so they must be the same person here. Emitted values stay raw.
        ByKey      = New-Object 'System.Collections.Generic.Dictionary[string,int]' ([System.StringComparer]::OrdinalIgnoreCase)
        Identities = New-Object 'System.Collections.Generic.List[hashtable]'
        Anchors    = New-Object 'System.Collections.Generic.List[string]'
        # Origin of each kept residence (the identity carries the value): never emitted.
        ResidenceOrigins     = New-Object 'System.Collections.Generic.List[int]'
        # People whose rows disagree on their residence at its current origin, with the first value
        # ignored. Judged at the flush: a stronger origin read later settles the disagreement.
        ResidenceDivergences = New-Object 'System.Collections.Generic.Dictionary[int,string]'
    }
}

function Add-MergeFragment($buffer, [string]$role, [string]$anchor, [hashtable]$identity, [int]$residenceOrigin) {
    $trimmed  = $anchor.Trim()
    # NUL cannot appear in a line read by ReadLine, so no anchor can spoof another role's key.
    $key      = "$role$([char]0)$trimmed"
    $position = 0

    if (-not $buffer.ByKey.TryGetValue($key, [ref]$position)) {
        $buffer.ByKey[$key] = $buffer.Identities.Count
        [void]$buffer.Identities.Add($identity)
        [void]$buffer.Anchors.Add($trimmed)
        [void]$buffer.ResidenceOrigins.Add($residenceOrigin)
        return
    }

    # Hashtables are reference types: folding mutates the kept fragment in place.
    Merge-Identity $buffer.Identities[$position] $identity $buffer.Anchors[$position]
    Merge-Residence $buffer $position $identity $residenceOrigin
}

# Emits the folded identities in order of first appearance and returns their count, split by role.
# Every fragment of a bucket shares its role by construction (it is half the key), so the kept
# identity's own role is the bucket's.
function Emit-MergeBuffer($buffer) {
    $tally = New-RoleTally
    foreach ($identity in $buffer.Identities) {
        Emit-Identity $identity
        Add-RoleCount $tally $identity.Role 1
    }

    # Two schools of files disagreeing is a defect OF THE BATCH: said once, counting people. Two STATED
    # residences disagreeing (two residence columns) is a defect OF THE PERSON: named, like a
    # divergent first name.
    $schoolConflicts = 0
    foreach ($entry in $buffer.ResidenceDivergences.GetEnumerator()) {
        if ($buffer.ResidenceOrigins[$entry.Key] -eq $ResidenceOrigin.FileSchool) { $schoolConflicts++; continue }
        $kept = $buffer.Identities[$entry.Key]['Scope'].Trim()
        Emit-Log 'Warning' "'$($buffer.Anchors[$entry.Key])': conflicting 'Scope' across rows — kept '$kept', ignored '$($entry.Value)'."
    }

    if ($schoolConflicts -gt 0) {
        Emit-Log 'Warning' ("$schoolConflicts people appear in several files stating different schools: " +
            "each kept the residence of the first file read. Map a Residence column, or fill " +
            "'residence_district', to state it explicitly.")
    }

    return $tally
}

function Merge-Identity([hashtable]$kept, [hashtable]$incoming, [string]$anchor) {
    foreach ($field in $ScalarFields) { Merge-Scalar $kept $incoming $field $anchor }
    foreach ($field in $ListFields)   { Merge-List   $kept $incoming $field }
}

# First non-empty wins. A later fragment disagreeing is reported as a warning, never as a failure:
# dropping a 3 000-teacher import over one divergent birth date is out of proportion with the
# defect.
function Merge-Scalar([hashtable]$kept, [hashtable]$incoming, [string]$field, [string]$anchor) {
    $new = $incoming[$field]
    if ([string]::IsNullOrWhiteSpace($new)) { return }

    $old = $kept[$field]
    if ([string]::IsNullOrWhiteSpace($old)) { $kept[$field] = $new; return }

    # Case and padding differences are not worth a warning: the kept value is emitted raw, and the
    # engine sanitizes it.
    if ($old.Trim() -ine $new.Trim()) {
        Emit-Log 'Warning' "'$anchor': conflicting '$field' across rows — kept '$($old.Trim())', ignored '$($new.Trim())'."
    }
}

# The residence of ONE person: the cascade of Resolve-Residence, applied across its rows. The
# stronger origin wins whatever the order; a weaker one is dropped without a word, as within a row.
# Between two values of the SAME origin the first read stays (the rule of every scalar), and the
# disagreement is noted for Emit-MergeBuffer.
function Merge-Residence($buffer, [int]$position, [hashtable]$incoming, [int]$incomingOrigin) {
    $kept       = $buffer.Identities[$position]
    $keptOrigin = $buffer.ResidenceOrigins[$position]

    if ($incomingOrigin -gt $keptOrigin) {
        $kept['Scope'] = $incoming['Scope']
        $buffer.ResidenceOrigins[$position] = $incomingOrigin
        [void]$buffer.ResidenceDivergences.Remove($position)
        return
    }
    if ($incomingOrigin -lt $keptOrigin -or $keptOrigin -eq $ResidenceOrigin.None) { return }

    $old = $kept['Scope']
    $new = $incoming['Scope']
    if ($old.Trim() -ine $new.Trim() -and -not $buffer.ResidenceDivergences.ContainsKey($position)) {
        $buffer.ResidenceDivergences[$position] = $new.Trim()
    }
}

function Merge-List([hashtable]$kept, [hashtable]$incoming, [string]$field) {
    $new = $incoming[$field]
    if ($null -eq $new -or $new.Count -eq 0) { return }

    $old = $kept[$field]
    if ($null -eq $old) { $kept[$field] = $new; return }

    $kept[$field] = @($old) + @($new)
}

# ---------------------------------------------------------------------
# HANDLERS - called by Invoke-ThothConnector
# ---------------------------------------------------------------------

function Invoke-Inspect($req) {
    $ctx = $req.Context

    # ConvertFrom-Json unseals a single-element JSON list into a scalar: on an object, .Count is 1
    # (PS 3+) but [0] would not index it as an array. Forcing the type restores a real array.
    # @(...) is NOT a substitute: on a $null it yields an empty array, hiding the absence instead
    # of reporting it.
    [object[]]$sources = $ctx.Sources

    if ($null -eq $sources -or $sources.Count -eq 0) {
        Emit-InspectionResult $false $false @() @() 'No file was provided.' $null; return
    }

    # Inspection describes ONE shape, so it reads ONE source. The engine inspects each source of a
    # batch separately (a single source per call), which is what lets a batch mix shapes: each gets
    # its own mapping screen, and its own mapping at ingest.
    $source   = $sources[0]
    $settings = Get-SourceSettings $ctx $source

    $filePath = $source.PhysicalPath
    if (-not (Test-Path -LiteralPath $filePath)) {
        Emit-InspectionResult $false $false @() @() 'File not found.' $null; return
    }

    $delimiter   = Get-Param $settings 'delimiter' ';'
    $hasHeader   = (Get-Param $settings 'has_header' 'true') -ieq 'true'

    # `target_role` is declared IsRequired in the manifest: a silent default would turn a teacher
    # file into a student import - and since reconciliation is global per role, the real students
    # missing from that batch would become deletion candidates. Fail fast.
    $targetRole  = Get-Param $settings 'target_role' $null
    if ([string]::IsNullOrWhiteSpace($targetRole)) {
        Emit-InspectionResult $false $false @() @() "Missing required parameter 'target_role'." $null
        return
    }

    if ($targetRole -notin @('Student', 'Teacher', 'Staff')) {
        Emit-InspectionResult $false $false @() @() "Invalid user type: '$targetRole'." $null
        return
    }

    # The unusable settings ("custom" with nothing entered, an unknown encoding, a one-letter date
    # pattern) are judged without reading any data: refuse them HERE, in the panel the operator just
    # typed in, rather than two screens later. The ingest keeps its own check - an autonomous run
    # never inspects. Encoding first: it says how the bytes are read, before the date format says
    # how a cell is read.
    $encodingName  = Resolve-EncodingName $settings
    $encodingError = Find-EncodingError $encodingName
    if ($null -ne $encodingError) {
        Emit-InspectionResult $false $false @() @() $encodingError $null
        return
    }
    $encoding = Get-StrictEncoding $encodingName

    $dateFormatError = Find-DateFormatError (Resolve-DateFormats $settings)
    if ($null -ne $dateFormatError) {
        Emit-InspectionResult $false $false @() @() $dateFormatError $null
        return
    }

    # The bytes are judged BEFORE a line is decoded: a preview with the accents right, or a named
    # refusal - never a sample of U+FFFD the operator would have to interpret.
    $byteMismatch = Find-ByteMismatch $filePath $encoding (Get-SourceLabel $source)
    if ($null -ne $byteMismatch) {
        Emit-InspectionResult $false $false @() @() $byteMismatch $null
        return
    }

    $reader = [System.IO.StreamReader]::new($filePath, $encoding, $true)
    try {
        $headerLine = $reader.ReadLine()
        if ([string]::IsNullOrWhiteSpace($headerLine)) {
            Emit-InspectionResult $false $false @() @() 'The file is empty.' $null; return
        }

        $headers = $headerLine.Split($delimiter)
        $columns = New-Object System.Collections.Generic.List[string]
        $samples = New-Object System.Collections.Generic.List[hashtable]

        if ($hasHeader) {
            foreach ($h in $headers) { [void]$columns.Add($h.Trim()) }
        } else {
            for ($i = 0; $i -lt $headers.Length; $i++) { [void]$columns.Add((Get-PositionalColumnName $i)) }
            $row = @{}
            for ($i = 0; $i -lt $columns.Count; $i++) {
                $row[$columns[$i]] = if ($i -lt $headers.Length) { $headers[$i] } else { '' }
            }
            [void]$samples.Add($row)
        }

        $rowsRead = 0
        while ($rowsRead -lt 5) {
            $dataLine = $reader.ReadLine()
            if ($null -eq $dataLine) { break }
            if ([string]::IsNullOrWhiteSpace($dataLine)) { continue }
            $values = $dataLine.Split($delimiter)
            $row = @{}
            for ($i = 0; $i -lt $columns.Count; $i++) {
                $row[$columns[$i]] = if ($i -lt $values.Length) { $values[$i] } else { '' }
            }
            [void]$samples.Add($row)
            $rowsRead++
        }

        # The inspection only repeats the operator's answer, read from the CALL like the ingestion
        # does: nothing is looked up in the file.
        $crossReferenceTargets = if (Test-Merging $ctx) { @(Get-MergeAnchor $ctx) } else { @() }
        Emit-InspectionResult $true $true $columns.ToArray() $samples.ToArray() $null $targetRole $null $true $crossReferenceTargets
    } finally {
        $reader.Dispose()
    }
}

# Turns one CSV line into an identity: emitted straight away in `one` mode (returns 1), buffered
# for folding in `many` mode (returns 0 - the population is only known at the end of the run).
function Add-Row([string[]]$values, [int]$number, $options, $layout, [string]$sourceLabel, [int]$anchorIndex, $buffer) {
    $row = Build-Identity $values $options $layout

    if ($null -eq $buffer) {
        Emit-Identity $row.Identity
        return 1
    }

    $anchor = if ($anchorIndex -lt $values.Length) { $values[$anchorIndex] } else { $null }
    if ([string]::IsNullOrWhiteSpace($anchor)) {
        # Emitting the unfolded row would reintroduce the very duplicate this mode removes, and the
        # batch would carry an identity nothing can reconcile. Fail loudly.
        throw (New-IngestRefusal "Cross-referencing is enabled but line $number of '$sourceLabel' has no value in the column mapped to '$($options.MergeAnchor)'.")
    }

    Add-MergeFragment $buffer $options.Role $anchor $row.Identity $row.ResidenceOrigin
    return 0
}

# Parses a single CSV file. Returns the number of identities EMITTED: the row count in `one` mode,
# and 0 in `many` mode, where rows only fill the buffer.
# Headers (and therefore the column->property indexes) are resolved PER FILE, against the mapping
# THIS source carries: a consolidated batch may mix sources whose columns differ in order and in name.
function Process-File($source, [string]$sourceLabel, $mapping, $options, [string]$delimiter, $buffer) {
    $reader = [System.IO.StreamReader]::new($source.PhysicalPath, $options.Encoding, $true)
    try {
        $firstLine = $reader.ReadLine()
        if ([string]::IsNullOrWhiteSpace($firstLine)) { return 0 }

        $headers = $firstLine.Split($delimiter)
        $idx = @{
            SourceId           = (Get-ColumnIndex 'SourceId'                 $mapping $headers $options.HasHeader)
            SamAccountName     = (Get-ColumnIndex 'SamAccountName'           $mapping $headers $options.HasHeader)
            UserPrincipalName  = (Get-ColumnIndex 'UserPrincipalName'        $mapping $headers $options.HasHeader)
            FirstName          = (Get-ColumnIndex 'FirstName'                $mapping $headers $options.HasHeader)
            LastName           = (Get-ColumnIndex 'LastName'                 $mapping $headers $options.HasHeader)
            BirthDate          = (Get-ColumnIndex 'BirthDate'                $mapping $headers $options.HasHeader)
            Class              = (Get-ColumnIndex 'Class'                    $mapping $headers $options.HasHeader)
            StudentGroups      = (Get-ColumnIndex 'StudentPedagogicalGroups' $mapping $headers $options.HasHeader)
            Scope              = (Get-ColumnIndex 'Scope'                    $mapping $headers $options.HasHeader)
            Schools            = (Get-ColumnIndex 'Schools'                  $mapping $headers $options.HasHeader)
            TeacherClasses     = (Get-ColumnIndex 'TeacherClasses'           $mapping $headers $options.HasHeader)
            TeacherGroups      = (Get-ColumnIndex 'TeacherPedagogicalGroups' $mapping $headers $options.HasHeader)
            TeacherSubjects    = (Get-ColumnIndex 'TeacherSubjects'          $mapping $headers $options.HasHeader)
            Description        = (Get-ColumnIndex 'Description'              $mapping $headers $options.HasHeader)
            Mail               = (Get-ColumnIndex 'Mail'                     $mapping $headers $options.HasHeader)
        }

        $layout = @{
            Indexes   = $idx
            FileScope = $source.Scope
        }

        $anchorIndex = -1
        if ($null -ne $buffer) { $anchorIndex = Resolve-AnchorColumn $options.MergeAnchor $idx }

        $count      = 0
        $lineNumber = 1

        if (-not $options.HasHeader) {
            $count += Add-Row $headers $lineNumber $options $layout $sourceLabel $anchorIndex $buffer
        }

        while (($line = $reader.ReadLine()) -ne $null) {
            $lineNumber++
            if ([string]::IsNullOrWhiteSpace($line)) { continue }
            $count += Add-Row $line.Split($delimiter) $lineNumber $options $layout $sourceLabel $anchorIndex $buffer
        }

        return $count
    } finally {
        $reader.Dispose()
    }
}

function Invoke-Ingest($req) {
    $syncId = if ($req.SyncId) { $req.SyncId } else { '' }

    $ctx = $req.Context

    # Same hardening as in Invoke-Inspect: a single-element JSON list unseals into a scalar.
    [object[]]$sources = $ctx.Sources

    if ($null -eq $sources -or $sources.Count -eq 0) {
        Emit-IngestionResult $false $syncId 0 0 0 'No file was provided.'; return
    }

    # The role is declared PER SOURCE: one call may carry the students, the teachers and the staff
    # at once, and the IngestionResult has three counters precisely so it can say so. It is resolved
    # for EVERY source before the first identity is built - same reason as the structural guard
    # below: refusing halfway would leave a half batch behind.
    #
    # `target_role` is declared IsRequired in the manifest: a silent default would turn a teacher
    # file into a student import - and since reconciliation is global per role, the real students
    # missing from that batch would become deletion candidates. Fail fast, on an absent value as
    # well as on an unreadable one (same allow-list as Invoke-Inspect), and NAME the source at fault.
    $rolesBySource = New-Object 'System.Collections.Generic.List[string]'
    foreach ($source in $sources) {
        $sourceSettings = Get-SourceSettings $ctx $source
        $sourceLabel    = Get-SourceLabel $source
        $targetRole     = Get-Param $sourceSettings 'target_role' $null

        if ([string]::IsNullOrWhiteSpace($targetRole)) {
            Emit-IngestionResult $false $syncId 0 0 0 "Missing required parameter 'target_role' for '$sourceLabel'."; return
        }
        if ($targetRole -notin @('Student', 'Teacher', 'Staff')) {
            Emit-IngestionResult $false $syncId 0 0 0 "Invalid user type: '$targetRole' for '$sourceLabel'."; return
        }
        [void]$rolesBySource.Add($targetRole)
    }

    # Cross-referencing stays a decision of the CALL: it folds the sources of one call together, so
    # it cannot differ from one source to the next.
    $mergeAnchor = Get-MergeAnchor $ctx

    $merging = Test-Merging $ctx

    $tally = New-RoleTally
    try {
        # Every source is checked against ITS mapping BEFORE the first identity is built: a mapped
        # column the file no longer carries is a refusal, never a shrug (see Find-SourceStructureError).
        $structureError = Find-SourceStructureError $ctx $sources
        if ($null -ne $structureError) {
            Emit-IngestionResult $false $syncId 0 0 0 $structureError; return
        }

        $buffer = if ($merging) { New-MergeBuffer } else { $null }

        # Process EVERY source, each with ITS OWN role, ITS OWN mapping and ITS OWN reading settings.
        # That is what lets a reference file and its complement, two schools exporting different
        # columns, or three files describing three different populations, fold into one batch - and, in `many` mode, into one identity per person.
        for ($i = 0; $i -lt $sources.Count; $i++) {
            $source   = $sources[$i]
            $settings = Get-SourceSettings $ctx $source

            $options = @{
                Role              = $rolesBySource[$i]
                ListDelimiter     = (Get-Param $settings 'list_delimiter' ',')
                ScopeDelimiter    = (Get-Param $settings 'scope_delimiter' '#')
                HasHeader         = ((Get-Param $settings 'has_header' 'true') -ieq 'true')
                # Find-SourceStructureError above already refused, for EVERY source, a "custom"
                # with nothing entered and an unknown name: the name resolves here without a guard.
                Encoding          = (Get-StrictEncoding (Resolve-EncodingName $settings))
                # Read PER SOURCE, like its neighbours: two files of one batch may state two
                # different date formats.
                DateFormats       = (Resolve-DateFormats $settings)
                ResidenceDistrict = (Get-Cleaned (Get-Param $settings 'residence_district' $null))
                MergeAnchor       = $mergeAnchor
            }

            $processed = Process-File `
                $source `
                (Get-SourceLabel $source) `
                $source.Mapping.FieldMappings `
                $options `
                (Get-Param $settings 'delimiter' ';') `
                $buffer

            Add-RoleCount $tally $options.Role $processed
        }

        # Merged mode counts IDENTITIES, not rows: Process-File returns 0 while filling the buffer,
        # and the folded population is only known once the last source has been read. The buffer
        # counts by role on its own - a folded identity belongs to the role of the fragments that
        # built it, which the (role, anchor) key keeps homogeneous.
        if ($null -ne $buffer) { $tally = Emit-MergeBuffer $buffer }
    }
    catch [System.ApplicationException] {
        # Refusal we authored: the message is already the operator-facing one, no wrapping.
        Emit-IngestionResult $false $syncId 0 0 0 $_.Exception.Message; return
    }
    catch {
        Emit-IngestionResult $false $syncId 0 0 0 "Ingestion error: $($_.Exception.Message)"; return
    }

    Emit-IngestionResult $true $syncId $tally.Students $tally.Teachers $tally.Staff $null
}

# ---------------------------------------------------------------------
# ENTRY POINT
# ---------------------------------------------------------------------
Invoke-ThothConnector -OnInspect { param($req) Invoke-Inspect $req } `
                     -OnIngest  { param($req) Invoke-Ingest  $req }
