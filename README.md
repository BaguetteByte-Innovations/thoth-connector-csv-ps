# Thoth.Connectors.CsvPs

The **open PowerShell reference connector** for Thoth: a CSV connector written as a plain PowerShell
script. This is the example to read for a sysadmin tweaking the parsing in place — no compiler, no
SDK, just a script the engine runs as a child process and talks to over stdin/stdout.

> **A connector parses; it does not clean.** The script reads the CSV, builds raw identities, and
> emits them as NDJSON on stdout. All housekeeping (trim, empty-to-null, format validation) is done
> engine-side.

## Flavor

This connector exists in two flavors. This is the **PowerShell** one.

> **Behavior is identical to the native flavor** — same settings, same scope cascade, same
> cross-referencing rules, same refusals, same messages word for word.

## What it handles

| Source shape | Supported |
|---|---|
| One file, one population | yes |
| School carried by a column, or by a `SCHOOL#VALUE` prefix, or stated per file | yes |
| One person spread over several rows, or several files | yes — cross-referencing on an anchor |
| Files with different columns or different delimiters, cross-referenced | yes — each source carries its own mapping and reading settings |
| Several roles (students, teachers, staff) in one batch | yes — the role is declared file by file |
| A role varying row by row (a `Role` column) | **no**, deliberately |
| Values containing the delimiter, a quote or a line break (RFC 4180 quoting) | **no** — pick a delimiter that never appears in the data |
| Dates in a format other than ISO 8601 | yes — `date_format`, stated for the batch and adjustable per shape, `yyyy-MM-dd` by default |

**Settings, the scope cascade, the cross-referencing rules and the refusals are documented once**, in
the native flavor's README:
**[thoth-connector-csv](https://github.com/BaguetteByte-Innovations/thoth-connector-csv#what-it-handles)**.
Everything written there applies here unchanged — documenting it twice would invite the two
descriptions to drift apart.

## What differs from the native flavor

Nothing functional. In particular, **dates are read exactly the same way**: same `date_format`
setting, same patterns parsed against the invariant culture, same refusals word for word. The
script never leans on the console's culture, which would make one file read one way on a French
machine and another way elsewhere.

Two operational facts worth knowing:

- **Memory.** Cross-referencing holds an index until the last source is read. On 20 000 identities
  the index itself costs about 8 MB — but the PowerShell runtime's own footprint is around 150 MB,
  against roughly 40 MB for the AOT binary. The fold is not the expense; the host is.
- **Speed.** Roughly 30 % slower on the same input, dominated by NDJSON writing rather than parsing.

Pick this flavor to **read and modify** the parsing without a toolchain; pick the native one for
large nightly runs.

## Files

| File                | Role                                                                                          |
|---------------------|-----------------------------------------------------------------------------------------------|
| `script.ps1`        | The connector — **CSV parsing only**: the `Invoke-Inspect` / `Invoke-Ingest` handlers.        |
| `thoth-protocol.ps1` | The reusable PowerShell "SDK": the protocol plumbing. Source-agnostic. |
| `manifest.json`     | The manifest read by the engine's scanner (same logical fields as the other variants).         |
| `config.example.json` | Example `config.json`: predefined mappings per role, and the autonomous section. |

The split is the point of this example: `thoth-protocol.ps1` is the part to copy as-is into a new
PowerShell connector; `script.ps1` is the only part to rewrite for a different source format.

Two conventions worth keeping when rewriting `script.ps1`:

- **Operator refusals are returned, not thrown.** A function that detects a source contradiction
  returns its message and the caller emits it verbatim; wrapping it in an exception would bury it
  under a generic crash prefix. Genuine crashes keep the exception path.
- **Structural checks run before any emission.** A refusal must produce **no identity at all** — so
  the checks that read a source's first line (mapped columns present, `target_role` declared) run as
  a pass over every source before the first row is parsed, never file by file inside the loop.

## Protocol (stdin/stdout NDJSON)

The engine sends a single JSON request line on stdin (`{ "Mode": "inspect" | "ingest", ... }`). The
script answers with newline-delimited JSON, each line carrying a prefix:

| Prefix | Meaning                                                       |
|--------|---------------------------------------------------------------|
| `idy:` | One raw identity (emitted per row during ingest).             |
| `log:` | A log line (`{ "Level", "Message" }`) surfaced by the engine. |
| `res:` | The terminal result (inspection or ingestion result).         |

The script exits `0` once the request is processed, a refusal included, and `1` when the request
itself cannot be handled (empty or unreadable request, unknown mode, crash) — after writing a
failure result line in every case.

## Encoding constraint (important)

Both `.ps1` files are saved as **UTF-8 with BOM** and set console encoding to **UTF-8 without BOM in
all three directions** at the top of the script:

```powershell
$utf8NoBom = New-Object System.Text.UTF8Encoding($false)
[Console]::OutputEncoding = $utf8NoBom
[Console]::InputEncoding  = $utf8NoBom
$OutputEncoding           = $utf8NoBom
```

- The **file BOM** is required so Windows PowerShell 5.1 reads the source as UTF-8 (without it, it
  falls back to the ANSI codepage and corrupts non-ASCII text).
- This section is about the encoding of the **scripts** and of the **protocol streams**. The encoding
  of the **CSV files read** is a separate matter: it is the `encoding` setting of the manifest
  (`utf-8` by default; a file that is not in the stated encoding is refused), with exactly the same behavior and messages as the native connector — see its README.
- The **no-BOM console encoding** is required so no stray BOM prefix corrupts the NDJSON streams.
  All three directives matter: `OutputEncoding`/`InputEncoding` drive the console streams,
  `$OutputEncoding` drives the PowerShell pipe.

## Predefined mapping & autonomous import

Predefined mappings and the autonomous section live in a `config.json` placed next to the script.
See `config.example.json`.

A predefined mapping per role pre-fills the column mapping screen; it is matched against the file's
headers **case-insensitively**, so a source writing `prenom` where the config says `Prenom` still
matches.

⚠ An autonomous mapping is **frozen in the file** and replayed nightly against sources nobody
controls. The day one of them renames a column, the ingestion is **refused** rather than importing
that dimension as empty. A column simply **omitted** from the mapping is a different matter: with no
key to confront, it carries no value — a list dimension (groups, schools, assignments) is then
cleared in the directory, a single value (description, birth date) is left as it is.

Sources can be local (`file://`, including UNC shares) or remote (`http(s)://`, with optional
authentication headers). Schemes, header formats and the handling of the secret they carry are
covered in the [Thoth documentation](https://thoth-iam.com/docs/en/connectors/#local-or-remote-sources).

## Install this connector

Each version is published as a [release](https://github.com/BaguetteByte-Innovations/thoth-connector-csv-ps/releases/latest):
a zip of the connector folder, to extract into `connectors-ext/csv_ps/`, with its `SHA256SUMS.txt`
and a provenance attestation (`gh attestation verify <zip> --repo BaguetteByte-Innovations/thoth-connector-csv-ps`).

See the **[Thoth documentation](https://thoth-iam.com/docs/en/connectors/#installation)**.

## Reference

- **[Developer documentation](https://thoth-iam.com/docs/en/develop/)** — how a Thoth connector works: the exchange with the engine, `manifest.json`, `config.json`, the identities it emits.
- **[`Thoth.Integration.Abstractions`](https://www.nuget.org/packages/Thoth.Integration.Abstractions)** — the shared contract (NuGet package) this connector mirrors.
