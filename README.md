# Thoth.Connectors.CsvPs

The CSV connector of [Thoth](https://thoth-iam.com/), an identity management suite for schools,
written as a plain PowerShell script: the Thoth engine runs it to import students, teachers and
staff from CSV files. This repository holds its source.

It behaves exactly like the native CSV connector shipped with Thoth
([thoth-connector-csv](https://github.com/BaguetteByte-Innovations/thoth-connector-csv)): same
settings, same refusals, same messages. Two differences only:

- **it reads and changes without a toolchain**: `script.ps1` holds the CSV parsing, the only part to
  rewrite for another source format; `thoth-protocol.ps1` holds the exchange with the engine, to
  copy as-is into a new PowerShell connector;
- **it is slower and heavier**: about 30 % slower, and the PowerShell runtime weighs around 150 MB
  against 40 MB for the native executable. The native flavor suits large nightly imports.

## Documentation

- **[Using the connector](https://thoth-iam.com/docs/en/connectors-csv/)** — the expected file, the
  settings, residence and assignments, cross-referencing, refusals, `config.json`.
- **[Installing a connector](https://thoth-iam.com/docs/en/connectors/#installation)**.
- **[Writing a connector](https://thoth-iam.com/docs/en/develop/)** — including the reference of the
  `thoth-protocol.ps1` module.

## Download

Each version is published as a [release](https://github.com/BaguetteByte-Innovations/thoth-connector-csv-ps/releases/latest):
a zip to extract into `connectors-ext/csv_ps/`, its `SHA256SUMS.txt` and a provenance attestation.
