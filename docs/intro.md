<!--
SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>

SPDX-License-Identifier: ISC
-->

# OpenCitations Meta

## Quick start

Install:

```bash
pip install oc_meta
```

Run the main processing pipeline:

```bash
python -m oc_meta.run.meta_process -c meta_config.yaml
```

## Input format

Meta expects CSV files with these columns:

| Column | Description |
|--------|-------------|
| `id` | Space-separated identifiers (`doi:10.1162/qss_a_00292 pmid:38034492`) |
| `title` | Title of the work |
| `author` | Semicolon-separated names with optional identifiers (`Peroni, Silvio [orcid:0000-0003-0530-4305]; Shotton, David`) |
| `pub_date` | ISO 8601 date (`2024-01-22`, `2024-01`, or `2024`) |
| `venue` | Container title with optional identifier (`Quantitative Science Studies [issn:2641-3337]`) |
| `volume` | Volume number |
| `issue` | Issue number |
| `page` | Page range (`50-75`) |
| `type` | Resource type (`journal article`, `book chapter`, `proceedings article`, etc.) |
| `publisher` | Publisher name with optional identifier (`MIT Press [crossref:281]`) |
| `editor` | Same format as `author` |

See the [CSV format reference](30-csv-format.md) for the complete specification.

## Documentation

- [Configuration](02-configuration.md) — YAML config file setup
- [Preprocessing](03-preprocessing.md) — Filter and prepare input data
- [Processing](04-processing.md) — Run the main pipeline
- [Verification](06-verification.md) — Validate output
- [Editing entities](08-editing-entities.md) — Modify existing RDF
- [Merge](11-merge-overview.md) — Detect and merge duplicates
- [Info dir](20-infodir-overview.md) — Filesystem counter management
- [Benchmark](27-running-benchmarks.md) — Performance measurement
- [Testing](31-testing.md) — Test infrastructure and fixtures
