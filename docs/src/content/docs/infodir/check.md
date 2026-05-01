---
# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

title: Check info dir
description: Verify filesystem counters against RDF provenance files
---

Verifies that filesystem counter files are consistent with the provenance data in the RDF files. Performs two checks:

- **Entity counters** (`info_file_*.txt`): the global counter for each entity type must be greater than or equal to the maximum resource number found in the provenance files.
- **Provenance counters** (`prov_file_*.txt`): the counter value for each entity must match the maximum snapshot number found in its provenance file.

## Usage

```bash
uv run python -m oc_meta.run.infodir.check <directory> <info_dir> [-o OUTPUT]
```

## Parameters

| Parameter | Required | Description |
|-----------|----------|-------------|
| `directory` | Yes | Path to the RDF directory |
| `info_dir` | Yes | Base directory for counter files |
| `-o`, `--output` | No | Output JSON report path (default: `check_info_dir_report.json`) |

## Process

1. Loads all counter files from the info directory into memory
2. Collects all provenance ZIP files from the RDF directory
3. Processes each ZIP in parallel: extracts entity URIs and max snapshot numbers, compares against in-memory provenance counters
4. After processing all files, compares the global max resource number per entity type against entity counters
5. Writes a structured JSON report

## Example

```bash
uv run python -m oc_meta.run.infodir.check /srv/oc_meta/rdf /srv/oc_meta/info_dir -o /tmp/report.json
```

## Output

A JSON report with the following structure:

```json
{
  "timestamp": "2026-05-01T12:00:00+00:00",
  "root_path": "/srv/oc_meta/rdf",
  "info_dir": "/srv/oc_meta/info_dir",
  "total_zip_files": 1364452,
  "total_mismatched_entity_counters": 1,
  "total_mismatched_prov_counters": 3,
  "mismatched_entity_counters": [
    {
      "prefix": "060",
      "short_name": "br",
      "expected_min": 500000,
      "actual": 400000
    }
  ],
  "mismatched_prov_counters": [
    {
      "entity_uri": "https://w3id.org/oc/meta/br/06101234",
      "expected": 3,
      "actual": 2,
      "zip_file": "/srv/oc_meta/rdf/br/060/10000/1000/prov/se.zip"
    }
  ]
}
```
