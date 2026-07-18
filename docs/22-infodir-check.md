<!--
SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>

SPDX-License-Identifier: ISC
-->

# Check info dir

Verifies filesystem counter files against both current RDF entities and provenance:

- **Entity counters** (`info_file_*.txt`): the counter must equal the maximum resource number found in current data or provenance.
- **Provenance counters** (`prov_file_*.txt`): each value must match the maximum snapshot number found in the RDF provenance.
- **File structure**: expected files, numeric values, and exact line counts must match.
- **Missing provenance**: every current entity without a provenance snapshot is reported.

## Usage

```bash
uv run python -m oc_meta.run.infodir.check <directory> <info_dir> [options]
```

## Parameters

| Parameter | Required | Description |
|-----------|----------|-------------|
| `directory` | Yes | Path to the zipped JSON-LD RDF directory |
| `info_dir` | Yes | Base directory for counter files |
| `-o`, `--output` | No | Output JSON report path; defaults to `check_info_dir_report.json` |
| `--workers` | No | Worker processes; defaults to `4` |
| `--max-examples` | No | Examples retained per category; defaults to `100` |
| `--temp-dir` | No | Temporary storage directory; defaults to the parent of `info_dir` |

## Process

1. Reconstructs expected provenance counters in fixed-width temporary files.
2. Scans current entities and derives exact entity counter values.
3. Reads each text counter file sequentially and compares values in blocks.
4. Counts every difference while retaining only the configured number of examples.
5. Writes a JSON report.

## Example

```bash
uv run python -m oc_meta.run.infodir.check \
  /srv/oc_meta/rdf \
  /srv/oc_meta/info_dir \
  --workers 8 \
  -o /tmp/report.json
```

## Status and exit codes

- `aligned`, exit `0`: counters and source data agree.
- `warnings`, exit `1`: counters agree, but current entities without provenance exist.
- `mismatched`, exit `1`: counter values or files differ.
- `scan_failed`, exit `2`: the RDF source could not be scanned.

## Output

```json
{
  "timestamp": "2026-07-18T12:00:00+00:00",
  "status": "mismatched",
  "root_path": "/srv/oc_meta/rdf",
  "info_dir": "/srv/oc_meta/info_dir",
  "entity_counter_mismatches": {
    "total": 1,
    "examples": [
      {
        "prefix": "060",
        "short_name": "br",
        "expected": 500000,
        "actual": 400000,
        "relation": "too_low"
      }
    ],
    "truncated": false
  }
}
```

The report contains the same `total`, `examples`, and `truncated` structure for provenance mismatches, counter file errors, and live entities without provenance.
