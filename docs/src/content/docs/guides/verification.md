---
# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

title: Verification
description: Verify that processing completed correctly
---

After running Meta, use the verification script to check that all identifiers were processed correctly and have associated data in the triplestore.

## Running verification

```bash
uv run python -m oc_meta.run.meta.check_results <CONFIG_PATH> <OUTPUT_FILE>
```

Example:

```bash
uv run python -m oc_meta.run.meta.check_results meta_config.yaml report.json
```

The script exits with code 0 if all checks pass, or 1 if any errors are found.

## What it checks

### 1. Identifier analysis

The script parses all identifiers from input CSV files, including:

- `id` column (DOIs, PMIDs, etc.)
- `author` column (ORCID identifiers)
- `editor` column (ORCID identifiers)
- `publisher` column (Crossref identifiers)
- `venue` column (ISSNs, ISBNs)

### 2. OMID verification

For each identifier, the script queries the triplestore to check:

- Does the identifier have an associated OMID?
- Does any identifier have multiple OMIDs? (indicates disambiguation issues)

### 3. Data graph verification

Since RDF files are always generated:

- Verifies that RDF files exist for each entity
- Reports missing data graphs

### 4. Provenance verification

For each OMID found:

- Queries the provenance triplestore
- Verifies provenance graphs exist
- Reports OMIDs missing provenance data

## Output format

The script produces a JSON report with the following structure:

```json
{
  "status": "PASS",
  "timestamp": "2026-04-04T12:00:00",
  "config_path": "/path/to/meta_config.yaml",
  "total_files_processed": 3,
  "files": [
    {
      "file": "input.csv",
      "total_rows": 100,
      "rows_with_ids": 95,
      "total_identifiers": 200,
      "identifiers_with_omids": 190,
      "identifiers_without_omids": 10
    }
  ],
  "summary": {
    "total_rows": 100,
    "total_identifiers": 200,
    "identifiers_with_omids": 190,
    "identifiers_without_omids": 10,
    "omids_with_provenance": 185,
    "omids_without_provenance": 5
  },
  "errors": [
    {
      "type": "missing_omid",
      "schema": "doi",
      "value": "10.1234/example",
      "file": "input.csv",
      "row": 5,
      "column": "id"
    }
  ],
  "warnings": [
    {
      "type": "multiple_omids",
      "identifier": "doi:10.1234/duplicate",
      "omid_count": 2,
      "omids": ["https://w3id.org/oc/meta/br/0601", "https://w3id.org/oc/meta/br/0602"],
      "occurrences": [{"file": "input.csv", "row": 10, "column": "id"}]
    }
  ]
}
```

### Status semantics

- **`status: "PASS"`**: all identifiers have OMIDs and all OMIDs have provenance. Exit code 0.
- **`status: "FAIL"`**: at least one error found. Exit code 1.

### Error types

- **`missing_omid`**: an identifier from the input CSV has no corresponding OMID in the triplestore. Indicates a processing failure.
- **`missing_provenance`**: an OMID exists in the triplestore but has no provenance record. Indicates incomplete ingestion.

### Warning types

- **`multiple_omids`**: an identifier is associated with more than one OMID across files. Indicates a disambiguation issue that should be resolved via the merge pipeline.
