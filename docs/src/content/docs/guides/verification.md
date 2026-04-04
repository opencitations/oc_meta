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
uv run python -m oc_meta.run.meta.check_results meta_config.yaml report.txt
```

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

The script reports issues grouped by category:

```
=== Verification Report ===

Identifiers without OMID:
  doi:10.1234/missing-entity-1
  doi:10.1234/missing-entity-2

Identifiers with multiple OMIDs:
  doi:10.1234/duplicate-entity -> omid:br/060/1, omid:br/060/2

OMIDs without provenance:
  omid:br/060/12345

Summary:
  Total identifiers: 50000
  Identifiers with OMID: 49998
  Identifiers without OMID: 2
  OMIDs with provenance: 49995
  OMIDs without provenance: 3
```