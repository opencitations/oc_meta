---
# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

title: Verify merge
description: Check that merge operations completed successfully
---

These scripts verify that merge operations completed correctly by checking RDF files, provenance, and the triplestore. If issues are found, they generate SPARQL queries to fix them.

## Scripts

Three scripts are available, one for each entity type:

| Script | Entity type |
|--------|-------------|
| `check_merged_brs_results.py` | Bibliographic resources (BR) |
| `check_merged_ras_results.py` | Responsible agents (RA) |
| `check_merged_ids_results.py` | Identifiers (ID) |

## Usage

```bash
uv run python -m oc_meta.run.merge.check_merged_brs_results <CSV_FOLDER> <RDF_DIR> --meta_config <CONFIG> --query_output <OUTPUT_DIR>
```

| Argument | Description |
|----------|-------------|
| `CSV_FOLDER` | Folder containing merge CSV files (with `Done` column) |
| `RDF_DIR` | Path to RDF directory |
| `--meta_config` | Path to meta configuration file |
| `--query_output` | Folder where fix queries will be saved |

## Example

```bash
uv run python -m oc_meta.run.merge.check_merged_brs_results \
  groups/ \
  /data/rdf \
  --meta_config meta_config.yaml \
  --query_output fix_queries/
```

## What gets checked

For each row marked as `Done=True` in the CSV files:

**RDF files:**
- Surviving entity exists
- Merged entities are deleted
- Entity constraints are valid (types, identifiers, required properties)

**Provenance:**
- Correct number of snapshots
- Sequential snapshot numbering
- Generation and invalidation timestamps
- Derivation chain (`prov:wasDerivedFrom`)
- Merge snapshots derived from multiple sources

**Triplestore (SPARQL):**
- Surviving entity exists
- Merged entities don't exist
- No references to merged entities remain

## Entity-specific constraints

**BR (bibliographic resources):**
- Must be `fabio:Expression`
- At most two types
- At least one identifier
- At most one title, partOf, publication date, sequence identifier

**RA (responsible agents):**
- Must be `foaf:Agent`
- Exactly one type
- At least one identifier
- At least one name property (name, givenName, or familyName)

**ID (identifiers):**
- Must be `datacite:Identifier`
- Exactly one `usesIdentifierScheme`
- Exactly one `hasLiteralValue`

## Fix queries

When issues are found with merged entities that still exist or are still referenced, the script generates SPARQL UPDATE queries in the output folder:

```
fix_queries/
├── update_12345.sparql
├── update_12346.sparql
└── ...
```

Each query deletes the merged entity's triples and redirects references to the surviving entity.

## Parallel processing

The scripts use multiprocessing to check entities in parallel. They group entities by file to minimize file I/O (each RDF file is opened once for all entities it contains).
