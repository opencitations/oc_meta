---
# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

title: Group entities
description: Prepare duplicate entities for parallel merging
---

The grouping script analyzes merge instructions and groups related entities together. This enables parallel processing without conflicts.

## Usage

```bash
uv run python -m oc_meta.run.merge.group_entities <CSV_FILE> <OUTPUT_DIR> <META_CONFIG> [OPTIONS]
```

| Argument | Description |
|----------|-------------|
| `CSV_FILE` | CSV file with merge instructions (from [find duplicates](/oc_meta/merge/find_duplicates/)) |
| `OUTPUT_DIR` | Directory for grouped CSV files |
| `META_CONFIG` | Path to Meta config file |

| Option | Default | Description |
|--------|---------|-------------|
| `--min_group_size` | 50 | Minimum entities per group |

## Example

```bash
uv run python -m oc_meta.run.merge.group_entities \
  duplicates.csv \
  groups/ \
  meta_config.yaml \
  --min_group_size 100
```

## What the script does

### 1. Identifies relationships

Queries the SPARQL endpoint to find all entities related to those being merged:

- Author/editor references
- Publisher references
- Venue containment
- Identifier assignments

### 2. Groups by RDF connections

Uses a Union-Find (disjoint set) algorithm to group entities that share relationships. If A is related to B, and B is related to C, then A, B, and C end up in the same group.

### 3. Groups by file range

Entities sharing the same RDF file path are grouped together. The script calculates file paths from OMIDs using the config settings (`supplier_prefix`, `dir_split_number`, `items_per_file`).

For example, these entities share file `br/060/10000/1000.zip`:
- `br/060/1`
- `br/060/500`
- `br/060/999`

### 4. Balances workloads

Small independent groups are combined until they reach `min_group_size`. Large interconnected groups are kept separate. The goal is balanced worker loads.

## Output

The script creates multiple CSV files in the output directory:

```
groups/
├── group_0001.csv
├── group_0002.csv
├── group_0003.csv
└── ...
```

Each file contains merge instructions for related entities that should be processed together.

## Config settings used

From `meta_config.yaml`:

| Setting | Purpose |
|---------|---------|
| `triplestore_url` | Query entity relationships |
| `supplier_prefix` | Calculate file paths |
| `dir_split_number` | Calculate directory structure |
| `items_per_file` | Calculate file assignments |
| `zip_output_rdf` | Determine file extensions |