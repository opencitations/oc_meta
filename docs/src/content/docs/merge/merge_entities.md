---
# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

title: Merge entities
description: Execute merge operations to consolidate duplicates
---

The merge script processes CSV files with merge instructions and consolidates duplicate entities.

## Usage

```bash
uv run python -m oc_meta.run.merge.entities <CSV_FOLDER> <META_CONFIG> <RESP_AGENT> [OPTIONS]
```

| Argument | Description |
|----------|-------------|
| `CSV_FOLDER` | Folder with merge instruction CSVs |
| `META_CONFIG` | Path to Meta config file |
| `RESP_AGENT` | Responsible agent URI for provenance |

| Option | Default | Description |
|--------|---------|-------------|
| `--entity_types` | ra br id | Entity types to merge (space-separated) |
| `--stop_file` | stop.out | File to trigger graceful stop |
| `--workers` | 4 | Parallel workers |

## Examples

Basic merge:

```bash
uv run python -m oc_meta.run.merge.entities \
  groups/ \
  meta_config.yaml \
  https://w3id.org/oc/meta/prov/pa/1
```

With more workers:

```bash
uv run python -m oc_meta.run.merge.entities \
  groups/ \
  meta_config.yaml \
  https://w3id.org/oc/meta/prov/pa/1 \
  --workers 8
```

Merge only bibliographic resources:

```bash
uv run python -m oc_meta.run.merge.entities \
  groups/ \
  meta_config.yaml \
  https://w3id.org/oc/meta/prov/pa/1 \
  --entity_types br
```

## CSV input format

Each CSV file should have:

```csv
surviving_entity,merged_entities
https://w3id.org/oc/meta/br/060/1,https://w3id.org/oc/meta/br/060/2;https://w3id.org/oc/meta/br/060/3
```

Use output from [find duplicates](/oc_meta/merge/find_duplicates/) or [group entities](/oc_meta/merge/group_entities/).

## What the merge does

For each row, the script:

1. **Loads entities** from RDF files
2. **Copies identifiers** from merged entities to surviving entity
3. **Fills metadata gaps** (title, date, etc.) from merged entities
4. **Updates references** in other entities pointing to merged entities
5. **Keeps author/editor chains** from surviving entity (merged entity's chains are discarded)
6. **Records provenance** for the merge operation
7. **Invalidates merged entities** marking them as merged
8. **Writes updated RDF** back to files
9. **Uploads changes** to triplestore

## File locking

The script uses `FileLock` from [`oc_ocdm.Storer`](https://github.com/opencitations/oc_ocdm/blob/main/oc_ocdm/storer.py) to prevent concurrent writes to the same file. Even with proper grouping, locks provide a safety net.

## Graceful interruption

To stop processing cleanly:

```bash
touch stop.out
```

The script will:
1. Finish current merge operations
2. Save progress
3. Exit with status code 0

To resume, run the same command again. Already-processed files are skipped.

## Progress tracking

The script tracks processed files in memory. If interrupted and resumed, it re-processes from the beginning of the current file but skips completed files.

For very long-running merges, monitor output for progress:

```
Processing group_0001.csv: 45/100 entities
Processing group_0001.csv: 46/100 entities
...
```