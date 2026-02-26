---
title: Preprocessing
description: Filter and optimize CSV files before processing
---

The preprocessing script filters and optimizes CSV files before they enter the main Meta pipeline. This step is optional but recommended for large datasets.

## What preprocessing does

1. **Removes duplicates** across all input files
2. **Filters existing IDs** that are already in the database (optional)
3. **Splits large files** into smaller, manageable chunks

## Basic usage

Deduplicate and split files without checking existing IDs:

```bash
uv run python -m oc_meta.run.meta.preprocess_input <INPUT_DIR> <OUTPUT_DIR>
```

## With storage checking

Check against Redis to filter out IDs that already exist:

```bash
uv run python -m oc_meta.run.meta.preprocess_input input/ output/ --storage-type redis
```

Check against SPARQL endpoint:

```bash
uv run python -m oc_meta.run.meta.preprocess_input input/ output/ \
  --storage-type sparql \
  --sparql-endpoint http://localhost:8890/sparql
```

## Options

| Option | Default | Description |
|--------|---------|-------------|
| `--rows-per-file` | 3000 | Number of rows per output file |
| `--storage-type` | none | Storage to check IDs against (`redis` or `sparql`) |
| `--redis-host` | localhost | Redis hostname |
| `--redis-port` | 6379 | Redis port |
| `--redis-db` | 10 | Redis database number |
| `--sparql-endpoint` | - | SPARQL endpoint URL (required if storage type is `sparql`) |

## Example with all options

```bash
uv run python -m oc_meta.run.meta.preprocess_input input/ output/ \
  --rows-per-file 5000 \
  --storage-type redis \
  --redis-host 192.168.1.100 \
  --redis-port 6380 \
  --redis-db 5
```

## Output report

The script prints a summary when finished:

```
Processing Report:
==================================================
Storage type used: REDIS
Total input files processed: 10
Total input rows: 150000
Rows discarded (duplicates): 12500
Rows discarded (existing IDs): 8200
Rows written to output: 129300

Percentages:
Duplicate rows: 8.3%
Existing IDs: 5.5%
Processed rows: 86.2%
```