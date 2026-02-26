---
title: Generate CSV
description: Generate CSV dump from RDF data
---

The CSV generator creates a CSV dump from the RDF data.

## Usage

```bash
uv run python -m oc_meta.run.meta.generate_csv -c <CONFIG> -o <OUTPUT_DIR> [OPTIONS]
```

### Required arguments

| Argument | Description |
|----------|-------------|
| `-c, --config` | Path to Meta configuration file |
| `-o, --output_dir` | Directory where CSV files will be stored |

### Optional arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `--redis-host` | `localhost` | Redis server hostname |
| `--redis-port` | `6379` | Redis server port |
| `--redis-db` | `2` | Redis database number for caching |
| `--workers` | `4` | Number of parallel workers |
| `--clean` | - | Clear checkpoint and Redis cache before starting |

### Example

```bash
uv run python -m oc_meta.run.meta.generate_csv \
    -c meta_config.yaml \
    -o /data/csv_dump \
    --workers 8
```

## Input

The script reads RDF data from the directory specified by `output_rdf_dir` in the configuration file. It expects:

- JSON-LD files compressed in ZIP archives
- Standard OpenCitations Meta directory structure (`br/`, `ra/`, `id/`, `ar/`, `re/`)
- Files organized by supplier prefix and numeric ranges

The script only processes bibliographic resources (BR), but resolves related entities:

- **Identifiers (ID)**: DOI, PMID, PMCID, ISBN, ISSN, etc.
- **Responsible agents (RA)**: Authors, editors, publishers with their identifiers
- **Agent roles (AR)**: Links between BR and RA with ordering via `hasNext`
- **Resource embodiments (RE)**: Page numbers

## Output

### CSV files

Output files are named `output_1.csv`, `output_2.csv`, etc., with a maximum of 3000 rows per file.

The CSV format matches the standard Meta input format:

| Column | Description |
|--------|-------------|
| `id` | Space-separated identifiers (OMID + external IDs) |
| `title` | Publication title |
| `author` | Semicolon-separated authors in format `Family, Given [identifiers]` |
| `issue` | Issue number |
| `volume` | Volume number |
| `venue` | Venue title with identifiers |
| `page` | Page range (e.g., `123-456`) |
| `pub_date` | Publication date |
| `type` | Publication type (e.g., `journal article`, `book chapter`) |
| `publisher` | Publisher with identifiers |
| `editor` | Semicolon-separated editors |

### Checkpoint file

The script creates `processed_br_files.txt` in the output directory to track which RDF files have been processed. This enables resumability: if the script is interrupted, it will skip already processed files on restart.

### Redis cache

Processed OMIDs are stored in Redis to avoid duplicates. The cache persists across runs unless `--clean` is specified.

## Processing details

### Skipped entities

The following entity types are skipped as standalone records (they appear only as venue containers):

- Journal volumes (`fabio:JournalVolume`)
- Journal issues (`fabio:JournalIssue`)

### Author ordering

Authors, editors, and publishers are ordered by following the `oc:hasNext` chain from agent roles. The script detects the first agent role (one not referenced by any other `hasNext`) and follows the chain to maintain correct ordering.

### Venue hierarchy

For journal articles, the script traverses the `frbr:partOf` hierarchy:

```
Article → Issue → Volume → Journal
```

Extracting issue number, volume number, and journal title with identifiers.

### Cycle detection

The script includes safeguards against:

- Cycles in `hasNext` chains (max iterations limit)
- Cycles in venue hierarchy (visited set + max depth of 5)

Warnings are printed when cycles are detected.

## Resumability

The script supports resuming interrupted processing:

1. **File-level checkpoint**: Tracks processed RDF files in `processed_br_files.txt`
2. **Entity-level cache**: Stores processed OMIDs in Redis

To start fresh, use the `--clean` flag:

```bash
uv run python -m oc_meta.run.meta.generate_csv \
    -c meta_config.yaml \
    -o /data/csv_dump \
    --clean
```

This removes the checkpoint file and clears the Redis cache.

## Performance

- Uses multiprocessing with configurable worker count
- LRU cache (2000 entries) for loaded JSON files
- Redis pipeline batching for efficient cache operations
- Progress bar shows processing status and time estimates
