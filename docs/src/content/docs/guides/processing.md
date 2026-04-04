---
# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

title: Processing
description: How the main Meta pipeline works
---

The main Meta process reads CSV files, curates the data, generates RDF, and uploads to a triplestore.

## Running Meta

```bash
uv run python -m oc_meta.run.meta_process -c meta_config.yaml
```

## What happens during processing

### 1. Preparation

- Creates output directories (`info_dir`, `output_csv_dir`, `output_rdf_dir`)
- Initializes Redis connection for OMID counter handling
- Generates `time_agnostic_library_config.json` for provenance queries (if it doesn't exist)
- Loads list of already processed files from cache to skip them

### 2. Data curation

The [**Curator**](https://github.com/opencitations/oc_meta/blob/master/oc_meta/core/curator.py) processes each CSV row:

- Parses identifiers from the `id` column and validates their syntax (DOI regex, ORCID checksum, ISSN checksum, etc.)
- Normalizes metadata: title casing, date format standardization, author name parsing
- Uses [**ResourceFinder**](https://github.com/opencitations/oc_meta/blob/master/oc_meta/lib/finder.py) to query the triplestore and check if entities already exist
- Builds in-memory dict indexes with data from existing entities
- Outputs a curated CSV file with normalized data and assigned OMIDs

### 3. Index building

During curation, Meta builds indexes that map:

- `index_id_ra`: Identifiers → Responsible agent OMIDs
- `index_id_br`: Identifiers → Bibliographic resource OMIDs
- `re_index`: Resource embodiment data
- `ar_index`: Agent role sequences (author/editor chains)
- `VolIss`: Volume/issue structure for venues

These indexes avoid repeated SPARQL queries during RDF creation.

### 4. RDF creation

The [**Creator**](https://github.com/opencitations/oc_meta/blob/master/oc_meta/core/creator.py) generates RDF using [**GraphSet**](https://github.com/opencitations/oc_ocdm/blob/master/oc_ocdm/graph/graph_set.py):

- **Bibliographic resources (BR)**: Articles, books, journals, proceedings, etc.
- **Responsible agents (RA)**: Authors, editors, publishers (persons or organizations)
- **Identifiers (ID)**: DOIs, ORCIDs, ISSNs, ISBNs, etc.
- **Agent roles (AR)**: [Proxy entities](https://doi.org/10.1145/2362499.2362502) linking BR to RA, with role type and sequence (hasNext chain)
- **Resource embodiments (RE)**: Page ranges

After entity creation, [**ProvSet**](https://github.com/opencitations/oc_ocdm/blob/master/oc_ocdm/prov/prov_set.py) generates provenance snapshots tracking creation time, responsible agent, and primary source.

### 5. Storage

Meta runs four parallel processes using `multiprocessing`:

1. **Data RDF storage**: Writes data entities to JSON-LD files
2. **Provenance RDF storage**: Writes provenance to JSON-LD files
3. **Data SPARQL generation**: Generates SPARQL UPDATE queries for data triplestore
4. **Provenance SPARQL generation**: Generates SPARQL UPDATE queries for provenance triplestore

After query generation, Meta uploads SPARQL queries to both triplestores using [`piccione.upload_sparql_updates`](https://github.com/opencitations/piccione/blob/main/src/piccione/upload/on_triplestore.py).

## Multiprocessing

Meta processes CSV files sequentially (one at a time), but uses parallel processes within each file for I/O operations. This design is hard-coded for stability reasons: Virtuoso does not handle parallel SPARQL queries well.

For each file, Meta spawns up to 4 parallel processes:
- 2 for RDF file storage (data + provenance)
- 2 for SPARQL query generation (data + provenance)

## Manual upload

If the automatic upload fails mid-process (connection timeout, triplestore restart, etc.), you can retry with the manual upload script:

```bash
uv run python -m oc_meta.run.upload.on_triplestore <ENDPOINT_URL> <SPARQL_FOLDER>
```

Options:

| Option | Default | Description |
|--------|---------|-------------|
| `--batch_size` | 10 | Quadruples per batch |
| `--cache_file` | ts_upload_cache.json | Track processed files |
| `--failed_file` | failed_queries.txt | Log failed queries |
| `--stop_file` | .stop_upload | Touch this file to stop gracefully |

The script tracks progress in the cache file, so you can restart without reprocessing completed files.

## Error handling

Meta tracks progress in two files inside `base_output_dir`:

- **`cache.txt`**: Lists successfully processed CSV files. On restart, Meta skips files already in this list.
- **`errors.txt`**: Logs failed files with their error messages (filename + traceback).

If a file fails, Meta logs the error and continues with the next file. At the end of a complete run, `cache.txt` is renamed with a timestamp (e.g., `cache_2024-01-15T10_30_00.txt`).

## Output files

RDF files are always generated. When `rdf_files_only: false` (default), SPARQL queries are also produced:

```
output/
├── br/                      # Bibliographic resources
│   └── 060/                 # Supplier prefix
│       └── 10000/           # Entities 1-10000 (dir_split_number)
│           ├── 1000.zip     # Entities 1-1000 (items_per_file)
│           ├── 2000.zip     # Entities 1001-2000
│           └── ...
├── ra/                      # Responsible agents
├── id/                      # Identifiers
├── ar/                      # Agent roles
├── re/                      # Resource embodiments
└── prov/                    # Provenance graphs
```

The directory structure is determined by `dir_split_number` (entities per subdirectory) and `items_per_file` (entities per JSON file). For example, with `dir_split_number: 10000` and `items_per_file: 1000`, entity `br/060/15234` is stored in `br/060/20000/16000.zip`.
