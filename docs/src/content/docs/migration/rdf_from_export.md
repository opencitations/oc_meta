---
title: RDF from export
description: Import gzipped RDF files into OC Meta directory structure
---

Processes gzipped RDF files (JSON-LD or N-Quads) and organizes them into OC Meta's standard directory structure with configurable splitting parameters.

## Usage

```bash
uv run python -m oc_meta.run.migration.rdf_from_export <input_folder> <output_root> [options]
```

## Parameters

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `input_folder` | Yes | - | Folder containing gzipped input files |
| `output_root` | Yes | - | Root folder for output OC Meta RDF files |
| `--base_iri` | No | https://w3id.org/oc/meta/ | Base URI of entities |
| `--file_limit` | No | 10000 | Number of files per folder |
| `--item_limit` | No | 1000 | Number of items per file |
| `--zip_output` | No | True | Zip output JSON files |
| `--input_format` | No | jsonld | Input format: `jsonld` or `nquads` |
| `--chunk_size` | No | 1000 | Files to process before merging |
| `--cache_file` | No | None | File to store processed file names |
| `--stop_file` | No | ./.stop | File to signal process termination |

## Process

1. Reads gzipped RDF files from the input folder
2. Parses each file according to the specified format
3. Extracts entity URIs and determines output paths based on OC Meta's structure
4. Writes entities to appropriate JSON-LD files in the output directory
5. Merges files in parallel after each chunk is processed

## Example

```bash
uv run python -m oc_meta.run.migration.rdf_from_export /data/export /srv/oc_meta/rdf \
    --input_format nquads \
    --file_limit 10000 \
    --item_limit 1000 \
    --cache_file /tmp/processed.txt
```

## Graceful shutdown

Create a `.stop` file (or the path specified with `--stop_file`) to gracefully terminate the process after the current chunk completes.
