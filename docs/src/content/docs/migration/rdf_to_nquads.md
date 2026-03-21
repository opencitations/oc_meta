---
# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

title: RDF to N-Quads
description: Convert JSON-LD archives to N-Quads format
---

Recursively searches for ZIP files containing JSON-LD data and converts the content to N-Quads format.

## Usage

```bash
uv run python -m oc_meta.run.migration.rdf_to_nquads <input_dir> <output_dir> [options]
```

## Parameters

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `input_dir` | Yes | - | Directory containing ZIP files (searched recursively) |
| `output_dir` | Yes | - | Output directory for converted .nq files |
| `-m`, `--mode` | No | `all` | Mode: `all` for all ZIP files, `data` for entity data only, `prov` for provenance only |
| `-w`, `--workers` | No | CPU count | Number of worker processes |
| `-c`, `--compress` | No | disabled | Compress output files using 7z format |

## Modes

### All mode (default)

Processes all ZIP files, both entity data and provenance.

```bash
uv run python -m oc_meta.run.migration.rdf_to_nquads /srv/oc_meta/rdf /data/nquads
```

### Data mode

Searches for numeric ZIP files (e.g., `1000.zip`, `2000.zip`) excluding `se.zip` and files inside `prov/` directories.

```bash
uv run python -m oc_meta.run.migration.rdf_to_nquads /srv/oc_meta/rdf /data/meta_nquads --mode data
```

### Provenance mode

Searches for `se.zip` files in `prov/` directories. These contain provenance snapshots.

```bash
uv run python -m oc_meta.run.migration.rdf_to_nquads /srv/oc_meta/rdf /data/provenance_nquads --mode prov
```

## Compression

By default, output files are written as plain text `.nq` files. Use the `--compress` flag to compress each output file individually using 7z format:

```bash
uv run python -m oc_meta.run.migration.rdf_to_nquads /srv/oc_meta/rdf /data/nquads --compress
```

Each N-Quads file is compressed into its own `.nq.7z` archive. 7z offers better compression ratios than ZIP, reducing storage requirements for large datasets.

## Process

1. Recursively finds ZIP files based on the selected mode
2. For each archive, extracts the JSON-LD content
3. Converts JSON-LD to N-Quads using rdflib
4. Writes the output to a flat directory with filenames derived from the path

## Output naming

Output filenames are derived from the relative path of the source file, with path separators replaced by dashes:

- Input: `ra/0610/10000/1000/prov/se.zip` → Output: `ra-0610-10000-1000-prov-se.nq`
- Input: `br/060/10000/1000.zip` → Output: `br-060-10000-1000.nq`

With `--compress` enabled:
- Input: `br/060/10000/1000.zip` → Output: `br-060-10000-1000.nq.7z`

## Examples

Convert all RDF data:

```bash
uv run python -m oc_meta.run.migration.rdf_to_nquads /srv/oc_meta/rdf /data/all_nquads \
    --workers 8
```

Convert entity data only:

```bash
uv run python -m oc_meta.run.migration.rdf_to_nquads /srv/oc_meta/rdf /data/meta_nquads \
    --mode data --workers 8
```

Convert provenance only:

```bash
uv run python -m oc_meta.run.migration.rdf_to_nquads /srv/oc_meta/rdf /data/provenance_nquads \
    --mode prov --workers 8
```

Convert with 7z compression:

```bash
uv run python -m oc_meta.run.migration.rdf_to_nquads /srv/oc_meta/rdf /data/compressed_nquads \
    --compress --workers 8
```

## Report

At completion, the script reports the number of successfully processed files and failures:

```
Final report
  Success: 12345
  Failed:  0
```
