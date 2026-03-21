---
# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

title: Triples
description: Count RDF triples or quads in compressed or uncompressed files
---

Counts RDF triples or quads in files using parallel processing. Supports ZIP, GZIP, and uncompressed files. The output dynamically shows "triples" or "quads" based on the RDF format (quads for `nquads`, triples for `nt` and `json-ld`).

## Usage

```bash
uv run python -m oc_meta.run.count.triples <DIRECTORY> [OPTIONS]
```

## Options

| Option | Default | Description |
|--------|---------|-------------|
| `--pattern` | `*.nq.gz` | Glob pattern for locating files |
| `--format` | `nquads` | RDF format: `nquads`, `nt`, `json-ld` |
| `--recursive` | false | Search subdirectories recursively |
| `--prov-only` | false | Count only files in `prov` subdirectories |
| `--data-only` | false | Count only files not in `prov` subdirectories |
| `--workers` | CPU count | Number of parallel workers |
| `--show-per-file` | false | Print count for each file |
| `--keep-going` | false | Continue processing even if errors occur |

## Examples

Count quads in gzip-compressed N-Quads files:

```bash
uv run python -m oc_meta.run.count.triples /data/rdf --recursive
```

Count triples in ZIP files containing JSON-LD:

```bash
uv run python -m oc_meta.run.count.triples /data/rdf --pattern "*.zip" --format json-ld --recursive
```

Count only data (exclude provenance):

```bash
uv run python -m oc_meta.run.count.triples /data/rdf --recursive --data-only
```

Count only provenance:

```bash
uv run python -m oc_meta.run.count.triples /data/rdf --recursive --prov-only
```

Show per-file counts with 8 workers:

```bash
uv run python -m oc_meta.run.count.triples /data/rdf --recursive --workers 8 --show-per-file
```
