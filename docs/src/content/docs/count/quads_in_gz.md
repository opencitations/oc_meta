---
title: Quads in GZ
description: Count RDF quads in gzip-compressed N-Quads files
---

Counts RDF quads in gzip-compressed N-Quads files using parallel processing.

## Usage

```bash
uv run python -m oc_meta.run.count.quads_in_gz <DIRECTORY> [OPTIONS]
```

## Options

| Option | Default | Description |
|--------|---------|-------------|
| `--pattern` | `*.nq.gz` | Glob pattern for files |
| `--format` | `nquads` | RDF format (`nquads` or `trig`) |
| `--recursive` | false | Search subdirectories |
| `--workers` | CPU count | Number of parallel workers |
| `--show-per-file` | false | Print the quad count for each processed file |
| `--keep-going` | false | Keep processing files even if errors occur |

## Examples

Count quads in all .nq.gz files:

```bash
uv run python -m oc_meta.run.count.quads_in_gz /data/rdf
```

Recursive search with 8 workers:

```bash
uv run python -m oc_meta.run.count.quads_in_gz /data/rdf --recursive --workers 8
```

Count uncompressed N-Quads files:

```bash
uv run python -m oc_meta.run.count.quads_in_gz /data/rdf --pattern "*.nq"
```
