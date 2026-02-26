---
title: Triples in RDF
description: Count RDF triples in compressed files
---

Counts RDF triples in ZIP or GZ compressed files using parallel processing.

## Usage

```bash
uv run python -m oc_meta.run.count.triples_in_rdf <DIRECTORY> <COMPRESSION_TYPE> <DATA_FORMAT> [OPTIONS]
```

## Arguments

| Argument | Description |
|----------|-------------|
| `DIRECTORY` | Directory containing the RDF files |
| `COMPRESSION_TYPE` | File type: `zip`, `gz`, `json`, `ttl` |
| `DATA_FORMAT` | RDF format: `json-ld`, `turtle`, `nquads` |

## Options

| Option | Description |
|--------|-------------|
| `--prov_only` | Count only provenance files |
| `--data_only` | Count only data files (exclude provenance) |

## Examples

Count triples in ZIP files (JSON-LD):

```bash
uv run python -m oc_meta.run.count.triples_in_rdf /data/rdf zip json-ld
```

Count only data triples (exclude provenance):

```bash
uv run python -m oc_meta.run.count.triples_in_rdf /data/rdf zip json-ld --data_only
```

Count only provenance triples:

```bash
uv run python -m oc_meta.run.count.triples_in_rdf /data/rdf zip json-ld --prov_only
```
