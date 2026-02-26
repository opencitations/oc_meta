---
title: Generate info dir
description: Build Redis counters from RDF files
---

Scans the RDF directory structure and populates Redis with the correct counter values based on existing entities.

## Usage

```bash
uv run python -m oc_meta.run.infodir.gen <directory> [options]
```

## Parameters

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `directory` | Yes | - | Path to the RDF directory (containing `br/`, `ra/`, etc.) |
| `--redis-host` | No | localhost | Redis server host |
| `--redis-port` | No | 6379 | Redis server port |
| `--redis-db` | No | 6 | Redis database number |

## Process

1. For each entity type (`br`, `ra`, `ar`, `re`, `id`), the script finds the highest numbered entity in each supplier prefix folder
2. Sets the main counter for each entity type and prefix
3. Scans all provenance files in parallel to find the highest snapshot number for each entity
4. Updates the provenance counters in batch

## Example

```bash
uv run python -m oc_meta.run.infodir.gen /srv/oc_meta/rdf \
    --redis-host localhost \
    --redis-port 6379 \
    --redis-db 5
```

This reads all RDF files under `/srv/oc_meta/rdf` and populates Redis database 5 with the counter values.