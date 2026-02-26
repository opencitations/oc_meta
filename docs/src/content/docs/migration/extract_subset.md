---
title: Extract subset
description: Extract a subset of data from a SPARQL endpoint
---

Extracts a subset of RDF data from a SPARQL endpoint by querying instances of a specified class and recursively following URI references. Outputs the result in N-Quads format.

## Usage

```bash
uv run python -m oc_meta.run.migration.extract_subset [options]
```

## Parameters

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `--endpoint` | No | http://localhost:8890/sparql | SPARQL endpoint URL |
| `--class` | No | http://purl.org/spar/fabio/Expression | Class URI to extract instances of |
| `--limit` | No | 1000 | Maximum number of initial entities |
| `--output` | No | output.nq | Output file name |
| `--compress` | No | False | Compress output with gzip |
| `--retries` | No | 5 | Maximum retries for failed queries |

## Process

1. Queries the endpoint for subjects that are instances of the specified class
2. For each entity, fetches all triples where it appears as subject
3. Recursively processes any URI found as object
4. Serializes the collected data as N-Quads

## Example

Extract 500 bibliographic resources with their related entities:

```bash
uv run python -m oc_meta.run.migration.extract_subset \
    --endpoint http://localhost:8890/sparql \
    --class http://purl.org/spar/fabio/Expression \
    --limit 500 \
    --output subset.nq.gz \
    --compress
```

## Use cases

- Create test datasets from production data
- Extract samples for debugging
- Migrate specific portions of a triplestore
