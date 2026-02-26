---
title: Getting started
description: Install OpenCitations Meta and run your first processing job
---

## Installation

Install via pip:

```bash
pip install oc_meta
```

For development, clone the repository and use [uv](https://docs.astral.sh/uv/):

```bash
git clone https://github.com/opencitations/oc_meta.git
cd oc_meta
uv sync
```

## Prerequisites

Meta requires:

- **Python 3.10+**
- **Redis** for counter handling and caching
- **Triplestore** (Virtuoso or Blazegraph) for RDF storage

For local development, you can use Docker.

Redis:

```bash
docker run -d --name redis -p 6379:6379 redis:latest
```

Virtuoso (data):

```bash
docker run -d --name virtuoso-data -p 8890:8890 -p 1111:1111 openlink/virtuoso-opensource-7:latest
```

Virtuoso (provenance):

```bash
docker run -d --name virtuoso-prov -p 8891:8890 -p 1112:1111 openlink/virtuoso-opensource-7:latest
```

## Your first run

1. **Create a configuration file** (`meta_config.yaml`):

```yaml
triplestore_url: "http://127.0.0.1:8890/sparql"
provenance_triplestore_url: "http://127.0.0.1:8891/sparql"
base_iri: "https://w3id.org/oc/meta/"
context_path: "https://w3id.org/oc/corpus/context.json"
resp_agent: "https://w3id.org/oc/meta/prov/pa/1"
source: "https://api.crossref.org/"

redis_host: "localhost"
redis_port: 6379
redis_db: 0
redis_cache_db: 1

supplier_prefix: "060"
dir_split_number: 10000
items_per_file: 1000

input_csv_dir: "/path/to/input"
```

2. **Prepare input CSV** with these columns:

| Column | Example |
|--------|---------|
| `id` | `doi:10.1162/qss_a_00292` |
| `title` | `OpenCitations Meta` |
| `author` | `Peroni, Silvio [orcid:0000-0003-0530-4305]; Shotton, David` |
| `pub_date` | `2024-01-22` |
| `venue` | `Quantitative Science Studies [issn:2641-3337]` |
| `volume` | `5` |
| `issue` | `1` |
| `page` | `50-75` |
| `type` | `journal article` |
| `publisher` | `MIT Press [crossref:281]` |
| `editor` | (same format as author) |

See [CSV format](/oc_meta/reference/csv_format/) for supported identifiers and formats

3. **Run processing**:

```bash
uv run python -m oc_meta.run.meta_process -c meta_config.yaml
```

See the [configuration reference](/oc_meta/guides/configuration/) for all available options.

## Typical workflow

A production workflow usually follows these steps:

1. **Preprocess** - Deduplicate input and filter existing IDs
2. **Process** - Run the main Meta pipeline
3. **Verify** - Check that all identifiers were processed correctly

Preprocess (optional but recommended):

```bash
uv run python -m oc_meta.run.meta.preprocess_input input/ preprocessed/ --storage-type redis
```

Process:

```bash
uv run python -m oc_meta.run.meta_process -c meta_config.yaml
```

Verify:

```bash
uv run python -m oc_meta.run.meta.check_results meta_config.yaml --output report.txt
```

## Next steps

- [Configuration reference](/oc_meta/guides/configuration/) - All configuration options
- [Preprocessing](/oc_meta/guides/preprocessing/) - Filter and deduplicate input data
- [Processing](/oc_meta/guides/processing/) - How the pipeline works
- [CSV format](/oc_meta/reference/csv_format/) - Input format and supported identifiers
