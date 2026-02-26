---
title: Generating test data
description: Create synthetic bibliographic records for benchmarks
---

Generates CSV files with synthetic bibliographic metadata for benchmark testing.

## Usage

```bash
uv run python -m oc_meta.run.benchmark.generate_benchmark_data -o <OUTPUT> [options]
```

## Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `-o`, `--output` | Required | Output CSV file path |
| `-s`, `--size` | 100 | Number of records to generate |
| `--seed` | 42 | Random seed for reproducibility |

## Example

```bash
uv run python -m oc_meta.run.benchmark.generate_benchmark_data \
    -o test_data.csv \
    -s 1000 \
    --seed 123
```

## Generated data

Each record includes:

| Field | Values |
|-------|--------|
| id | Synthetic DOI (10.1038/benchmark.NNNNNN), optionally PMID |
| title | Random selection from sample titles |
| author | 1-5 authors with ORCID identifiers |
| pub_date | Random date 2015-2024 |
| venue | Random journal with ISSN |
| volume | 1-50 |
| issue | 1-12 |
| page | Random page range |
| type | journal article, review, conference paper, etc. |
| publisher | Random publisher with Crossref ID |

The generator uses fixed sample data to produce realistic but synthetic records. Same seed produces identical output.
