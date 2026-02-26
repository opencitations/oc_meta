---
title: Running benchmarks
description: Measure Meta processing pipeline performance
---

The benchmark module measures end-to-end performance of the Meta processing pipeline, from CSV input to triplestore upload.

## Usage

```bash
uv run python -m oc_meta.run.benchmark -c <CONFIG> [options]
```

## Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `-c`, `--config` | Required | Path to benchmark config YAML |
| `--sizes` | None | Generate N synthetic records. Multiple values for scalability analysis |
| `--runs` | 1 | Execute benchmark multiple times for statistical analysis |
| `--seed` | 42 | Random seed for reproducible data |
| `--fresh-data` | False | Generate new data for each run |
| `--no-cleanup` | False | Skip database reset after benchmark |
| `--update-scenario` | False | Test graph diff performance (preload partial, then complete data) |
| `--preload-high-authors` | None | Preload BR with N authors before benchmark |

## Examples

Single run with 100 synthetic records:

```bash
uv run python -m oc_meta.run.benchmark -c benchmark_config.yaml --sizes 100
```

Statistical analysis with 5 runs:

```bash
uv run python -m oc_meta.run.benchmark -c benchmark_config.yaml --sizes 100 --runs 5
```

Scalability analysis across multiple sizes:

```bash
uv run python -m oc_meta.run.benchmark -c benchmark_config.yaml --sizes 10 50 100 500 --runs 3
```

Update scenario (tests graph diff when updating existing entities):

```bash
uv run python -m oc_meta.run.benchmark -c benchmark_config.yaml --sizes 100 --update-scenario
```

High-author stress test (simulates ATLAS paper with 2869 authors):

```bash
uv run python -m oc_meta.run.benchmark -c benchmark_config.yaml --preload-high-authors 2869
```

## Output

Reports are saved in `oc_meta/run/benchmark/reports/`:

- `benchmark_<size>.json` - raw timing data and statistics
- `benchmark_<size>.png` - phase breakdown and throughput charts

## Metrics collected

- Total duration
- Throughput (records/sec)
- Per-phase timing: curation (collect IDs, clean, merge), RDF creation, storage
- Memory usage per phase
- 95% confidence intervals (with multiple runs)
- Outlier detection
