---
# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

title: Benchmark configuration
description: Configure benchmark environment and parameters
---

Benchmarks require a dedicated configuration file pointing to test databases. This isolates benchmark runs from production data.

## Configuration file

The benchmark uses a standard Meta config YAML with test-specific values. A reference config is provided at `oc_meta/run/benchmark/benchmark_config.yaml`.

## Required settings

```yaml
# Test triplestore endpoints
triplestore_url: http://127.0.0.1:8805/sparql
provenance_triplestore_url: http://127.0.0.1:8806/sparql

# Benchmark directories
input_csv_dir: oc_meta/run/benchmark/input
base_output_dir: oc_meta/run/benchmark/output
output_rdf_dir: oc_meta/run/benchmark/output/

# Test Redis (separate from production)
redis_host: localhost
redis_port: 6381
redis_db: 10
cache_db: 11
```

## Test databases

Start the test databases before running benchmarks:

```bash
./test/start-test-databases.sh
```

This starts:
- Virtuoso (data): port 8805
- Virtuoso (provenance): port 8806
- Redis: port 6381

## Cleanup

By default, the benchmark resets all test databases after each run:
- Virtuoso: `RDF_GLOBAL_RESET()` via Docker exec
- Redis: `FLUSHDB` on counter and cache databases
- Output files: deleted

Use `--no-cleanup` to preserve data for inspection.
